/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.scraper.webscraper

import groovy.json.JsonSlurper
import groovy.time.TimeCategory
import groovy.util.logging.Log4j2
import io.techmap.scrape.data.Company
import io.techmap.scrape.data.Job
import io.techmap.scrape.data.Location
import io.techmap.scrape.data.shared.TagType
import io.techmap.scrape.helpers.DataCleaner
import org.bson.Document
import org.jsoup.HttpStatusException
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import java.time.ZonedDateTime

@Log4j2
class BritishjobsScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "uk", url: "https://www.britishjobs.co.uk"]
	]
	static final String    baseSourceID = 'britishjobs_'

    BritishjobsScraper(Integer sourceToScrape) {
		super(sources, baseSourceID)
		this.sourceToScrape = sourceToScrape
		this.source = this.sources[sourceToScrape]
		this.sourceID = this.baseSourceID + this.source.id
		log.info "Using userAgent: ${USER_AGENT}"
	}
	
	@Override
	int scrape() {
		super.initScrape()
		
		Integer jobsInSourceCount = 0
		def startPage = loadPage("${source.url}")
		final def startCookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

		// the first page does not contain groups, only listing page
		def formElement = startPage.select("form[name=search]").first()
		def parameterElement = formElement?.select("input[type=hidden]")?.first()
		// NOTE: parameter is required, without it you'll get main page
		def listingPageLink = formElement?.absUrl("action") + "?" + parameterElement?.attr("name") + "=" + parameterElement?.attr("value")
		def listingPage = loadPage(listingPageLink)

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		// NOTE: for a tag class should be without is-selected part
		def groups = listingPage.select("div[data-faceted-industry-m] a[class=faceted-search-filter--checkbox]")?.sort { it.text() }?.sort { it.text() } // sort necessary for compare with status.lastCategory
		def itemsAmountParameter = listingPage.select("#per-page option")?.last()?.attr("value")?.split("\\?")?.last()?.split("&")?.first()

		for (Element group in groups) {
			def status = db.loadStatus(sourceID + "-${group.text()}")
			this.cookiesForThread."${Thread.currentThread().getId()}" = startCookies

			// add parameter to increase amount of results per page and make less requests
			// NOTE: it can be extracted only from listing page, so I've added it here to have ability use in method
			status.put("itemsAmount", itemsAmountParameter)

			int jobsInCategoryCount = scrapePageGroup(group, status)
			jobsInSourceCount += jobsInCategoryCount
			if (maxDocsToPrint <= 0) break
		}
		
		return jobsInSourceCount
	}
	
	@Override
	int scrapePageGroup(Element group, Map status) {
		def startTime = new Date()
		log.debug "... starting to scrape group ${group.text()}"
		def nextURL = group.absUrl("href") // Problem: first page has no offset

		// add parameter to increase amount of results per page and make less requests
		if (status.get("itemsAmount")) {
			nextURL += ("?" + status.get("itemsAmount"))
		}

		def paginationPage = loadPage(nextURL)
		int maxJobsInGroup = (paginationPage?.select("p.search-header__results")?.text()?.split("of")?.last()?.replaceAll("\\D", "") ?: 0)?.toInteger()

		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			// NOTE: this mechanism ready the "next" URL in a pagination - some pages might require different approaches (e.g., increasing a "page" parameter)
			def jobLinks = paginationPage?.select("#searchResults li.results__item h2.job__title a")
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			// if (jobsInJobListCount <= 0) break // switch deep scraping (when disabled) and shallow scraping (when enabled)
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			offset = Math.max(status.lastOffset as Integer ?: 0, jobLinks.size() ?: 100)
			nextURL = paginationPage?.select("a.pagination__next")?.first()?.absUrl("href")
			if (nextURL) {
				paginationPage = loadPage(nextURL)
				status.lastOffset = offset
				db.saveStatus(status)
			}
		}
		status.lastOffset = 0 // Reset
		db.saveStatus(status)
		log.info "Scraped ${"$jobsInGroupCount".padLeft(5)} of ${"$maxJobsInGroup".padLeft(6)} jobs in group ${group.text()} in " + TimeCategory.minus(new Date(), startTime)
		return jobsInGroupCount
	}
	
	@Override
	int scrapePageList(Elements pageElements, Map extraData) {
		int jobsInPageCount = 0
		for (pageElement in pageElements) {
			String jobPageURL = pageElement?.absUrl("href")
			String idInSource = pageElement.attr("data-job-id") ?: jobPageURL.split("job/").last().split("/").first()
			if (!db.jobExists(sourceID, idInSource)) {
				extraData.idInSource = idInSource
				if (scrapePage(jobPageURL, extraData)) jobsInPageCount++
			}
			if (maxDocsToPrint <= 0) break
		}
		return jobsInPageCount
	}
	
	// @formatter:off (to keep the code in a more tabular form - "Align variables in columns" only works for class fields )
	@Override
	boolean scrapePage(String pageURL, Map extraData) {
		try {
			def jobPage = loadPage(pageURL)
			if (!jobPage) return false

			/*******************************/
			/* Extract data in JSON format */
			/*******************************/

			final JsonSlurper jsonSlurper = new JsonSlurper()	// thread safe and serializable - alternative: new HashMap<>(jsonSlurper.parseText(jsonText))
			def dataRaw		= jobPage?.select("script")*.html().find { it.contains("JobPosting") }
			def data		= jsonSlurper.parseText(dataRaw ?: "{}")
			def dataRaw2	= jobPage?.select("script")*.html().find { it.contains("dataLayer.push") }?.split("dataLayer\\.push\\(")?.last()?.replaceAll("\\);", "")
			def data2		= jsonSlurper.parseText(dataRaw2 ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: data2?.JOB_ID
			job.url			= pageURL ?: jobPage?.select("link[rel=canonical]")?.first()?.attr("href") ?: data?.url
			job.name		= data?.title ?: data2?.JOB_TITLE

			// what does it mean "language tag of the job ads content! - not the website's language" ?
			// for this source it's the same - we have only one source
			job.locale		= jobPage.select("html")?.first()?.attr("lang")

			job.html		= jobPage?.select(".job--standard")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:]
			if (data)	job.json.pageData = data
			if (data2)	job.json.pageData2 = data2

			try { job.dateCreated = ZonedDateTime.parse(data?.datePosted)?.toLocalDateTime() } catch (e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= data?.employmentType
			job.position.contractType	= data2?.JOB_TYPE ?: jobPage?.select("div.job__detail:contains(Type) dd.job__details-value")?.first()?.text()

			job.salary.text 			= jobPage?.select("dd[data-jd-salary]")?.first()?.text() ?: data2?.JOB_SALARY
			job.salary.value 			= (data?.baseSalary?.value?.minValue ?: 0) as Double
			job.salary.value 			= (job.salary.value ?: data?.baseSalary?.value?.maxValue ?: 0) as Double
			job.salary.period 			= data?.baseSalary?.value?.unitText
			job.salary.currency			= data?.baseSalary?.currency

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category

			def industries 						= data?.industry as String
			industries							= industries ?: data2?.JOB_INDUSTRY as String
			if (industries)	{
				job.orgTags."${TagType.INDUSTRIES}" = (job.orgTags."${TagType.INDUSTRIES}" ?: []) + industries.split("/")
			}

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location = new Location()
			location.source = sourceID
			location.orgAddress.addressLine 	= jobPage?.select("dd[data-jd-location]")?.first()?.text()

			location.orgAddress.countryCode		= data?.jobLocation?.address?.addressCountry?.name
			location.orgAddress.district		= data?.jobLocation?.address?.addressRegion
			location.orgAddress.city			= data?.jobLocation?.address?.addressLocality

			location.orgAddress.postCode		= data?.jobLocation?.address?.postalCode
			location.orgAddress.geoPoint.lat	= data?.jobLocation?.geo?.latitude as Double
			location.orgAddress.geoPoint.lng	= data?.jobLocation?.geo?.longitude as Double

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company = new Company()
			company.source		= sourceID
			company.idInSource	= data2?.JOB_COMPANY_ID
			company.name		= data?.hiringOrganization?.name ?: data2?.JOB_COMPANY_NAME
			company.name		= company.name ?: jobPage.select("span[data-jd-company]")?.first()?.text()
			// only internal link
			def companyLink 	= jobPage.select("span[data-jd-company] a")?.first()?.absUrl("href")
			companyLink 		= companyLink ?: data?.hiringOrganization?.sameAs
			company.urls		= [("$sourceID" as String): companyLink]
			company.ids			= [("$sourceID" as String): company.idInSource]

			/*******************/
			/* Store page data */
			/*******************/

			Document rawPage = new Document()
			rawPage.url		= job.url
			rawPage.html	= jobPage.html()

			return crossreferenceAndSaveData(job, location, company, rawPage)
		} catch (HttpStatusException e) {
			log.error "$e for $pageURL"
		} catch (IOException e) {
			log.error "$e for $pageURL"
		} catch (NullPointerException e) {
			log.error "$e for $pageURL" // probably a problem with SimpleDateFormat (do not store job)
		} catch (e) {
			log.error "$e for $pageURL"
			e.printStackTrace()
		}
		return false
	}
}
