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
class SuperjobScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "ru", url: "https://www.superjob.ru"]
	]
	static final String    baseSourceID = 'superjob_'

    SuperjobScraper(Integer sourceToScrape) {
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
		
		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		def groups = startPage?.select("div*.f-test-nav-tag-group-Po_otraslyam ul li a")?.sort { it.text() }
		
		for (Element group in groups) {
			def status = db.loadStatus(sourceID + "-${group.text()}")
			this.cookiesForThread."${Thread.currentThread().getId()}" = startCookies
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
		def nextURL = group.absUrl("href")
		// exclude region filtering to get all offers
		if (nextURL) {
			nextURL += "?noGeo=1"
		}
		def paginationPage = loadPage(nextURL)
		
		int maxJobsInGroup = (paginationPage?.select("div span:contains(Найдено )")?.first()?.text()?.replaceAll("\\D", "") ?: 0)?.toInteger()
		
		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			def jobLinks = paginationPage?.select("div*.f-test-vacancy-item a[target=_blank]")

			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			nextURL = paginationPage?.select("a*.f-test-button-dalshe")?.first()?.absUrl("href")
			offset = Math.max((status.lastOffset as Integer) ?: 0, jobLinks?.size() ?: 20)
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
			// id is presented only with digits
			String idInSource = jobPageURL?.split("-")?.last()?.replaceAll("\\D", "")
			if (!db.jobExists(sourceID, idInSource)) {
				extraData.idInSource = idInSource
				if (scrapePage(jobPageURL, extraData)) jobsInPageCount++
			}
			if (maxDocsToPrint <= 0) break
		}
		return jobsInPageCount
	}
	
	@Override
	boolean scrapePage(String pageURL, Map extraData) {
		try {
			def jobPage = loadPage(pageURL)
			if (!jobPage) return false

			/*******************************/
			/* Extract data in JSON format */
			/*******************************/

			final JsonSlurper jsonSlurper = new JsonSlurper()	// thread safe and serializable - alternative: new HashMap<>(jsonSlurper.parseText(jsonText))
			def dataRaw		= jobPage?.select("div*.f-test-vacancy-base-info script[type=application/ld+json]")?.last()?.html()
			def data		= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job 		= new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: jobPage.select("div*.f-test-title span")?.first()?.text()?.replaceAll("\\D", "")
			job.url			= pageURL ?: jobPage?.select("link[rel=canonical]")?.first()?.attr("href") ?: data?.url
			job.name		= data?.title
			job.locale		= jobPage.select("meta[property*=locale]")?.first()?.attr("content")
			job.html		= jobPage?.select("div[spacing=4]>div:first-child")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:]
			if (data) {
				job.json.pageData = data
			}

			try { job.dateCreated 		= ZonedDateTime.parse(data?.datePosted)?.toLocalDateTime() } catch (e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= data?.employmentType

			job.salary.text				= jobPage.select("span*.ZON4b")?.first()?.text()
			job.salary.value			= data?.baseSalary?.value?.minValue as Double
			job.salary.value			= job.salary.value ?: data?.baseSalary?.value?.maxValue as Double
			job.salary.period			= data?.baseSalary?.value?.unitText
			job.salary.currency			= data?.baseSalary?.currency

			// contact was not found

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 					= new Location()
			location.source 					= sourceID
			location.orgAddress.addressLine 	= jobPage.select("div*.f-test-address span")?.first()?.text()

			location.orgAddress.district		= data?.jobLocation?.address?.addressRegion
			location.orgAddress.city			= data?.jobLocation?.address?.addressLocality
			location.orgAddress.street			= data?.jobLocation?.address?.streetAddress

			location.orgAddress.geoPoint.lat	= data?.jobLocation?.geo?.latitude
			location.orgAddress.geoPoint.lng	= data?.jobLocation?.geo?.longitude

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 	= new Company()
			company.source		= sourceID
			company.idInSource	= data?.identifier?.value
			company.name		= data?.identifier?.name ?: data?.hiringOrganization?.name
			def companyLink 	= data?.hiringOrganization?.sameAs
			company.urls		= [("$sourceID" as String): companyLink]
			company.ids			= [("$sourceID" as String): company.idInSource]
			if (company.idInSource == "anonymous") return false // job is from anonymous company

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
