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

import java.time.LocalDate
import java.time.ZonedDateTime

@Log4j2
class AarpScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "us", url: "https://jobs.aarp.org"]
	]
	static final String    baseSourceID = 'aarp_'

    AarpScraper(Integer sourceToScrape) {
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
		def groups = startPage?.select("footer[class^=ws-footer] div.container ul li a")?.sort { it.text() } // sort necessary for compare with status.lastCategory
		
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
		def paginationPage = loadPage(nextURL)
		
		int maxJobsInGroup = paginationPage?.select("div[class^=page-header] h2")?.text()?.replaceAll("\\D", "")?.toInteger()
		
		int offset = 0
		int jobsInGroupCount = 0

		// NOTE: Site shows in browser that there are more 400_000 offers in one group,
		// in downloaded Document this number less - nearly 200 offers
		// but actually it gives only 40 pages to extract (40 * 25 = 1000 offers in one group)
		while (nextURL) {
			def jobLinks = paginationPage?.select("div.list-jobs div.job-title a.job-link")
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			// if (jobsInJobListCount <= 0) break // switch deep scraping (when disabled) and shallow scraping (when enabled)
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			nextURL = paginationPage?.select("nav.aarp-pagination-wrapper div.pagination-next-prev a")?.last()?.absUrl("href")
			offset =  Math.max(status.lastOffset as Integer ?: 0, (nextURL?.split("=")?.last()).toInteger() ?: 25).toInteger()
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
			String idInSource = jobPageURL?.split("-")?.last()
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

			final JsonSlurper jsonSlurper 	= new JsonSlurper()	// thread safe and serializable - alternative: new HashMap<>(jsonSlurper.parseText(jsonText))
			def dataRaw						= jobPage?.select("script[type=application/ld+json]")?.first()?.childNode(0)?.toString()?.replaceAll("\n", "")
			def data						= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job 		= new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: (data?.url as String)?.split("-")?.last()
			job.url			= pageURL ?: data?.url
			job.name		= data.title ?: jobPage.select("div.view-job-view h1")?.first()?.text()

			// There is no such parameter
			job.locale		= "en_US"

			job.html		= jobPage?.select("div.view-job-view")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= data
			if (data)	job.json.pageData = data

			try { job.dateCreated = ZonedDateTime.parse(data.datePosted)?.toLocalDateTime() } catch (e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= data.employmentType

			job.salary.text 	 		= (jobPage.select("div.job-stats b")[1] as String).contains("Salary") ? (jobPage.select("div.job-stats b")[1].text() + " " + (jobPage.select("div.job-stats p")[1].text())) : ""
			job.salary.currency 		= data.baseSalary?.currency
			job.salary.value	 		= (data.baseSalary?.value ?: data.baseSalary?.minValue) as Double

			job.orgTags."${TagType.CATEGORIES}" 	= (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category
			job.orgTags."${TagType.WORK_TYPES}" 	= (job.orgTags."${TagType.WORK_TYPES}" ?: []) + data.employmentType
			job.orgTags."${TagType.QUALIFICATIONS}" = (job.orgTags."${TagType.QUALIFICATIONS}" ?: []) + data.educationRequirements
			job.orgTags."${TagType.SKILLS}" 		= (job.orgTags."${TagType.SKILLS}" ?: []) + ((data.experienceRequirements && data.experienceRequirements != "0") ? data.data.experienceRequirements : [])

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 				= new Location()
			location.source 				= sourceID
			location.orgAddress.addressLine = jobPage.select("div.job-stats p")?.first()?.text()
			location.orgAddress.postCode 	= data?.jobLocation?.address?.postalCode
			location.orgAddress.county 		= data?.jobLocation?.address?.addressCountry
			location.orgAddress.state 		= data?.jobLocation?.address?.addressRegion
			location.orgAddress.city 		= data?.jobLocation?.address?.addressLocality
			location.orgAddress.street 		= data?.jobLocation?.address?.streetAddress
			location.orgAddress.houseNumber = data?.jobLocation?.address?.streetAddress ? (data?.jobLocation?.address?.streetAddress as String).split(" ").first().replaceAll("\\D", "") : ""
			location.orgAddress.countryCode	= location.orgAddress.country == "United States" ? "US" : this.source.id.toString().toUpperCase()

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 	= new Company()
			company.source		= sourceID
			company.idInSource	= (data?.hiringOrganization?.url as String).split("-").last().replaceAll("\\D", "")
			company.name		= data?.hiringOrganization?.name
			company.name		= company.name ?: jobPage?.select("h4.top-company-name a.employer-link")?.first()?.text()

			def companyLink = jobPage?.select("h4.top-company-name a.employer-link")?.first()?.absUrl("href")
			// link redirects to AARP page with real company link, but it needs additional request
			// not all such pages contains the link and if it's redundant - just comment this code
			if (companyLink) {
				def companyPage = loadPage(companyLink)
				companyLink = companyPage.select("div[class~=company-description] a")?.last()?.absUrl("href")
			}

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
