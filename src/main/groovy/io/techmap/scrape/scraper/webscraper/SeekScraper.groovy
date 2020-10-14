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
class SeekScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "au", url: "https://www.seek.com.au"],
			[id: "nz", url: "https://www.seek.co.nz"]
	]
	static final String    baseSourceID = 'seek_'

    SeekScraper(Integer sourceToScrape) {
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
		def groups = startPage?.select("a[data-automation=quick-search-classification-link]")?.sort { it.text() } // sort necessary for compare with status.lastCategory
		
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
		def nextURL = group.absUrl("href") // Problem: first page has no offset
		def paginationPage = loadPage(nextURL)
		
		int maxJobsInGroup = (paginationPage?.select("strong[data-automation=totalJobsCount]")?.first()?.text() ?: "")?.replaceAll("\\D", "")?.toInteger()
		
		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			// NOTE: this mechanism ready the "next" URL in a pagination - some pages might require different approaches (e.g., increasing a "page" parameter)
			def jobLinks = paginationPage?.select("a[data-automation=jobTitle]")
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			// if (jobsInJobListCount <= 0) break // switch deep scraping (when disabled) and shallow scraping (when enabled)
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			// This resource shows offers till 200th page however in this page next button is present
			// After loading page 201 all data disappears and next button too
			def nextPageButtonURL = paginationPage?.select("a[data-automation=page-next]")?.first()?.absUrl("href")
			offset = Math.max(status.lastOffset as Integer ?: 0, jobsInJobListCount ?: 22)?.toInteger()
			nextURL = nextPageButtonURL
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
			String idInSource = jobPageURL?.split("\\?")?.first()?.split("/")?.last()
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

			final JsonSlurper jsonSlurper = new JsonSlurper()
			def dataRaw		= jobPage?.select("script[data-automation=server-state]")?.first()?.html()?.split("window.SEEK_REDUX_DATA = ")?.last()?.split("window.SK_DL = ")?.first()?.trim()
			dataRaw			= dataRaw?.substring(0, dataRaw.length()-1) // remove ;
			dataRaw			= dataRaw.replaceAll("undefined", "\"undefined\"")
			def data		= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: data?.jobdetails?.result?.id
			job.url			= jobPage?.select("link[rel=canonical]")?.first()?.absUrl("href") ?: pageURL // NOTE: canonical URL is cleaner
			job.name		= data?.jobdetails?.result?.title
			job.locale		= jobPage.select("meta[property=og:locale]")?.first()?.attr("content")

			job.html		= jobPage?.select("div[data-automation=jobDescription]")?.first()?.html()	// NOTE: job.html only requires the jobAd description - the whole page is stored in the rawPage!
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:]
			if (data) job.json.pageData = data // NOTE: json is a map of json objects - many website have multiple json object (especially if they use schema.org's JobPosting)

			try { job.dateCreated = ZonedDateTime.parse(data?.jobdetails?.result?.listingDate)?.toLocalDateTime() } catch (e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= data?.jobdetails?.result?.workType
			
			job.salary.text				= data?.jobdetails?.result?.salary
			try {
				def salaryValue				= (data?.jobdetails?.result?.salary as String)?.split("-")?.first()?.trim()?.replaceAll(",", "")
				salaryValue					= salaryValue ? salaryValue?.substring(1, salaryValue?.length()) : ""
				salaryValue					= salaryValue ? (salaryValue?.contains(".") ?: salaryValue + ".00") : ""
				job.salary.value			= salaryValue ? salaryValue as Double: null
			} catch(e) {}
//			job.salary.currency			= "USD" // ??? will be incorrect in New Zealand (nz) and Australia (au)
			job.salary.period			= data?.jobdetails?.result?.salaryType?.replaceAll(/Package/,'')

			List<Map> contactParams 			= data?.jobdetails?.result?.contactMatches
			for (Map element: contactParams) {
				if ((element?.type as String)?.equalsIgnoreCase("email"))		job.contact.email = element?.value
				else if ((element?.type as String)?.equalsIgnoreCase("phone"))	job.contact.phone = element?.value
				else log.warn "Unused contactParams: $element"
			}
			
			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category
			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + data?.jobdetails?.result?.classification?.description
			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + data?.jobdetails?.result?.subClassification?.description
			job.orgTags."${TagType.SKILLS}" 	= (job.orgTags."${TagType.SKILLS}" ?: []) + data?.jobdetails?.result?.roleRequirements

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 				= new Location()
			location.source 				= sourceID
			location.idInSource				= data?.jobdetails?.result?.locationId
			try {
				location.orgAddress.addressLine = jobPage.select("dd")[1]?.text()	// WARN: selector looks to generic and can select wrong info
			} catch (e) {
			}
			location.orgAddress.country 	= data?.jobdetails?.result?.locationHierarchy?.nation
			location.orgAddress.state 		= data?.jobdetails?.result?.locationHierarchy?.state
			location.orgAddress.city 		= data?.jobdetails?.result?.locationHierarchy?.city
			location.orgAddress.district	= data?.jobdetails?.result?.locationHierarchy?.area
			location.orgAddress.quarter		= data?.jobdetails?.result?.locationHierarchy?.suburb	// WARN: county is an area larger than a city

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 	= new Company()
			company.source		= sourceID
			company.idInSource	= data?.jobdetails?.result?.advertiser?.id				// WARN: data?.jobdetails?.result?.companyReview?.companyId exists too
			company.name		= data?.jobdetails?.result?.advertiser?.description		// WARN: data?.jobdetails?.result?.companyReview?.companyName exists too
			company.urls		= [("$sourceID" as String): ""]							// WARN: data?.jobdetails?.result?.companyReview?.companyProfileUrl exists too
			company.ids			= [("$sourceID" as String): company.idInSource]
def test = "https://www.seek.com.au/" + data?.jobdetails?.result?.companyReview?.companyProfileUrl
			/*******************/
			/* Store page data */
			/*******************/

			Document rawPage 	= new Document()
			rawPage.url			= job.url
			rawPage.html		= jobPage.html()

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
