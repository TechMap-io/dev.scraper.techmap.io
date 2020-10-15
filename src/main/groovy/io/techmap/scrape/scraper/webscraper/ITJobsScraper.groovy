/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.scraper.webscraper


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
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors

@Log4j2
class ITJobsScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "us", url: "https://www.itjobslist.com"]
	]
	static final String    baseSourceID = 'itjobslist_'

	/**
	 * NOTE: This resource contains little information.
	 * Only after registration you are able to apply the offer
	 * and get new source with details - https://www.indeed.com
	 *
	 */
    ITJobsScraper(Integer sourceToScrape) {
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

		// No headers or additional parameters required.
		// It doesn't work with old implementation - response is encoded
		// I've created method where you can use additional parameters in request for future resources
		def startPage = loadPageWithParameters("${source.url}", null)
		final def startCookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

		// It's necessary to go to All jobs page before to get categories
		def allJobsPageLink = startPage.select("div.jp_top_jobs_category i[class~=fa-th-large]")?.first()?.parent()?.select("h3 a")?.first()?.absUrl("href")
		def allJobsPage
		if (allJobsPageLink) {
			allJobsPage = loadPageWithParameters(allJobsPageLink, null)
		} else {
			return jobsInSourceCount
		}

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		def groups = allJobsPage?.select(".jp_rightside_job_categories_wrapper h4:Contains(by Category)")?.first()?.parent()?.parent()?.select("a")?.sort { it.text() } // NOTE: using Contains() to select more precise
		// sort necessary for compare with status.lastCategory
		
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
		def paginationPage = loadPageWithParameters(nextURL, null)
		
		int maxJobsInGroup = (paginationPage?.select("#containerIntro h1")?.text()?.replaceAll("\\D", "") ?: 0)?.toInteger() // NOTE: ".mnjbtit" looks generated and could change often

		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			// NOTE: this mechanism ready the "next" URL in a pagination - some pages might require different approaches (e.g., increasing a "page" parameter)
			// but in this source page (as number) is added to the end of url
			Elements jobLinks = paginationPage?.select("#grid a h2")*.closest("a") // NOTE: ".jbtit" looks generated and could change often
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			if (maxDocsToPrint <= 0) break
			// Get next URL and load page for next iteration
			offset = Math.max(status.lastOffset as Integer ?: 0 , jobLinks.size())
			def nextPageButtonURL = paginationPage.select("ul.pagination li a:Contains(Next)") // NOTE: using containing text might be safer on the last page
			nextURL = nextPageButtonURL.text().equalsIgnoreCase("next") ? nextPageButtonURL.absUrl("href") : null
			if (nextURL) {
				paginationPage = loadPageWithParameters(nextURL, null)
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
			String idInSource = jobPageURL.split("-").last()
			if (!db.jobExists(sourceID, idInSource)) {
				extraData.idInSource = idInSource
				def workType = pageElement.select("ul li i:last-child.fa-clock-o")?.first()?.parent()?.text()?.split(" ")?.last() // WARN: only select if it is really the  last child (could be missing or first)
				// work type can be extracted only here
				// but it seems static value for all offers
				extraData.workType = workType
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
			def jobPage = loadPageWithParameters(pageURL, null)
			if (!jobPage) return false

			/*******************************/
			/* Extract data in JSON format */
			/*******************************/

			// resource doesn't contains json data with offer details
			// final JsonSlurper jsonSlurper = new JsonSlurper()	// thread safe and serializable - alternative: new HashMap<>(jsonSlurper.parseText(jsonText))
			// def dataRaw		= jobPage?.select("script")
			// def data		= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: pageURL.split("-").last()
			job.url			= pageURL ?: jobPage?.select("link[rel=canonical]")?.first()?.absUrl("href")
			job.name		= jobPage.select("div.jp_job_des h1.ttt")?.first()?.text()
			job.locale		= "en_US" // only hardcode - there is missed such data in page
			job.html		= jobPage?.select("div.jp_job_res")?.first()?.html() // NOTE: job.html only requires the jobAd description - the whole page is stored in the rawPage!
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:] // resource doesn't contains json data with offer details
			def postedDate  = jobPage.select("div.jp_job_res p:Contains(Posted on)")?.last()?.text() // NOTE: dangerous but lets try if all jobs
			if (postedDate.containsIgnoreCase("Posted on :")) {
				postedDate  = postedDate.replaceAll("Posted on :", "").trim()
			}
			else {
				postedDate = ""
			}
			if (postedDate) {
				try {
					job.dateCreated = ZonedDateTime.parse(postedDate, DateTimeFormatter.RFC_1123_DATE_TIME).toLocalDateTime()
				}
				catch (Exception e) {}
			}

			job.position.name			= job.name
			job.position.workType		= extraData.workType

			job.orgTags."${TagType.CATEGORIES}" 	= (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category
			job.orgTags."${TagType.WORK_TYPES}"		= [job.position.workType]

			/**********************/
			/* Fill Location data */
			/**********************/

			// only name, city, state and sometimes postCode are presented
			Location location 				= new Location()
			location.source 				= sourceID
			location.orgAddress.source 		= sourceID
			location.orgAddress.companyName	= jobPage.select("div.jp_job_des ul li i.fa-suitcase")?.first()?.parent()?.text()		// NOTE its safer to select the suitcase icon
			location.orgAddress.addressLine	= jobPage.select("div.jp_job_des ul li i.fa-map-marker")?.first()?.parent()?.text()	// NOTE its safer to select the map icon
			location.orgAddress.country		= "USA"
			location.orgAddress.state		= location.orgAddress.addressLine?.split(",")?.last()?.replaceAll("\\d", "")
			location.orgAddress.city		= location.orgAddress.addressLine?.split(",")?.first()
			location.orgAddress.postCode	= location.orgAddress.addressLine?.split(",")?.last()?.replaceAll("\\D", "")

			/*********************/
			/* Fill Company data */
			/*********************/

			// only name, city, state and sometimes postCode are presented
			Company company = new Company()
			company.source		= sourceID
			company.name		= location.orgAddress.companyName
			company.name		= company.name ?: jobPage.select("div.jp_job_des ul li")?.first()?.text()
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
