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

import java.time.LocalDate

@Log4j2
class ReedScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "uk", url: "https://www.reed.co.uk"]
	]
	static final String    baseSourceID = 'reed_'

    ReedScraper(Integer sourceToScrape) {
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
		def groups = startPage?.select("a.gtmSectorLink")?.sort { it.text() } // sort necessary for compare with status.lastCategory
		
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
		
		int maxJobsInGroup = (paginationPage?.select("div.page-title span.count")?.first()?.text()?.replaceAll("\\D", "") ?: 0)?.toInteger()
		
		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			def jobLinks = paginationPage?.select("#server-results article h3.title a")
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			// if (jobsInJobListCount <= 0) break // switch deep scraping (when disabled) and shallow scraping (when enabled)
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			offset  = Math.max(status.lastOffset as Integer ?: 0, jobLinks.size() ?: 25)
			nextURL = paginationPage?.select("a#nextPage")?.first()?.absUrl("href")
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
			String idInSource = pageElement.attr("data-id") ?: pageElement.attr("value")
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

			// Json data was not fond

//			final JsonSlurper jsonSlurper = new JsonSlurper()	// thread safe and serializable - alternative: new HashMap<>(jsonSlurper.parseText(jsonText))
//			def dataRaw		= jobPage?.select("script")*.html()
//			def data		= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: pageURL?.split("\\?")?.first()?.split("/")?.last()
			job.url			= jobPage?.select("link[rel=canonical]")?.first()?.attr("href") ?: pageURL
			job.name		= jobPage.select(".job-header h1")?.first()?.text()

			job.html		= jobPage?.select("div[class=col-lg-12]")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:] // NOTE: any json was not found


			job.position.name			= job.name
			def jobTypesRow				= jobPage.select("span[data-qa=jobTypeLbl]")?.first()?.text()
			if (jobTypesRow && jobTypesRow.contains(",")) {
				job.position.workType		= jobTypesRow.split(",").last().trim()
				job.position.contractType	= jobTypesRow.split(",").first().trim()
			}
			def jobScriptText			= jobPage.select("script").find({it.html().contains("jobType: '")}).html()
			job.position.contractType	= job.position.contractType ?: jobScriptText?.split("jobType: '")?.last()?.split("'")?.first()

			try { job.dateCreated = LocalDate.parse(jobScriptText?.split("jobPostedDate: '")?.last()?.split("'")?.first(), "dd/MM/yyyy")?.atStartOfDay() } catch (e) { /*ignore*/ }

			job.salary.text 			= jobPage.select("span[data-qa=salaryLbl]")?.first()?.text()
			job.salary.value			= jobPage.select("span[itemprop=baseSalary] meta[itemprop=minValue]")?.first()?.attr("content") as Double
			job.salary.value			= job.salary.value ?: jobPage.select("span[itemprop=baseSalary] meta[itemprop=maxValue]")?.first()?.attr("content") as Double
			job.salary.period			= jobPage.select("span[itemprop=baseSalary] meta[itemprop=unitText]")?.first()?.attr("content")
			job.salary.currency			= jobPage.select("span[itemprop=baseSalary] meta[itemprop=currency]")?.first()?.attr("content")

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category
			job.orgTags."${TagType.INDUSTRIES}" = (job.orgTags."${TagType.INDUSTRIES}" ?: []) + jobScriptText?.split("jobSector: '")?.last()?.split("'")?.first()

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location = new Location()
			location.source = sourceID
			location.orgAddress.addressLine = jobPage.select("#jobCountry")?.first()?.parent()?.text()
			location.orgAddress.countryCode = jobPage.select("meta[itemprop=addressCountry]")?.attr("content")
			location.orgAddress.country 	= jobPage.select("#jobCountry")?.first()?.attr("Value")
			location.orgAddress.district 	= jobPage.select("span[data-qa=localityLbl]")?.text()
			location.orgAddress.district 	= location.orgAddress.district ?: jobPage.select("meta[itemprop=addressRegion]")?.first()?.attr("content")
			location.orgAddress.city 		= jobPage.select("span[data-qa=regionLbl]")?.first()?.text()
			location.orgAddress.postCode 	= jobPage.select("meta[itemprop=postalCode]")?.first()?.attr("content")

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company = new Company()
			company.source		= sourceID
			def companyLink 	= jobPage.select("a[data-gtm-value=recruiter_name_click]")?.first()?.absUrl("href")
			company.idInSource	= companyLink ? companyLink.split("\\?")?.first()?.split("/")?.last()?.replaceAll("\\D", "") : ""
			company.name		= jobPage.select("a[data-gtm-value=recruiter_name_click]")?.first()?.text()

			company.urls		= [("$sourceID" as String): companyLink]
			company.ids			= [("$sourceID" as String): company.idInSource]
			if (company.name?.find(/(?i)Jobs via StepStone/)) return false // job is from another Stepstone portal
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
