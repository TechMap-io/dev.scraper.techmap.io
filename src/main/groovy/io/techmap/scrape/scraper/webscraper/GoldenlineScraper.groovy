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
class GoldenlineScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "pl", url: "https://www.goldenline.pl/praca"]
	]
	static final String    baseSourceID = 'goldenline_'

    GoldenlineScraper(Integer sourceToScrape) {
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

		// the first page doesn't contains all groups
		def groupPageLink = startPage.select("div*.popular-column:contains(Industries) a.show-more")?.first()?.absUrl("href")
		def groupPage = loadPage(groupPageLink)

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		def groups = groupPage?.select("ul.bullets li a")?.sort { it.text() }
		groups?.shuffle() // to randomize entry point and shallow scrape (due to captcha block) - conflicts with status
		
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
		
		int maxJobsInGroup = (paginationPage?.select("div#branches input[checked=checked]")?.first()?.parent()?.attr("data-count") ?: 0)?.toInteger()
		
		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			def jobLinks = paginationPage?.select("ul.items-list li.stats-data-offer a")

			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			nextURL = paginationPage?.select("li.next a")?.first()?.absUrl("href")
			offset = Math.max((status.lastOffset as Integer) ?: 0, jobLinks?.size())
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
			String idInSource = pageElement.parent().attr("data-id")
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

			if (jobPage.select(".g-recaptcha")) { // IP-based block: happens after ~20-30 pageloads
				log.error "Blocked with Captcha!"
				return false
			}
			
			/*******************************/
			/* Extract data in JSON format */
			/*******************************/

			// json data was not found

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job 		= new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: jobPage.select("script")?.find({it.html()contains("offerId")})?.html()?.split("offerId")?.last()?.split(";")?.first()?.replaceAll("\\D", "")
			job.url			= pageURL ?: jobPage.select("link[rel=canonical]")?.first()?.attr("href")
			job.name		= jobPage.select("h1.o-job-title")?.first()?.text() ?: jobPage.select("p.job-ad-position")?.first()?.text()
			// NOTE! There are three different pages with offers - Try to handle every variant
//			job.html		= (jobPage?.select("div*.firm-meta-info")?.first()?.html() ?: "") + (jobPage?.select("div#main-gl-offer")?.first()?.html() ?: "") + (jobPage?.select("div#H4link_main")?.first()?.html() ?: "")
			job.html		= jobPage?.select("#H4link_main,#main-gl-offer,#main")?.first()?.html()	// "div*.firm-meta-info" looks corrupt, (#main was new (4th variant?))

			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:]

			try { job.dateCreated 		= LocalDate.parse(jobPage.select("p.job-ad-date")?.first()?.text()?.replaceAll("Added: ", ""), "dd.MM.yyyy")?.atStartOfDay() } catch (e) { /*ignore*/ }

			job.position.name			= job.name

			job.salary.text				= jobPage.select("p*.job-ad-salary")?.first()?.text()
			job.salary.value			= (jobPage.select("p*.job-ad-salary")?.first()?.text()?.split("-")?.first()?.replaceAll("\\D", "") ?: 0) as Double
			job.salary.value			= job.salary.value ?: (jobPage.select("p*.job-ad-salary")?.first()?.text()?.split("-")?.last()?.replaceAll("\\D", "") ?: 0) as Double
			job.salary.currency			= job.salary.text?.contains("PLN") ? "PLN" : job.salary.text?.contains("EUR") ? "EUR" : job.salary.text?.contains("€") ? "EUR" : ""

			// contact was not found

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 					= new Location()
			location.source 					= sourceID
			location.orgAddress.addressLine 	= jobPage.select("div.job-ad-info__item")?.first()?.text()?.replaceAll(/(?i)\s*(City|town|\/|:|Province)\s*/, ', ')?.replaceAll(/([\s,]+[\s,]+)+/,', ')?.replaceAll(/^[\s,]+|[\s,]+$/,'')	// NOTE: just cleanup for Geocoder

			location.orgAddress.state			= jobPage.select("p.job-ad-regions > span")?.first()?.text()//?.replaceAll("Province:", "") // NOTE: replace not necessary; district is smaller than city
			location.orgAddress.city			= jobPage.select("p.job-ad-city > strong")?.first()?.text()

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 	= new Company()
			company.source		= sourceID
			company.idInSource	= jobPage.select("script")?.find({it.html()contains("offerCompanyId")})?.html()?.split("offerCompanyId")?.last()?.split(";")?.first()?.replaceAll("\\D", "")
			company.name		= jobPage.select("a.firm-name")?.first()?.text() ?: jobPage.select("script")?.find({it.html()contains("offerCompany =")})?.html()?.replaceAll(/(?s).*offerCompany = '([^']+)';.*/,'$1')
//			company.ids			= [("$sourceID" as String): company.idInSource]	// NOTE: interesting but not an ID we can find the Company-page with
			if (!company.name || company.idInSource == "anonymous") {
				return false
			} // job is from anonymous company
			def companyLink = jobPage.select(".firm-profile-box a.btn-primary")?.first()?.absUrl("href")?.replaceAll(/\/opinie\/?$/,'')
			if (companyLink) {
				company.idInSource	= companyLink?.replaceAll(/.*\/([^\/]+)$/,'$1')
				company.urls		= [("$sourceID" as String): companyLink]
				company.ids			= [("$sourceID" as String): company.idInSource]
			}

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
