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

import java.time.LocalDateTime

@Log4j2
class RabotaScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "ua", url: "https://rabota.ua"]
	]
	static final String    baseSourceID = 'rabota_'

	RabotaScraper(Integer sourceToScrape) {
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
		def groupPageLink = startPage.select("div*.f-additional-links-holder a")?.first()?.absUrl("href")
		def groupPage = loadPage(groupPageLink)

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		def groups = groupPage?.select("div*.f-rubrics-innerpaddings a*.f-visited-enable")?.sort { it.text() }
		
		for (Element group in groups) {
			def status = db.loadStatus(sourceID + "-${group.ownText()}")
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
		log.debug "... starting to scrape group ${group.ownText()}"
		def nextURL = group.absUrl("href")
		def paginationPage = loadPage(nextURL)
		
		int maxJobsInGroup = (paginationPage?.select("span#ctl00_content_vacancyList_ltCount span.fd-fat-merchant")?.first()?.text()?.replaceAll("\\D", "") ?: 0)?.toInteger()
		
		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			def jobLinks = paginationPage?.select("h2.card-title a.ga_listing")
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.ownText()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			nextURL = paginationPage?.select("dd.nextbtn a")?.first()?.absUrl("href")
			offset = Math.max((status.lastOffset as Integer) ?: 0, jobLinks?.size()?:20)
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
			String idInSource = jobPageURL?.split("vacancy")?.last()
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
			def dataRaw		= jobPage?.select("script")?.find({it?.html()?.contains("vacancy_Id")})?.html()?.replaceAll("var ruavars = |;", "")
			def data		= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job 		= new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: data?.vacancy_Id
			job.url			= pageURL ?: jobPage.select("link[rel=canonical]")?.first()?.absUrl("href")
			job.name		= data?.vacancy_Name ?: jobPage.select("h1")?.first()?.text()
			job.html		= jobPage.select("div*.santa-bg-white")?.first()?.html()
			job.locale		= jobPage.select("meta[property*=locale]")?.first()?.attr("content")
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= data ?: [:]

			try { job.dateCreated 		= LocalDateTime.parse(data?.vacancy_VacancyDate) } catch (e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= data?.vacancy_ScheduleName

			job.salary.text				= jobPage.select("div*.santa-bg-white p*.santa-typo-important")?.first()?.parent()?.text()
			job.salary.value			= (data?.vacancy_Salary ?: 0) as Double
			job.salary.value			= job.salary.value ?: (jobPage.select("div*.santa-bg-white p*.santa-typo-important")?.first()?.text()?.replaceAll("\\D", "") ?: 0) as Double
			job.salary.currency			= "UAH"

			job.contact.name 			= data?.vacancy_ContactPerson
			job.contact.phone 			= data?.vacancy_ContactPhone

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category
			job.orgTags."${TagType.INDUSTRIES}" = (job.orgTags."${TagType.INDUSTRIES}" ?: []) + (data?.vacancy_AlertSynonymNames as String)?.split(",")?.toList()
			job.orgTags."${TagType.SKILLS}" = (job.orgTags."${TagType.SKILLS}" ?: []) + data?.vacancy_LanguageDescription

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 					= new Location()
			location.source 					= sourceID
			location.orgAddress.addressLine 	= jobPage.select("span:contains(Адрес:)")?.first()?.parent()?.select("span")?.last()?.text()
			location.orgAddress.city			= data?.vacancy_CityName

			// street and county can be placed in different order in "addressLine" and it's impossible to find correct values

			location.orgAddress.geoPoint.lat	= data?.vacancy_Latitude
			location.orgAddress.geoPoint.lng	= data?.vacancy_Longitude

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 	= new Company()
			company.source		= sourceID
			company.idInSource	= data?.vacancy_NotebookId ?: pageURL.split("company").last().split("/").first()
			company.name		= data?.vacancy_CompanyName ?: jobPage.select("div*.mini-profile-container h3")?.first()?.text()
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
