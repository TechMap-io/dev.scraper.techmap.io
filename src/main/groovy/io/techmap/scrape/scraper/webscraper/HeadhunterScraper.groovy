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
class HeadhunterScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "ru", url: "https://hh.ru"],
			[id: "ua", url: "https://grc.ua"],
			[id: "by", url: "https://jobs.tut.by"],
			[id: "uz", url: "https://hh.uz"],
			[id: "kz", url: "https://hh.kz"],
			[id: "az", url: "https://hh1.az"],
			[id: "ge", url: "https://headhunter.ge"],
			[id: "kg", url: "https://headhunter.kg"]
	]
	static final String    baseSourceID = 'headhunter_'

    HeadhunterScraper(Integer sourceToScrape) {
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
		def groups = startPage?.select("div[data-qa=index-section-catalog] ul li a")?.sort { it.text() }
		
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
		
		int maxJobsInGroup = (paginationPage?.select("a[data-totalvacancies]")?.first()?.attr("data-totalvacancies") ?: 0)?.toInteger()
		
		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			def jobLinks = paginationPage?.select("a[data-qa=vacancy-serp__vacancy-title]")

			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			nextURL = paginationPage?.select("a[data-qa=pager-next]")?.first()?.absUrl("href")
			offset = Math.max((status.lastOffset as Integer) ?: 0, jobLinks?.size() ?: 50)
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
			// it will be great to use org.apache.commons.lang3.StringUtils in future to get id after last "/"
			String idInSource = jobPageURL?.replaceAll("\\D", "")
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
			def dataRaw		= jobPage?.select("#HH-Lux-InitialState")?.html()
			def data		= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job 		= new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: data?.vacancyView?.vacancyId
			job.url			= pageURL ?: jobPage?.select("link[rel=canonical]")?.first()?.attr("href")
			job.name		= data?.vacancyView?.name
			job.locale		= data?.userTargeting?.locale
			job.html		= jobPage?.select("div.row-content")?.last()?.select("div[class*=bloko-column_container]")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:]
			if (data) {
				job.json.pageData = data
			}

			try { job.dateCreated 		= ZonedDateTime.parse(data?.vacancyView?.publicationDate)?.toLocalDateTime() } catch (e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= data?.vacancyView?.get("@workSchedule")
			job.position.contractType	= data?.vacancyView?.employment?.get("@type")

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category
			job.orgTags."${TagType.INDUSTRIES}" = (job.orgTags."${TagType.INDUSTRIES}" ?: []) + data?.vacancyView?.specializations?.profArea?.first()?.trl
			job.orgTags."${TagType.SKILLS}" 	= (job.orgTags."${TagType.SKILLS}" ?: []) + data?.vacancyView?.keySkills?.keySkill

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 					= new Location()
			location.source 					= sourceID
			location.orgAddress.addressLine 	= jobPage.select("span[data-qa=vacancy-view-raw-address]")?.first()?.ownText()
			location.orgAddress.addressLine 	= location.orgAddress.addressLine ?: jobPage.select("p[data-qa=vacancy-view-location]")?.first()?.text()
			location.orgAddress.addressLine 	= location.orgAddress.addressLine ?: data?.vacancyView?.address?.displayName

			location.orgAddress.countryCode		= data?.vacancyView?.area?.get("@countryIsoCode")
			location.orgAddress.district		= data?.vacancyView?.area?.regionName
			location.orgAddress.city			= data?.vacancyView?.address?.city
			location.orgAddress.street			= data?.vacancyView?.address?.street
			location.orgAddress.houseNumber		= data?.vacancyView?.address?.building

			location.orgAddress.geoPoint.lat	= data?.vacancyView?.address?.mapData?.points?.marker?.lat
			location.orgAddress.geoPoint.lng	= data?.vacancyView?.address?.mapData?.points?.marker?.lng

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 	= new Company()
			company.source		= sourceID
			company.idInSource	= data?.vacancyView?.company?.id
			company.name		= data?.vacancyView?.company?.visibleName ?: jobPage.select("a[data-qa=vacancy-company-name]")?.first()?.text()
			// internal link is present only
			def companyLink 	= jobPage.select("a[data-qa=vacancy-company-name]")?.first()?.absUrl("href")
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
