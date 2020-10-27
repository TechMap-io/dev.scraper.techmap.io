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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Log4j2
class JobinmoscowScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "ru", url: "http://www.jobinmoscow.ru/"]
	]
	static final String    baseSourceID = 'jobinmoscow_'

    JobinmoscowScraper(Integer sourceToScrape) {
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

		// Main page does not contain groups, only search page has such ability
		def searchPageLink 	= startPage?.select("a#search-href")?.first()?.absUrl("href") ?: ""
		def searchPage 		= loadPage(searchPageLink)

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		def groups = searchPage?.select("select[name=razdel] option:not(option[selected])")?.sort { it.text() }

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

		// generate group link as search link with parameters
		def searchPageLink 	= source.url + "searchv.php"		// think it's better to hardcode than load page again
		def nextURL = searchPageLink +
				"?" 				 + 						// add parameters
				"srtime=90&" 		 + 						// search period 90 days before
				"maxThread=100&" 	 +						// results on page 100
				"razdel=" + group.attr("value") 	// group id for search

		def paginationPage = loadPage(nextURL)

		// find redundant offers(top for any group)
		int offersToExclude = paginationPage?.select("div.block-result-red2")?.size()
		int maxJobsInGroup = (paginationPage?.select("div.search-line-res p")?.first()?.text()?.replaceAll("\\D", "") ?: 0).toInteger()
		if (maxJobsInGroup >= offersToExclude)
			maxJobsInGroup -= offersToExclude

		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			def jobLinks = paginationPage?.select("div.block-result-grey2 p.title a")

			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"

			if (maxDocsToPrint <= 0) break

			// Get next URL and load page for next iteration
			nextURL = paginationPage?.select("a#next_page")?.first()?.absUrl("href")
			offset = Math.max((status.lastOffset as Integer) ?: 0, jobLinks?.size() ?: 100)
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
			// id is present as parameter "link" and contains digits only
			String idInSource = jobPageURL?.replaceAll("\\D", "")
			idInSource = idInSource ?: jobPageURL?.split("link=")?.last()
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

			// page does not contain json data
//			final JsonSlurper jsonSlurper = new JsonSlurper()	// thread safe and serializable - alternative: new HashMap<>(jsonSlurper.parseText(jsonText))
//			def dataRaw		= jobPage?.select("")?.html()
//			def data		= jsonSlurper.parseText(dataRaw ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job 		= new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource
			job.url			= pageURL
			job.name		= jobPage?.select("div.vacansy div.vacansy-block h1")?.first()?.text()
			job.html		= jobPage?.select(".vacansy .left .info-vacansy")?.first()?.html()	// WARN: your selector will select "other jobs" (Похожие вакансии уборщик/уборщица)
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:]

			try {
				def dateText = jobPage?.select("p.date")?.first()?.text()?.replaceAll("Размещено ", "")
				Locale locale = new Locale("ru");
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMMM yyyy", locale);
				job.dateCreated 		= LocalDate.parse(dateText, formatter)?.atStartOfDay()
			} catch (e) { /*ignore*/ }
			try {
				if (!job.dateCreated && jobPage?.select("p.date")?.first()?.text()?.contains("Размещено сегодня")) {
					job.dateCreated = LocalDate.now()?.atStartOfDay()
				}
			} catch (e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= jobPage.select("div.vacansy div.info-vacansy div.left-info-vacansy p:contains(График работы:)")?.first()?.text()
			if (job.position.workType?.startsWith("График работы:")) {
				job.position.workType = job.position.workType?.replaceAll("График работы: ", "")?.trim()
			} else { job.position.workType = "" }
			job.position.contractType	= jobPage.select("div.vacansy div.info-vacansy div.left-info-vacansy p:contains(Занятость:)")?.first()?.text()
			if (job.position.contractType?.startsWith("Занятость:")) {
				job.position.contractType = job.position.contractType?.replaceAll("Занятость: ", "")?.trim()
			} else { job.position.contractType = "" }

			job.salary.text				= jobPage.select("p.pay")?.first()?.text()
			job.salary.value			= (job.salary.text?.replaceAll("\\D", "") ?: 0 )?.toDouble()
			job.salary.currency			= "RUR"

			job.contact.name			= jobPage.select("div#contacts p")?.first()?.ownText()
			job.contact.email			= jobPage.select("div#contacts p a")?.last()?.text()
			job.contact.phone			= jobPage.select("div#contacts p")[1]?.ownText()

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 					= new Location()
			location.source 					= sourceID
			def locationElement					= jobPage.select("div.vacansy div.left-block p.city")?.first()
			location.orgAddress.addressLine 	= locationElement?.text() ?: jobPage.select("div.vacansy .right-info-vacansy p b:Contains(Адрес:)")?.first()?.parent()?.ownText() // TODO: check which address is more specific - e.g. for http://www.jobinmoscow.ru/linkvac.php?link=1150202551

			location.orgAddress.county			= locationElement?.select("a")?.find({it.attr("href").contains("regionid")})?.text()
			location.orgAddress.city			= locationElement?.select("a")?.find({it.attr("href").contains("cityid")})?.text()
			location.orgAddress.district		= locationElement?.select("a")?.find({it.attr("href").contains("srdistrict")})?.text() // NOTE: county is an area larger than a city
			location.orgAddress.quarter			= locationElement?.select("a")?.find({it.attr("href").contains("metroid")})?.text()?.replaceAll(/^/,'метро: ')	// NOTE: not really the street - using quarter

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 	= new Company()
			company.source		= sourceID
			company.idInSource	= jobPage.select("div.vacansy div.info-vacansy div.right-info-vacansy p:contains(Работодатель) a")?.first()?.attr("href")?.replaceAll("\\D", "")
			company.name		= jobPage.select("div.vacansy div.info-vacansy div.right-info-vacansy p:contains(Работодатель) a")?.first()?.text()
			company.name		= company.name ?: jobPage.select("div.vacansy div.info-vacansy div.right-info-vacansy p:contains(Работодатель)")?.first()?.text()?.replaceAll(/^\s*Работодатель[:\s]+/,'') // WARN: does not necessarily has a link!
			// internal link is present only
			def companyLink 	= jobPage.select("div.vacansy div.info-vacansy div.right-info-vacansy p:contains(Работодатель) a")?.first()?.absUrl("href")
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
