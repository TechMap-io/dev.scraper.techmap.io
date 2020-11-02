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
import java.time.format.DateTimeFormatter

@Log4j2
class EfinancialcareersScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 **/
	static final ArrayList sources      = [
			[id: "uk", url: "https://www.efinancialcareers.co.uk"],
			[id: "be", url: "https://www.efinancialcareers.be"],
			[id: "dk", url: "https://www.efinancialcareers.dk"],
			[id: "fi", url: "https://www.efinancialcareers.fi"],
			[id: "fr", url: "https://www.efinancialcareers.fr"],
			[id: "de", url: "https://www.efinancialcareers.de"],
			[id: "ie", url: "https://www.efinancialcareers.ie"],
			[id: "it", url: "https://www.efinancialcareers.it"],
			[id: "lu", url: "https://www.efinancialcareers.lu"],
			[id: "me", url: "https://www.efinancialcareers-gulf.com"], // middle-east region
			[id: "nl", url: "https://www.efinancialcareers.nl"],
			[id: "no", url: "https://www.efinancialcareers-norway.com"],
			[id: "ru", url: "https://www.efinancialcareers.ru"],
			[id: "za", url: "https://www.efinancialcareers.co.za"],
			[id: "se", url: "https://www.efinancialcareers.se"],
			[id: "ch", url: "https://www.efinancialcareers.ch"],
			[id: "ca", url: "https://www.efinancialcareers-canada.com"],
			[id: "us", url: "https://www.efinancialcareers.com"],
			[id: "au", url: "https://www.efinancialcareers.com.au"],
			[id: "cn", url: "https://www.efinancialcareers.cn"],
			[id: "hk", url: "https://www.efinancialcareers.hk"],
			[id: "jp", url: "https://www.efinancialcareers.jp"],
			[id: "my", url: "https://www.efinancialcareers.my"],
			[id: "sg", url: "https://www.efinancialcareers.sg"]

	]
	static final String    baseSourceID = 'efinancialcareers_'

    EfinancialcareersScraper(Integer sourceToScrape) {
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

		// yhe first page contains only part of groups
		def groupPageLink = startPage.select("#job-sectors-list div.view-all a")?.first()?.absUrl("href")
		def groupPage = loadPage(groupPageLink)

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		def groups = groupPage?.select("#jobsBySector ul li a")?.sort { it.text() } // sort necessary for compare with status.lastCategory
		
		for (Element group in groups) {
			def status = db.loadStatus(sourceID + "-${group.text()?.split(" \\(")?.first()}")
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

		int maxJobsInGroup = paginationPage?.select("span[data-tile=numFound]")?.first()?.text() as Integer ?: 0
		maxJobsInGroup = maxJobsInGroup ?: paginationPage?.select("h2.desc")?.first()?.text()?.replaceAll("\\D", "") as Integer
		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			def jobLinks = paginationPage?.select(".jobList > li.jobPreview h2 a")
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()?.split(" \\(")?.first()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"
			
			if (maxDocsToPrint <= 0) break
			
			// Get next URL and load page for next iteration
			offset = Math.max(status.lastOffset as Integer?: 0, jobLinks.size() ?: 24)
			nextURL = paginationPage.select("a.nextPage")?.first()?.absUrl("href")
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
			String idInSource = jobPageURL?.split(".id0")?.last()
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
			def dataRaw		= jobPage?.select("script")?.find({it?.html()?.contains("jobObj")})?.html()?.split("ssdl.jobObj = ")?.last()?.split("ssdl.session")?.first()?.replaceAll("'", "\"")
			def data		= jsonSlurper.parseText(dataRaw ?: "{}")
			def dataRaw2	= jobPage?.select("script#jobPosting")?.html()
			def data2		= jsonSlurper.parseText(dataRaw2 ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: data?.jobId
			job.url			= pageURL ?: jobPage?.select("link[rel=canonical]")?.first()?.attr("href")
			job.name		= data?.job_title ?: data2?.title

			job.locale		= jobPage?.select("script")?.find({it?.html()?.contains("efcLocale")})?.html()?.split("\"efcLocale\":\"")?.last()?.split("\",\"")?.first()

			job.html		= jobPage?.select("div[class=container]")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:]
			if (data)	job.json.pageData = data
			if (data2)	job.json.pageData2 = data2

			try {
				job.dateCreated 			= ZonedDateTime.parse(data2?.datePosted, DateTimeFormatter.ofPattern( "EEE MMM d HH:mm:ss zzz yyyy" , Locale.US))?.toLocalDateTime()
			} catch (e) { /*ignore*/ }

			def workTypeAndContractTypeRaw 	= jobPage.select("div#jobDetailStrickyScrollUnderDiv div[class=col-12]")?.first()?.text()
			workTypeAndContractTypeRaw 		= workTypeAndContractTypeRaw.contains(", ") ? workTypeAndContractTypeRaw : ""
			job.position.name				= job.name
			job.position.workType			= workTypeAndContractTypeRaw?.split(", ")?.last() ?: data2?.employmentType?.replaceAll("_", " ")
			job.position.contractType		= workTypeAndContractTypeRaw?.split(", ")?.first() ?: data?.position_type

			job.salary.text					= data2?.baseSalary?.value ?: jobPage.select("div#jobDetailStrickyScrollUnderDiv i.fa-money")?.first()?.parent()?.text()

			job.referenceID					= jobPage?.select("div*.no-gutters div:contains(Job ID:)")?.first()?.ownText()?.replaceAll("Job ID:", "")?.trim()

			job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category
			job.orgTags."${TagType.INDUSTRIES}" = (job.orgTags."${TagType.INDUSTRIES}" ?: []) + data?.jobSector

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location = new Location()
			location.source 					= sourceID
			location.orgAddress.addressLine 	= jobPage.select("#jobDetailStrickyScrollUnderDiv div[class=col]")?.first()?.ownText()?.replaceAll("in ", "")
			location.orgAddress.countryCode 	= data2?.jobLocation?.address?.addressCountry
			location.orgAddress.country 		= data?.jobCountry[0]
			location.orgAddress.state 			= data?.jobState[0]
			location.orgAddress.state 			= location.orgAddress.state ?: data2?.jobLocation?.address?.addressRegion
			location.orgAddress.city 			= data?.jobCity[0]
			location.orgAddress.city 			= location.orgAddress.city ?: data2?.jobLocation?.address?.addressLocality
			location.orgAddress.street 			= data2?.jobLocation?.address?.streetAddress
			location.orgAddress.postCode 		= data2?.jobLocation?.address?.postalCode
			location.orgAddress.geoPoint.lat 	= data?.latitude as Double
			location.orgAddress.geoPoint.lng 	= data?.longitude as Double

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company = new Company()
			company.source		= sourceID
			company.idInSource	= (data2?.url as String)?.split(".br0")?.last()
			company.name		= data?.companyName ?: data2?.hiringOrganization?.name
			company.name		= company.name ?: jobPage.select("#jobDetailStrickyScrollUnderDiv div[class=col] strong")?.first()?.text()
			// only internal with offers
			def companyLink 	= data2?.url
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
