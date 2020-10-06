/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.scraper.webscraper

import groovy.json.JsonSlurper
import groovy.time.TimeCategory
import groovy.util.logging.Log4j2
import io.techmap.scrape.data.Company
import io.techmap.scrape.data.Job
import io.techmap.scrape.data.Location
import io.techmap.scrape.data.shared.Address
import io.techmap.scrape.data.shared.Contact
import io.techmap.scrape.data.shared.GeoPoint
import io.techmap.scrape.data.shared.TagType
import io.techmap.scrape.helpers.DataCleaner
import org.bson.Document
import org.jsoup.HttpStatusException
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import java.time.ZonedDateTime

@Log4j2
class TotaljobsScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources      = [
			[id: "en", url: "https://www.totaljobs.com/"]
	]
	static final String    baseSourceID = 'totaljobs_'

    TotaljobsScraper(Integer sourceToScrape) {
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

        // find groups for jobs from start page
        def groupPageLink = startPage?.select("#navbar-desktop-site-links ul li a")[0]?.absUrl("href")
        this.cookiesForThread."${Thread.currentThread().getId()}" = startCookies
        def groupPage = loadPage(groupPageLink)

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
        // Jobs are selected only by category? but not by location
		def groups = groupPage?.select("div[class\$=keywords-list] ul li h3 a")?.sort { it.text() }

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


        int offersAmount = paginationPage.select("div.job-title").size()
        int totalAmount = paginationPage.select("div.page-title").first().text().replaceAll("\\D", "").toInteger()

		// we should include the first one without numeration parameter
		int lastPageInGroup = ((totalAmount / offersAmount) + 1) as Integer
		int maxJobsInGroup = totalAmount
		int jobsInGroupCount = 0
		// find all job links and reload listPage
		int pageNumber = 1
		while (pageNumber <= lastPageInGroup) {
			log.debug "... scrape list page ${pageNumber} with ${offersAmount} offers in group ${group.text()}"
			// extract job offer links
			def jobOffers = paginationPage.select("div.job-title a")
			// check jobs and break loop if missed - total amount can be higher than expected
			// f.e I've got more 4000 offers in downloaded document,
			// but actually(after script) there were less than 2000
			if (jobOffers) {
				for (Element element : jobOffers) {
					if (scrapePage(element?.absUrl("href"), [category: group.text()])) {
						jobsInGroupCount++
					}
				}
			} else {
				break
			}
			// set and load next page
			pageNumber++
			def nextPageURL = nextURL + "?page=" + pageNumber
			paginationPage = loadPage(nextPageURL)
		}
		status.lastOffset = 0 // Reset
		db.saveStatus(status)

		log.info "Scraped ${"$jobsInGroupCount".padLeft(5)} of ${"$maxJobsInGroup".padLeft(6)} jobs in group ${group.text()} in " + TimeCategory.minus(new Date(), startTime)
		return jobsInGroupCount
	}

	@Override
	int scrapePageList(Elements pageElements, Map extraData) {
		// all list pages are extracted in scrapePageGroup() with added page parameter
		return 0
	}

	// @formatter:off (to keep the code in a more tabular form - "Align variables in columns" only works for class fields )
	@Override
	boolean scrapePage(String pageURL, Map extraData) {
		try {
			def jobPage = loadPage(pageURL)
			if (!jobPage) return false

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
 			job.idInSource	= extraData.idInSource ?: jobPage.select("#jobId")?.first()?.attr("value")
			job.url			= pageURL ?: jobPage?.select("link[rel=canonical]")?.first()?.attr("href")
			job.name		= jobPage.select("h1.brand-font").first().text()

			job.locale		= jobPage.select("meta[property=og:locale]")?.first()?.attr("content")
//			job.referenceID = jobPage.select("ul li span.reference")
//					.stream()
//					.filter(it -> it.text().contains("Reference"))
//					?.findFirst()
//					?.get()
//					?.parent()
//					?.text()
//					?.replaceAll("Reference: ", "")
			// NOTE: you could have used JSoup's "Contains()" and "ownText()" feature
			job.referenceID = jobPage.select("ul li span.reference:Contains(Reference)")?.first()?.parent()?.ownText()
			
//			job.html		= jobPage?.select("div[class^=container job-content]")?.first()?.html()
			// WARN: job.html and job.text is meant to contain only the text describing the job - not the whole job ad
			job.html		= jobPage?.select(".job-content")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)

			def jobScriptJsonText 	= jobPage.select("#jobPostingSchema")?.first()?.childNode(0)?.toString()?.replaceAll("\n", "")?.trim()

			job.json			= [:]

			// some address parameters can be taken from job script
			Address address 	= new Address()
			address.source			= sourceID

			if (jobScriptJsonText) {
				final JsonSlurper jobJsonSlurper = new JsonSlurper()
				def jobJsonMap 				= jobJsonSlurper.parseText(jobScriptJsonText)
//				job.json.baseSalary			= jobJsonMap?.baseSalary?.value?.value
//				job.json.unitText			= jobJsonMap?.baseSalary?.value?.unitText
//				job.json.currency			= jobJsonMap?.baseSalary?.currency
//				job.json.title				= jobJsonMap?.title
//				job.json.url				= jobJsonMap?.url
//				job.json.datePosted			= jobJsonMap?.datePosted
//				job.json.industry			= jobJsonMap?.industry
//				job.json.employmentType		= jobJsonMap?.employmentType
//				job.json.hiringOrganization = jobJsonMap?.hiringOrganization?.name
//				job.json.employmentType		= jobJsonMap?.employmentType
//				job.json.jobLocation		= jobJsonMap?.jobLocation?.address?.addressLocality
				// WARN: storing the whole json object is OK: just in case additional info is added later by the website
				if (jobJsonMap)	job.json.schemaOrg = jobJsonMap

				// some data are in json like posted date or work type
//				job.dateCreated 			= ZonedDateTime.parse("2020-10-06T08:43+10:00").toLocalDateTime()
				// WARN: you used a placeholder date
				job.dateCreated 			= ZonedDateTime.parse(jobJsonMap?.datePosted).toLocalDateTime()
				job.position.workType		= jobJsonMap?.employmentType

				address.country				= jobJsonMap?.jobLocation?.address?.addressCountry
				address.district			= jobJsonMap?.jobLocation?.address?.addressRegion
				address.city    			= jobJsonMap?.jobLocation?.address?.addressLocality
				address.geoPoint			= new GeoPoint(
						"lat":jobJsonMap?.jobLocation?.geo?.latitude as Double,
						"lng":jobJsonMap?.jobLocation?.geo?.longitude as Double
				)
// WARN: you forgot to scrape job.salary
			}

			job.position.name				= job.name
			job.position.contractType		= jobPage?.select("li.job-type icon")?.first()?.text()

			// career levels can be extracted only if mentioned in job name
			List<String> careerLevels		= new ArrayList<>()
			if (job.name.containsIgnoreCase("senior")) {
				careerLevels.add("Senior")
			}
			if (job.name.containsIgnoreCase("middle")) {
				careerLevels.add("Middle")
			}
			if (job.name.containsIgnoreCase("junior")) {
				careerLevels.add("Junior")
			}
			job.position.careerLevel = String.join(", ", careerLevels)

			/**********************/
			/* Fill Location data */
			/**********************/

			/**********************/
			/* -------AND-------- */
			/**********************/
// NOTE: Not our preferred way: now the collection of data (e.g., for address or job) is all over the place
			/*********************/
			/* Fill Company data */
			/*********************/

			Location location 			= new Location()
			location.source 			= sourceID

			Company company = new Company()
			company.source		= sourceID

			def companyScriptJsonText 	= jobPage.select("script[language=JavaScript]")?.first()?.childNode(0)?.toString()?.replaceAll("var analytics = ", "")?.replaceAll("\n", "")?.trim()
			job.json			= [:]
			if (companyScriptJsonText) {
				final JsonSlurper companyJsonSlurper = new JsonSlurper()
				def companyJsonMap 		= companyJsonSlurper.parseText(companyScriptJsonText)
				if (companyJsonMap)	job.json.pageData = companyJsonMap
//				location.description	= companyJsonMap?.City		// WARN: location.description is the description of the company (at that location)
//				location.name			= companyJsonMap?.JobTitle	// WARN: location.name is the name of the company not the job
//				location.dateCreated	= job.dateCreated			// WARN: location.dateCreated is the date the location was created in the MongoDB - it does not need to be set the the newest date of a job
				location.idInSource		= companyJsonMap?.JobId as String

				address.companyName 	= companyJsonMap?.CompanyName
//				address.addressLine 	= companyJsonMap?.City
				// NOTE: address.addressLine should be the address as stated on the job ad
				address.addressLine 	= jobPage.select(".job-summary .location")?.text() ?: data2?.City
//				address.postCode	 	= companyJsonMap?.JobPostcode
				// NOTE: address.postCode has two possible sources - sometime only one is used
//				address.postCode	 	= companyJsonMap?.JobPostcode ?: jobJsonMap?.jobLocation?.address?.postalCode  // WARN: this will not work as jobJsonMap is defined in another scope
				address.county 			= companyJsonMap?.CompanyName

				location.orgAddress		= address
//				location.contacts		= [new Contact("name": jobPage.select("ul li span.reference")
//												.stream()
//												.filter(it -> it.text().contains("Contact"))
//												?.findFirst()
//												?.get()
//												?.parent()
//												?.text()
//												?.replaceAll("Contact: ", "")
//										)]
				// NOTE: you could have used JSoup's "Contains()" and "ownText()" feature - and the contact belongs to job - location will collect all contacts of all jobs over time
				job.contact		= new Contact("name": jobPage.select("ul li span.reference:Contains(Contact)")?.first()?.parent()?.ownText())

				company.idInSource	= companyJsonMap?.CompanyId
				company.name		= companyJsonMap?.CompanyName
//				company.url 		= jobPage.select("#companyJobsLink").first()?.absUrl("href")
				// WARN: company.url is meant to be the company's website - e.g., https://www.hays.com
				company.urls		= [("$sourceID" as String): jobPage.select("#companyJobsLink").first()?.absUrl("href")]
				company.ids			= [("$sourceID" as String): company.idInSource]

//				job.orgTags."${TagType.COMPANY_TYPES}"		= [companyJsonMap?.SubDisciplines]
//				job.orgTags."${TagType.INDUSTRIES}"			= [companyJsonMap?.Disciplines]
				// WARN: Disciplines and SubDisciplines are Categories
				job.orgTags."${TagType.CATEGORIES}"			= [companyJsonMap?.Disciplines?.split("|"), companyJsonMap?.SubDisciplines?.split("|")]?.flatten()
				job.orgTags."${TagType.COMPANY_TYPES}"		= [companyJsonMap?.CompanyType]
//				job.orgTags."${TagType.INDUSTRIES}"			= [jobJsonMap?.industry] // WARN: this will not work as jobJsonMap is defined in another scope
//			job.orgTags."${TagType.JOBNAMES}"			= [job.name]
				// WARN: JOBNAME is the job category such as Accountant
				job.orgTags."${TagType.JOBNAMES}"			= [extraData.category, companyJsonMap?.NormalisedJobTitle]
			}

			job.orgTags."${TagType.CAREER_LEVELS}"		= [job.position.careerLevel]
//			job.orgTags."${TagType.CATEGORIES}"			= [job.orgCompany.name]
			job.orgTags."${TagType.CONTRACT_TYPES}"		= [job.position.contractType]
//			job.orgTags."${TagType.LANGUAGES}"			= ["en"]						// WARN: LANGUAGES are languages spoken in the company (location) - not the language of the job ad
			job.orgTags."${TagType.WORK_TYPES}"			= [job.position.workType]

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
