/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.scraper.webscraper.jobscraper

import geb.Browser
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
import io.techmap.scrape.helpers.ShutdownException
import io.techmap.scrape.scraper.webscraper.AWebScraper
import org.bson.Document
import org.jsoup.HttpStatusException
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import java.time.LocalDateTime
import java.time.ZonedDateTime

@Log4j2
class TotaljobsJobsScraper extends AWebScraper {

	static final ArrayList sources      = [
			[id: "uk", url: "https://www.totaljobs.com/"]
	]
	// NOTE: Sub-Sources contain: "CityJobs","Jobsite","RetailChoice","CareerStructure","Caterer","Milkround" (*.com ?)
	static final String    baseSourceID = 'totaljobs_'
	
	TotaljobsJobsScraper(Integer sourceToScrape) {
		super(sources, baseSourceID)
		this.sourceToScrape = sourceToScrape
		this.source = this.sources[sourceToScrape]
		this.sourceID = this.baseSourceID + this.source.id
		log.info "Using userAgent:                   ${userAgent}"
	}

	@Override
	int scrape() {
		def sourceStatus = super.initScrape()
		def startTime = new Date()
		
		Browser browser = loadBrowser() // gets the active Browser (or opens it the first time)
		browser.go "${source.url}"
		simulateHumanDelay(2000)

		// Accept cookies
		if (browser.find("button.accept-button-new")?.first()?.isDisplayed()) {
			browser.find("button.accept-button-new")?.first()?.click()
			simulateHumanDelay(1000)
		}

		// Go to group/sector page
		browser.find("#navbar-desktop-site-links ul li a")?.first()?.click()
		simulateHumanDelay(2000)

		// Identify groups of jobs such as categories, industries or Jobnames we can iterate over
		def pseudoGroups = browser.find("div[class*=keywords-list] ul li h3 a")
		def groups = pseudoGroups?.collect {
			def e = new Element("a")
			e.text(it.text () ?: it.@innerHTML?.trim())
			e.attr("href", it.attr("href"))
			return e
		}
		if (!groups || groups?.size() == 0) {
			log.fatal "Could not find any groups / categories!"
			return -1
		}

		Integer jobsInSourceCount = 0
		for (Element group in groups) {
			// find the last group/category scraped and scrape from there
			if (sourceStatus.lastCategory > group.text()) continue
			sourceStatus.lastCategory = group.text()
			db.saveStatus(sourceStatus)
			
			int jobsInCategoryCount = scrapePageGroup(group, sourceStatus)
			jobsInSourceCount += jobsInCategoryCount
		}
		sourceStatus.lastCategory = ""
		db.saveStatus(sourceStatus)

		log.info "End of scraping ${"$jobsInSourceCount".padLeft(5)} jobs in " + TimeCategory.minus(new Date(), startTime )
		return jobsInSourceCount
	}

	@Override
	int scrapePageGroup(Element group, Map status) {
		def startTime = new Date()
		log.debug "... starting to scrape group ${group.text()}"

		Browser browser = loadBrowser() // gets the active Browser (or opens it the first time)
		def nextURL = group.attr("href")
		browser.go nextURL
		simulateHumanDelay(5000)

		// get max number of jobs and last page in group
		int offersPerPage		= browser.find("[data-at=job-item], div.job-title")?.size()
		def totalOffers			= browser.find("span[class*=total-results], div.page-title")?.first()?.text()?.replaceAll("\\D", "")?.toInteger()
		int lastPageInGroup		= ((totalOffers / offersPerPage) + 1) as Integer
		int maxJobsInGroup		= totalOffers
		
		int jobsInGroupCount	= 0
		int pageNumber			= 1
		while (pageNumber <= lastPageInGroup) {
			log.debug "... scrape list page ${pageNumber} with ${offersPerPage} offers in group ${group.text()}"
			
			// extract job ad links and create JSoup Elements list
			def links = browser.find("[data-at=job-item] a[data-at=job-item-title], .job-results .job .job-title a")*.attr("href")
			Elements jobOffers = links.collect {
				Element e = new Element("a")
				e.attr("href", it)
				return e
			}
			if (!jobOffers) break
			
			jobsInGroupCount += scrapePageList(jobOffers, [category: group.text()])
			log.debug "... scraped ${"$jobsInGroupCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs on page $pageNumber in group ${group.text()}"

			// click next page on pagination
			def nextPage = browser.find("div[class*=PaginationWrapper] a[title=Next],.pagination .next")
			if (!nextPage) break
			browser.interact { moveToElement(nextPage) }
			nextPage.click()
			simulateHumanDelay(1000)
			pageNumber++
		}

		log.info "Scraped ${"$jobsInGroupCount".padLeft(5)} of ${"$maxJobsInGroup".padLeft(6)} jobs in group ${group.text()} in " + TimeCategory.minus(new Date(), startTime)
		return jobsInGroupCount
	}

	@Override
	int scrapePageList(Elements pageElements, Map extraData) {
		int jobsInCategoryCount	= 0
		for (Element element in pageElements) {
			String pageURL = element.attr("href")
			String idInSource = pageURL?.replaceAll(/.*-job(\d+)$/, '$1')
			if (!db.jobExists(sourceID, idInSource)) {
				extraData.idInSource = idInSource
				if (scrapePage(pageURL, extraData)) {
					jobsInCategoryCount++
				}
			}
		}
		return jobsInCategoryCount
	}

	// @formatter:off (to keep the code in a more tabular form - "Align variables in columns" only works for class fields )
	@Override
	boolean scrapePage(String pageURL, Map extraData) {
		try {
			Browser browser = loadBrowser() // gets the active Browser (or opens it the first time)
			browser.driver.executeScript("""window.open("$pageURL","_blank");""")
			ArrayList<String> tabs = new ArrayList<String> (browser.driver.getWindowHandles())
			browser.driver.switchTo().window(tabs.get(1)) // switches to new tab - already Default with Chrome
			simulateHumanDelay(1000)

			/*******************************/
			/* Extract data in JSON format */
			/*******************************/

			final JsonSlurper jsonSlurper = new JsonSlurper()	// thread safe and serializable - alternative: new HashMap<>(jsonSlurper.parseText(jsonText))
			def jobScriptJsonText = browser?.find("script#jobPostingSchema")?.first().@innerHTML?.replaceAll("\n", "")?.trim()
			def data = jsonSlurper.parseText(jobScriptJsonText ?: "{}")
			def companyJsonText = browser?.find("script[language=JavaScript]")?.first().@innerHTML?.replaceAll("var analytics = ", "")?.replaceAll("\n", "")?.trim()
			def data2 = jsonSlurper.parseText(companyJsonText ?: "{}")
			if (!data && data2)		log.debug "Missing JSON data on page: " + pageURL; //return false
			if (data && !data2)		{
				log.debug "Missing JSON data2 on page: " + pageURL
				browser.driver.close() // close tab with job
				browser.driver.switchTo().window(tabs.get(0)) // switch to main screen (search)
				return false
			}
			if (!data && !data2)	{
				log.debug "Missing JSON data & data2 on page: " + pageURL
				browser.driver.close() // close tab with job
				browser.driver.switchTo().window(tabs.get(0)) // switch to main screen (search)
				return false
			}

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: data2?.JobId ?: browser.find("body > #jobId")?.first()?.attr("value")
			job.url			= pageURL ?: browser.find("head > link[rel=canonical]")?.first()?.attr("href")
			job.name		= data?.title ?: data2?.JobTitle ?: browser.find("h1.brand-font").first().text()

			job.locale		= browser.find("head > meta[property*=locale]")?.first()?.attr("content")
			job.referenceID = browser.find("ul li span.reference", text: browser.iContains("Reference"))?.first()?.parent()?.text()?.replaceAll(/Reference:\s*/,'')

			job.html		= data?.description ?: browser.find(".job-description")?.first()?.@outerHTML
			job.text		= DataCleaner.stripHTML(data?.description ?: job.html)

			job.json		= [:]
			if (data)	job.json.schemaOrg	= data
			if (data2)	job.json.pageData	= data2

			// some data are in json like posted date or work type
			try { job.dateCreated 		= ZonedDateTime.parse(data?.datePosted ?: browser.find("head > meta[property*=published_time]")?.first()?.attr("content")).toLocalDateTime() }	catch (e) { /* ignore */}
			job.position.name			= job.name
			job.position.workType		= data?.employmentType
			job.position.contractType	= data2?.JobContractTypes ?: data2?.JobTypes ?: browser.find("section.job-summary li.job-type.icon")?.first()?.text()

			job.salary.text				= data2?.JobSalary ?: data?.baseSalary ? "${data?.baseSalary?.value?.value} (${data?.baseSalary?.currency} per ${data ?.baseSalary?.value?.unitText})".replaceAll(/\s*(null)\s*/,"") : null
			job.salary.currency			= data.salaryCurrency

			job.contact		= new Contact("name": browser.find("ul li span.reference", text: browser.iContains("Contact"))?.first()?.parent()?.text()?.replaceAll(/Contact:\s*/,''))

			job.orgTags."${TagType.COMPANY_TYPES}"		= [data2?.CompanyType]
			job.orgTags."${TagType.INDUSTRIES}"			= [data?.industry, extraData.category]
			job.orgTags."${TagType.CATEGORIES}"			= [data2?.Disciplines?.split(/\|/), data2?.SubDisciplines?.split(/\|/)]?.flatten()
			job.orgTags."${TagType.JOBNAMES}"			= [data2?.NormalisedJobTitle]

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location 		= new Location()
			location.source 		= sourceID
			Address address 		= new Address()
			address.source			= sourceID
			address.companyName 	= data2?.CompanyName
			address.addressLine 	= data2?.JoinedLocation?.split(/\|/)?.reverse()?.join(', ')?.replaceAll(/^(.)/, "${data2.City}, \$1")?.replaceAll(/(^[,\s]+|[,\s]+$)/, '')
											?: browser.find("section.job-summary .location")?.text()
											?: data2?.City
			address.countryCode		= data?.jobLocation?.address?.addressCountry?.replace('GB','UK')
			address.county			= data?.jobLocation?.address?.addressRegion
			address.city			= data2?.City			?: data?.jobLocation?.address?.addressLocality
			address.postCode	 	= data2?.JobPostcode	?: data?.jobLocation?.address?.postalCode
			try {
				def parts = data2.JoinedLocation?.split(/\|/)
				address.countryCode = address.countryCode	?: parts?.getAt(0)
				address.county		= address.county		?: parts?.getAt(1)
				address.city		= address.city			?: parts?.getAt(2)
			} catch (e) { /*ignore*/ }
			address.geoPoint		= new GeoPoint(
					"lat":	data?.jobLocation?.geo?.latitude	as Double,
					"lng":	data?.jobLocation?.geo?.longitude	as Double
			)
			location.orgAddress		= address

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company 		= new Company()
			company.source			= sourceID
			company.idInSource		= data2?.CompanyId
			company.name			= data2?.CompanyName
			def companyURL = data?.hiringOrganization?.url ?: browser.find("section.job-summary #companyJobsLink").first()?.attr("href")

			company.urls			= [("$sourceID" as String): companyURL]
			company.ids				= [("$sourceID" as String): companyURL?.replaceAll(/.*jobs-at\/([^\/]+).*/,'$1')]

			/*******************/
			/* Store page data */
			/*******************/

			Document rawPage = new Document()
			rawPage.url			= job.url
			rawPage.html		= browser.driver.pageSource

			browser.driver.close() // close tab with job
			browser.driver.switchTo().window(tabs.get(0)) // switch to main screen (search)
			return crossreferenceAndSaveData(job, location, company, rawPage)
		} catch (HttpStatusException e) {
			log.error "$e for $pageURL"
			println "\n---\n${browser.page()}\n---\n"
			println "\n---\n${browser.getDriver()?.pageSource}\n---\n"
			browser.report "$source-Exception"
		} catch (IOException e) {
			log.error "$e for $pageURL"
			println "\n---\n${browser.page()}\n---\n"
			println "\n---\n${browser.getDriver()?.pageSource}\n---\n"
			browser.report "$source-Exception"
		} catch (NullPointerException e) {
			log.error "$e for $pageURL" // probably a problem with SimpleDateFormat (do not store job)
			println "\n---\n${browser.page()}\n---\n"
			println "\n---\n${browser.getDriver()?.pageSource}\n---\n"
			browser.report "$source-Exception"
		} catch (ShutdownException e) {
			throw new ShutdownException(e.getMessage())
		} catch (e) {
			log.error "$e for $pageURL"
			e.printStackTrace()
			println "\n---\n${browser.page()}\n---\n"
			println "\n---\n${browser.getDriver()?.pageSource}\n---\n"
			browser.report "$source-Exception"
		}
		return false
	}
}
