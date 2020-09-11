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
class StepstoneScraper extends AWebScraper {

	/**
	 * List of websites with different TLDs but the same page structure
	 * Normally the oldest source will be selected - in this test environment only the first is used
	 **/
	static final ArrayList sources = [
			[id: "nl", url: "https://www.stepstone.nl/en"],
			[id: "be", url: "https://www.stepstone.be/en"],
			[id: "de", url: "https://www.stepstone.de"],
			[id: "at", url: "https://www.stepstone.at"],
			[id: "fr", url: "https://www.stepstone.fr"],
			[id: "pl", url: "https://www.stepstone.pl"]
	]
	static final String baseSourceID = 'stepstone_'

	StepstoneScraper(Integer sourceToScrape) {
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
		def groups = startPage?.select("#jobsbycategory ul li a")?.sort { it.text() } // sort necessary for compare with status.lastCategory

		for (Element group in groups) {
			def status = db.loadStatus(sourceID+"-${group.text()}")
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

		int maxJobsInGroup = (paginationPage?.select(".at-resultstitle > span > strong > b > strong")?.text() ?: 0)?.toInteger()

		int offset = 0
		int jobsInGroupCount = 0
		while (nextURL) {
			// NOTE: this mechanism ready the "next" URL in a pagination - some pages might require different approaches (e.g., increasing a "page" parameter)
			def jobLinks = paginationPage?.select(".job-elements-list .job-element a.job-element__url,article[id^=job-item] a[target=_blank]")
			int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
			jobsInGroupCount += jobsInJobListCount
			log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"

			// if (jobsInJobListCount <= 0) break // switch deep scraping (when disabled) and shallow scraping (when enabled)
			if (maxDocsToPrint <= 0) break

			// Get next URL and load page for next iteration
			def nextPageButtonURL = paginationPage?.select(".hidden-xs .pagination-resultlist .pagination-resultlist__btn_next a, a[title=Next], a[aria-label='next page']")?.getAt(0)?.absUrl("href")
			offset = Math.max(status.lastOffset ?: 0, (nextPageButtonURL?.replaceAll(/.*[\?&]of=(\d+)&.*/, '$1') ?: 25)?.toInteger())
			nextURL = nextPageButtonURL?.replaceAll(/&li=(\d+)/, '')?.replaceAll(/&of=(\d+)&/, "&of=$offset&li=100&")
			if (nextURL) {
				paginationPage = loadPage(nextURL)
				status.lastOffset = offset
				db.saveStatus(status)
			}
		}
		status.lastOffset = 0 // Reset
		db.saveStatus(status)
		log.info "Scraped ${"$jobsInGroupCount".padLeft(5)} of ${"$maxJobsInGroup".padLeft(6)} jobs in group ${group.text()} in " + TimeCategory.minus( new Date() , startTime )
		return jobsInGroupCount
	}

	@Override
	int scrapePageList(Elements pageElements, Map extraData) {
		int jobsInPageCount = 0
		for (pageElement in pageElements) {
			String jobPageURL = pageElement?.absUrl("href")
			String idInSource = jobPageURL?.replaceAll(/.*-([^\-\/\&]+)-inline.*$/,'$1')?.trim()
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
			def dataRaw		= jobPage?.select("script")*.html().find { it.contains("var utag_data") }?.replaceAll(/(?s).*var utag_data = ([^;]+);/, '$1')?.replaceAll(/(?s)\s+/, ' ')
			def data		= jsonSlurper.parseText(dataRaw ?: "{}")
			def dataRaw2	= jobPage?.select("script")*.html().find {	it.contains("LocationOnlyMapBlock") }?.replaceAll(/(?s).*LocationOnlyMapBlock = ([^;]+);/, '$1')?.replaceAll(/(?s)\s+/, ' ')
			def data2		= jsonSlurper.parseText(dataRaw2 ?: "{}")
			def dataRaw2b	= jobPage?.select("script")*.html().find {	it.contains("LocationWithCommuteTimeBlock") }?.replaceAll(/(?s).*LocationWithCommuteTimeBlock = ([^;]+);/, '$1')?.replaceAll(/(?s)\s+/, ' ')
			data2 = data2 ?: jsonSlurper.parseText(dataRaw2b ?: "{}")
			def data3
			try { data3		= jsonSlurper.parseText(jobPage?.select(".js-sticky-bar")*.attr("io.techmap.scrape.data-apply-button") ?: "{}") } catch(e) { /*ignore*/ }
			def dataRaw4	= jobPage?.select("script#js-section-preloaded-HeaderStepStoneBlock")?.first()?.html()?.replaceAll(/(?s).*PRELOADED_STATE__.HeaderStepStoneBlock = ([^;]+);/, '$1')?.replaceAll(/(?s)\s+/, ' ')
			def data4		= jsonSlurper.parseText(dataRaw4 ?: "{}")
			def dataRaw5	= jobPage?.select("script")*.html().find { it.contains("var emailLinkApplyButtonConfig") }?.replaceAll(/(?s).*var emailLinkApplyButtonConfig = ([^;]+);/, '$1')?.replaceAll(/(?s)\s+/, ' ')
			def data5		= jsonSlurper.parseText(dataRaw5 ?: "{}")

			/*****************/
			/* Fill Job data */
			/*****************/

			Job job = new Job()
			job.source		= sourceID
			job.idInSource	= extraData.idInSource ?: data?.listing__listing_id ?: data4?.listingData?.id ?: data5?.listingId
			job.url			= pageURL ?: jobPage?.select("link[rel=canonical]")?.first()?.attr("href")
			job.name		= data?.listing__title?.trim() ?: data4?.listingData?.title

			job.locale		= data5?.locale ?: data3?.locale
			job.locale		= job.locale ?: (data?.page__language && data?.page__country) ? "${data?.page__language?.toLowerCase()}_${data?.page__country?.toUpperCase()}" : ""
			job.locale		= job.locale ?: (data4?.language && data4.country) ? "${data4?.language?.toLowerCase()}_${data4?.country?.toUpperCase()}" : ""
			job.locale		= job.locale ?: data2?.language
			if (job.locale?.size() == 2) job.locale = "${job.locale}_${this.source.id?.toUpperCase()}"

			job.html		= jobPage?.select(".listing-content div[class*=ContentBlock]")?.first()?.html()
			job.text		= DataCleaner.stripHTML(job.html)
			job.json		= [:] // NOTE: stepstone does not use schemaOrg!!!
			if (data)	job.json.pageData	= data
			if (data2)	job.json.pageData2	= data2
			if (data3)	job.json.pageData3	= data3
			if (data4)	job.json.pageData4	= data4
			if (data5)	job.json.pageData5	= data5

			try { job.dateCreated	= ZonedDateTime.parse(data4?.listingData?.metaData?.onlineDate)?.toLocalDateTime() 	} catch(e) { /*ignore*/ }
			try { job.dateCreated	= job.dateCreated ?: ZonedDateTime.parse(jobPage?.select("[class*=at-listing__list] time")?.attr("datatime")).toLocalDateTime()	} catch(e) { /*ignore*/ }
			try { job.dateCreated	= job.dateCreated ?: LocalDate.parse(jobPage?.select("span[itemprop=datePosted]")?.first()?.text()).atStartOfDay()	} catch(e) { /*ignore*/ }

			job.position.name			= job.name
			job.position.workType		= jobPage?.select(".at-listing__list-icons_work-type").text()		?: data4?.listingData?.metaData?.workType
			job.position.contractType	= jobPage?.select(".at-listing__list-icons_contract-type").text()	?: data4?.listingData?.metaData?.contractType

			job.orgTags."${TagType.CATEGORIES}"	= (job.orgTags."${TagType.CATEGORIES}"		?: []) + extraData?.category
			jobPage?.select("#company-hub-block-companyCard section ol > li")*.text()*.split(",")?.flatten()*.trim()?.unique()?.minus("")?.minus(null)?.each {
				job.orgTags."${TagType.INDUSTRIES}" = (job.orgTags."${TagType.INDUSTRIES}" ?: []) + it
			}
			jsonSlurper.parseText(jobPage?.select("div[io.techmap.scrape.data-block=app-benefitsForListing]")?.first()?.attr("io.techmap.scrape.data-initialdata") ?: "{}")?.benefits*.benefitName?.unique()?.each {
				job.orgTags."${TagType.COMPANY_BENEFITS}" = (job.orgTags."${TagType.COMPANY_BENEFITS}" ?: []) + it
			}

			/**********************/
			/* Fill Location data */
			/**********************/

			Location location	= new Location()
			location.source		= sourceID
			try {
				location.orgAddress.addressLine = data2?.addressText ?: data4?.listingData?.metaData?.location
				location.orgAddress.addressLine = location.orgAddress.addressLine ?: jobPage.select("footer section address")?.first()?.ownText()
			} catch (e) { }
			location.orgAddress.city = data?.listing__location_name ?: data4?.listingData?.metaData?.location
			try {
				def addressParts = location.orgAddress.addressLine?.split(",")*.trim()
				if (addressParts.size() >= 2) {
					location.orgAddress.country	= location.orgAddress.country	?: addressParts?.getAt(-1)
					location.orgAddress.city	= location.orgAddress.city		?: addressParts?.getAt(-2)
					location.orgAddress.street	= location.orgAddress.street	?: addressParts?.getAt(-3)
				}
			} catch (e) { }
			location.orgAddress.countryCode		= location.orgAddress.country ? "" : this.source.id // WARN: does not work for stepstone.de --> includes at and ch
			// location.orgAddress.country			= location.orgAddress.country		?: this.source.id
			location.orgAddress.postCode		= data?.listing__zipcode ?: location.orgAddress.city?.find(/\d{4,5}/) // Austria 4, Germany 5 ???
			location.orgAddress.city			= location.orgAddress.postCode ? location.orgAddress.city?.minus(location.orgAddress.postCode)?.trim() : location.orgAddress.city
			location.orgAddress.geoPoint.lat	= data2?.latitude as Double
			location.orgAddress.geoPoint.lng	= data2?.longitude as Double

			/*********************/
			/* Fill Company data */
			/*********************/

			Company company		= new Company()
			company.source		= sourceID
			company.idInSource	= data?.listing__company_id ?: data4?.companyData?.id ?: data5?.companyId
			company.name		= data2?.addressHead ?: data4?.companyData?.name
			company.name		= company.name ?: jobPage?.select(".listing__top-info .listing__company-name")?.text() ?: data4?.companyData?.name ?: data2?.addressHead
			def companyLink		= data4?.companyData?.companyPageURL?.replaceAll(/(\/cmp\/..\/[^\/]+).*/, '$1')
			companyLink			= companyLink ?: jobPage?.select("a.at-header-company-logo[href*=/cmp/]")?.first()?.absUrl("href")?.replaceAll(/(\/cmp\/..\/[^\/]+).*/, '$1')
			companyLink			= companyLink ?: jobPage?.select("a.at-company-hub-link")?.first()?.absUrl("href")?.replaceAll(/\/?\?.*/, '')
			companyLink			= companyLink ?: jobPage?.select(".listing__top-info .listing__company-name a")?.first()?.absUrl("href")?.replaceAll(/\/?\?.*/, '')
			// NOTE: "jobs.html" (de) must be removed but "work.html" (be) should not be removed !?
			companyLink			= companyLink?.replaceAll(/\/jobs.html$/,'')
			company.urls		= [("$sourceID" as String): companyLink]
			company.ids			= [("$sourceID" as String): company.idInSource]
			if (company.name?.find(/(?i)Jobs via StepStone/)) return false // job is from another Stepstone portal
			if (company.idInSource == "anonymous") return false // job is from anonymous company

			/*******************/
			/* Store page data */
			/*******************/

			Document rawPage = new Document()
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
