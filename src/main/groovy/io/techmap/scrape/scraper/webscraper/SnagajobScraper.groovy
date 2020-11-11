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

import java.time.LocalDate
import java.time.ZonedDateTime

@Log4j2
class SnagajobScraper extends AWebScraper {

    /**
     * List of websites with different TLDs but the same page structure
     * Normally the oldest source will be selected - in this test environment only the first is used
     **/
    static final ArrayList sources = [
            [id: "us", url: "https://www.snagajob.com/jobs-by/industry"]
    ]
    static final String baseSourceID = 'snagajob_'

    SnagajobScraper(Integer sourceToScrape) {
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
	
		def groups = startPage.select(".industries > div > a")?.sort { it.text() } // sort necessary for compare with status.lastCategory

        for (group in groups) {
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

        int maxJobsInGroup = (paginationPage?.select("h1.results-heading")?.text()?.replaceAll("\\D", "") ?: 0)?.toInteger()

        int offset = 0
        int jobsInGroupCount = 0
        while (nextURL) {
            // NOTE: this mechanism ready the "next" URL in a pagination - some pages might require different approaches (e.g., increasing a "page" parameter)
            def jobLinks = paginationPage?.select("job-overview[itemprop=itemListElement] meta[itemprop=url]")
            int jobsInJobListCount = scrapePageList(jobLinks, [category: group.text()])
            jobsInGroupCount += jobsInJobListCount
            log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"

            // if (jobsInJobListCount <= 0) break // switch deep scraping (when disabled) and shallow scraping (when enabled)
            if (maxDocsToPrint <= 0) break

            // Get next URL and load page for next iteration
            offset = Math.max(status.lastOffset ?: 0, jobLinks.size() ?: 15)?.toInteger()
            nextURL = paginationPage?.select("noscript a:Contains(Next)")?.first()?.absUrl("href")
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
            String jobPageURL = pageElement?.absUrl("content")
            String idInSource = jobPageURL?.split("/")?.last()
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

            final JsonSlurper jsonSlurper = new JsonSlurper()
            def dataRaw = jobPage?.select("script")?.find({it.html().contains("JobPosting")})?.html()
            def data = jsonSlurper.parseText(dataRaw ?: "{}")
			def dataRaw2 = jobPage?.select("script#seeker-state")?.first()?.html()?.replaceAll(/\&q;/,'"')
			def data2 = jsonSlurper.parseText(dataRaw2 ?: "{}").entrySet().iterator().next().getValue().body
			if (data2?.isHoneypot) {
				sleep 1	// WARN: looks like this is a Honeypot to identify scrapers - but the flag is only visible after loading and snagajob blocks after ~20 pages (but unblocks after 19 blocka ???)
			}
			
            /*****************/
            /* Fill Job data */
            /*****************/

            Job job         = new Job()
            job.source      = sourceID
            job.idInSource  = extraData.idInSource ?: jobPage.select("div.posting-details span[class=ng-star-inserted]")?.first()?.text()?.replaceAll("\\D", "")
            job.url         = pageURL ?: data?.url
            job.name        = data?.title ?: data2?.title ?: jobPage.select("h2.h1")?.first()?.text()

            job.html = jobPage?.select("jobs-description")?.first()?.html() ?: data2.description ?: data2.postingOverview
            job.text = DataCleaner.stripHTML(job.html)
            job.json = [:]
            if (data)	job.json.schemaOrg	= data
			if (data2)	job.json.pageData	= data2
			
            try { job.dateCreated = ZonedDateTime.parse(data?.datePosted)?.toLocalDateTime() } catch (e) { /*ignore*/ }
			try { job.dateCreated = job.dateCreated ?: ZonedDateTime.parse(data2?.internalCreateDate)?.toLocalDateTime()} catch (e) { /*ignore*/ }
			try { job.dateCreated = job.dateCreated ?: LocalDate.parse(jobPage.select("div.posting-details span.divider")?.last()?.text()?.replaceAll("\\D", ""), "yyyyMMdd").atStartOfDay() } catch (e) { /*ignore*/ }
			try { job.datesUpdated += ZonedDateTime.parse(data2?.internalUpdateDate)?.toLocalDateTime()} catch (e) { /*ignore*/ }

            job.position.name           = job.name
            job.position.workType       = jobPage.select("job-details i.icon-time")?.first()?.parent()?.text() ?: data?.employmentType

            job.salary.text             = data2?.wage?.text // NOTE: estimates are too dangerous
            job.salary.currency         = data2?.wage?.currency ?: data?.estimatedSalary?.get(0)?.currency
            job.salary.value            = data2?.wage?.value	?: data2?.wage?.median
			job.salary.value            = job.salary.value		?: data2?.wage?.minimum ?: data?.estimatedSalary?.get(0)?.minValue
			job.salary.value            = job.salary.value		?: data2?.wage?.maximum ?: data?.estimatedSalary?.get(0)?.maxValue
            job.salary.period           = data?.estimatedSalary?.get(0)?.unitText
	
			job.orgTags."${TagType.CATEGORIES}"	= (job.orgTags."${TagType.CATEGORIES}" ?: [])	+ extraData?.category
			job.orgTags."${TagType.INDUSTRIES}"	= (job.orgTags."${TagType.INDUSTRIES}" ?: [])	+ data2?.primaryIndustryName
			job.orgTags."${TagType.INDUSTRIES}"	= (job.orgTags."${TagType.INDUSTRIES}" ?: [])	+ data2?.industries
			job.orgTags."${TagType.JOBNAMES}"	= (job.orgTags."${TagType.JOBNAMES}" ?: [])		+ data2?.titleNormalized
			job.orgTags."${TagType.JOBNAMES}"	= (job.orgTags."${TagType.JOBNAMES}" ?: [])		+ data2?.seoJobTitle
			job.orgTags."${TagType.COMPANY_BENEFITS}"	= (job.orgTags."${TagType.COMPANY_BENEFITS}" ?: [])		+ data2?.qualifications?.jobBenefits
			job.orgTags."${TagType.QUALIFICATIONS}"		= (job.orgTags."${TagType.QUALIFICATIONS}" ?: [])		+ data2?.qualifications?.jobRequirements
			job.orgTags."${TagType.KEYWORDS}"		= (job.orgTags."${TagType.KEYWORDS}" ?: [])		+ data2?.fextures*.name // NOTE: "fextures" is not a misspelling!

            /**********************/
            /* Fill Location data */
            /**********************/

            Location location = new Location()
            location.source = sourceID
            location.orgAddress.addressLine     = jobPage.select("job-details i.icon-location")?.first()?.parent()?.parent()?.text() ?: data2?.location?.locationName
            location.orgAddress.countryCode     = data?.jobLocation?.address?.addressCountry?.name
            location.orgAddress.state           = data?.jobLocation?.address?.addressRegion		?: data2?.location?.stateProvince ?: data2?.location?.stateProvinceCode
            location.orgAddress.city            = data?.jobLocation?.address?.addressLocality	?: data2?.location?.city
            location.orgAddress.street          = data?.jobLocation?.address?.streetAddress
			location.orgAddress.postCode        = data?.jobLocation?.address?.postalCode		?: data2?.location?.postalCode
			location.orgAddress.street        	= data2?.location?.addressLine1
            location.orgAddress.geoPoint.lat    = (data?.jobLocation?.geo?.latitude				?: data2?.latitude)		as Double
            location.orgAddress.geoPoint.lng    = (data?.jobLocation?.geo?.longitude			?: data2?.longitude)	as Double

            /*********************/
            /* Fill Company data */
            /*********************/

            Company company     = new Company()
            company.source      = sourceID
			company.url        	= data?.hiringOrganization?.url
			company.name        = data?.hiringOrganization?.name ?: jobPage.select("job-details .company-name")?.first()?.text()
			def companyLink = jobPage.select("job-details .company-name")?.first()?.parent()?.absUrl("href")
            company.urls = [("$sourceID" as String): companyLink]
            company.ids  = [("$sourceID" as String): companyLink?.replaceAll(/.*\/company\/([^\/\?]+)/,'$1')]

            /*******************/
            /* Store page data */
            /*******************/

            Document rawPage = new Document()
            rawPage.url = job.url
            rawPage.html = jobPage.html()

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
