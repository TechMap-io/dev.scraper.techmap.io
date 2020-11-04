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
            [id: "us", url: "https://www.snagajob.com"]
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

        // NOTE: to load page correctly do not use old method, it returns encoded page that cannot be parsed
        def startPage = loadPageWithParameters("${source.url}", [:])
        final def startCookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

        // The first page doesn't contain groups
        final JsonSlurper jsonSlurper = new JsonSlurper()
        def searchPageScript = startPage.select("script[type=application/ld+json]")?.find({ it.html().contains("SearchAction") })?.html()
        def searchData = jsonSlurper.parseText(searchPageScript)
        def searchLink = (searchData?.potentialAction?.target as String)?.split("/q-")?.first()
        def searchPage = loadPageWithParameters(searchLink, [:])

        // Identify groups of jobs such as categories, industries or Jobnames we can iterate over
        def parametersScript = searchPage.select("script#seeker-state")?.html()?.replaceAll("&q;", "\"")?.replaceAll("&a;", "&")
        def parametersData = jsonSlurper.parseText(parametersScript)
        def groupSearchParams = parametersData?.values()?.find({ it.body.filterGroups })?.body?.filterGroups?.find({ it.name == "Industry" })
        def groups = groupSearchParams?.list?.sort { it?.name } // sort necessary for compare with status.lastCategory

        for (Map group in groups) {
            def status = db.loadStatus(sourceID + "-${group.name}")
            this.cookiesForThread."${Thread.currentThread().getId()}" = startCookies
            // this resource cannot be parsed by old way
            // there are a lot of scripts and json data(with search parameters) to get next links and content
            group += [  searchLink: searchLink,
                        urlParams: [
                            "${groupSearchParams.value}" : group.value]]
            int jobsInCategoryCount = scrapePageGroupWithParameters(group, status)
            jobsInSourceCount += jobsInCategoryCount
            if (maxDocsToPrint <= 0) break
        }
        return jobsInSourceCount
    }

    @Override
    int scrapePageGroup(Element group, Map status) {
        return 0
    }

    int scrapePageGroupWithParameters(Map group, Map status) {
        def startTime = new Date()
        log.debug "... starting to scrape group ${group.name}"

        def nextURL         = group.searchLink
        def paginationPage  = loadPageWithParameters(nextURL, [urlParams: group.urlParams])

        int maxJobsInGroup = (paginationPage?.select("h1.results-heading")?.text()?.replaceAll("\\D", "") ?: 0)?.toInteger()

        int offset = 0
        int jobsInGroupCount = 0
        while (nextURL) {
            // NOTE: this mechanism ready the "next" URL in a pagination - some pages might require different approaches (e.g., increasing a "page" parameter)
            def jobLinks = paginationPage?.select("job-overview[itemprop=itemListElement] meta[itemprop=url]")
            int jobsInJobListCount = scrapePageList(jobLinks, [category: group.name])
            jobsInGroupCount += jobsInJobListCount
            log.debug "... scraped ${"$jobsInJobListCount".padLeft(4)} of ${"$maxJobsInGroup".padLeft(5)} jobs with offset $offset in group ${group.text()}"

            // if (jobsInJobListCount <= 0) break // switch deep scraping (when disabled) and shallow scraping (when enabled)
            if (maxDocsToPrint <= 0) break

            // Get next URL and load page for next iteration
            offset = Math.max(status.lastOffset ?: 0, jobLinks.size() ?: 15)?.toInteger()
            nextURL = paginationPage?.select("a:contains(Next)")?.first()?.absUrl("href")
            if (nextURL) {
                paginationPage = loadPageWithParameters(nextURL, [urlParams: group.urlParams])
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
            def jobPage = loadPageWithParameters(pageURL, [:])
            if (!jobPage) return false

            /*******************************/
            /* Extract data in JSON format */
            /*******************************/

            final JsonSlurper jsonSlurper = new JsonSlurper()
            def dataRaw = jobPage?.select("script")?.find({it.html().contains("JobPosting")})?.html()
            def data = jsonSlurper.parseText(dataRaw ?: "{}")

            /*****************/
            /* Fill Job data */
            /*****************/

            Job job         = new Job()
            job.source      = sourceID
            job.idInSource  = extraData.idInSource ?: jobPage.select("div.posting-details span[class=ng-star-inserted]")?.first()?.text()?.replaceAll("\\D", "")
            job.url         = pageURL ?: data?.url
            job.name        = data?.title ?: jobPage.select("h2.h1")?.first()?.text()

            job.html = jobPage?.select("job-details")?.first()?.html()
            job.text = DataCleaner.stripHTML(job.html)
            job.json = [:]
            if (data) job.json.pageData = data

            try {
                job.dateCreated = ZonedDateTime.parse(data?.datePosted)?.toLocalDateTime()
                job.dateCreated = job.dateCreated ?: LocalDate.parse(jobPage.select("div.posting-details span.divider")?.last()?.text()?.replaceAll("\\D", ""), "yyyyMMdd").atStartOfDay()
            } catch (e) { /*ignore*/ }

            job.position.name           = job.name
            job.position.workType       = jobPage.select("job-details i.icon-time")?.first()?.parent()?.text() ?: data?.employmentType

            job.salary.text             = jobPage.select("#estimatedWage")?.first()?.text()
            job.salary.currency         = data?.estimatedSalary?.get(0)?.currency
            job.salary.value            = data?.estimatedSalary?.get(0)?.minValue
            job.salary.value            = job.salary.value ?: data?.estimatedSalary?.get(0)?.maxValue
            job.salary.period           = data?.estimatedSalary?.get(0)?.unitText

            job.orgTags."${TagType.CATEGORIES}" = (job.orgTags."${TagType.CATEGORIES}" ?: []) + extraData?.category

            /**********************/
            /* Fill Location data */
            /**********************/

            Location location = new Location()
            location.source = sourceID
            location.orgAddress.addressLine     = jobPage.select("job-details i.icon-location")?.first()?.parent()?.parent()?.text()
            location.orgAddress.countryCode     = data?.jobLocation?.address?.addressCountry?.name
            location.orgAddress.state           = data?.jobLocation?.address?.addressRegion
            location.orgAddress.city            = data?.jobLocation?.address?.addressLocality
            location.orgAddress.street          = data?.jobLocation?.address?.streetAddress
            location.orgAddress.postCode        = data?.jobLocation?.address?.postalCode
            location.orgAddress.geoPoint.lat    = data?.jobLocation?.geo?.latitude as Double
            location.orgAddress.geoPoint.lng    = data?.jobLocation?.geo?.longitude as Double

            /*********************/
            /* Fill Company data */
            /*********************/

            Company company     = new Company()
            company.source      = sourceID
            company.name        = data?.hiringOrganization?.name ?: jobPage.select("job-details .company-name")?.first()?.text()

            company.urls = [("$sourceID" as String): companyLink]
            company.ids  = [("$sourceID" as String): company.idInSource] // no company id was found

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
