
package io.techmap.scrape.scraper.webscraper.companyscraper

import groovy.time.TimeCategory
import groovy.util.logging.Log4j2
import io.techmap.scrape.data.Company
import io.techmap.scrape.data.Location
import io.techmap.scrape.data.shared.Contact
import io.techmap.scrape.data.shared.TagType
import io.techmap.scrape.helpers.ShutdownException
import io.techmap.scrape.scraper.webscraper.AWebScraper
import org.bson.Document
import org.jsoup.HttpStatusException
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import java.text.ParseException

@Log4j2
class DiceCompaniesScraper extends AWebScraper {

	// final String userAgent = super.userAgents?.json?.chrome?.macos?.ua
	// final String userAgent = "Googlebot/2.1 (+http://www.google.com/bot.html)"
	// @Override String getUserAgent() { return userAgent }

	static final ArrayList sources = [
			[id: "us", url: "https://www.dice.com/jobs/browsejobs/q-company-dc-pound-jobs"]
	]
	static final String baseSourceID = 'dice_'
	
	DiceCompaniesScraper(Integer sourceToScrape) {
		super(sources, baseSourceID)
		this.sourceToScrape = sourceToScrape
		this.source = sources[this.sourceToScrape]
		this.sourceID = baseSourceID + this.source.id
		log.info "Using userAgent:                   ${userAgent}"
	}

	@Override
	int scrape() {
		def sourceStatus = super.initScrape()
		def startTime = new Date()

		def startPage = loadPage("${source.url}")
		def groups = startPage?.select("a[href*=q-company-dc]")?.unique{it.attr("href") }?.sort { it.text() } // sort necessary for compare with status.lastCategory
		if (!groups || groups?.size() == 0) {
			log.fatal "Could not find any industries in following page with userAgent $userAgent! \n${startpage?.html()}"
			return -1
		}
		
		Integer companiesInSourceCount = 0
		def threads = []
		for (Element letter in groups) {
			if (sourceStatus.lastCategory > letter.text()) continue // skip to letter of last scrape (if interrupted)
			sourceStatus.lastCategory = letter.text()
			db.saveStatus(sourceStatus)
			
			companiesInSourceCount += scrapePageGroup(letter, null)
		}
		sourceStatus.lastCategory = ""
		db.saveStatus(sourceStatus)

		log.info "End of scraping ${"$companiesInSourceCount".padLeft(5)} companies in " + TimeCategory.minus( new Date() , startTime )
		return companiesInSourceCount
	}

	@Override
	int scrapePageGroup(Element group, Map status) {
		def startTime = new Date()
		log.debug "... starting to scrape group ${group.text()}"
		def nextURL = group.absUrl("href")
		def companyListPage = loadPage(nextURL)
		
		// NOTE: Dice does not use any pagination - but only show Top 50?
		def companyLinks = companyListPage?.select("a[href*=/company/]")
		companiesInIndustryCount = scrapePageList(companyLinks, [industry: group.text()])
		
		log.info "Scraped ${"$companiesInIndustryCount".padLeft(5)} of ${"${companyLinks?.size()}".padLeft(6)} companies in group ${group.text()} in " + TimeCategory.minus( new Date() , startTime )
		return companiesInIndustryCount
	}

	@Override
	int scrapePageList(Elements pageElements, Map extraData) {
		Integer companiesCount = 0
		for (String companyPageURL in pageElements*.absUrl("href")) {
			String idInSource = companyPageURL?.replaceAll(/.*\/company\/([^\?\/]+).*/,'$1')?.trim()
			if (idInSource == "JA202001") continue // "Dice General Jobs Careers"
			if (!db.companyExists(sourceID, idInSource)) {
				extraData.idInSource = idInSource
				extraData.companyPageURL = companyPageURL
				scrapePage(companyPageURL, extraData)
				companiesCount++
			}
		}
		return companiesCount
	}

	@Override
	boolean scrapePage(String pageURL, Map extraData) {
		try {
			def companyPage = loadPage(pageURL)
			if (!companyPage) return false

			Location location = new Location()
			location.source = sourceID
			location.orgAddress.source		= sourceID
			location.orgAddress.addressLine	= companyPage?.select(".details .location")?.first()?.text()?.trim()

			Company company = new Company()
			company.source		= sourceID
			company.idInSource	= extraData.idInSource
			company.name		= companyPage?.select(".details .employer")?.first()?.text()?.trim() ?: companyPage?.select("h1.company-name")?.first()?.text()?.trim()
			company.description	= companyPage?.select(".company-overview")?.first()?.text()?.trim()
			company.urls		= [("$sourceID" as String): extraData.companyPageURL]
			company.ids			= [("$sourceID" as String): extraData.idInSource]
			
			company.url					= companyPage?.select("a[target=_blank]:Contains(Company Website)")?.first()?.attr("href")?.trim()
			company.url					= company.url ?: companyPage?.select('a[target=_blank]:Matches(^Website$)')?.first()?.attr("href")?.trim()
			company.url					= company.url ?: companyPage?.select('a[target=_blank]:Matches(^Home.?page$)')?.first()?.attr("href")?.trim()
			company.info.careerpageURL	= companyPage?.select("a[target=_blank]:Contains(Careers)")?.first()?.attr("href")?.trim()
			company.info.careerpageURL	= company.info.careerpageURL ?: companyPage?.select("a[target=_blank]:Contains(Career Website)")?.first()?.attr("href")?.trim()
			company.info.careerpageURL	= company.info.careerpageURL ?: companyPage?.select("a[target=_blank]:Contains(Job openings)")?.first()?.attr("href")?.trim()
			company.info.careerpageURL	= company.info.careerpageURL ?: companyPage?.select("a[target=_blank]:Contains(Job Postings)")?.first()?.attr("href")?.trim()
			company.info.facebookURL	= companyPage?.select("a[target=_blank]:Contains(Facebook)")?.first()?.attr("href")?.trim()
			company.info.linkedinURL	= companyPage?.select("a[target=_blank]:Contains(Linkedin)")?.first()?.attr("href")?.trim()
			company.info.twitterURL		= companyPage?.select("a[target=_blank]:Contains(Twitter)")?.first()?.attr("href")?.trim()

			company.info.companySize	= companyPage?.select("a[target=_blank]:Contains(Employees)")?.first()?.text()?.replaceAll(/\(\d{4}\)/,'')?.trim()

			Document rawPage = new Document()
			rawPage.category	= "companies"
			rawPage.url			= extraData.companyPageURL
			rawPage.html		= companyPage.html()

			return crossreferenceAndSaveData(null, location, company, rawPage)
		} catch (HttpStatusException e) {
			log.error "$e"
		} catch (ParseException e) {
			log.error "$e"
		} catch (IOException e) {
			log.error "$e"
		} catch (ShutdownException e) {
			throw new ShutdownException(e.getMessage())
		} catch (e) {
			e.printStackTrace()
		}
		return false
	}
}
