/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.scraper

import groovy.json.JsonBuilder
import groovy.time.TimeCategory
import groovy.util.logging.Log4j2
import io.techmap.scrape.data.Company
import io.techmap.scrape.data.Job
import io.techmap.scrape.data.Location
import io.techmap.scrape.helpers.ShutdownException
import io.techmap.scrape.scraper.webscraper.AWebScraper
import org.bson.Document

@Log4j2
abstract class AScraper {

	static int jobsInAllSourcesCount = 0

	static Integer maxDocsToPrint = 10

	Integer sourceToScrape
	Map source
	String sourceID

	/**************************/
	/* Thread-related Methods */
	/**************************/

	static void startOne(Class<? extends AScraper> scraper, Integer sourceToScrape) {
		def startTime = new Date()

		def myRunnable = scraper.newInstance(sourceToScrape)
		try {
			scraper.jobsInAllSourcesCount = myRunnable.scrape()
		} catch (ShutdownException e) {
			log.debug e.getMessage()
		}
		if (myRunnable instanceof AWebScraper) {
			def webscraper = (AWebScraper) myRunnable
			if (webscraper.browser) {
				try { webscraper.browser.close() } catch(e) { /* ignore */ }
			}
		}

		log.info "End of scraping ${scraper.jobsInAllSourcesCount?.toString()?.padLeft(5)} jobs in source ${myRunnable.sourceID} within " + TimeCategory.minus( new Date() , startTime )
	}

	/******************/
	/* Helper Methods */
	/******************/

	static boolean crossreferenceAndSaveData(Job job, Location location, Company company, Document rawPage) {
		if (location && company)	location.name		= company.name
		if (location && job){
			location.contacts	= (location.contacts ?: []) + job.contact
			job.orgAddress		= location.orgAddress
		}
		if (company && job){
			job.orgCompany		= company
		}

		if (company  && !company.isValid())		return false
		if (location && !location.isValid())	return false
		if (job		 && !job.isValid())			return false

		// Print JSON lines to check output
		println new JsonBuilder( job )//.toPrettyString()
		// println new JsonBuilder( location )//.toPrettyString()
		// println new JsonBuilder( company )//.toPrettyString()
		if (--maxDocsToPrint <= 0) throw new ShutdownException("Reached maxDocsToPrint: shutting down system.")

		// NOTE: rawPage will be send to AWS S3 (sourcecode not included) - it needs the fields html and url
		if (!rawPage.html || !rawPage.url) log.error "RawPage is missing essential data!"

		return true
	}
}
