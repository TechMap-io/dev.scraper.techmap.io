package io.techmap.scrape.scraper

import groovy.json.JsonBuilder
import groovy.util.logging.Log4j2
import io.techmap.scrape.data.Company
import io.techmap.scrape.data.Job
import io.techmap.scrape.data.Location

import org.bson.Document

@Log4j2
abstract class AScraper {

	static int jobsInAllSourcesCount = 0

	static Integer maxDocsToPrint = 30

	Integer sourceToScrape
	Map source
	String sourceID

	/**************************/
	/* Thread-related Methods */
	/**************************/

	static String startOne(Class<? extends AScraper> scraper, Integer sourceToScrape) {
		def myRunnable = scraper.newInstance(sourceToScrape)

		scraper.jobsInAllSourcesCount = myRunnable.scrape()

		return myRunnable.sourceID
	}

	/******************/
	/* Helper Methods */
	/******************/

	boolean crossreferenceAndSaveData(Job job, Location location, Company company, Document rawPage) {
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
		maxDocsToPrint--

		// NOTE: rawPage will be send to AWS S3 (sourcecode not included) - it needs the fields html and url
		if (!rawPage.html || !rawPage.url) log.error "RawPage is missing essential data!"

		return true
	}
}
