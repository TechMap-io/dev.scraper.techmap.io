package io.techmap.scrape.connectors

import groovy.util.logging.Log4j2
import org.bson.Document
import java.time.LocalDateTime

/** MongoDB connector (Hollowed out)
 * Do use the methods in the scraper class! (as in the example scraper)
 */
@Singleton
@Log4j2
class MongoDBConnector {

	MongoDBConnector init() {
		// NOTE: the method was gutted / is a stub
		return this
	}

	/***********************/
	/* Job related Methods */
	/***********************/

	/** Tests if the job was already scraped (and exists in the DB)
	 **/
	Boolean jobExists(String sourceID, String idInSource) {
		// NOTE: the method was gutted / is a stub
		return false
	}

	/**************************/
	/* Status related Methods */
	/**************************/

	/** Loads the last status as stored in the db in order to pick up if the process / container crashed or was blocked
	 **/
	Document loadStatus(String source) {
		// NOTE: the method was gutted / is a stub
		def status = new Document()
		status.source = source?.toLowerCase()
		return status
	}

	/** Saves the last status in the db in order to pick up if the process / container crashed or was blocked
	 **/
	Document saveStatus(Document status) {
		// NOTE: the method was gutted / is a stub
		status.lastUpdated = LocalDateTime.now()
		return status
	}

}
