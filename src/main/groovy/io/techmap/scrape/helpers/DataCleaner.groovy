package io.techmap.scrape.helpers

import groovy.util.logging.Log4j2
import org.jsoup.Jsoup

@Log4j2
class DataCleaner {

	/******************/
	/* Helper Methods */
	/******************/

	static String stripHTML(String input) {
		if (!input) return ""
		return Jsoup.parse("<div>$input</div>")?.text()?.trim()
	}

}
