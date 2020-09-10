package io.techmap.scrape.scraper.webscraper

import groovy.util.logging.Log4j2
import io.techmap.scrape.connectors.MongoDBConnector
import io.techmap.scrape.scraper.AScraper
import org.bson.Document
import org.jsoup.Connection
import org.jsoup.HttpStatusException
import org.jsoup.Jsoup

@Log4j2
abstract class AWebScraper extends AScraper {
	public static MongoDBConnector db = MongoDBConnector.getInstance() // NOTE: needed for payload

	Map cookiesForThread = [:]
	final Integer timeout = 1000 * 60	// use timeout of 60 seconds

	final String userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.123 Safari/537.36"
	String getUserAgent() { return userAgent }

	AWebScraper(ArrayList sources, String baseSourceID) {
		// Load userAgents and set random as default (can be overwritten by Scraper)
	}

	protected Document initScrape() {
		log.info "Start of scraping '${source.url}'"
		def orgStatus = db.loadStatus(sourceID)
		db.saveStatus(orgStatus) // save to signal that this source is already in use (esp. for long running processes/containers)
		return orgStatus
	}

	def loadResponse = { String url ->
		if (Thread.currentThread().isInterrupted()) return null	// Needed to prevent "MongoInterruptedException"???

		def cookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

		def response = Jsoup.connect(url)
				.timeout(timeout)
				.userAgent(userAgent)
				.header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
				.header("Accept-Encoding", "gzip, deflate, br")
				.header("Accept-Language", "en-US,en;q=0.9,de;q=0.8,es;q=0.7")
				.header("Cache-Control", "no-cache") // for Monster.at
				.header("Connection", "keep-alive") // for Monster.at
				.header("Pragma", "keep-alive") // for Monster.at
				.header("Sec-Fetch-Dest", "document")
				.header("Sec-Fetch-Mode", "navigate")
				.header("Sec-Fetch-User", "?1")
				.header("Upgrade-Insecure-Requests", "1")
				.followRedirects(true)
				.ignoreContentType(true)
				.ignoreHttpErrors(true)
		if (url.contains(".indeed.")) {
			response = response.header("Sec-Fetch-Site", "same-origin")	// for indeed
		} else if (url.contains("meinestadt.de")) { // special handing due to many more "404" for meinestadt.de
			response = response.timeout(timeout * 4)
			response = response.header("Sec-Fetch-Site", "none")
			response = response.header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3")
		} else {
			response = response.header("Sec-Fetch-Site", "none")
		}
		if (!url.contains("dice.com"))		response = response.cookies(cookies)

		if (!cookies) response = response.referrer("https://www.google.com/")
		response = response.execute()
		def newCookies = response.cookies()
		if (newCookies) this.cookiesForThread."${Thread.currentThread().getId()}" = (cookies ?: [:]) + newCookies
		return response
	}

	/** For loading a URL and getting a parsed Document **/
	def loadPage = { String url ->
		try {
			if (Thread.currentThread().isInterrupted()) return null	// Needed to prevent "MongoInterruptedException"???
			def response = loadResponse(url)
			def parsedDocument = response?.parse()
			if (response.statusCode() != 200) {
				sleep 3 * 1000 // short wait to take pressure of the target server
				response = loadResponse(url) // retry
				if (response.statusCode() == 200) return response?.parse()

				if (url.contains("stepstone")		&& response.statusCode() == 410) return null // Job is not available anymore
				if (url.contains("stepstone")		&& response.statusCode() == 403) return null // Job is not available anymore

				log.warn "StatusCode ${response.statusCode()} for page at URL $url"
				this.cookiesForThread."${Thread.currentThread().getId()}" = [:] // reset cookies
				return null
			}
			return parsedDocument
		} catch (java.lang.OutOfMemoryError e) {
			log.warn "$e for URL ${url}"
			e.printStackTrace()
		} catch (IllegalArgumentException e) { // url is null
			log.warn "$e for URL ${url}"
		} catch (HttpStatusException e) {
			if (e.statusCode == 410) log.debug "HTTP Status 410 - URL is offline: ${url}"
			else log.warn "$e #1 for $url"
		} catch (java.io.IOException e) {
			if (e.toString().contains("Mark invalid")) log.debug "Mark invalid - URL is offline: ${url}"
			else log.warn "$e #2 for $url"
		} catch (ConnectException e) {
			log.warn "$e for URL ${url}"
		} catch (UnknownHostException e) {
			log.warn "$e for URL ${url} - Sleep for 0"
		} catch (e) {
			log.warn "$e for URL ${url} - Sleep for 0"
		}
		return null
	}

	/** For loading JSON, XML, etc. **/
	def loadParseable = { url, payload ->
		try {
			def cookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

			def response = Jsoup.connect(url)
					.timeout(timeout)
					.ignoreContentType(true)
					.userAgent(userAgent)
					.cookies(cookies)
					.method(Connection.Method.POST)
					.header("Accept", "*/*")
					.header("Accept-Encoding", "gzip, deflate, br")
					.header("Accept-Language", "en-US,en;q=0.9,de;q=0.8,es;q=0.7")
					.header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
					.header("X-Requested-With", "XMLHttpRequest") // For stellenanzeigen.de
					.followRedirects(true)
					.ignoreContentType(true)
					.ignoreHttpErrors(true)
			if (payload) {
				for (load in payload) {
					response = response.data("${load.key}", "${load.value}")
				}
			}
			response = response.execute()
			def newCookies = response.cookies()
			if (newCookies) this.cookiesForThread."${Thread.currentThread().getId()}" += newCookies
			return response?.body()
		} catch (IllegalArgumentException e) {
			log.debug "$e for URL ${url}"
		} catch (ConnectException e) {
			log.warn "$e for URL ${url}"
		} catch (UnknownHostException e) {
			log.warn "$e for URL ${url} - Sleep for 0"
			// sleep 5000 // maybe IP switched?
		} catch (e) {
			log.info "$e for URL ${url}"
		}
		return null
	}

}
