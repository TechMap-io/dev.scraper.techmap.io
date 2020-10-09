/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.scraper.webscraper

import groovy.util.logging.Log4j2
import io.techmap.scrape.connectors.MongoDBConnector
import io.techmap.scrape.scraper.AScraper
import org.bson.Document
import org.jsoup.Connection
import org.jsoup.HttpStatusException
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

@Log4j2
abstract class AWebScraper extends AScraper {
	static MongoDBConnector db = MongoDBConnector.getInstance()

	Map cookiesForThread = [:]	// used to handle cookies in multiple threads
	static final Integer	TIMEOUT		= 1000 * 60	// 60 seconds
	static String			USER_AGENT	= "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.123 Safari/537.36"

	AWebScraper(ArrayList sources, String baseSourceID) {
	}

	/********************/
	/* Abstract Methods */
	/********************/

	/** The main entry point to start the scraping process */
	abstract protected int scrape()

	/** Method to handle a group of pages such as catgories, industries, or letters */
	abstract protected int scrapePageGroup(Element group, Map status)

	/** Method to loop over a list of pages on a paginated list of jobs */
	abstract protected int scrapePageList(Elements jobElements, Map extraData)

	/** Method to scrape one specific page identified by its URL */
	abstract protected boolean scrapePage(String jobPageURL, Map extraData)

	/*********************************************/
	/* Helper Methods for all Web-based Scrapers */
	/*********************************************/

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
				.timeout(TIMEOUT)
				.userAgent(USER_AGENT)
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
			response = response.timeout(TIMEOUT * 4)
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
					.timeout(TIMEOUT)
					.ignoreContentType(true)
					.userAgent(USER_AGENT)
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

	/** For loading a URL with parameters and getting a parsed Document **/
	def loadPageWithParameters = { String url, Map params ->
		try {
			if (Thread.currentThread().isInterrupted()) return null	// Needed to prevent "MongoInterruptedException"???
			def response = loadResponseWithParameters(url, params)
			def parsedDocument = response?.parse()
			if (response.statusCode() != 200) {
				sleep 3 * 1000 // short wait to take pressure of the target server
				response = loadResponseWithParameters(url, params) // retry
				if (response.statusCode() == 200) return response?.parse()
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

	def loadResponseWithParameters = { String url, Map params ->
		if (Thread.currentThread().isInterrupted()) return null	// Needed to prevent "MongoInterruptedException"???

		def cookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

		def response = Jsoup.connect(url)
				.timeout(TIMEOUT)
				.userAgent(USER_AGENT)
				.followRedirects(true)
				.ignoreContentType(true)
				.ignoreHttpErrors(true)
		if (params?.headers) response.headers(params.headers as Map)
		if (!cookies) response = response.referrer("https://www.google.com/")
		response = response.execute()
		def newCookies = response.cookies()
		if (newCookies) this.cookiesForThread."${Thread.currentThread().getId()}" = (cookies ?: [:]) + newCookies
		return response
	}

}
