/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.scraper.webscraper

import geb.Browser
import groovy.util.logging.Log4j2
import io.techmap.scrape.connectors.MongoDBConnector
import io.techmap.scrape.scraper.AScraper
import org.bson.Document
import org.jsoup.Connection
import org.jsoup.HttpStatusException
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import org.openqa.selenium.chrome.ChromeDriver
import org.openqa.selenium.chrome.ChromeDriverService
import org.openqa.selenium.chrome.ChromeOptions
import java.util.logging.Level
import java.util.logging.Logger
import java.util.concurrent.TimeUnit

@Log4j2
abstract class AWebScraper extends AScraper {
	static MongoDBConnector db = MongoDBConnector.getInstance()

	Map cookiesForThread = [:]	// used to handle cookies in multiple threads
	static final Integer	TIMEOUT		= 1000 * 60	// 60 seconds
	static String			USER_AGENT	= "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.123 Safari/537.36"
	String getUserAgent()	{ return USER_AGENT }
	
	Browser			browser
	ChromeDriver 	driver // Chromedriver - only initiated if needed
	ChromeOptions	chromeOptions
	static Boolean	headless		= false // false true
	static Random	random			= new Random()
	
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
		
		Map<String, String> cookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

		def response = Jsoup.connect(url)
				.timeout(TIMEOUT)
				.userAgent(USER_AGENT)
				.cookies(cookies)
				.header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
				.header("Accept-Language", "en-US,en;q=0.9,de;q=0.8,es;q=0.7")
				.header("Cache-Control", "no-cache")
				.header("Connection", "keep-alive")
				.header("Pragma", "keep-alive")
				.header("Sec-Fetch-Dest", "document")
				.header("Sec-Fetch-Mode", "navigate")
				.header("Sec-Fetch-User", "?1")
				.header("Upgrade-Insecure-Requests", "1")
				.followRedirects(true)
				.ignoreContentType(true)
				.ignoreHttpErrors(true)
		if (url.contains(".indeed.")) {
			response = response.header("Sec-Fetch-Site", "same-origin")
			sleep 2000
		} else {
			response = response.header("Sec-Fetch-Site", "none")
		}

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
		} catch (OutOfMemoryError e) {
			log.warn "$e for URL ${url}"
			e.printStackTrace()
		} catch (IllegalArgumentException e) { // url is null
			log.warn "$e for URL ${url}"
		} catch (HttpStatusException e) {
			if (e.statusCode == 410) log.debug "HTTP Status 410 - URL is offline: ${url}"
			else log.warn "$e #1 for $url"
		} catch (IOException e) {
			if (e.toString().contains("Mark invalid")) log.debug "Mark invalid - URL is offline: ${url}"
			else log.warn "$e #2 for $url"
		} catch (e) {
			log.warn "$e for URL ${url} - Sleep for 0"
		}
		return null
	}

	/** For loading JSON, XML, etc. **/
	def loadParseable = { url, payload ->
		try {
			Map<String, String> cookies = this.cookiesForThread."${Thread.currentThread().getId()}" ?: [:]

			def response = Jsoup.connect(url)
					.timeout(TIMEOUT)
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
	
	/**********************************/
	/* Geb & Selenium related Section */
	/**********************************/
	
	def simulateHumanDelay = { Integer avgTime = 1000
		long choosenTime = ((0.75 + random.nextInt(50) / 100) * avgTime)
		Thread.sleep(choosenTime) // wait/sleep avgTime +/- 25%
	}
	
	protected Browser loadBrowser() {
		if (this.browser) return this.browser
		
		// create driver if not exists
		def driver = getSeleniumDriver()
		
		this.browser = new Browser(driver: driver)
		return this.browser
	}
	
	private ChromeDriver getSeleniumDriver(String proxy = "") {
		// Disable webdriver & selenium logs
		System.setProperty(ChromeDriverService.CHROME_DRIVER_SILENT_OUTPUT_PROPERTY, "true")
		System.setProperty("webdriver.chrome.silentOutput", "true")
		Logger.getLogger("org.openqa.selenium").setLevel(Level.OFF)
		Logger.getLogger("org.openqa.selenium.WebDriverException").setLevel(Level.OFF)
		Logger.getLogger("org.openqa.selenium.NoSuchSessionException").setLevel(Level.OFF)
		
		log.info "Running on OS                     '${System.getProperty("os.name")}'"
		log.info "Running on OS Architecture        '${System.getProperty("os.arch")}'"
		System.setProperty("geb.build.reportsDir",	"/app/screenshots")
		switch(System.getProperty("os.name")) {
			case ~/(?i).*mac.*/:
				System.setProperty("webdriver.chrome.driver",	"driver/chromedriver_mac64")
				System.setProperty("geb.build.reportsDir",		"screenshots")
				break
			case ~/(?i).*nux.*/:	System.setProperty("webdriver.chrome.driver", "/app/chromedriver");				break	// For use in Docker container
			case ~/(?i).*nix.*/:	System.setProperty("webdriver.chrome.driver", "driver/chromedriver_unix64");	break
			case ~/(?i).*win.*/:	System.setProperty("webdriver.chrome.driver", "driver/chromedriver_win32");		break
			default:				System.setProperty("webdriver.chrome.driver", "chromedriver")
		}
		log.info "Using chromedriver at             '${System.getProperty("webdriver.chrome.driver")}'"
		log.info "Saving screenshots to             '${System.getProperty("geb.build.reportsDir")}'"
		
		// Init chromedriver
		this.chromeOptions = new ChromeOptions()
		this.chromeOptions.addArguments("--window-size=1440,1080")
		this.chromeOptions.addArguments("--ignore-certificate-errors")
		if (headless) {
			// Headless based on https://www.scrapingbee.com/blog/introduction-to-chrome-headless/
			this.chromeOptions.addArguments("--headless")
			this.chromeOptions.addArguments("--disable-gpu")
			this.chromeOptions.addArguments("--no-sandbox")					// for Docker! (not necessary on Mac)
			this.chromeOptions.addArguments("--disable-dev-shm-usage")		// for Docker! (not necessary on Mac)
			this.chromeOptions.addArguments("--whitelisted-ips")			// for Docker! (not necessary on Mac)
			log.debug "Using Chrome                      headless"
		} else {
			log.debug "Using Chrome                      NOT headless"
		}
		if (proxy) {
			this.chromeOptions.addArguments("--proxy-server=$proxy")
			log.info "Trying proxy:                     $proxy"
		}
		this.driver = new ChromeDriver(this.chromeOptions)
		// Change timeouts to reduce "Timed out receiving message from renderer" and "screenshot failed, retrying timeout: Timed out receiving message from renderer"
		this.driver.manage().timeouts().pageLoadTimeout	(60L, TimeUnit.SECONDS)	// NOTE: negative value means indefinite
		this.driver.manage().timeouts().setScriptTimeout(30L, TimeUnit.SECONDS)
		// this.driver.manage().timeouts().implicitlyWait	(30L, TimeUnit.SECONDS) // WARN: will add a delay of 30 seconds to every command that selenium runs?
		return this.driver
	}
	
}
