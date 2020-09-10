package io.techmap.scrape

import groovy.util.logging.Log4j2

import io.techmap.scrape.connectors.MongoDBConnector

import io.techmap.scrape.scraper.AScraper
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext

@Log4j2
class Main {
	static MongoDBConnector db = MongoDBConnector.getInstance().init()

	static void main(String[] args) {
		LoggerContext context = (LoggerContext) LogManager.getContext(false)
		def setRootLogLevel = { level ->
			context.getConfiguration().getLoggerConfig(LogManager.ROOT_LOGGER_NAME).setLevel(level)
			context.updateLoggers()
		}
		// setRootLogLevel(Level.DEBUG)	// programmatically set log level
		setRootLogLevel(Level.INFO)		// programmatically set log level

		printSystemInfo()

		System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2")

		args = args*.split(',').flatten()*.trim() // Ugly workaround as Intellij does not allow multiple arguments
		def command = (args.length >= 1 ? args?.getAt(0) : "")
		def arg2 = (args.length >= 2 ? args?.getAt(1) : "")
		def arg3 = (args.length >= 3 ? args?.getAt(2) : "")
		log.info "Starting with args: $args"

		switch (command) {
			case "selfTest":				log.info "Selftest successful: Looks like I'm running OK!"; break
			case "scrape":					scrapeSource(arg2, arg3); break
			default:
				log.warn "No command '$command' found with args: '$args'"
				break
		}

		log.info "Finnished run with args: $args"
	}

	static void scrapeSource(String source, String locale = null) {
		def scraper = Class.forName("io.techmap.scrape.scraper.webscraper.${source}Scraper")
		triggerScraper(scraper, source, locale)
	}

	private static int triggerScraper(Class<? extends AScraper> scraper, String source, String locale) {

		// NOTE: normally the oldest source/website is read from the DB (e.g., stepstone_nl) - here we always use the first
		int sourceIndex = 0

		// Start scraper on source with sourceIndex
		def sourceID = scraper.startOne(scraper, sourceIndex)

		log.info "End of scraping ${scraper.jobsInAllSourcesCount} jobs of source ${sourceID}"
		return scraper.jobsInAllSourcesCount
	}

	private static void printSystemInfo(String[] args) {
		log.info "Program is using Java version ${System.getProperty("java.version")}"
		log.info "Program is using Groovy version ${GroovySystem.version}"
		def DOCKER_TAG = System.getenv("DOCKER_TAG")//System.getProperty("DOCKER_TAG")
		if (DOCKER_TAG) log.info "Program is using Docker image version '${DOCKER_TAG}'"
		else log.info "Program is NOT running in Docker container!"

		// Print System statistics
		long maxMemory = Runtime.getRuntime().maxMemory() // This will return Long.MAX_VALUE if there is no preset limit
		log.info("Max memory available (GigaBytes): " + (maxMemory == Long.MAX_VALUE ? "no limit" : (maxMemory / (1024 * 1024 * 1024))))
		log.info("Total memory in use (GigaBytes):  " + (Runtime.getRuntime().totalMemory() / (1024 * 1024 * 1024)))
		log.info("Free memory (GigaBytes):          " + (Runtime.getRuntime().freeMemory() / (1024 * 1024 * 1024)))
		log.info("Available processors (cores):     " + Runtime.getRuntime().availableProcessors())

		String IP = ""
		try {
			IP = new URL("https://checkip.amazonaws.com").getText()?.replaceAll(/\s+/, "")
			log.info "Using IP Adress: '$IP'"
		} catch (e) {
			log.debug "Could not access https://checkip.amazonaws.com : $e"
			try {
				IP = new URL("http://icanhazip.com").getText()?.replaceAll(/\s+/, "")
				log.info "Using IP Adress: '$IP'"
			} catch (f) {
				log.warn "Could not access https://checkip.amazonaws.com nor http://icanhazip.com : \n$e \n$f"
			}
		}
	}

}
