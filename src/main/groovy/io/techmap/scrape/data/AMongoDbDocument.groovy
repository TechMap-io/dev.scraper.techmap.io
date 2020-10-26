/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.data

import groovy.util.logging.Log4j2
import io.techmap.scrape.data.shared.TagType

import java.time.LocalDateTime

/** Superclass for all MongoDB documents with general fields */
@Log4j2
abstract class AMongoDbDocument {
	/** Name of the source such as "stepstone_de" */
	String	source	= ""

	/** ID of the company in the source such as "234551" (might not exist for locations or companies) */
	String	idInSource	= ""

	/** Name of the document (job title or company name) */
	String	name	= ""

	/** URL to Job in source or URL to company's website (e.g., "https://sap.com" or "https://sap.de") */
	String	url		= ""

	/** date the document was created in the Source (probably null for location and company */
	LocalDateTime	dateCreated = null

	/** Lists of Tags such as skills, industries, etc. (see TagType class for categories) */
	Map<TagType, HashSet<String>> orgTags = [:]

	AMongoDbDocument() {
	}

	Boolean isValid() {
		if (!this.name || this.name?.find(/^(null|https?:\/\/)/)) {
			log.error "Invalid name for AMongoDbDocument with name '${this.name}' (idInSource: ${this.idInSource}) url: ${this.url}"
			return false
		}
		if (this.name?.count("${this.name?.take(3)}") >= 2) { // find duplicate names such as "LeedsLeeds" (probably a Jsoup select problem)
			log.error "Invalid name '${this.idInSource}' for company: $this"
			return false
		}
		if (this.url?.contains("@")) { // e.g., "https://E-Mail:chemnitz@accurat.eu"
			log.error "Corrupt url '${this.url}' contains '@': $this"
			// return false // DO NOT invalidate - will be extracted and handled elsewhere
		}
		// if (!this.url) return false // should not be possible for jobs (but can happen for locations and companies)
		if (!this.source || !(this.source ==~ /^[a-z_]+$/)) {
			log.error "Invalid source '${this.source}' for company - must be something like 'stepstone_de': $this"
			return false
		}
		if (this.idInSource && this.idInSource?.find(/^(null|https?:\/\/)/)) {
			log.error "Invalid idInSource '${this.idInSource}' for company: $this"
			return false
		}
		return true
	}

}
