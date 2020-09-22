/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.data

import groovy.util.logging.Log4j2

@Log4j2
class Company extends AMongoDbDocument {

	/** Description of the company as listed in a job or on a company page */
	String	description		= ""

	/** List of urls in sources such as [stepstone_de: "https://www.stepstone.de/cmp/de/LIMAX-Verwaltungs-GmbH-234551"] */
	Map		urls			= [:]

	/** List of ids in sources such as [stepstone_de: "234551"] */
	Map		ids				= [:]

	/** Additional information such as info.foundingDate, info.companySize, etc. */
	Map		info			= [:]

	Company() {
	}

	Boolean isValid() {
		if (!super.isValid()) return false
		this.urls.each {
			if (!it.key || !(it.key ==~ /^[a-z_]+$/)) {
				log.error "Invalid key '${it.key}' in company.urls  - must be something like 'https://stepstone.de/...': $this"
				return false
			}
		}
		this.ids.each {
			if (!it.key || !(it.key ==~ /^[a-z_]+$/)) {
				log.error "Invalid key '${it.key}' in company.ids  - must be something like 'stepstone_de': $this"
				return false
			}
		}
		return true
	}

	/**
	 * toString() implementation solely for debug purposes.
	 */
	String toString() {
		return "$name @ $source"?.replaceAll(/\b(null)\b/,'')
	}
}
