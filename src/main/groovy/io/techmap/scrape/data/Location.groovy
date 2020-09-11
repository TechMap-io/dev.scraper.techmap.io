package io.techmap.scrape.data

import groovy.util.logging.Log4j2
import io.techmap.scrape.data.shared.Address
import io.techmap.scrape.data.shared.Contact

@Log4j2
class Location extends AMongoDbDocument {

	/** Description of the company as listed in a job or on a company page */
	String	description		= ""	// if description is location-specific

	/** List of contacts - probably only one per job */
	ArrayList<Contact> contacts 		= new ArrayList<Contact>()

	/** Original address info without cleaning, geolocating, etc. */
	Address	orgAddress		= new Address()

	Location() {
	}

	Boolean isValid() {
		if (!super.isValid()) return false

		if (!this.orgAddress.isValid()) return false

		return true
	}

	/**
	 * toString() implementation solely for debug purposes.
	 */
	String toString() {
		return "${name} @ $source, ${contacts?.size()} contacts)"?.replaceAll(/\b(null)\b/,'')
	}

}

