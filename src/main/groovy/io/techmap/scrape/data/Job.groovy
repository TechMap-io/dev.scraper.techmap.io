package io.techmap.scrape.data

import dev.morphia.annotations.Entity
import groovy.util.logging.Log4j2
import io.techmap.scrape.data.shared.Address
import io.techmap.scrape.data.shared.Contact
import io.techmap.scrape.data.shared.Position
import io.techmap.scrape.data.shared.Salary

import java.time.LocalDateTime

@Log4j2
@Entity(value="jobs", noClassnameStored=true)
class Job extends AMongoDbDocument {

	/** Raw text of the job ad (generated via .text()method from JSoup) */
	String		text			= ""
	/** Raw HTML of the job ad (only main job part - not whole page) */
	String		html			= ""
	/** Full JSONs spread in the page's source code (esp. from schema.org in "application/ld+json") */
	Map			json			= [:]

	/** Locale / language tag of the job ads content - not the website's language */
	String		locale			= ""	// e.g., "de_CH" - might be lowercase (Dice)!

	/** ID the company uses internally for this job (might not always exist) */
	String		referenceID		= ""	// ID used by the company

	/** Additional info on the position such as required career level or contract type*/
	Position	position		= new Position()
	/** Additional info on salary such as currency and value */
	Salary		salary			= new Salary()
	/** Contact data like email, phone, etc. */
	Contact		contact			= new Contact()

	/** One or more dates the job ad was updated (not always available) */
	ArrayList<LocalDateTime>	datesUpdated	= new ArrayList<LocalDateTime>() // currently only for Monster?

	/** Stores the original address info */
	Address orgAddress			= new Address()

	/** Stores the original address info */
	Company	orgCompany			= new Company()

	Job() {
	}

	Boolean isValid() {
		if (!super.isValid())	return false
		if (!this.url)			return false
		if (!this.text || !this.html) {
			log.debug "Found job without text / html: " + this.url
			return false
		}
		if (!this.contact.isValid())	return false
		if (!this.position.isValid())	return false
		if (!this.salary.isValid())		return false

		return true
	}

	/**
	 * toString() implementation solely for debug purposes.
	 */
	String toString() {
		return "$name @ $source"?.replaceAll(/\b(null)\b/,'')
	}
}

