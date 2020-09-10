package io.techmap.scrape.data.shared

import dev.morphia.annotations.Embedded
import org.bson.Document

@Embedded
class Position {
	/** The job's name - it will be reduced later */
	String name
	/** Required minimum career level such as Senior, Junior */
	String careerLevel
	/** Comma separated list of contract characteristics such as Permanent contract, Minijob, Temporary, ... */
	String contractType
	/** Comma separated list of work characteristics such as Fulltime, Parttime, ... */
	String workType

	Position(Document doc) {
		if (!doc) return
		this.class.declaredFields.findAll { !it.synthetic }*.name.each {it ->
			this[it] = doc[it]
		}
	}

	Boolean isValid() {
		// NOTE: at least name should be set - but try to find info
		if (!this.name && !this.careerLevel && !this.contractType && !this.workType) {
			log.error "Invalid position without any data: $this"
			return false
		}
		return true
	}

	/**
	 * toString() implementation solely for debug purposes.
	 */
	String toString() {
		return "$name, $careerLevel, $contractType, $workType"?.replaceAll(/\b(null)\b/,'')
	}

}
