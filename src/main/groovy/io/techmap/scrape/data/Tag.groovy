package io.techmap.scrape.data

import io.techmap.scrape.data.shared.TagType

class Tag {
	/** Raw textual name of the Tag such as "Java" */
	String	name	= ""
	/** Controlled Type of the Tag such as TagType.SKILLS */
	TagType type	= null
	/** Textual source of the Tag such as "stepstone_de" or "techmap" (internal) */
	String	source	= ""

	Tag() {
	}

	Boolean isValid() {
		if (!name || !type || !source) return false
		return true
	}

	/**
	 * toString() implementation solely for debug purposes.
	 */
	String toString() {
		return "${this?.type ?: ''}: ${this?.name ?: ''}"
	}

}
