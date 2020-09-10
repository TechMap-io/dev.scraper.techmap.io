package io.techmap.scrape.data

import dev.morphia.annotations.Entity
import dev.morphia.annotations.Id

import io.techmap.scrape.data.shared.TagType
import org.bson.types.ObjectId

@Entity(value="tags", noClassnameStored=true)
class Tag {
	@Id
	ObjectId _id

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
