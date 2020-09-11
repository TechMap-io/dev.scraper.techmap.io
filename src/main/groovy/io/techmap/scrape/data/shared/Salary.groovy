package io.techmap.scrape.data.shared

import org.bson.Document

class Salary {
	/** Raw textual statement on salary */
	String text
	/** Value as a number on how much is payed (relative to the period) */
	Double value
	/** Currency (often abbreviated) the value refers to such as Euro, Pound, Dollar, ...*/
	String currency
	/** Period the salary is payed for (e.g., per hour, week, month, year, contract) */
	String period

	Salary(Document doc) {
		if (!doc) return
		this.class.declaredFields.findAll { !it.synthetic }*.name.each {it ->
			this[it] = doc[it]
		}
	}

	Boolean isValid() {
		// NOTE: can be empty - but try to find info
		// if (!this.text && !this.value && !this.currency && !this.period) {
		// 	log.error "Invalid salary without any data: $this"
		// 	return false
		// }
		return true
	}

	String toString() {
		return "$text, $value, $currency, $period"?.replaceAll(/\b(null)\b/,'')
	}
}
