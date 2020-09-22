/* Copyright © 2020, TechMap GmbH - All rights reserved. */
package io.techmap.scrape.data.shared

import groovy.util.logging.Log4j2

@Log4j2
class Contact {
	/** The contact's name such as 'John Doe' */
	String	name
	/** Textual representation of the phone number */
	String	phone
	/** Textual representation of the E-Mail */
	String	email
	/** Textual representation of the address */
	String	address

	Contact() {
	}

	Boolean isValid() {
		// NOTE: can be empty - but try to find info
		// if (!this.name && !this.phone && !this.email && !this.address) {
		// 	log.error "Invalid address without any data: $this"
		// 	return false
		// }
		return true
	}

	String toString() {
		return "$name, $phone, $email, $address"?.replaceAll(/\b(null)\b/,'')
	}
}
