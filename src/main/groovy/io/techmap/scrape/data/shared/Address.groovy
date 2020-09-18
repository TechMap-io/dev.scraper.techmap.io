package io.techmap.scrape.data.shared

import groovy.util.logging.Log4j2

@Log4j2
class Address {
	/** Name of the source such as "stepstone_de" */
	String	source

	/** Name of the company */
	String	companyName	= ""

	/** Original address of the company (raw / unparsed - as stated in job ad) */
	String	addressLine	= ""

	/** CountryCode of the company's address such as "us" (not always available) */
	String	countryCode	= ""

	/** Country of the company's address such as "United Kingdom" (should have) */
	String	country		= ""

	/** State of the company's address such as "California" or "CA" (not always available) */
	String	state		= ""

	/** County of the company's address such as "Mono County" (not always available) */
	String	county		= ""

	/** City of the company's address such as "San Francisco" (must have) */
	String	city		= ""

	/** Post Code / Zip code of the company's address such as "90210" (not always available) */
	String	postCode	= ""

	/** District of the company's address such as "Corona Heights" (not always available) */
	String	district	= ""

	/** Quarter of the company's address such as "Eureka Valley" (not always available) */
	String	quarter		= ""

	/** Street of the company's address such as "Twin Peaks Blvd" (can include housenumber) */
	String	street		= ""

	/** Housenumber / Street number of the company's address such as "10" (optional) */
	String	houseNumber	= ""

	/** Geo-Coordinates (latitude / longitude) of the company's address (not always available) */
	GeoPoint geoPoint	= new GeoPoint()

	Address() {
	}

	Boolean isValid() {
		if (this.city?.find(/\b(null|https?)\b/)) {
			log.error "Found address with corrupt city '${this.city}'"
			return false
		}
		if (!this.addressLine?.trim()) {
			log.error "Found address with corrupt addressLine '${this.addressLine}'"
			return false
		}
		return true
	}

	/**
	 * toString() implementation solely for debug purposes.
	 */
	String toString() {
		return "$source: $companyName, $street $houseNumber, $postCode $city, $country ($countryCode): $geoPoint (from: $addressLine)"?.replaceAll(/\b(null)\b/,'')
	}

}
