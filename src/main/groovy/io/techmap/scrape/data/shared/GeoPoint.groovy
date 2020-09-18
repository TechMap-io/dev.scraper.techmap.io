package io.techmap.scrape.data.shared

import groovy.util.logging.Log4j2

@Log4j2
class GeoPoint  {
	/** Latitude of the geopoint */
	Double	lat
	/** Longitude of the geopoint */
	Double	lng

	@Override
	String toString() {
		return "[lat: $lat, lng: $lng]" as String
	}

}
