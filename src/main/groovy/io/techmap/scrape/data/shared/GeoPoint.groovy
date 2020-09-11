package io.techmap.scrape.data.shared

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
