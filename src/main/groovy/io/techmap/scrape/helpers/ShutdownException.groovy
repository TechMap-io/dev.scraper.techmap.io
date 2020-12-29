package io.techmap.scrape.helpers

class ShutdownException extends Exception {
	ShutdownException(String errorMessage) {
		super(errorMessage)
	}
}
