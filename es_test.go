package es_test

import (
	"testing"

	es "github.com/ln80/event-store"
)

func TestLoggerBinding(t *testing.T) {
	// Capture regressions related to the binding of logging functions to
	// the internal logger package.
	// In such scenarios, the test will report a failure with the message:
	// "[build failed]"

	log := es.DiscardLogger()
	es.SetDefaultLogger(log)

}
