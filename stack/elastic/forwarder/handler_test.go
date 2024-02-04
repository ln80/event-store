//go:build unit

package main

import (
	"testing"

	es "github.com/ln80/event-store"
)

func TestHandler(t *testing.T) {

	t.Log("------->foo test run... " + es.FOO())
}
