package logger

import (
	"reflect"
	"testing"
)

func TestLogger(t *testing.T) {
	logger := New()

	if !logger.Enabled() {
		t.Fatalf("expect logger to be enabled")
	}

	SetDefault(logger)

	if want, got := logger, Default(); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
}
