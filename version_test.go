package es

import (
	"testing"
)

func TestVersion(t *testing.T) {
	var v Version = "v0.1.2"
	sv := v.Semver()

	if want, got := uint64(0), sv.Major(); want != got {
		t.Fatalf("expect %d, %d be equals", want, got)
	}
	if want, got := uint64(1), sv.Minor(); want != got {
		t.Fatalf("expect %d, %d be equals", want, got)
	}
	if want, got := uint64(2), sv.Patch(); want != got {
		t.Fatalf("expect %d, %d be equals", want, got)
	}
}
