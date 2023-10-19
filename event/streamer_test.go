package event

import (
	"testing"
)

func TestStreamer_Query(t *testing.T) {
	q1 := StreamerQuery{}
	q1.Build()
	if want, val := VersionMin, q1.From; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := VersionMax, q1.To; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}

	q2 := StreamerQuery{
		From:  NewVersion().Add(10, 0),
		Order: StreamerReplayOrderDESC,
	}
	q2.Build()
	if want, val := NewVersion().Add(10, 0), q2.From; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := VersionMax, q2.To; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := StreamerReplayOrderDESC, q2.Order; want != val {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
}
