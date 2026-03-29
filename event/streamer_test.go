package event

import (
	"testing"
)

func TestStreamReplay_Query(t *testing.T) {
	q1 := StreamReplayQuery{}
	q1.Build()
	if want, got := VersionMin, q1.From; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := VersionMax, q1.To; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}

	q2 := StreamReplayQuery{
		From:  NewVersion().Add(10, 0),
		Order: StreamOrderDESC,
	}
	q2.Build()
	if want, got := NewVersion().Add(10, 0), q2.From; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := VersionMax, q2.To; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := StreamOrderDESC, q2.Order; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
}
