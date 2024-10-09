package sourcing

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event"
)

func TestStream(t *testing.T) {
	ctx := context.Background()

	events := []any{
		&Event{Val: "1"},
		&Event{Val: "2"},
	}

	stmID := event.NewStreamID("globalID")

	stm := Wrap(ctx, stmID, event.VersionZero, events)

	if want, got := stmID, stm.ID(); want.String() != got.String() {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := event.NewVersion().Add(0, 1).EOF(), stm.Version(); !want.Equal(got) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := events, stm.Unwrap().Events(); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
}
