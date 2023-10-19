package sourcing

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/event/testutil"
)

func TestStream(t *testing.T) {
	ctx := context.Background()

	events := []any{
		&testutil.Event{Val: "1"},
		&testutil.Event{Val: "2"},
	}

	stmID := event.NewStreamID("globalID")

	stm := Wrap(ctx, stmID, event.VersionZero, events)

	if want, val := stmID, stm.ID(); want.String() != val.String() {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := event.NewVersion().Add(0, 1).EOF(), stm.Version(); !want.Equal(val) {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if want, val := events, stm.Unwrap().Events(); !reflect.DeepEqual(want, val) {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
}
