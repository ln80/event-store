package event

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ln80/event-store/event/testutil"
)

func TestStream_ID(t *testing.T) {
	streamID := NewStreamID("gstreamID", "p1", "p2", "p2")

	if want, val := strings.Join([]string{"gstreamID", "p1", "p2", "p2"}, StreamIDPartsDelimiter), streamID.String(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if want, val := "gstreamID", streamID.GlobalID(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if want, val := []string{"p1", "p2", "p2"}, streamID.Parts(); !reflect.DeepEqual(want, val) {
		t.Fatalf("expect %v, %v be equals", want, val)
	}
	if ok := streamID.Global(); ok {
		t.Fatal("expect stream is global be false, got true")
	}

	if ok := NewStreamID("gstreamID").Global(); !ok {
		t.Fatal("expect stream is global be true, got false")
	}

	streamID, err := ParseStreamID("gstreamID#service")
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if want, val := "gstreamID", streamID.GlobalID(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if ok := streamID.Global(); ok {
		t.Fatal("expect stream is global be false, got true")
	}
	streamID, err = ParseStreamID("gstreamID")
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if want, val := "gstreamID", streamID.GlobalID(); want != val {
		t.Fatalf("expect %s, %s be equals", want, val)
	}
	if ok := streamID.Global(); !ok {
		t.Fatal("expect stream is global, got false")
	}

	if _, err := ParseStreamID(""); !errors.Is(err, ErrInvalidStreamID) {
		t.Fatalf("expect err be %v, got %v", ErrInvalidStreamID, err)
	}
}

func TestStream_Validation(t *testing.T) {
	events := []any{
		&testutil.Event{
			Val: "1",
		},
		&testutil.Event{
			Val: "2",
		},
	}
	streamID := NewStreamID("gstreamID")

	t.Run("validate events", func(t *testing.T) {
		envs := Wrap(context.Background(), streamID, events)

		// ValidateEvent fails because cursor is nil
		ignored, err := ValidateEvent(envs[0], nil)
		if ignored {
			t.Fatal("expect event is not ignored, got true")
		}
		if !errors.Is(err, ErrCursorNotFound) {
			t.Fatalf("expect err be %v, got %v", ErrCursorNotFound, err)
		}

		// init empty cursor
		cur := NewCursor(streamID.String())

		// ValidateEvent failed due to sequence validation which is not set [by default] by Envelop call
		if _, err := ValidateEvent(envs[0], cur); !errors.Is(err, ErrInvalidStream) {
			t.Fatalf("expect err be %v, got %v", ErrInvalidStream, err)
		}

		// ValidateEvent success because we skipped the version sequence check
		if _, err := ValidateEvent(envs[0], cur, func(v *Validation) {
			v.SkipVersion = true
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// Set 1st evt version
		envs[0].(RWEnvelope).SetVersion(NewVersion())

		// ValidateEvent success coz 1st evt has a version & initial cursor does not
		// ValidateEvent will mutate the cursor by given it it's version sequence
		if _, err := ValidateEvent(envs[0], cur); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if _, err := ValidateEvent(envs[1], cur); !errors.Is(err, ErrInvalidStream) {
			t.Fatalf("expect err be %v, got %v", ErrInvalidStream, err)
		}

		// keep moving and validate the 2nd evt after a slight edit..
		envs[1].(RWEnvelope).SetVersion(NewVersion().Incr())
		envs[1].(RWEnvelope).SetAt(time.Now())
		if _, err := ValidateEvent(envs[1], cur); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// create a versioned stream chunk
		envs = Wrap(context.Background(), streamID, events,
			WithVersionIncr(NewVersion(), len(events), VersionSeqDiffPart))

		cur = NewCursor(streamID.String())

		// ValidateEvent success
		if _, err := ValidateEvent(envs[0], cur); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
	})

	t.Run("test validate stream chunk", func(t *testing.T) {
		withTimeHack := func(env RWEnvelope) {
			time.Sleep(1 * time.Nanosecond)
		}
		envs := Wrap(context.Background(), streamID, events, withTimeHack)
		if err := Stream(envs).Validate(func(v *Validation) {
			v.SkipVersion = true
		}); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		envs = Wrap(context.Background(), streamID, events, WithVersionIncr(NewVersion(), len(events), VersionSeqDiffPart), withTimeHack)
		if err := Stream(envs).Validate(); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
	})
}

func TestStream_Basic(t *testing.T) {
	events := []any{
		&testutil.Event{
			Val: "1",
		},
		&testutil.Event{
			Val: "2",
		},
	}
	streamID := NewStreamID("gstreamID")

	if !Stream([]Envelope{}).Empty() {
		t.Fatal("expect stream is empty, got false")
	}

	envs := Wrap(context.Background(), streamID, events,
		WithVersionIncr(NewVersion(), len(events), VersionSeqDiffPart))
	stm := Stream(envs)

	if stm.Empty() {
		t.Fatal("expect stream not empty, got empty")
	}
	if want, got := len(events), len(stm.EventIDs()); want != got {
		t.Fatalf("expect len %d, %d  be equals", want, got)
	}
	for i, evt := range stm.Events() {
		if want, val := events[i], evt; want != val {
			t.Fatalf("expect %v, %v be equals", want, val)
		}
	}
}
