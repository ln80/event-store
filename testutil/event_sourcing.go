package testutil

import (
	"context"
	"errors"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/event/sourcing"
)

func TestEventSourcingStore(t *testing.T, ctx context.Context, store sourcing.Store) {
	t.Run("event sourcing basic operations", func(t *testing.T) {
		streamID := event.NewStreamID(event.UID().String())

		// append events to stream with invalid current version of the stream
		stm := sourcing.Wrap(ctx, streamID, event.VersionMin, GenEvents(10))
		if err := store.AppendToStream(ctx,
			stm); !errors.Is(err, event.ErrAppendEventsFailed) {
			t.Fatalf("expect failed append err, got: %v", err)
		}

		// append events to stream
		stm = sourcing.Wrap(ctx, streamID, event.VersionZero, GenEvents(10))
		if err := store.AppendToStream(ctx, stm); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// append chunk twice
		err := store.AppendToStream(ctx, stm)
		if err == nil || !errors.Is(err, event.ErrAppendEventsConflict) {
			t.Fatalf("expect conflict error to occur, got: %v", err)
		}

		// test load appended stream
		rstm, err := store.LoadStream(ctx, streamID)

		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		if rid, id := stm.ID().String(), streamID.String(); rid != id {
			t.Fatalf("expect loaded stream ID be %s, got %s", rid, id)
		}
		renvs := rstm.Unwrap()
		if l := len(renvs); l != 10 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 10, l)
		}

		// check data integrity
		envs := stm.Unwrap()
		for i, env := range envs {
			if !CmpEnv(env, renvs[i]) {
				t.Fatalf("event %d data altered %v %v", i, FormatEnv(env), FormatEnv(renvs[i]))
			}
		}

		// append a second chunk record
		stm2 := sourcing.Wrap(ctx, streamID, stm.Version(), GenEvents(10))
		if err := store.AppendToStream(ctx,
			stm2); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// test load sub part of the stream
		rstm, err = store.LoadStream(ctx, streamID, stm.Version().Trunc().Add(0, 1), stm.Version().Trunc().Add(0, 3))
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		renvs = rstm.Unwrap()
		if l := len(renvs); l != 3 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 3, l)
		}
		if !CmpEnv(renvs[0], envs[1]) {
			t.Fatalf("event data altered %v %v", FormatEnv(renvs[0]), FormatEnv(envs[1]))
		}
	})
}
