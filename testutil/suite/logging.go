package test_suite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
)

func EventStoreTest(t *testing.T, ctx context.Context, store event.Store) {
	t.Run("basic operations", func(t *testing.T) {
		streamID := event.NewStreamID(event.UID().String())
		// test append events to stream
		envs := event.Wrap(ctx, streamID, testutil.GenEvents(10))
		if err := event.Stream(envs).Validate(func(v *event.Validation) {
			v.SkipVersion = true
		}); err != nil {
			t.Fatalf("expect to gen a valid chunk, got err: %v", err)
		}
		if err := store.Append(ctx, streamID, envs); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// test append chunk twice
		err := store.Append(ctx, streamID, envs)
		if err == nil || !errors.Is(err, event.ErrAppendEventsConflict) {
			t.Fatalf("expect conflict error to occur, got: %v", err)
		}

		// test load appended chunk
		renvs, err := store.Load(ctx, streamID)
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		if l := len(renvs); l != 10 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 10, l)
		}

		// assert data integrity
		for i, env := range envs {
			if !testutil.CmpEnv(env, renvs[i]) {
				t.Fatalf("event %d data altered %v %v", i, testutil.FormatEnv(env), testutil.FormatEnv(renvs[i]))
			}
		}

		// test loading a sub stream
		renvs, err = store.Load(ctx, streamID, time.Unix(0, 0), renvs[2].At())
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		if l := len(renvs); l != 3 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 3, l)
		}

		// assert data integrity
		for i, env := range renvs {
			if !testutil.CmpEnv(env, envs[i]) {
				t.Fatalf("event %d data altered %v %v", i, testutil.FormatEnv(env), testutil.FormatEnv(envs[i]))
			}
		}
	})

	t.Run("with expired events", func(t *testing.T) {
		streamID := event.NewStreamID(event.UID().String())

		// test append events to stream
		envs := event.Wrap(ctx, streamID, testutil.GenEvents(10), func(env event.RWEnvelope) {
			env.SetTTL(5 * time.Microsecond)
		})
		if err := event.Stream(envs).Validate(func(v *event.Validation) {
			v.SkipVersion = true
		}); err != nil {
			t.Fatalf("expect to generate a valid chunk, got err: %v", err)
		}
		if err := store.Append(ctx, streamID, envs); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// wait for events to expire
		time.Sleep(10 * time.Microsecond)

		renvs, err := store.Load(ctx, streamID)
		if err != nil {
			t.Fatalf("expect to load events got err %v", err)
		}
		if l := len(renvs); l != 0 {
			t.Fatalf("invalid loaded events length, must be %d got: %d", 0, l)
		}
	})
}
