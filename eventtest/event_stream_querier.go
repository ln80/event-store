package eventtest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ln80/event-store/event"
)

type TestEventStreamQuerierOptions struct {
	SupportOrderDESC bool
}

func TestEventStreamQuerier(t *testing.T, ctx context.Context, store interface {
	event.StreamQuerier
	event.Store
}, opts ...func(*TestEventStreamQuerierOptions)) {
	t.Helper()

	opt := &TestEventStreamQuerierOptions{
		SupportOrderDESC: false,
	}
	for _, optFn := range opts {
		if optFn == nil {
			continue
		}
		optFn(opt)
	}

	// global stream ID
	globalID := event.UID().String()

	// init two sub-streams
	streamID1 := event.NewStreamID(globalID, "service1")
	streamID2 := event.NewStreamID(globalID, "service2")

	// append events to sub-streams
	events1 := GenEvents(10)
	for _, evt := range events1 {
		if err := store.Append(ctx, streamID1, event.Wrap(ctx, streamID1, []any{evt})); err != nil {
			t.Fatalf("expect to append event, got err: %v", err)
		}
	}

	events2 := GenEvents(15)
	for _, evt := range events2 {
		if err := store.Append(ctx, streamID2, event.Wrap(ctx, streamID2, []any{evt})); err != nil {
			t.Fatalf("expect to append event, got err: %v", err)
		}
	}

	t.Run("querier basic", func(t *testing.T) {
		q := event.StreamQuery{
			RecordLimit: 50,
		}

		result, err := store.Query(ctx, event.NewStreamID(globalID), q)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		if want, l := 25, len(result.Events); want != l {
			t.Fatalf("expect events count be %d, got %d", want, l)
		}

		if opt.SupportOrderDESC {
			descQuery := event.StreamQuery{
				Order:       event.StreamOrderDESC,
				RecordLimit: 50,
			}

			descResult, err := store.Query(ctx, event.NewStreamID(globalID), descQuery)
			if err != nil {
				t.Fatalf("expect to query events, got err: %v", err)
			}

			if want, l := len(result.Events), len(descResult.Events); want != l {
				t.Fatalf("expect events count be %d, got %d", want, l)
			}

			// Sort descending events to compare with ascending
			slices.SortFunc(descResult.Events, func(a, b event.Envelope) int {
				return b.At().Compare(a.At())
			})

			for i := 0; i < len(result.Events); i++ {
				if want, got := result.Events[i], descResult.Events[i]; !CmpEnv(want, got) {
					t.Fatalf("expect %v, %v be equals", FormatEnv(want), FormatEnv(got))
				}
			}
		}
	})

	t.Run("querier with time range", func(t *testing.T) {
		t.Helper()

		// Get current time for range testing
		now := time.Now().UTC()
		from := now.Add(-time.Hour)
		to := now.Add(time.Hour)

		q := event.StreamQuery{
			From: from,
			To:   to,
		}

		result, err := store.Query(ctx, event.NewStreamID(globalID), q)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		// All events should be within the time range
		for _, evt := range result.Events {
			if evt.At().Before(from) || evt.At().After(to) {
				t.Fatalf("expect event time %v to be within range [%v, %v]", evt.At(), from, to)
			}
		}
	})

	t.Run("querier with record limit", func(t *testing.T) {
		t.Helper()

		q := event.StreamQuery{
			RecordLimit: 5,
		}

		result, err := store.Query(ctx, event.NewStreamID(globalID), q)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		if want, got := 5, len(result.Events); want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}

		if opt.SupportOrderDESC {
			descQuery := event.StreamQuery{
				RecordLimit: 5,
				Order:       event.StreamOrderDESC,
			}

			descResult, err := store.Query(ctx, event.NewStreamID(globalID), descQuery)
			if err != nil {
				t.Fatalf("expect to query events, got err: %v", err)
			}

			if want, got := 5, len(descResult.Events); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}
	})

	t.Run("querier with cursor", func(t *testing.T) {
		t.Helper()

		// First query to get cursor
		q := event.StreamQuery{
			RecordLimit: 10,
		}

		result, err := store.Query(ctx, event.NewStreamID(globalID), q)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		if result.Cursor == nil {
			t.Skip("cursor not supported by implementation")
		}

		// Second query using cursor
		cursorQuery := event.StreamQuery{
			RecordLimit: 10,
			Cursor:      result.Cursor,
		}

		cursorResult, err := store.Query(ctx, event.NewStreamID(globalID), cursorQuery)
		if err != nil {
			t.Fatalf("expect to query events with cursor, got err: %v", err)
		}

		// Should get different events (or empty if no more)
		if len(result.Events) > 0 && len(cursorResult.Events) > 0 {
			if result.Events[0].ID() == cursorResult.Events[0].ID() {
				t.Fatalf("expect cursor to return different events")
			}
		}
	})

	t.Run("querier with filters", func(t *testing.T) {
		t.Helper()

		// Test with user filter
		q := event.StreamQuery{
			Users: []string{"test-user"},
		}

		result, err := store.Query(ctx, event.NewStreamID(globalID), q)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		// All events should have the specified user
		for _, evt := range result.Events {
			if evt.User() != "test-user" {
				t.Fatalf("expect event user to be 'test-user', got %s", evt.User())
			}
		}

		// Test with type filter
		typeQuery := event.StreamQuery{
			Types: []string{"test-event"},
		}

		typeResult, err := store.Query(ctx, event.NewStreamID(globalID), typeQuery)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		// All events should have the specified type
		for _, evt := range typeResult.Events {
			if evt.Type() != "test-event" {
				t.Fatalf("expect event type to be 'test-event', got %s", evt.Type())
			}
		}

		// Test with IP address filter
		ipQuery := event.StreamQuery{
			IPAddrs: []string{"127.0.0.1"},
		}

		ipResult, err := store.Query(ctx, event.NewStreamID(globalID), ipQuery)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		// All events should have the specified IP address
		for _, evt := range ipResult.Events {
			if evt.IPAddr() != "127.0.0.1" {
				t.Fatalf("expect event IP to be '127.0.0.1', got %s", evt.IPAddr())
			}
		}
	})

	t.Run("querier with combined filters", func(t *testing.T) {
		t.Helper()

		// Test with multiple filters combined
		q := event.StreamQuery{
			From:        time.Now().UTC().Add(-time.Hour),
			To:          time.Now().UTC().Add(time.Hour),
			RecordLimit: 5,
			Users:       []string{"test-user"},
			Types:       []string{"test-event"},
			Order:       event.StreamOrderASC,
		}

		result, err := store.Query(ctx, event.NewStreamID(globalID), q)
		if err != nil {
			t.Fatalf("expect to query events, got err: %v", err)
		}

		// Verify all filters are applied
		for _, evt := range result.Events {
			// Check time range
			if evt.At().Before(q.From) || evt.At().After(q.To) {
				t.Fatalf("expect event time %v to be within range [%v, %v]", evt.At(), q.From, q.To)
			}

			// Check user filter
			if evt.User() != "test-user" {
				t.Fatalf("expect event user to be 'test-user', got %s", evt.User())
			}

			// Check type filter
			if evt.Type() != "test-event" {
				t.Fatalf("expect event type to be 'test-event', got %s", evt.Type())
			}
		}

		// Check record limit
		if len(result.Events) > q.RecordLimit {
			t.Fatalf("expect events count to be <= %d, got %d", q.RecordLimit, len(result.Events))
		}
	})
}
