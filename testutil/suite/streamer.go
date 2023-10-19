package test_suite

import (
	"context"
	"slices"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
)

type EventStreamerSuiteOptions struct {
	PostAppend       func(id event.StreamID)
	SupportOrderDESC bool
}

func EventStreamerSuite(t *testing.T, ctx context.Context, store interface {
	event.Streamer
	event.Store
}, opts ...func(*EventStreamerSuiteOptions)) {
	opt := &EventStreamerSuiteOptions{
		PostAppend: func(id event.StreamID) {
		},
		SupportOrderDESC: false,
	}
	for _, optFn := range opts {
		if optFn == nil {
			continue
		}
		optFn(opt)
	}

	handlerFunc := func(events *[]event.Envelope) func(ctx context.Context, data event.StreamData) error {
		return func(ctx context.Context, data event.StreamData) error {
			if data.Type == event.StreamDataTypeRecord {
				*events = append(*events, data.Value.(event.Envelope))
			}
			return nil
		}
	}

	// global stream ID
	globalID := event.UID().String()

	// init two sub-streams
	streamID1 := event.NewStreamID(globalID, "service1")
	streamID2 := event.NewStreamID(globalID, "service2")

	// append events to sub-streams
	if err := store.Append(ctx, streamID1, event.Wrap(ctx, streamID1, testutil.GenEvents(10))); err != nil {
		t.Fatalf("expect to append events, got err: %v", err)
	}
	opt.PostAppend(streamID1)

	if err := store.Append(ctx, streamID2, event.Wrap(ctx, streamID2, testutil.GenEvents(15))); err != nil {
		t.Fatalf("expect to append events, got err: %v", err)
	}
	opt.PostAppend(streamID2)

	t.Run("basic", func(t *testing.T) {

		events := make([]event.Envelope, 0)
		q := event.StreamerQuery{}

		if err := store.Replay(ctx, event.NewStreamID(globalID), q, handlerFunc(&events)); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		if want, l := 25, len(events); want != l {
			t.Fatalf("expect events count be %d, got %d", want, l)
		}

		// check stream sequence
		cur := event.NewCursor(globalID)
		vOpt := func(v *event.Validation) {
			v.GlobalStream = true
		}
		for _, evt := range events {
			_, err := event.ValidateEvent(evt, cur, vOpt)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
		}

		if opt.SupportOrderDESC {

			descEvents := make([]event.Envelope, 0)
			q := event.StreamerQuery{
				Order: event.StreamerReplayOrderDESC,
			}

			if err := store.Replay(ctx, event.NewStreamID(globalID), q, handlerFunc(&descEvents)); err != nil {
				t.Fatalf("expect to append events, got err: %v", err)
			}

			if want, l := len(events), len(descEvents); want != l {
				t.Fatalf("expect events count be %d, got %d", want, l)
			}

			slices.SortFunc(descEvents, func(a, b event.Envelope) int {
				return a.GlobalVersion().Compare(b.GlobalVersion())
			})

			for i := 0; i < len(events); i++ {
				if want, got := events[i], descEvents[i]; !testutil.CmpEnv(want, got) {
					t.Fatalf("expect %v, %v be equals", testutil.FormatEnv(want), testutil.FormatEnv(got))
				}
			}
		}
	})

	t.Run("with_limit", func(t *testing.T) {
		// get first record events
		firstRecordEvents := make([]event.Envelope, 0)
		q := event.StreamerQuery{
			RecordLimit: 1,
		}

		if err := store.Replay(ctx, event.NewStreamID(globalID), q, handlerFunc(&firstRecordEvents)); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		// deduct the count of event based on the last append record operation
		if want, got := 10, len(firstRecordEvents); want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}

		// deduct the count of event based on the last append record operation
		if want, got := event.VersionZero.Add(1, 0).EOF(), firstRecordEvents[0].GlobalVersion(); !want.Equal(got) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}

		if q.Order == event.StreamerReplayOrderDESC {
			// get last record events
			lastRecordEvents := make([]event.Envelope, 0)
			q = event.StreamerQuery{
				Order:       event.StreamerReplayOrderDESC,
				RecordLimit: 1,
			}

			if err := store.Replay(ctx, event.NewStreamID(globalID), q, handlerFunc(&lastRecordEvents)); err != nil {
				t.Fatalf("expect to append events, got err: %v", err)
			}

			// deduct the count of event based on the last append record operation
			if want, got := 15, len(lastRecordEvents); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}

			// deduct the count of event based on the last append record operation
			if want, got := event.VersionZero.Add(2, 14).EOF(), lastRecordEvents[0].GlobalVersion(); !want.Equal(got) {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}
	})

	t.Run("advanced", func(t *testing.T) {

		events := make([]event.Envelope, 0)
		q := event.StreamerQuery{
			From: event.VersionMin.Add(0, 8),
			To:   event.VersionMin.Add(1, 2),
		}

		if err := store.Replay(ctx, event.NewStreamID(globalID), q, handlerFunc(&events)); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}

		if want, l := 5, len(events); want != l {
			t.Fatalf("expect events count be %d, got %d", want, l)
		}

		if q.Order == event.StreamerReplayOrderDESC {
			descEvents := make([]event.Envelope, 0)
			q := event.StreamerQuery{
				From:  event.VersionMin.Add(0, 8),
				To:    event.VersionMin.Add(1, 2),
				Order: event.StreamerReplayOrderDESC,
			}

			if err := store.Replay(ctx, event.NewStreamID(globalID), q, handlerFunc(&descEvents)); err != nil {
				t.Fatalf("expect to append events, got err: %v", err)
			}

			if want, l := 5, len(descEvents); want != l {
				t.Fatalf("expect events count be %d, got %d", want, l)
			}
		}

	})
}
