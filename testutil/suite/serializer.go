package test_suite

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
)

func TestSerializer(t *testing.T, ctx context.Context, ser event.Serializer) {
	stmID := event.NewStreamID("tenantID")

	t.Run("marshal_unmarshal single", func(t *testing.T) {
		evt := event.Wrap(ctx, stmID, testutil.GenEvents(1))[0]
		b, _, err := ser.MarshalEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		resEvt, err := ser.UnmarshalEvent(b)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// some implementation might lazy-unmarshal the original event
		_ = resEvt.Event()

		if want, got := evt, resEvt; !testutil.CmpEnv(want, got) {
			t.Fatalf("expect %s, %s  be equals", testutil.FormatEnv(want), testutil.FormatEnv(got))
		}

		// make sure we do not lose data even if we marshal x2
		b2, _, err := ser.MarshalEvent(resEvt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if want, got := b, b2; !reflect.DeepEqual(want, got) {
			t.Fatal("expect events binary be equals")
		}
	})

	t.Run("marshal_unmarshal batch", func(t *testing.T) {
		evts := event.Wrap(ctx, stmID, testutil.GenEvents(20))
		b, _, err := ser.MarshalEventBatch(evts)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		rEvts, err := ser.UnmarshalEventBatch(b)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if want, got := len(evts), len(rEvts); want != got {
			t.Fatalf("expect events len %d, %d be equals", want, got)
		}

		for i, rEvt := range rEvts {
			_ = rEvt.Event()
			if want, got := evts[i], rEvt; !testutil.CmpEnv(want, got) {
				t.Fatalf("expect %s, %s be equals", testutil.FormatEnv(want), testutil.FormatEnv(got))
			}
		}

		// make sure we do not lose data even if we marshal x2
		b2, _, err := ser.MarshalEventBatch(rEvts)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if want, got := b, b2; !reflect.DeepEqual(want, got) {
			t.Fatal("expect events binary be equals")
		}
	})
}
