package avro

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/eventtest"
)

func TestEvent(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, event.ContextNamespaceKey, "")
	stmID := event.NewStreamID("tenantID")

	defer event.NewRegister("service1").Clear()

	t.Run("simple", func(t *testing.T) {
		avroEvt := avroEvent{}
		if want, got := event.VersionZero, avroEvt.Version(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		if want, got := event.VersionZero, avroEvt.GlobalVersion(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		avroEvt.SetGlobalVersion(event.VersionMax)
		if want, got := event.VersionMax, avroEvt.GlobalVersion(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		if want, got := any(nil), avroEvt.Event(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		// check idempotency due to the existing of an internal cache
		if want, got := any(nil), avroEvt.Event(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
	})

	t.Run("convert", func(t *testing.T) {
		ver := event.VersionMin
		gVer := event.VersionMin

		evt := event.Wrap(ctx,
			stmID, eventtest.GenEvents(1),
			event.WithNameSpace("service1"),
			event.WithVersionIncr(ver, 1, event.VersionSeqDiffPart),
			event.WithGlobalVersionIncr(gVer, 1, event.VersionSeqDiffPart),
		)[0]

		avroEvt, err := convertEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if _, ok := any(avroEvt).(*avroEvent); !ok {
			t.Fatalf("expect event type be *avroEvent, got %T", avroEvt)
		}
		_ = avroEvt.Event()

		if want, got := evt, avroEvt; !eventtest.CmpEnv(want, got) {
			t.Fatalf("expect %s, %s  be equals", eventtest.FormatEnv(want), eventtest.FormatEnv(got))
		}
	})

	t.Run("original", func(t *testing.T) {
		evts := eventtest.GenEvents(1)

		avroEvt, err := convertEvent(event.Wrap(ctx, stmID, evts, event.WithNameSpace("service1"))[0])
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if want, got := reflect.TypeOf(evts[0]).Name(), reflect.TypeOf(avroEvt.Event()).Name(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}

		// hack force  event type
		type alteredEvent struct{}
		event.NewRegister("service1").Set(alteredEvent{})
		avroEvt.fEvent = alteredEvent{}

		if want, got := event.TypeOfWithNamespace("service1", evts[0]), avroEvt.Type(); want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}

		// upgrade event type based on current event registry state
		avroEvt.checkType("service1")

		if want, got := event.TypeOfWithNamespace("service1", alteredEvent{}), avroEvt.Type(); want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})
}
