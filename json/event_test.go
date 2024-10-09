package json

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/eventtest"
)

func TestEvent(t *testing.T) {
	eventtest.RegisterEvent("")

	// defer event.NewRegister("")

	ctx := context.Background()
	// ctx = context.WithValue(ctx, event.ContextNamespaceKey, "")
	stmID := event.NewStreamID("tenantID")

	t.Run("simple", func(t *testing.T) {
		jsonEvt := jsonEvent{}

		if want, got := event.VersionZero, jsonEvt.Version(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		if want, got := event.VersionZero, jsonEvt.GlobalVersion(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		jsonEvt.SetGlobalVersion(event.VersionMax)
		if want, got := event.VersionMax, jsonEvt.GlobalVersion(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		if want, got := any(nil), jsonEvt.Event(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
		// check idempotency due to the existing of an internal cache
		if want, got := any(nil), jsonEvt.Event(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
	})

	t.Run("convert", func(t *testing.T) {
		ver := event.VersionMin
		gVer := event.VersionMin

		evt := event.Wrap(ctx,
			stmID, eventtest.GenEvents(1),
			// event.WithNameSpace("service1"),
			event.WithVersionIncr(ver, 1, event.VersionSeqDiffPart),
			event.WithGlobalVersionIncr(gVer, 1, event.VersionSeqDiffPart),
		)[0]

		jsonEvt, err := convertEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if _, ok := any(jsonEvt).(*jsonEvent); !ok {
			t.Fatalf("expect event type be *jsonEvent, got %T", jsonEvt)
		}

		jsonEvt.reg = event.NewRegister("")

		_ = jsonEvt.Event()

		if want, got := evt, jsonEvt; !eventtest.CmpEnv(want, got) {
			t.Fatalf("expect %s, %s  be equals", eventtest.FormatEnv(want), eventtest.FormatEnv(got))
		}
	})

	t.Run("original", func(t *testing.T) {
		evts := eventtest.GenEvents(1)

		jsonEvt, err := convertEvent(event.Wrap(ctx, stmID, evts)[0])
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		// TODO get rid of this... error-prone
		jsonEvt.reg = event.NewRegister("")

		if want, got := reflect.TypeOf(evts[0]).Name(), reflect.TypeOf(jsonEvt.Event()).Name(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
	})

}
