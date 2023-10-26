package json

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
)

func TestEvent(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()
	// ctx = context.WithValue(ctx, event.ContextNamespaceKey, "")
	stmID := event.NewStreamID("tenantID")

	t.Run("convert", func(t *testing.T) {
		evt := event.Wrap(ctx, stmID, testutil.GenEvents(1))[0]

		jsonEvt, err := convertEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if _, ok := any(jsonEvt).(*jsonEvent); !ok {
			t.Fatalf("expect event type be *jsonEvent, got %T", jsonEvt)
		}

		// TODO get rid of this... error-prone
		jsonEvt.reg = event.NewRegister("")

		_ = jsonEvt.Event()

		if want, got := evt, jsonEvt; !testutil.CmpEnv(want, got) {
			t.Fatalf("expect %s, %s  be equals", testutil.FormatEnv(want), testutil.FormatEnv(got))
		}
	})

	t.Run("original", func(t *testing.T) {
		evts := testutil.GenEvents(1)

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