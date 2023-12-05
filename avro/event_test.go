package avro

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
)

func TestEvent(t *testing.T) {
	type Container struct {
		E1 *testutil.Event1
		E2 *testutil.Event2
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, event.ContextNamespaceKey, "")
	stmID := event.NewStreamID("tenantID")

	t.Run("convert", func(t *testing.T) {
		evt := event.Wrap(ctx, stmID, testutil.GenEvents(1), event.WithNameSpace("service1"))[0]

		avroEvt, err := convertEvent(evt)
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if _, ok := any(avroEvt).(*avroEvent); !ok {
			t.Fatalf("expect event type be *avroEvent, got %T", avroEvt)
		}
		_ = avroEvt.Event()

		if want, got := evt, avroEvt; !testutil.CmpEnv(want, got) {
			t.Fatalf("expect %s, %s  be equals", testutil.FormatEnv(want), testutil.FormatEnv(got))
		}
	})

	t.Run("original", func(t *testing.T) {
		evts := testutil.GenEvents(1)

		avroEvt, err := convertEvent(event.Wrap(ctx, stmID, evts, event.WithNameSpace("service1"))[0])
		if err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}

		if want, got := reflect.TypeOf(evts[0]).Name(), reflect.TypeOf(avroEvt.Event()).Name(); want != got {
			t.Fatalf("expect %s, %s  be equals", want, got)
		}
	})

}
