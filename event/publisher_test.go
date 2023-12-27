package event

import (
	"context"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event/testutil"
)

type pubEvent struct{ Val string }

func (pubEvent) EvDests() []string {
	return []string{"dest_1"}
}

func TestPublisher_Destinations(t *testing.T) {
	namespace := "testutil"
	ctx := context.WithValue(context.Background(), ContextNamespaceKey, namespace)

	NewRegister("").Clear()
	defer NewRegister("").Clear()

	type Event2 struct{ Val string }

	NewRegister(namespace).
		Set(pubEvent{}).
		Set(Event2{})

	if want, got := (&(pubEvent{})).EvDests(), publishDestinations(ctx, pubEvent{}); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := []string{}, publishDestinations(ctx, Event2{}); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}

	// force the use of global registry as fallback
	NewRegister("").
		Set(testutil.Event2{})

	if want, got := (&testutil.Event2{}).EvDests(), publishDestinations(ctx, Event2{}); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
}
