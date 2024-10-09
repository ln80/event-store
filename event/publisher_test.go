package event

import (
	"context"
	"reflect"
	"testing"
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

	// type Event22 struct{ Val string }

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
		Set(Event2{})

	if want, got := (&Event2{}).EvDests(), publishDestinations(ctx, Event2{}); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
}
