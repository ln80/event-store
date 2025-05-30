package json

import (
	"context"
	"errors"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/eventtest"
)

func BenchmarkSerializer(b *testing.B) {
	eventtest.RegisterEvent("")

	ser := NewEventSerializer("")
	eventtest.BenchmarkSerializer(b, ser)
}

func TestSerializer(t *testing.T) {
	ctx := context.Background()

	// Note register event is mandatory for json serializer

	t.Run("with namespace", func(t *testing.T) {
		eventtest.RegisterEvent("service1")

		ctx := context.WithValue(ctx, event.ContextNamespaceKey, "service1")
		ser := NewEventSerializer("service1")
		eventtest.TestSerializer(t, ctx, ser)
	})

	t.Run("without namespace", func(t *testing.T) {
		eventtest.RegisterEvent("")

		ser := NewEventSerializer("")
		eventtest.TestSerializer(t, ctx, ser)
	})
}

func TestSerializer_WithError(t *testing.T) {
	eventtest.RegisterEvent("")

	ctx := context.Background()

	t.Run("empty event", func(t *testing.T) {

		ser := NewEventSerializer("")

		_, err := ser.MarshalEvent(ctx, nil)
		if want, got := event.ErrMarshalEmptyEvent, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		_, err = ser.MarshalEventBatch(ctx, nil)
		if want, got := event.ErrMarshalEmptyEvent, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})
}
