package json

import (
	"context"
	"errors"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
)

func BenchmarkSerializer(b *testing.B) {
	testutil.RegisterEvent("")

	ser := NewEventSerializer("")
	testutil.BenchmarkSerializer(b, ser)
}

func TestSerializer(t *testing.T) {
	ctx := context.Background()

	// Note register event is mandatory for json serializer

	t.Run("with namespace", func(t *testing.T) {
		testutil.RegisterEvent("service1")

		ctx := context.WithValue(ctx, event.ContextNamespaceKey, "service1")
		ser := NewEventSerializer("service1")
		testutil.TestSerializer(t, ctx, ser)
	})

	t.Run("without namespace", func(t *testing.T) {
		testutil.RegisterEvent("")

		ser := NewEventSerializer("")
		testutil.TestSerializer(t, ctx, ser)
	})
}

func TestSerializer_WithError(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	t.Run("empty event", func(t *testing.T) {

		ser := NewEventSerializer("")

		_, _, err := ser.MarshalEvent(ctx, nil)
		if want, got := event.ErrMarshalEmptyEvent, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		_, _, err = ser.MarshalEventBatch(ctx, nil)
		if want, got := event.ErrMarshalEmptyEvent, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})
}
