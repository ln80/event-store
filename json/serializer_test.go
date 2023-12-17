package json

import (
	"context"
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
