package avro

import (
	"context"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
	test_suite "github.com/ln80/event-store/testutil/suite"
)

func BenchmarkSerializer(b *testing.B) {
	type Container struct {
		E1 *testutil.Event1
		E2 *testutil.Event2
	}

	ser := NewEventSerializer[Container]("")
	test_suite.BenchmarkSerializer(b, ser)
}

func TestSerializer(t *testing.T) {
	type Container struct {
		E1 *testutil.Event1
		E2 *testutil.Event2
	}

	ctx := context.Background()

	t.Run("with namespace", func(t *testing.T) {
		ctx = context.WithValue(ctx, event.ContextNamespaceKey, "service1")

		ser := NewEventSerializer[Container]("service1")

		test_suite.TestSerializer(t, ctx, ser)
	})

	t.Run("without namespace", func(t *testing.T) {
		testutil.RegisterEvent("")

		ser := NewEventSerializer[Container]("")

		test_suite.TestSerializer(t, ctx, ser)
	})
}
