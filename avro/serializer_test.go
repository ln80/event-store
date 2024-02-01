package avro

import (
	"context"
	"errors"
	"testing"

	avro_memory "github.com/ln80/event-store/avro/memory"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/testutil"
)

func BenchmarkSerializer(b *testing.B) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	registry := avro_memory.NewRegistry()

	ser := NewEventSerializer(ctx, registry)
	testutil.BenchmarkSerializer(b, ser)
}

func TestSerializer(t *testing.T) {
	ctx := context.Background()

	// cfg, err := config.LoadDefaultConfig(
	// 	context.Background(),
	// 	config.WithRegion("eu-west-1"),
	// )
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// client := glue.NewFromConfig(cfg)

	t.Run("with namespace", func(t *testing.T) {
		event.NewRegister("service1").
			Set(&testutil.Event1{}, event.WithAliases("EventA", "EventAA")).
			Set(&testutil.Event2{})

		ctx = context.WithValue(ctx, event.ContextNamespaceKey, "service1")

		registry := avro_memory.NewRegistry()
		// registry := avro_glue.NewRegistry("test_1", client)

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.Namespace = "service1"
		})

		testutil.TestSerializer(t, ctx, ser)
	})

	t.Run("without namespace", func(t *testing.T) {
		testutil.RegisterEvent("")

		registry := avro_memory.NewRegistry()
		ser := NewEventSerializer(ctx, registry)

		testutil.TestSerializer(t, ctx, ser)
	})
}

func TestSerializer_WithError(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	t.Run("readonly", func(t *testing.T) {
		registry := avro_memory.NewRegistry()

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.ReadOnly = true
		})

		_, err := ser.MarshalEvent(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), testutil.GenEvents(1))[0])
		if want, got := ErrReadOnlyModeEnabled, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		_, err = ser.MarshalEventBatch(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), testutil.GenEvents(1)))
		if want, got := ErrReadOnlyModeEnabled, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})

	t.Run("empty event", func(t *testing.T) {
		registry := avro_memory.NewRegistry()

		ser := NewEventSerializer(ctx, registry)

		_, err := ser.MarshalEvent(ctx, nil)
		if want, got := event.ErrMarshalEmptyEvent, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		_, err = ser.MarshalEventBatch(ctx, nil)
		if want, got := event.ErrMarshalEmptyEvent, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})

	t.Run("skip current schema", func(t *testing.T) {
		registry := avro_memory.NewRegistry()

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.SkipCurrentSchema = true
		})

		_, err := ser.MarshalEvent(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), testutil.GenEvents(1))[0])
		if want, got := event.ErrMarshalEventFailed, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		_, err = ser.MarshalEventBatch(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), testutil.GenEvents(1)))
		if want, got := event.ErrMarshalEventFailed, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})
}
