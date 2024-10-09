package avro

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/eventtest"
)

func BenchmarkSerializer(b *testing.B) {
	eventtest.RegisterEvent("")

	ctx := context.Background()

	dir := b.TempDir()
	registry := NewFSRegistry(os.DirFS(dir), func(fc *FSRegistryConfig) { fc.PersistDir = dir })

	ser := NewEventSerializer(ctx, registry)
	eventtest.BenchmarkSerializer(b, ser)
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
			Set(&eventtest.Event1{}, event.WithAliases("EventA", "EventAA")).
			Set(&eventtest.Event2{})

		ctx = context.WithValue(ctx, event.ContextNamespaceKey, "service1")

		// f := afero.NewMemMapFs()

		dir := t.TempDir()
		registry := NewFSRegistry(os.DirFS(dir), func(fc *FSRegistryConfig) { fc.PersistDir = dir })
		// registry := avro_glue.NewRegistry("test_1", client)

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.Namespace = "service1"
			esc.PersistCurrentSchema = true
		})

		eventtest.TestSerializer(t, ctx, ser)
	})

	t.Run("without namespace", func(t *testing.T) {
		eventtest.RegisterEvent("")

		dir := t.TempDir()
		registry := NewFSRegistry(os.DirFS(dir), func(fc *FSRegistryConfig) { fc.PersistDir = dir })

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.PersistCurrentSchema = true
		})

		eventtest.TestSerializer(t, ctx, ser)
	})
}

func TestSerializer_WithError(t *testing.T) {
	eventtest.RegisterEvent("")

	ctx := context.Background()

	t.Run("readonly", func(t *testing.T) {

		dir := t.TempDir()
		registry := NewFSRegistry(os.DirFS(dir), func(fc *FSRegistryConfig) { fc.PersistDir = dir })

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.ReadOnly = true
			esc.PersistCurrentSchema = true
		})

		_, err := ser.MarshalEvent(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), eventtest.GenEvents(1))[0])
		if want, got := ErrReadOnlyModeEnabled, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		_, err = ser.MarshalEventBatch(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), eventtest.GenEvents(1)))
		if want, got := ErrReadOnlyModeEnabled, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})

	t.Run("empty event", func(t *testing.T) {

		dir := t.TempDir()
		registry := NewFSRegistry(os.DirFS(dir), func(fc *FSRegistryConfig) { fc.PersistDir = dir })

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.PersistCurrentSchema = true
		})

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

		dir := t.TempDir()
		registry := NewFSRegistry(os.DirFS(dir), func(fc *FSRegistryConfig) { fc.PersistDir = dir })

		ser := NewEventSerializer(ctx, registry, func(esc *EventSerializerConfig) {
			esc.SkipCurrentSchema = true
		})

		_, err := ser.MarshalEvent(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), eventtest.GenEvents(1))[0])
		if want, got := event.ErrMarshalEventFailed, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		_, err = ser.MarshalEventBatch(ctx, event.Wrap(ctx, event.NewStreamID("service1", "id"), eventtest.GenEvents(1)))
		if want, got := event.ErrMarshalEventFailed, err; !errors.Is(got, want) {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})
}
