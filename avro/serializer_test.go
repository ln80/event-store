package avro

import (
	"context"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	avro_glue "github.com/ln80/event-store/avro/glue"
	avro_memory "github.com/ln80/event-store/avro/memory"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
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

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("eu-west-1"),
	)
	if err != nil {
		log.Fatal(err)
	}
	client := glue.NewFromConfig(cfg)

	t.Run("with namespace", func(t *testing.T) {
		event.NewRegister("service1").
			Set(&testutil.Event1{}, event.WithEventAliases("EventA", "EventAA")).
			Set(&testutil.Event2{})

		ctx = context.WithValue(ctx, event.ContextNamespaceKey, "service1")

		// registry := avro_memory.NewRegistry()
		registry := avro_glue.NewRegistry("test_1", client)

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
