package main

import (
	"context"
	"errors"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/ln80/event-store/avro"
	"github.com/ln80/event-store/avro/glue"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/json"
	"github.com/ln80/event-store/sns"
	"github.com/ln80/event-store/stack/elastic/utils"
)

var (
	pub        event.Publisher
	serializer event.Serializer
)

func init() {
	topic := os.Getenv("SNS_TOPIC")
	if topic == "" {
		log.Fatalf(`
			missed env params:
			SNS_TOPIC: %v,
		`, topic)
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
	)
	if err != nil {
		log.Fatal(err)
	}

	format := os.Getenv("ENCODING_FORMAT")
	switch format {
	case "JSON":
		serializer = json.NewEventSerializer("")
	case "AVRO":
		registry := glue.NewRegistry(
			os.Getenv("GlUE_SCHEMA_REGISTRY"),
			utils.InitGlueClient(cfg),
		)
		serializer = avro.NewEventSerializer(context.Background(), registry, func(esc *avro.EventSerializerConfig) {
			// do not lookup for current schema from code;
			// event registry at publisher-level is not aware of the domain events
			esc.SkipCurrentSchema = true
		})
	default:
		log.Fatal(errors.New("invalid encoding format: " + format))
	}

	pub = sns.NewPublisher(utils.InitSNSClient(cfg), topic, func(cfg *sns.PublisherConfig) {
		cfg.BatchRecordEnabled = os.Getenv("BATCH_RECORD_ENABLED") == "true"
		cfg.Serializer = serializer
	})
}

func main() {
	lambda.Start(makeHandler(pub, serializer))
}
