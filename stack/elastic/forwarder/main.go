package main

import (
	"context"
	"errors"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/ln80/event-store/avro"
	"github.com/ln80/event-store/avro/glue"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
	"github.com/ln80/event-store/json"
	"github.com/ln80/event-store/sns"
	"github.com/ln80/event-store/stack/elastic/utils"
)

var (
	pub        event.Publisher
	serializer event.Serializer
)

func init() {
	utils.InitLogger("elastic")

	cfg := utils.MustLoadConfig()

	format := utils.MustGetenv("ENCODING_FORMAT")
	switch format {
	case "JSON":
		serializer = json.NewEventSerializer("")
	case "AVRO":
		serializer = avro.NewEventSerializer(
			context.Background(),
			glue.NewRegistry(
				utils.MustGetenv("AVRO_GLUE_SCHEMA_REGISTRY"),
				utils.InitGlueClient(cfg),
			),
			func(esc *avro.EventSerializerConfig) {
				esc.SkipCurrentSchema = true
			},
		)
	default:
		logger.Default().Error(errors.New("invalid encoding format: "+format), "")
		os.Exit(1)
	}

	pub = sns.NewPublisher(utils.InitSNSClient(cfg), utils.MustGetenv("SNS_TOPIC"), func(cfg *sns.PublisherConfig) {
		cfg.BatchRecordEnabled = utils.MustGetenv("BATCH_RECORD_ENABLED") == "true"
		cfg.Serializer = serializer
	})
}

func main() {
	lambda.Start(makeHandler(pub, serializer))
}
