package main

import (
	"context"
	"errors"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	appconfig "github.com/ln80/event-store/appconfig"
	"github.com/ln80/event-store/avro"
	"github.com/ln80/event-store/control"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
	"github.com/ln80/event-store/json"
	"github.com/ln80/event-store/sns"
	"github.com/ln80/event-store/stack/elastic/shared"
)

var (
	publisher event.Publisher

	serializer event.Serializer

	emergencyRedirect shared.EmergencyRedirectFunc
)

func main() {
	ctx := context.Background()

	shared.InitLogger("elastic")

	cfg := shared.MustLoadConfig()

	format := shared.MustGetenv("ENCODING_FORMAT")
	switch format {
	case "JSON":
		serializer = json.NewEventSerializer("")
	case "AVRO":
		serializer = avro.NewEventSerializer(
			context.Background(),
			avro.NewGlueRegistry(
				shared.MustGetenv("AVRO_GLUE_SCHEMA_REGISTRY"),
				shared.InitGlueClient(cfg),
			),
			func(esc *avro.EventSerializerConfig) {
				esc.Namespace = ""
				esc.SkipCurrentSchema = true
			},
		)
	default:
		logger.Default().Error(errors.New("invalid encoding format: "+format), "")
		os.Exit(1)
	}

	publisher = sns.NewPublisher(shared.InitSNSClient(cfg), shared.MustGetenv("SNS_TOPIC"), func(cfg *sns.PublisherConfig) {
		cfg.BatchRecordEnabled = shared.MustGetenv("BATCH_RECORD_ENABLED") == "true"
		cfg.Serializer = serializer
	})

	found, application, environment, configuration := shared.MustParseConfigPathEnv()
	if found {
		loader, err := appconfig.NewLoader(ctx, shared.InitAppConfigClient(cfg), func(lc *appconfig.LoaderConfig) {
			lc.Application = application
			lc.Environment = environment
			lc.Configuration = configuration
		})
		if err != nil {
			logger.Default().Error(err, "")
			os.Exit(1)
		}
		featureToggler, err := control.NewFeatureToggler(ctx, loader)
		if err != nil {
			logger.Default().Error(err, "")
			os.Exit(1)
		}

		emergencyRedirect = shared.MakeEmergencyRedirect(
			shared.InitSNSClient(cfg),
			shared.MustGetenv("EMERGENCY_REDIRECT_FIFO_TOPIC"),
			featureToggler,
		)
	}

	lambda.Start(makeHandler(publisher, serializer, emergencyRedirect))
}
