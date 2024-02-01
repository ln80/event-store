package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	appconfig "github.com/ln80/event-store/appconfig"
	"github.com/ln80/event-store/control"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/internal/logger"
	"github.com/ln80/event-store/stack/elastic/shared"
)

var (
	indexer dynamodb.Indexer

	emergencyRedirect shared.EmergencyRedirectFunc
)

func main() {
	ctx := context.Background()

	shared.InitLogger("elastic")

	cfg := shared.MustLoadConfig()

	indexer = dynamodb.NewIndexer(shared.InitDynamodbClient(cfg), shared.MustGetenv("DYNAMODB_TABLE"))

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

	lambda.Start(makeHandler(indexer, emergencyRedirect))
}
