package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/json"
	"github.com/ln80/event-store/sns"
	"github.com/ln80/event-store/stack/elastic/utils"
)

var (
	pub event.Publisher
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

	pub = sns.NewPublisher(utils.InitSNSClient(cfg), topic, func(cfg *sns.PublisherConfig) {
		cfg.BatchRecordEnabled = os.Getenv("BATCH_RECORD_ENABLED") == "true"
	})
}

func main() {
	lambda.Start(makeHandler(pub, json.NewEventSerializer("")))
}
