package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/stack/elastic/utils"
)

var (
	indexer dynamodb.Indexer
)

func init() {
	table := os.Getenv("DYNAMODB_TABLE")
	if table == "" {
		log.Fatalf(`
			missed env params:
				DYNAMODB_TABLE: %v,
		`, table)
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
	)
	if err != nil {
		log.Fatal(err)
	}

	indexer = dynamodb.NewIndexer(utils.InitDynamodbClient(cfg), table)
}

func main() {

	// logger.Default().Info("boom")
	lambda.Start(makeHandler(indexer))
}
