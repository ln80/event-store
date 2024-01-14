package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/stack/elastic/utils"
)

var (
	indexer dynamodb.Indexer
)

func init() {
	utils.InitLogger("elastic")

	cfg := utils.MustLoadConfig()

	indexer = dynamodb.NewIndexer(utils.InitDynamodbClient(cfg), utils.MustGetenv("DYNAMODB_TABLE"))
}

func main() {
	lambda.Start(makeHandler(indexer))
}
