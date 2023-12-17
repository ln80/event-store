package utils

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func InitDynamodbClient(cfg aws.Config) *dynamodb.Client {
	return dynamodb.NewFromConfig(cfg)
}

func InitSNSClient(cfg aws.Config) *sns.Client {
	return sns.NewFromConfig(cfg)
}

func InitGlueClient(cfg aws.Config) *glue.Client {
	return glue.NewFromConfig(cfg)
}

// HackCtx to workaround https://github.com/aws/aws-sam-cli/issues/2510
// seems be fixed in sam cli 1.37.0
func HackCtx(ctx context.Context) context.Context {
	if os.Getenv("AWS_SAM_LOCAL") == "true" {
		return context.Background()
	}

	return ctx
}
