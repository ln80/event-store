package utils

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-xray-sdk-go/instrumentation/awsv2"
	"github.com/ln80/event-store/internal/logger"
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

func MustLoadConfig() aws.Config {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
	)
	if err != nil {
		logger.Default().Error(err, "failed to load AWS config")
		os.Exit(1)
	}

	awsv2.AWSV2Instrumentor(&cfg.APIOptions)

	return cfg
}
