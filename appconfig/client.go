package appconfig

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appconfigdata"
)

type ClientAPI interface {
	StartConfigurationSession(ctx context.Context, params *appconfigdata.StartConfigurationSessionInput, optFns ...func(*appconfigdata.Options)) (*appconfigdata.StartConfigurationSessionOutput, error)
	GetLatestConfiguration(ctx context.Context, params *appconfigdata.GetLatestConfigurationInput, optFns ...func(*appconfigdata.Options)) (*appconfigdata.GetLatestConfigurationOutput, error)
}

func NewClient(cfg aws.Config) ClientAPI {
	return appconfigdata.NewFromConfig(cfg)
}
