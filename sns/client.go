package sns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type ClientAPI interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)

	// PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

func init() {

	// c := sns.New()

	// c.PublishBatch()
}
