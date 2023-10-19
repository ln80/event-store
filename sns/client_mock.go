package sns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type clientMock struct {
	err    error
	traces map[string][]*sns.PublishInput
}

// var _ ClientAPI = &clientMock{}

func (c *clientMock) Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.traces == nil {
		c.traces = make(map[string][]*sns.PublishInput)
	}
	if _, ok := c.traces[*params.TopicArn]; !ok {
		c.traces[*params.TopicArn] = []*sns.PublishInput{}
	}
	c.traces[*params.TopicArn] = append(c.traces[*params.TopicArn], params)

	return &sns.PublishOutput{}, nil
}
