package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/ln80/event-store/control"
	"github.com/ln80/event-store/dynamodb"
	_sns "github.com/ln80/event-store/sns"
)

var (
	ErrEmergencyRedirectFailed = errors.New("emergency redirect failed")
)

type EmergencyRedirectFunc func(ctx context.Context, ev events.DynamoDBEventRecord, action control.Feature) (redirected bool, err error)

func MakeEmergencyRedirect(client _sns.ClientAPI, topic string, feature control.FeatureToggler) EmergencyRedirectFunc {
	return func(ctx context.Context, ev events.DynamoDBEventRecord, action control.Feature) (redirected bool, err error) {
		if feature == nil {
			return
		}

		gstmID := ev.Change.NewImage[dynamodb.GIDAttribute].String()

		defer func() {
			if err != nil {
				err = fmt.Errorf("%w: %v stream=%s", ErrEmergencyRedirectFailed, err, gstmID)
			}
		}()

		toggles, err := feature.Get(ctx, gstmID)
		if err != nil {
			return
		}
		if err = toggles.Enabled(action); err == nil {
			return
		}

		var b []byte
		b, err = json.Marshal(ev)
		if err != nil {
			return
		}

		_, err = client.Publish(ctx, &sns.PublishInput{
			Message: aws.String(string(b)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"action": {
					DataType:    aws.String("String"),
					StringValue: aws.String(string(action)),
				},
			},
			MessageDeduplicationId: aws.String(ev.EventID),
			MessageGroupId:         aws.String(gstmID),
			TopicArn:               aws.String(topic),
		})
		if err != nil {
			return
		}

		redirected = true
		return
	}
}
