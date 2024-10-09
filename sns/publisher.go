package sns

import (
	"context"
	"encoding/base64"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/json"
	"github.com/ln80/event-store/logger"
)

type PublisherConfig struct {
	Serializer event.Serializer
	// BatchEnabled if "true" the publisher groups record events in a single SNS message.
	BatchRecordEnabled bool
}

type Publisher struct {
	api   ClientAPI
	topic string
	*PublisherConfig
}

var _ event.Publisher = &Publisher{}

func NewPublisher(api ClientAPI, topic string, opts ...func(cfg *PublisherConfig)) *Publisher {
	if api == nil {
		panic("event publisher invalid SNS client: nil value")
	}
	pub := &Publisher{
		api:   api,
		topic: topic,
		PublisherConfig: &PublisherConfig{
			Serializer:         json.NewEventSerializer(""),
			BatchRecordEnabled: false,
		},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(pub.PublisherConfig)
	}

	return pub
}

var _ event.Publisher = &Publisher{}

// Publish implements event.Publisher
func (p *Publisher) Publish(ctx context.Context, events []event.Envelope) (err error) {
	if len(events) == 0 || p.topic == "" {
		return
	}

	// validate that events belong to the same global stream and are in consecutive sequence
	if err = event.Stream(events).Validate(func(v *event.Validation) {
		v.GlobalStream = true
	}); err != nil {
		return
	}

	if p.BatchRecordEnabled {
		err = p.publishRecord(ctx, events)
		return
	}
	err = p.publish(ctx, events)
	return
}

func (p *Publisher) publish(ctx context.Context, events []event.Envelope) error {
	log := logger.FromContext(ctx).WithName("sns")
	log.V(1).Info("Do SNS Publish", "count", len(events))
	// Note using the SNS built-in PublishBatch method might be performant, but it presents challenges to handle:
	// - batch message size limit is the same as the size a single message using Publish method
	// - The retry logic of partially failed batch might corrupt the publishing order.
	for _, evt := range events {
		msg, err := p.Serializer.MarshalEvent(ctx, evt)
		if err != nil {
			return err
		}
		log.V(1).Info("Do SNS Publish", "types", evt.Type())
		body := base64.StdEncoding.EncodeToString(msg)
		attributes := map[string]types.MessageAttributeValue{
			"StmID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(evt.StreamID()),
			},
			"Types": {
				DataType:    aws.String("String"),
				StringValue: aws.String(evt.Type()),
			},
		}
		if dests := evt.Dests(); len(dests) > 0 {
			attributes["Dests"] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(strings.Join(dests, ",")),
			}
		}

		if _, err = p.api.Publish(ctx, &sns.PublishInput{
			Message:                aws.String(body),
			TopicArn:               aws.String(p.topic),
			MessageAttributes:      attributes,
			MessageDeduplicationId: aws.String(evt.GlobalStreamID() + "@" + evt.GlobalVersion().String()),
			MessageGroupId:         aws.String(evt.GlobalStreamID()),
		}); err != nil {
			return err
		}
	}

	return nil

}

func (p *Publisher) publishRecord(ctx context.Context, events []event.Envelope) error {
	log := logger.FromContext(ctx).WithName("sns")
	log.V(1).Info("Do SNS Publish BATCH", "count", len(events))

	record, err := p.Serializer.MarshalEventBatch(ctx, events)
	if err != nil {
		return err
	}
	body := base64.StdEncoding.EncodeToString(record)

	_types := make([]string, 0)
	for _, evt := range events {
		_types = append(_types, evt.Type())
	}
	// slices.Sort(_types)
	sort.Slice(_types, func(i, j int) bool {
		return _types[i] <= _types[j]
	})

	// _types = slices.Compact(_types)
	_types = p.compact(_types)

	dests := make([]string, 0)
	for _, evt := range events {
		dests = append(dests, evt.Dests()...)
	}

	sort.Slice(dests, func(a, b int) bool {
		return dests[a] <= dests[b]
	})
	// slices.Sort(dests)

	// dests = slices.Compact(dests)
	dests = p.compact(dests)

	attributes := map[string]types.MessageAttributeValue{
		"Types": {
			DataType:    aws.String("String"),
			StringValue: aws.String(strings.Join(_types, ",")),
		},
	}
	if len(dests) > 0 {
		attributes["Dests"] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(strings.Join(dests, ",")),
		}
	}
	_, err = p.api.Publish(ctx, &sns.PublishInput{
		Message:                aws.String(body),
		TopicArn:               aws.String(p.topic),
		MessageAttributes:      attributes,
		MessageDeduplicationId: aws.String(events[0].GlobalStreamID() + "@" + events[0].GlobalVersion().String()),
		MessageGroupId:         aws.String(events[0].GlobalStreamID()),
	})

	return err
}

// compact has to be deprecated in favor of slices.Compact func once go 1.21 is supported.
func (p *Publisher) compact(s []string) []string {
	if len(s) < 2 {
		return s
	}
	i := 1
	for k := 1; k < len(s); k++ {
		if s[k] != s[k-1] {
			if i != k {
				s[i] = s[k]
			}
			i++
		}
	}
	return s[:i]
}
