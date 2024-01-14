package main

import (
	"context"
	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
	"github.com/ln80/event-store/stack/elastic/utils"
)

type handler func(ctx context.Context, event events.DynamoDBEvent) error

func makeHandler(pub event.Publisher, ser event.Serializer) handler {
	return func(ctx context.Context, event events.DynamoDBEvent) error {
		ctx = utils.HackCtx(ctx)
		for _, ev := range event.Records {
			if err := handleRecord(ctx, pub, ser, ev); err != nil {
				return err
			}
		}
		return nil
	}
}

func handleRecord(ctx context.Context, pub event.Publisher, ser event.Serializer, ev events.DynamoDBEventRecord) (err error) {
	if utils.RecordHashKey(ev.Change.Keys) == "internal" {
		return
	}
	raw := ev.Change.NewImage

	log := logger.WithStream(logger.Default(), raw[dynamodb.GIDAttribute].String()).
		WithValues("record", utils.RecordKeys(ev.Change.Keys))
	if t, ok := raw[dynamodb.TraceIDAttribute]; ok {
		var (
			seg *xray.Segment
		)
		ctx, seg = utils.WithTracing(ctx, t.String(), "Forwarder",
			"gstmID", raw[dynamodb.GIDAttribute].String(),
			"gver", raw[dynamodb.GVerAttribute].String(),
		)
		if seg != nil {
			log = logger.WithTrace(log, seg.TraceID)
			defer seg.Close(err)
		}
	}
	ctx = logger.NewContext(ctx, log)

	switch ev.EventName {
	case "MODIFY":
		rec := dynamodb.Record{
			Item: dynamodb.Item{
				HashKey:  ev.Change.NewImage["_pk"].String(),
				RangeKey: ev.Change.NewImage["_sk"].String(),
			},
		}
		var attrMap map[string]types.AttributeValue
		attrMap, err = utils.FromDynamoDBEventAVMap(ev.Change.NewImage)
		if err != nil {
			return
		}
		if err = attributevalue.UnmarshalMap(attrMap, &rec); err != nil {
			return
		}
		if rec.HashKey == "" {
			return
		}
		var events []event.Envelope
		events, err = dynamodb.UnpackRecord(ctx, rec, ser)
		if err != nil {
			return
		}
		if err = pub.Publish(ctx, events); err != nil {
			return
		}
	default:
		log.Error(errors.New("unauthorized action"), "action", ev.EventName)
	}

	return

}
