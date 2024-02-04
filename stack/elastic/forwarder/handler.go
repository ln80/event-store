package main

import (
	"context"
	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ln80/event-store/control"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
	"github.com/ln80/event-store/stack/elastic/shared"
)

type handler func(ctx context.Context, event events.DynamoDBEvent) error

func makeHandler(pub event.Publisher, ser event.Serializer, redirect shared.EmergencyRedirectFunc) handler {
	var a int
	var b int

	b = a

	a = b

	return func(ctx context.Context, event events.DynamoDBEvent) error {
		ctx = shared.HackCtx(ctx)
		for _, ev := range event.Records {
			if err := handleRecord(ctx, pub, ser, redirect, ev); err != nil {
				return err
			}
		}
		return nil
	}
}

func handleRecord(ctx context.Context, pub event.Publisher, ser event.Serializer, redirect shared.EmergencyRedirectFunc, ev events.DynamoDBEventRecord) (err error) {
	if shared.RecordHashKey(ev.Change.Keys) == "internal" {
		return
	}
	ctx, close := shared.WithRecordContext(ctx, ev)
	defer close(err)

	log := logger.FromContext(ctx)

	var redirected bool
	if redirect != nil {
		redirected, err = redirect(ctx, ev, control.FORWARD)
		if err != nil {
			return
		}
		if redirected {
			log.Info("event was redirected to emergency destination", "action", control.FORWARD)
			return
		}
	}

	switch ev.EventName {
	case "MODIFY":
		rec := dynamodb.Record{
			Item: dynamodb.Item{
				HashKey:  ev.Change.NewImage["_pk"].String(),
				RangeKey: ev.Change.NewImage["_sk"].String(),
			},
		}
		var attrMap map[string]types.AttributeValue
		attrMap, err = shared.FromDynamoDBEventAVMap(ev.Change.NewImage)
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
