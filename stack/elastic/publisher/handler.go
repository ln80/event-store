package main

import (
	"context"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/stack/elastic/utils"
)

type handler func(ctx context.Context, event events.DynamoDBEvent) error

func makeHandler(pub event.Publisher, ser event.Serializer) handler {
	return func(ctx context.Context, event events.DynamoDBEvent) error {
		ctx = utils.HackCtx(ctx)
		for _, ev := range event.Records {
			if key := ev.Change.NewImage["_pk"].String(); key == "internal" {
				continue
			}

			switch ev.EventName {
			case "MODIFY":
				rec := dynamodb.Record{
					Item: dynamodb.Item{
						HashKey:  ev.Change.NewImage["_pk"].String(),
						RangeKey: ev.Change.NewImage["_sk"].String(),
					},
				}
				attrMap, err := utils.FromDynamoDBEventAVMap(ev.Change.NewImage)
				if err != nil {
					return err
				}
				if err := attributevalue.UnmarshalMap(attrMap, &rec); err != nil {
					return err
				}
				if rec.HashKey == "" {
					continue
				}
				events, err := dynamodb.UnmarshalRecord(rec, ser)
				if err != nil {
					return err
				}

				if err := pub.Publish(ctx, events); err != nil {
					return err
				}
			default:
				log.Printf("unauthorized action: %s change: %v", ev.EventName, ev.Change)
			}
		}

		return nil
	}
}
