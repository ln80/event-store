package dynamodb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ln80/aws-toolkit-go/dynamodbtest"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/eventtest"
	"github.com/ln80/event-store/json"
)

func TestEventIndexer(t *testing.T) {
	ctx := context.Background()

	ser := json.NewEventSerializer("")

	dynamodbtest.WithTables(t, dbsvc, dynamodbtest.TableConfig{
		TableList: dynamodbtest.TableList(StoreCreateTableInput("table")),
	}, func(dbsvc *dynamodb.Client, tableNames []string) {
		table := tableNames[0]

		getRecord := func(id event.StreamID) (Record, error) {
			expr, _ := expression.
				NewBuilder().
				WithKeyCondition(
					expression.Key(HashKey).
						Equal(expression.Value(recordHashKey(event.NewStreamID(id.GlobalID())))).And(
						expression.Key(RangeKey).
							BeginsWith(strings.Join(id.Parts(), event.StreamIDPartsDelimiter) + "@t_")),
				).
				Build()
			out, err := dbsvc.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String(table),
				KeyConditionExpression:    expr.KeyCondition(),
				FilterExpression:          expr.Filter(),
				ProjectionExpression:      expr.Projection(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
				ConsistentRead:            aws.Bool(true),
				ScanIndexForward:          aws.Bool(false),
				Limit:                     aws.Int32(1),
			})
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			rec := Record{}
			if err = attributevalue.UnmarshalMap(out.Items[0], &rec); err != nil {
				t.Fatal("expect err be nil, got", err)
			}

			return rec, nil
		}
		t.Run("basic", func(t *testing.T) {
			globalID := event.UID().String()

			indexer := NewIndexer(dbsvc, table)

			store := NewEventStore(dbsvc, table)

			evtAt := time.Now()
			okEnvs_1 := event.Wrap(ctx, event.NewStreamID(globalID), []any{
				&eventtest.Event1{
					Val: "test content a 1",
				},
				&eventtest.Event2{
					Val: "test content a 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Second)
			})

			if err := store.Append(ctx, event.NewStreamID(globalID), okEnvs_1); err != nil {
				t.Fatal("expect err be nil, got", err)
			}

			err := indexer.Index(ctx, makeRecord(ser, globalID, okEnvs_1))
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			rr, err := getRecord(event.NewStreamID(globalID))
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			events, err := UnpackRecord(ctx, rr, ser)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			for i, evt := range events {
				if want, got := event.VersionMin, evt.GlobalVersion(); !got.Equal(want.Add(0, uint8(i))) {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			}

			// force in-memory cache sequence reset
			indexer = NewIndexer(dbsvc, table)

			// assert global sequence is incremented
			okEnvs_2 := event.Wrap(ctx, event.NewStreamID(globalID), []any{
				&eventtest.Event1{
					Val: "test content b 1",
				},
				&eventtest.Event2{
					Val: "test content b 2",
				},
			}, func(env event.RWEnvelope) {
				env.SetAt(evtAt)
				evtAt = evtAt.Add(1 * time.Second)
			})

			if err = store.Append(ctx, event.NewStreamID(globalID), okEnvs_2); err != nil {
				t.Fatal("expect err be nil, got", err)
			}

			rec := makeRecord(ser, globalID, okEnvs_2)
			err = indexer.Index(ctx, rec)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			rr, err = getRecord(event.NewStreamID(globalID))
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			events, err = UnpackRecord(ctx, rr, ser)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			for i, evt := range events {
				if want, got := event.VersionMin.Incr(), evt.GlobalVersion(); !got.Equal(want.Add(0, uint8(i))) {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			}

			// test idempotency behavior
			// use new indexer instance to clear any in-memory cache
			indexer = NewIndexer(dbsvc, table)
			// re-index the same record, make sure stream sequence remains the same
			err = indexer.Index(ctx, rec)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			rr, err = getRecord(event.NewStreamID(globalID))
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			events, err = UnpackRecord(ctx, rr, ser)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			for i, evt := range events {
				if want, got := event.VersionMin.Incr(), evt.GlobalVersion(); !got.Equal(want.Add(0, uint8(i))) {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			}

			// assert idempotency behavior considering the in-memory cache
			err = indexer.Index(ctx, rec)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			rr, err = getRecord(event.NewStreamID(globalID))
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			events, err = UnpackRecord(ctx, rr, ser)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			for i, evt := range events {
				if want, got := event.VersionMin.Incr(), evt.GlobalVersion(); !got.Equal(want.Add(0, uint8(i))) {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			}
		})
	})
}
