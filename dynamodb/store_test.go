package dynamodb

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ln80/aws-toolkit-go/dynamodbtest"
	"github.com/ln80/event-store/event"
	event_errors "github.com/ln80/event-store/event/errors"
	"github.com/ln80/event-store/event/sourcing"
	"github.com/ln80/event-store/eventtest"
)

func TestNewEventStore(t *testing.T) {
	tcs := []struct {
		dbsvc AdminAPI
		table string
		ok    bool
	}{
		{
			dbsvc: nil,
			table: "table name",
			ok:    false,
		},
		{
			dbsvc: dbsvc,
			table: "",
			ok:    false,
		},
		{
			dbsvc: nil,
			table: "",
			ok:    false,
		},
	}

	for i, tc := range tcs {
		t.Run("tc:"+strconv.Itoa(i), func(t *testing.T) {
			defer func() {
				r := recover()
				if tc.ok {
					if r != nil {
						t.Fatal("expect to not panic, got", r)
					}
				} else {
					if r == nil {
						t.Fatal("expect to panic")
					}
				}
			}()

			NewEventStore(tc.dbsvc, tc.table, nil, nil)
		})
	}
}

func TestEventStore_WithTx(t *testing.T) {
	ctx := context.Background()

	addItem := func(table, hashKey, rangeKey string) any {
		other := Item{
			HashKey:  hashKey,
			RangeKey: rangeKey,
		}
		r, _ := attributevalue.MarshalMap(other)
		expr, _ := expression.
			NewBuilder().
			WithCondition(
				expression.AttributeNotExists(
					expression.Name(RangeKey),
				),
			).Build()
		input := &dynamodb.PutItemInput{
			TableName:                 aws.String(table),
			Item:                      r,
			ConditionExpression:       expr.Condition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),

			ReturnConsumedCapacity:              types.ReturnConsumedCapacityIndexes,
			ReturnItemCollectionMetrics:         types.ReturnItemCollectionMetricsSize,
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}
		return input
	}

	dynamodbtest.WithTables(t, dbsvc, dynamodbtest.TableConfig{
		TableList: dynamodbtest.TableList(StoreCreateTableInput("table")),
	}, func(dbsvc *dynamodb.Client, tableNames []string) {
		table := tableNames[0]
		store := NewEventStore(dbsvc, table)

		t.Run("conflict", func(t *testing.T) {
			streamID := event.NewStreamID(event.UID().String())
			stm := sourcing.Wrap(ctx, streamID, event.VersionZero, eventtest.GenEvents(5))
			err := store.AppendToStream(ctx, stm, func(opt *event.AppendConfig) {
				opt.AddToTx = func(ctx context.Context) (items []any) {
					item := addItem(table,
						recordHashKey(streamID),
						recordRangeKeyWithVersion(streamID, event.VersionZero.Incr()),
					)
					return []any{item}
				}
			})
			if want, got := event.ErrAppendEventsConflict, err; !errors.Is(got, want) {
				t.Fatalf("expect %v,%v be equals", want, got)
			}
			if ok, target := event_errors.ErrAs[*types.TransactionCanceledException](err); !ok {
				t.Fatalf("expect %v as %T", err, target)
			}
		})
		t.Run("success", func(t *testing.T) {
			streamID := event.NewStreamID(event.UID().String())
			stm := sourcing.Wrap(ctx, streamID, event.VersionZero, eventtest.GenEvents(5))
			if err := store.AppendToStream(ctx, stm, func(opt *event.AppendConfig) {
				items := []any{
					addItem(table,
						recordHashKey(streamID),
						recordRangeKeyWithVersion(streamID, event.VersionZero.Add(10, 0)),
					),
				}
				opt.AddToTx = func(ctx context.Context) []any {
					return items
				}
			}); err != nil {
				t.Fatalf("expect to append events, got err: %v", err)
			}
		})
	})

}
func TestEventStore(t *testing.T) {
	ctx := context.Background()

	dynamodbtest.WithTables(t, dbsvc, dynamodbtest.TableConfig{
		TableList: dynamodbtest.TableList(StoreCreateTableInput("table")),
	}, func(dbsvc *dynamodb.Client, tableNames []string) {
		table := tableNames[0]

		eventtest.TestEventLoggingStore(t, ctx, NewEventStore(dbsvc, table))

		eventtest.TestEventSourcingStore(t, ctx, NewEventStore(dbsvc, table))

		indexer := NewIndexer(dbsvc, table)

		eventtest.TestEventStreamer(t, ctx, NewEventStore(dbsvc, table), func(opt *eventtest.TestEventStreamerOptions) {
			opt.SupportOrderDESC = true

			// Do force the indexing of the last persisted record
			opt.PostAppend = func(id event.StreamID) {
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
				if err := indexer.Index(ctx, rec); err != nil {
					t.Fatal("expect err be nil, got", err)
				}
			}
		})
	})
}
