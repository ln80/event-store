package dynamodb

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/testutil"
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
				if tc.ok {
					if r := recover(); r != nil {
						t.Fatal("expect to not panic, got", r)
					}
				} else {
					if r := recover(); r == nil {
						t.Fatal("expect to panic")
					}
				}

			}()

			NewEventStore(tc.dbsvc, tc.table, nil, nil)
		})
	}
}
func TestEventStore(t *testing.T) {
	ctx := context.Background()

	withTable(t, dbsvc, func(table string) {

		testutil.TestEventLoggingStore(t, ctx, NewEventStore(dbsvc, table))

		testutil.TestEventSourcingStore(t, ctx, NewEventStore(dbsvc, table))

		indexer := NewIndexer(dbsvc, table)

		testutil.TestEventStreamer(t, ctx, NewEventStore(dbsvc, table), func(opt *testutil.TestEventStreamerOptions) {

			// turn on this option so we can test replay event in DESC order / get recent
			opt.SupportOrderDESC = true

			// Do force the last persisted record indexing (aka set global version)
			opt.PostAppend = func(id event.StreamID) {
				expr, _ := expression.
					NewBuilder().
					WithKeyCondition(
						expression.Key(HashKey).
							Equal(expression.Value(recordHashKey(event.NewStreamID(id.GlobalID())))).And(
							expression.Key(RangeKey).
								BeginsWith(strings.Join(id.Parts(), event.StreamIDPartsDelimiter) + "t_")),
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
