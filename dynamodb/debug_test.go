package dynamodb

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ln80/event-store/avro"
	avro_memory "github.com/ln80/event-store/avro/memory"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/event/sourcing"
	"github.com/ln80/event-store/internal/testutil"
)

func TestDebug(t *testing.T) {
	t.Skip()
	ctx := context.TODO()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatal(err)
		return
	}
	dbsvcreal := dynamodb.NewFromConfig(cfg)
	table := "tmp-event-table-debug"

	registry := avro_memory.NewRegistry()
	ser := avro.NewEventSerializer(ctx, registry, func(esc *avro.EventSerializerConfig) {
		esc.Namespace = ""
	})

	_ = ser

	store := NewEventStore(dbsvcreal, table, func(sc *StoreConfig) {
		// sc.Serializer = ser
	})

	streamID := event.NewStreamID(event.UID().String())

	// append events to stream
	// stm := sourcing.Wrap(ctx, streamID, event.VersionZero, testutil.GenEvents(10))
	// if err := store.AppendToStream(ctx, stm); err != nil {
	// 	t.Fatalf("expect to append events, got err: %v", err)
	// }

	stm := sourcing.Wrap(ctx, streamID, event.VersionZero, testutil.GenEvents(10))

	otherInput := func(id event.StreamID, ver event.Version) any {
		other := Item{
			HashKey:  recordHashKey(id),
			RangeKey: recordRangeKeyWithVersion(id, ver),
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

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := store.AppendToStream(ctx, stm, func(opt *event.AppendConfig) {
			opt.AddToTx = func(ctx context.Context) (items []any) {
				return []any{otherInput(streamID, event.VersionZero.Add(2, 0))}
			}
		}); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := store.AppendToStream(ctx, stm, func(opt *event.AppendConfig) {
			opt.AddToTx = func(ctx context.Context) (items []any) {
				return []any{otherInput(streamID, event.VersionZero.Add(30, 0))}
			}
		}); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := store.AppendToStream(ctx, stm, func(opt *event.AppendConfig) {
			opt.AddToTx = func(ctx context.Context) (items []any) {
				return []any{otherInput(streamID, event.VersionZero.Add(40, 0))}
			}
		}); err != nil {
			t.Fatalf("expect to append events, got err: %v", err)
		}
	}()

	wg.Wait()

	// stm = sourcing.Wrap(ctx, streamID, stm.Version(), testutil.GenEvents(100))
	// if err := store.AppendToStream(ctx, stm); err != nil {
	// 	t.Fatalf("expect to append events, got err: %v", err)
	// }

	rstm, err := store.LoadStream(ctx, streamID)
	if err != nil {
		t.Fatalf("expect to load events got err %v", err)
	}
	if rid, id := stm.ID().String(), streamID.String(); rid != id {
		t.Fatalf("expect loaded stream ID be %s, got %s", rid, id)
	}
	renvs := rstm.Unwrap()
	if l := len(renvs); l != 10 {
		t.Fatalf("invalid loaded events length, must be %d got: %d", 10, l)
	}

	// handlerFunc := func(events *[]event.Envelope) func(ctx context.Context, data event.StreamData) error {
	// 	return func(ctx context.Context, data event.StreamData) error {
	// 		if data.Type == event.StreamDataTypeRecord {
	// 			*events = append(*events, data.Value.(event.Envelope))
	// 		}
	// 		return nil
	// 	}
	// }

	// q := event.StreamerQuery{
	// 	// RecordLimit: 1,
	// }

	// indexer := NewIndexer(dbsvc, table)
	// postAppend := func(id event.StreamID) {
	// 	time.Sleep(time.Second)
	// 	expr, _ := expression.
	// 		NewBuilder().
	// 		WithKeyCondition(
	// 			expression.Key(HashKey).
	// 				Equal(expression.Value(recordHashKey(event.NewStreamID(id.GlobalID())))).And(
	// 				expression.Key(RangeKey).
	// 					BeginsWith(strings.Join(id.Parts(), event.StreamIDPartsDelimiter) + "@v_")),
	// 		).
	// 		Build()
	// 	out, err := dbsvc.Query(ctx, &dynamodb.QueryInput{
	// 		TableName:                 aws.String(table),
	// 		KeyConditionExpression:    expr.KeyCondition(),
	// 		FilterExpression:          expr.Filter(),
	// 		ProjectionExpression:      expr.Projection(),
	// 		ExpressionAttributeNames:  expr.Names(),
	// 		ExpressionAttributeValues: expr.Values(),
	// 		ConsistentRead:            aws.Bool(true),
	// 		ScanIndexForward:          aws.Bool(false),
	// 		Limit:                     aws.Int32(1),
	// 	})
	// 	if err != nil {
	// 		t.Fatal("expect err be nil, got", err)
	// 	}
	// 	rec := Record{}
	// 	if err = attributevalue.UnmarshalMap(out.Items[0], &rec); err != nil {
	// 		t.Fatal("expect err be nil, got", err)
	// 	}
	// 	if err := indexer.Index(ctx, rec); err != nil {
	// 		t.Fatal("expect err be nil, got", err)
	// 	}
	// }
	// postAppend(streamID)

	// rEvents := make([]event.Envelope, 0)
	// if err := store.Replay(ctx, event.NewStreamID(streamID.GlobalID()), q, handlerFunc(&rEvents)); err != nil {
	// 	t.Fatalf("expect to append events, got err: %v", err)
	// }

	// t.Logf("replay global stream count %d", len(rEvents))
}
