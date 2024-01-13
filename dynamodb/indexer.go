package dynamodb

import (
	"context"
	"fmt"
	"sync"

	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
)

var (
	ErrIndexingRecordFailed = errors.New("indexing record has failed")
)

type Indexer interface {
	Index(ctx context.Context, rec Record) (err error)
}

type IndexerConfig struct{}

type indexer struct {
	api   ClientAPI
	table string
	cfg   *IndexerConfig

	checkpoint *indexerCheckpoint
	mu         sync.RWMutex
}

// NewIndexer returns a dynamodb global stream indexer.
// It keeps track of the global stream current version and increments sequence accordingly by using a Dynamodb LSI
//
// It has an in-memory cache that works effectively only if the instance keeps receiving records from the same global stream.
// Otherwise the cache is cleared and synced with the new global stream's current state.
//
// The use of a such cache is only possible because Dynamodb Lambda event source mapper allows it:
//   - It does not concurrently distribute a partition-related record to different Lambda instances.
//   - The partition key and global stream have a one-to-one relation.
//
// The later one-to-one point might change in the future to overcome the 10GB size limit.
// This may require a draining logic to allow a safer transition to the next partition.
func NewIndexer(api ClientAPI, table string, opts ...func(cfg *IndexerConfig)) *indexer {
	if api == nil {
		panic("event indexer invalid Dynamodb client: nil value")
	}
	if table == "" {
		panic("event indexer invalid Dynamodb table name: empty value")
	}

	indexer := &indexer{
		api:        api,
		table:      table,
		checkpoint: &indexerCheckpoint{},
		cfg:        &IndexerConfig{},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(indexer.cfg)
	}

	return indexer
}

func (i *indexer) Index(ctx context.Context, rec Record) (err error) {
	log := logger.WithStream(
		logger.
			FromContext(ctx).
			WithName("dynamodb").
			WithValues("record", rec.Keys()),
		rec.GID,
	)
	log.V(1).Info("Do index record")

	globalID := rec.GID

	defer func() {
		if err != nil {
			log.V(1).Info("Failed to index record", "error", err.Error())
			err = fmt.Errorf("%w: for %s, err: %v", ErrIndexingRecordFailed, globalID, err)
			return
		}
		log.V(1).Info("Record indexed")
	}()

	if len(rec.Events) == 0 {
		err = fmt.Errorf("empty record %v", rec.Keys())
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	swapped := i.checkpoint.swap(globalID)
	if swapped {
		log.V(1).Info("Current global stream changed", "old_gstmID", i.checkpoint.globalID, "new_gstmID", globalID)
	} else {
		log.V(1).Info("Current global stream remains the same")
	}

	prevGVer := i.checkpoint.Version()
	if prevGVer == event.VersionZero {
		var prevIndexedRecord *Record
		prevIndexedRecord, err = i.prevIndexedRecord(ctx, rec)
		if err != nil {
			return
		}
		if prevIndexedRecord == nil {
			log.V(1).Info("Previous indexed record not found, previous global stream version is set to zero")
		} else {
			prevGVer, err = event.ParseVersion(prevIndexedRecord.Item.LSIRangeKey)
			if err != nil {
				return
			}
			i.checkpoint.update(prevIndexedRecord.RangeKey, prevGVer)
		}
	}

	if i.checkpoint.Key() == rec.RangeKey {
		log.V(0).Info("Skip indexing the same record", "gver", i.checkpoint.Version())
		return
	}

	currentGVer := prevGVer.Incr()
	if err = i.updateRecordIndex(ctx, rec, currentGVer); err != nil {
		return
	}
	i.checkpoint.update(rec.RangeKey, currentGVer)

	return
}

func (i *indexer) prevIndexedRecord(ctx context.Context, rec Record) (*Record, error) {
	expr, _ := expression.
		NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(rec.HashKey)),
		).
		WithProjection(expression.NamesList(
			expression.Name(HashKey),
			expression.Name(RangeKey),
			expression.Name(LocalIndexRangeKey),
		)).
		Build()

	out, err := i.api.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(i.table),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
		IndexName:                 aws.String(LocalIndex),
		ScanIndexForward:          aws.Bool(false),
		Limit:                     aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}

	if l := len(out.Items); l == 0 {
		return nil, nil
	}

	prevIndexedRecord := &Record{}
	if err := attributevalue.UnmarshalMap(out.Items[0], prevIndexedRecord); err != nil {
		return nil, err
	}

	return prevIndexedRecord, nil
}

func (i *indexer) updateRecordIndex(ctx context.Context, rec Record, version event.Version) error {
	expr, _ := expression.
		NewBuilder().
		WithUpdate(
			expression.
				Set(expression.Name(GVerAttribute), expression.Value(version.String())).
				Set(expression.Name(LocalIndexRangeKey), expression.Value(version.String())),
		).
		// Make sure the item already exists and is not indexed
		WithCondition(
			expression.AttributeNotExists(
				expression.Name(GVerAttribute),
			).And(
				expression.AttributeExists(
					expression.Name(HashKey),
				),
			).And(expression.AttributeExists(
				expression.Name(RangeKey),
			)),
		).Build()
	if _, err := i.api.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		Key: map[string]types.AttributeValue{
			HashKey:  &types.AttributeValueMemberS{Value: rec.HashKey},
			RangeKey: &types.AttributeValueMemberS{Value: rec.RangeKey},
		},
		TableName:                 aws.String(i.table),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	}); err != nil {
		return err
	}

	return nil
}

// indexerCheckpoint used by indexer, note that it's not THREAD SAFE.
type indexerCheckpoint struct {
	globalID string
	version  event.Version
	key      string
}

func (ic *indexerCheckpoint) swap(id string) bool {
	var swapped bool
	if ic.globalID != id {
		ic.globalID = id
		swapped = true
	}

	return swapped
}

func (ic *indexerCheckpoint) update(rangeKey string, ver event.Version) {
	ic.key = rangeKey
	ic.version = ver
}

func (ic *indexerCheckpoint) Key() string {
	return ic.key
}

func (ic *indexerCheckpoint) Version() event.Version {
	return ic.version
}
