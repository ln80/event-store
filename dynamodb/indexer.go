package dynamodb

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ln80/event-store/event"
)

var (
	ErrIndexingRecordFailed = errors.New("indexing record failed")
)

type Indexer interface {
	Index(ctx context.Context, rec Record) (err error)
	// Publish(ctx context.Context, events []event.Envelope) (err error)
}

type IndexerConfig struct{}

type indexerCache struct {
	globalID string
	ver      event.Version
	rangeKey string
}

type indexer struct {
	svc   ClientAPI
	table string

	*IndexerConfig

	// cache contains the last processed record infos (global stream ID, rangeKey, version)
	cache *indexerCache
	mu    sync.RWMutex
}

// NewIndexer returns a dynamodb global stream indexer.
// It keeps track of the global stream current version and increments sequence accordingly.
// Indexer has an in-memory cache that works effectively only if the instance receives
// records that belong to the same global stream, otherwise the cache is cleared and
// synced with the new global stream current state.
//
// Note that Dynamodb change stream Lambda source mapper allows the use of a such cache mechanism:
// It does not concurrently distribute a partition-related events to different Lambda instances.
// At the moment the partition key and global stream have a one-to-one relation.
// Changing this in the future requires a draining logic to allow a safe transition to the next partition
// (for a given global stream).
func NewIndexer(dbsvc ClientAPI, table string, opts ...func(cfg *IndexerConfig)) *indexer {
	if dbsvc == nil {
		panic("event indexer invalid Dynamodb client: nil value")
	}
	if table == "" {
		panic("event indexer invalid Dynamodb table name: empty value")
	}

	indexer := &indexer{
		svc:           dbsvc,
		table:         table,
		cache:         &indexerCache{},
		IndexerConfig: &IndexerConfig{},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(indexer.IndexerConfig)
	}

	return indexer
}

func (i *indexer) getCache(id string) *indexerCache {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.cache.globalID != id {
		log.Println("global stream indexing cache changed", i.cache.globalID, id)
		i.cache = &indexerCache{
			globalID: id,
		}
	}

	return i.cache
}

func (i *indexer) setCache(cache *indexerCache) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.cache = cache
}

func (i *indexer) Index(ctx context.Context, rec Record) (err error) {
	// do not skip empty record; this might be error-prone
	// this logic is subject to change...
	if len(rec.Events) == 0 {
		return event.Err(ErrIndexingRecordFailed, "empty record: "+rec.HashKey+" "+rec.RangeKey)
	}

	var (
		globalID                  string        // global stream id
		isRetry                   bool          // indicates whether or not record is already processed (handles idempotency)
		previousGVer, currentGVer event.Version // last and current version to persist into the record
	)

	globalID = rec.GID

	defer func() {
		if err == nil && globalID != "" && !isRetry {
			cache := i.getCache(globalID)

			cache.ver = cache.ver.Incr()
			cache.rangeKey = rec.RangeKey

			i.setCache(cache)

		}
	}()

	if ver := i.getCache(globalID).ver; ver != event.VersionZero {
		previousGVer = ver
	} else {
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

		out, err := i.svc.Query(ctx, &dynamodb.QueryInput{
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
			return event.Err(ErrIndexingRecordFailed, globalID, err)
		}

		if l := len(out.Items); l == 0 {
			previousGVer = event.VersionZero
		} else {
			lastRecord := Record{}
			if err := attributevalue.UnmarshalMap(out.Items[0], &lastRecord); err != nil {
				return event.Err(ErrIndexingRecordFailed, globalID, err)
			}

			previousGVer, err = event.ParseVersion(lastRecord.Item.LSIRangeKey)
			if err != nil {
				return event.Err(ErrIndexingRecordFailed, globalID, err)
			}

			// refresh cache
			i.setCache(&indexerCache{
				ver:      previousGVer,
				rangeKey: lastRecord.RangeKey,
				globalID: globalID,
			})
		}
	}

	// TBD: I'm not sure about throwing this error (preventive mode)
	// if i.getCache(globalID).rangeKey > rec.RangeKey {
	// 	return nil, event.Err(ErrIndexingRecordFailed, globalID, "old record behind the current checkpoint")
	// }

	if i.getCache(globalID).rangeKey == rec.RangeKey {
		isRetry = true
	}

	currentGVer = previousGVer
	// do not increment the global version if it's a retry
	if !isRetry {
		currentGVer = currentGVer.Incr()
	}

	// If it's not a retry then update the record's global version
	if !isRetry {
		expr, _ := expression.
			NewBuilder().
			WithUpdate(
				expression.
					Set(expression.Name(GVerAttribute), expression.Value(currentGVer.String())).
					Set(expression.Name(LocalIndexRangeKey), expression.Value(currentGVer.String())),
			).
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
		if _, err = i.svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
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
			// if IsConditionCheckFailure(err) && isRetry {
			// 	return events, nil // tolerate conditional failure is
			// }
			return event.Err(ErrIndexingRecordFailed, globalID, err)
		}
	}

	return nil
}
