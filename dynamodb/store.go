package dynamodb

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ln80/event-store/event"
	event_errors "github.com/ln80/event-store/event/errors"
	"github.com/ln80/event-store/event/sourcing"
	"github.com/ln80/event-store/internal/logger"
	"github.com/ln80/event-store/json"
)

var (
	_ event.Store    = &Store{}
	_ sourcing.Store = &Store{}
	_ event.Streamer = &Store{}
)

// StoreConfig presents the config of dynamodb-based event store implementation.
type StoreConfig struct {
	// Serializer presents the event serializer. By default, the store uses the JSON event serializer.
	Serializer event.Serializer
	// AppendEventOptions presents the default pre-append options applied in every events' Append call.
	AppendEventOptions []func(*event.AppendConfig)
	// RecordSizeLimit describes the size limit in bytes of events' appended to the stream.
	// It's ignored if empty.
	RecordSizeLimit int
}

// Store implements event.Store, sourcing.Store, and event.Streamer interfaces
type Store struct {
	*sourcingStore
	*loggingStore
	*streamerStore
}

// baseStore presents the basic store used by both Logging and Event-sourcing ones.
type baseStore struct {
	api   ClientAPI
	table string
	cfg   *StoreConfig
}

func (s *baseStore) resolveAppendConfig(opts []func(*event.AppendConfig)) event.AppendConfig {
	cfg := event.AppendConfig{}
	for _, optFn := range s.cfg.AppendEventOptions {
		if optFn == nil {
			continue
		}
		optFn(&cfg)
	}
	for _, optFn := range opts {
		if optFn == nil {
			continue
		}
		optFn(&cfg)
	}

	return cfg
}

func (s *baseStore) doAppend(ctx context.Context, id event.StreamID, ver *event.Version, events []event.Envelope, keysFn func() (string, string), opts []func(*event.AppendConfig)) (err error) {
	if len(events) == 0 {
		return
	}
	defer func() {
		if !event_errors.ErrIs(err, nil, event.ErrAppendEventsConflict) {
			err = event_errors.Err(event.ErrAppendEventsFailed, id.String(), err)
			return
		}
	}()
	log := logger.FromContext(ctx).WithName("base")

	ses, ok := SessionFrom(ctx)
	if !ok {
		ses = NewSession(s.api)
	}
	defer func() {
		if cc := ses.ConsumedCapacity(); !cc.IsZero() {
			log.V(1).Info("Append events consumed capacity", "capacity", cc)
		}
	}()

	acfg := s.resolveAppendConfig(opts)

	explicitTx := false
	if !ses.HasTx() && acfg.AddToTx != nil {
		explicitTx = true
		if err = ses.StartTx(); err != nil {
			return
		}
		if err = ses.AddToTx(ctx, acfg.AddToTx(ctx)); err != nil {
			return
		}
	}

	b, err := s.cfg.Serializer.MarshalEventBatch(ctx, events)
	if err != nil {
		return
	}

	if limit := s.cfg.RecordSizeLimit; limit > 0 {
		if size := len(b); size > limit {
			err = fmt.Errorf("%w: record size %d", event.ErrEventSizeLimitExceeded, size)
			return
		}
	}

	var ttl time.Duration
	for _, evt := range events {
		if evt.TTL() > ttl {
			ttl = evt.TTL()
		}
	}
	rttl := int64(0)
	if ttl > 0 {
		rttl = time.Now().Add(ttl).Unix()
	}

	hk, rk := keysFn()
	r := Record{
		Item: Item{
			HashKey:  hk,
			RangeKey: rk,
			TTL:      rttl,
		},
		Events: b,
		Since:  events[0].At().UnixNano(),
		Until:  events[len(events)-1].At().UnixNano(),
		GID:    id.GlobalID(),
	}

	if ver != nil {
		r.Version = ver.String()
	}
	if acfg.AddTracing != nil {
		r.TraceID = acfg.AddTracing(ctx)
	}

	var mr map[string]types.AttributeValue
	mr, err = attributevalue.MarshalMap(r)
	if err != nil {
		return
	}

	expr, err := expression.
		NewBuilder().
		WithCondition(
			expression.AttributeNotExists(
				expression.Name(RangeKey),
			),
		).Build()
	if err != nil {
		return
	}

	if err = ses.Put(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(s.table),
		Item:                      mr,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),

		ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
		// ReturnItemCollectionMetrics:         types.ReturnItemCollectionMetricsSize,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}); err != nil {
		if IsConditionCheckFailure(err) {
			err = event_errors.Err(event.ErrAppendEventsConflict, id.String(), err)
			return
		}
		return
	}

	if explicitTx {
		if err = ses.CommitTx(ctx); err != nil {
			if ok, found := IsConditionCheckFailureWithItem(err, hk, rk); ok && found {
				err = event_errors.Err(event.ErrAppendEventsConflict, id.String(), err)
				return
			}
			return
		}
	}
	return
}

func (s *baseStore) doLoad(ctx context.Context, id event.StreamID, from, to string) (events []event.Envelope, err error) {
	defer func() {
		if err != nil {
			err = event_errors.Err(event.ErrLoadEventFailed, id.String(), err)
			return
		}
	}()

	log := logger.FromContext(ctx).WithName("base")

	cc := &consumedCapacity{}
	defer func() {
		if !cc.IsZero() {
			log.V(1).Info("Load events consumed capacity", "capacity", cc)
		}
	}()

	expr, err := expression.
		NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(recordHashKey(id))).
				And(expression.
					Key(RangeKey).
					Between(expression.Value(from), expression.Value(to))),
		).
		WithFilter(
			expression.Name(TTLAttribute).AttributeNotExists().
				Or(
					expression.Name(TTLAttribute).GreaterThan(expression.Value(time.Now().Unix())),
				),
		).
		Build()
	if err != nil {
		return
	}
	p := dynamodb.NewQueryPaginator(s.api, &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityIndexes,
	})

	events = make([]event.Envelope, 0)
	for p.HasMorePages() {
		var out *dynamodb.QueryOutput
		out, err = p.NextPage(ctx)
		if out != nil {
			addConsumedCapacity(cc, out.ConsumedCapacity)
		}
		if err != nil {
			return
		}
		records := make([]Record, 0)
		err = attributevalue.UnmarshalListOfMaps(out.Items, &records)
		if err != nil {
			return
		}
		for _, rec := range records {
			var chunk []event.Envelope
			chunk, err = s.cfg.Serializer.UnmarshalEventBatch(ctx, rec.Events)
			if err != nil {
				return
			}
			events = append(events, chunk...)
		}
	}
	return
}

// NewEventStore returns an implementation of event store interfaces.
// It panics if ClientAPI or table are empty.
func NewEventStore(api ClientAPI, table string, opts ...func(*StoreConfig)) *Store {
	if api == nil {
		panic("event store invalid Dynamodb client: nil value")
	}
	if table == "" {
		panic("event store invalid Dynamodb table name: empty value")
	}

	base := &baseStore{
		api:   api,
		table: table,
		cfg: &StoreConfig{
			Serializer: json.NewEventSerializer(""),
		},
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(base.cfg)
	}

	s := &Store{
		sourcingStore: &sourcingStore{
			baseStore: base,
			checkpoint: &sourcingCheckpoint{
				checkpoint: make(map[string]event.Version),
			},
		},
		loggingStore: &loggingStore{
			baseStore:          base,
			recordMaxTimeFrame: 5 * time.Second,
		},
		streamerStore: &streamerStore{baseStore: base},
	}

	return s
}

type loggingStore struct {
	*baseStore
	recordMaxTimeFrame time.Duration
}

// AppendToStream implements event.Store interface
func (s *loggingStore) Append(ctx context.Context, id event.StreamID, events []event.Envelope, opts ...func(*event.AppendConfig)) (err error) {
	log := logger.WithStream(logger.FromContext(ctx), id.String()).WithName("dynamodb/logging")
	ctx = logger.NewContext(ctx, log)

	// normalize returned err
	defer func() {
		if !event_errors.ErrIs(err, nil, event.ErrAppendEventsFailed, event.ErrAppendEventsConflict) {
			err = event_errors.Err(event.ErrAppendEventsFailed, id.String(), err)
			return
		}
	}()

	l := len(events)
	if l == 0 {
		return
	}

	since, until := events[0].At(), events[l-1].At()
	if frame := until.Sub(since); frame > s.recordMaxTimeFrame {
		err = fmt.Errorf("record max time frame exceeded %v", frame)
		return
	}

	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithTimestamp(id, until)
	}

	err = s.doAppend(ctx, id, nil, events, keysFn, opts)
	return
}

// AppendToStream implements event.Store interface
func (s *loggingStore) Load(ctx context.Context, id event.StreamID, trange ...time.Time) (events []event.Envelope, err error) {
	log := logger.WithStream(logger.FromContext(ctx), id.String()).WithName("dynamodb/logging")
	ctx = logger.NewContext(ctx, log)

	// normalize returned err
	defer func() {
		if !event_errors.ErrIs(err, nil, event.ErrInvalidStream, event.ErrLoadEventFailed) {
			err = event_errors.Err(event.ErrLoadEventFailed, id.String(), err)
			return
		}
	}()

	since, until := event.TimeRange(trange)

	events, err = s.doLoad(ctx, id,
		recordRangeKeyWithTimestamp(id, since.Add(-1*s.recordMaxTimeFrame)),
		recordRangeKeyWithTimestamp(id, until.Add(time.Millisecond)),
	)
	if err != nil {
		return
	}
	filtered := make([]event.Envelope, 0)
	for _, evt := range events {
		if evt.At().Before(since) || evt.At().After(until) {
			continue
		}
		if evt.Event() == nil {
			err = event_errors.Err(event.ErrInvalidStream, id.String(), "empty event data")
			return
		}
		filtered = append(filtered, evt)
	}
	return filtered, nil
}

type sourcingCheckpoint struct {
	checkpoint map[string]event.Version
	mu         sync.RWMutex
}

func (s *sourcingCheckpoint) check(id event.StreamID, ver event.Version) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ver.After(s.checkpoint[id.String()]) {
		s.checkpoint[id.String()] = ver.Trunc()
	}
}

func (s *sourcingCheckpoint) latest(id event.StreamID) (ver event.Version, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ver, ok = s.checkpoint[id.String()]
	return
}

type sourcingStore struct {
	*baseStore
	checkpoint *sourcingCheckpoint
}

// AppendToStream implements sourcing.Store interface
func (s *sourcingStore) AppendToStream(ctx context.Context, stm sourcing.Stream, optFns ...func(opt *event.AppendConfig)) (err error) {
	if stm.Empty() {
		return
	}
	id := stm.ID()

	log := logger.WithStream(logger.FromContext(ctx), id.String()).WithName("dynamodb/sourcing")
	ctx = logger.NewContext(ctx, log)

	defer func() {
		if !event_errors.ErrIs(err, nil, event.ErrAppendEventsConflict, event.ErrAppendEventsFailed) {
			err = event_errors.Err(event.ErrAppendEventsFailed, id.String(), err)
			return
		}
	}()
	if err = stm.Validate(); err != nil {
		return
	}

	// update stream's in-memory checkpoint if the new chunk is successfully appended
	defer func() {
		if err == nil {
			s.checkpoint.check(stm.ID(), stm.Version())
		}
	}()

	ver := stm.Version()

	// Note that "check previous record exists" and "append new record" operations are not transactional,
	// a dirty read may occur if table is corrupted, e.g, the previous record is deleted just after the read.
	// This is unlikely to happen and the current solution is much more cost-effective comparing to
	// using dynamodb transaction with Check operation.
	if err = s.previousRecordExists(ctx, id, ver.Trunc()); err != nil {
		return
	}
	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithVersion(stm.ID(), ver)
	}
	err = s.doAppend(ctx, id, &ver, stm.Unwrap(), keysFn, optFns)
	return
}

// AppendToStream implements sourcing.Store interface
func (s *sourcingStore) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (stm *sourcing.Stream, err error) {
	log := logger.WithStream(logger.FromContext(ctx), id.String()).WithName("dynamodb/sourcing")
	ctx = logger.NewContext(ctx, log)

	defer func() {
		if err == nil {
			s.checkpoint.check(stm.ID(), stm.Version())
		}
	}()

	// normalize returned err
	defer func() {
		if !event_errors.ErrIs(err, nil, event.ErrInvalidStream, event.ErrLoadEventFailed) {
			err = event_errors.Err(event.ErrLoadEventFailed, id.String(), err)
			return
		}
	}()

	from, to := event.VersionRange(vrange)
	var events []event.Envelope
	events, err = s.doLoad(ctx, id,
		recordRangeKeyWithVersion(id, from),
		recordRangeKeyWithVersion(id, to),
	)
	if err != nil {
		return
	}
	filtered := make([]event.Envelope, 0)
	for _, evt := range events {
		if evt.Version().Before(from) || evt.Version().After(to) {
			continue
		}
		if evt.Event() == nil {
			err = event_errors.Err(event.ErrInvalidStream, id.String(), "empty event data")
			return
		}
		filtered = append(filtered, evt)
	}

	stm = sourcing.NewStream(
		id, filtered,
	)
	return
}

// previousRecordExists ensures the given record is in a consecutive sequence in the given stream.
func (s *sourcingStore) previousRecordExists(ctx context.Context, id event.StreamID, recordCur event.Version) error {
	if recordCur.Before(event.VersionMin) || recordCur.Equal(event.VersionMin) {
		return nil
	}

	log := logger.FromContext(ctx)

	recordPrev := recordCur.Decr()
	latest, found := s.checkpoint.latest(id)
	if !found || recordPrev.After(latest) {
		cc := &consumedCapacity{}
		defer func() {
			if !cc.IsZero() {
				log.V(1).Info("Refresh in-memory checkpoint consumed capacity", "capacity", cc)
			}
		}()
		log.V(1).Info("Do refresh in-memory checkpoint cache")
		expr, _ := expression.
			NewBuilder().
			WithProjection(expression.NamesList(
				expression.Name(VerAttribute),
			)).Build()
		out, err := s.api.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(s.table),
			Key: map[string]types.AttributeValue{
				HashKey:  &types.AttributeValueMemberS{Value: recordHashKey(id)},
				RangeKey: &types.AttributeValueMemberS{Value: recordRangeKeyWithVersion(id, recordPrev)},
			},
			ConsistentRead:           aws.Bool(true),
			ProjectionExpression:     expr.Projection(),
			ExpressionAttributeNames: expr.Names(),
		})
		if out != nil {
			addConsumedCapacity(cc, out.ConsumedCapacity)
		}
		if err != nil {
			return err
		}
		if len(out.Item) == 0 {
			return fmt.Errorf("previous stream version %v not found", recordPrev)
		}
		log.V(1).Info("checkpoint in-memory cache has been refreshed")
		s.checkpoint.check(id, recordPrev)
		latest, _ = s.checkpoint.latest(id)
	}
	if !recordPrev.Equal(latest) {
		return fmt.Errorf("invalid stream expected previous version %v", recordPrev)
	}
	return nil
}

type streamerStore struct {
	*baseStore
}

// Replay replays event from the global stream according to the query filters.
//
// Note that the global stream is eventually consistent.
func (s *streamerStore) Replay(ctx context.Context, id event.StreamID, q event.StreamerQuery, h event.StreamerHandler) (err error) {
	log := logger.WithStream(logger.FromContext(ctx), id.String()).WithName("dynamodb/streamer")
	ctx = logger.NewContext(ctx, log)

	// only global streams are supported for replay; truncate sub-stream parts.
	if !id.Global() {
		id = event.NewStreamID(id.GlobalID())
	}
	defer func() {
		if !event_errors.ErrIs(err, nil, event.ErrLoadEventFailed) {
			err = event_errors.Err(event.ErrAppendEventsFailed, id.String(), err)
			return
		}
	}()

	q.Build()

	cc := &consumedCapacity{}
	defer func() {
		if !cc.IsZero() {
			log.V(1).Info("Replay events consumed capacity", "capacity", cc, "query", q)
		}
	}()

	indexForward := true
	if q.Order != event.StreamerReplayOrderASC {
		indexForward = false
	}

	expr, _ := expression.
		NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(recordHashKey(id))).
				And(
					expression.
						Key(LocalIndexRangeKey).
						Between(expression.Value(q.From.Trunc().String()), expression.Value(q.To.String()))),
		).Build()
	p := dynamodb.NewQueryPaginator(s.api, &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
		IndexName:                 aws.String(LocalIndex),
		ScanIndexForward:          aws.Bool(indexForward),
		Select:                    types.SelectAllAttributes,
		// Note: Dynamodb might not respect the "Limit" param if records total size exceed 1MB.
		// Thus, a page might contains less records than "Limit".
		// Other page queries will be performed by paginator to fetch the remaining records.
		Limit: aws.Int32(int32(q.RecordLimit)),

		ReturnConsumedCapacity: types.ReturnConsumedCapacityIndexes,
	})

	recordCount := 0
PAGE_LOOP:
	for p.HasMorePages() {
		var out *dynamodb.QueryOutput
		out, err = p.NextPage(ctx)
		if out != nil {
			addConsumedCapacity(cc, out.ConsumedCapacity)
		}
		if err != nil {
			return
		}
		records := []Record{}
		err = attributevalue.UnmarshalListOfMaps(out.Items, &records)
		if err != nil {
			return
		}
		for _, rec := range records {
			recordCount++
			if recordCount > int(q.RecordLimit) {
				continue PAGE_LOOP
			}

			var events []event.Envelope
			events, err = s.cfg.Serializer.UnmarshalEventBatch(ctx, rec.Events)
			if err != nil {
				return
			}
			var ver event.Version
			ver, err = event.ParseVersion(rec.GVersion)
			if err != nil {
				return
			}
			if q.Order == event.StreamerReplayOrderDESC {
				sort.Slice(events, func(a, b int) bool {
					return events[a].At().Compare(events[b].At()) >= 0
				})
			}
			l := len(events)

			for i, evt := range events {
				rev := event.MustGlobalVersionSetter(evt)

				var incr uint8
				if q.Order == event.StreamerReplayOrderDESC {
					incr = uint8(l - 1 - i)
				} else {
					incr = uint8(i)
				}

				ever := ver.Add(0, incr)
				if q.Order == event.StreamerReplayOrderDESC {
					if i == 0 {
						ever = ever.EOF()
					}
				} else {
					if i == l-1 {
						ever = ever.EOF()
					}
				}
				rev.SetGlobalVersion(ever)

				if !rev.GlobalVersion().Between(q.From, q.To) {
					continue
				}

				if err = h(ctx, event.StreamData{
					Type: event.StreamDataTypeRecord, Value: evt,
				}); err != nil {
					return
				}
			}
		}
	}
	return nil
}
