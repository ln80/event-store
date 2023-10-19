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
	"github.com/ln80/event-store/event/sourcing"
	"github.com/ln80/event-store/json"
)

const (
	EventSizeLimit = 256000 // 250 KB
)

var (
	_ event.Store    = &Store{}
	_ sourcing.Store = &Store{}
	_ event.Streamer = &Store{}
)

type StoreConfig struct {
	Serializer event.Serializer
}

// Store implements event.Store, sourcing.Store, and event.Streamer interfaces
type Store struct {
	svc   ClientAPI
	table string

	*StoreConfig

	mu sync.RWMutex

	// an in-memory checkpoint used to control versioned-streams (aka event sourcing streams...)
	checkpoint map[string]string
}

// NewEventStore returns an implementation of event store interfaces.
// It panics if ClientAPI or table are empty.
func NewEventStore(svc ClientAPI, table string, opts ...func(*StoreConfig)) *Store {
	if svc == nil {
		panic("event Store invalid Dynamodb client: nil value")
	}
	if table == "" {
		panic("event Store invalid Dynamodb table name: empty value")
	}
	s := &Store{
		svc:   svc,
		table: table,
		StoreConfig: &StoreConfig{
			Serializer: json.NewEventSerializer(""),
		},
		checkpoint: make(map[string]string),
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(s.StoreConfig)
	}

	return s
}

// AppendToStream implements event.Store interface
func (s *Store) Append(ctx context.Context, id event.StreamID, events []event.Envelope, optFns ...func(*event.AppendOptions)) error {
	l := len(events)
	if l == 0 {
		return nil
	}
	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithTimestamp(id, events[l-1].At())
	}
	return s.doAppend(ctx, id, nil, events, keysFn, optFns...)
}

// AppendToStream implements event.Store interface
func (s *Store) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
	var since, until, msince, muntil time.Time
	l := len(trange)
	if l > 1 {
		since, until = trange[0], trange[1]
	} else if l == 1 {
		since, until = trange[0], time.Now()
	} else {
		since, until = time.Unix(0, 0), time.Now()
	}

	// records are sorted by the last event timestamp
	// time diff between first and last event in the record should not exceed should not exceed 10 sec
	// TODO: enforce this invariant in Append method
	if !since.Before(time.Unix(5, 0)) && !since.IsZero() {
		msince = since.Add(-5 * time.Second)
	}
	muntil = until.Add(5 * time.Second)
	envs, err := s.doLoad(ctx, id,
		recordRangeKeyWithTimestamp(id, msince),
		recordRangeKeyWithTimestamp(id, muntil),
	)
	if err != nil {
		return nil, err
	}
	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.Event() == nil {
			return nil, event.Err(event.ErrInvalidStream, id.String(),
				"empty event data loaded from Store", "it's likely to be a lazily unmarshaling issue")
		}
		if env.At().Before(since) || env.At().After(until) {
			continue
		}
		fenvs = append(fenvs, env)
	}

	return fenvs, nil
}

// AppendToStream implements sourcing.Store interface
func (s *Store) AppendToStream(ctx context.Context, stm sourcing.Stream, optFns ...func(opt *event.AppendOptions)) (err error) {
	if stm.Empty() {
		return
	}
	if err = stm.Validate(); err != nil {
		return
	}

	// update stream's in-memory checkpoint if the new chunk is successfully appended
	defer func() {
		if err == nil {
			s.checkVersion(stm.ID(), stm.Version())
		}
	}()

	id := stm.ID()
	ver := stm.Version()
	keysFn := func() (string, string) {
		return recordHashKey(id), recordRangeKeyWithVersion(stm.ID(), ver)
	}
	// ignore "check previous record exists" if the chunk is supposed to be the first in stream.
	if ver.Trunc().After(event.VersionMin) {
		// Note that "check previous record" + "save new chunk" operations are not transactional,
		// a dirty read may occur if table is somehow corrupted/updated, which is unlikely the case.
		if err = s.previousRecordExists(ctx, id, ver); err != nil {
			return err
		}
	}
	// doAppend still perform another check to ensure the chunk version does not already exist.
	// Although it does not guarantees chunks' versions are in a consecutive sequence.
	err = s.doAppend(ctx, id, &ver, stm.Unwrap(), keysFn, optFns...)
	return
}

// AppendToStream implements sourcing.Store interface
func (s *Store) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (stm *sourcing.Stream, err error) {
	var from, to event.Version
	if l := len(vrange); l > 1 {
		from, to = vrange[0], vrange[1]
	} else if l == 1 {
		from, to = vrange[0], event.VersionMax
	} else {
		from, to = event.VersionMin, event.VersionMax
	}

	// update the stream in-memory checkpoint if recent stream events are successfully loaded
	if to == event.VersionMax {
		defer func() {
			if err == nil {
				s.checkVersion(stm.ID(), stm.Version())
			}
		}()
	}

	envs, err := s.doLoad(ctx, id, recordRangeKeyWithVersion(id, from),
		recordRangeKeyWithVersion(id, to))
	if err != nil {
		return nil, err
	}

	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.Event() == nil {
			return nil, event.Err(event.ErrInvalidStream, id.String(),
				"empty event data loaded from Store", "it's likely to be a lazily unmarshaling issue")
		}
		if env.Version().Before(from) || env.Version().After(to) {
			continue
		}
		fenvs = append(fenvs, env)
	}

	stm = sourcing.NewStream(
		id, fenvs,
	)
	return
}

// checkVersion set the given  stream version as the current one in the Store in-memory checkpoint
// Note that given version will be truncated and the fractionnal part will be removed
func (s *Store) checkVersion(id event.StreamID, ver event.Version) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checkpoint[id.String()] = ver.Trunc().String()
}

// lastCheckedVersion returns the given stream's current version from the memory cache if it exists.
func (s *Store) lastCheckedVersion(id event.StreamID) (ver string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ver, ok = s.checkpoint[id.String()]
	return
}

// previousRecordExists ensures the given record is in a consecutive sequence in the given stream.
// only applied for version-based streams
func (s *Store) previousRecordExists(ctx context.Context, id event.StreamID, verToAppend event.Version) error {
	// previous version should be equals to current stream version (without fractional part)
	previous := verToAppend.Decr()
	cacheUpdated := false

CHECK_PREVIOUS_RECORD:
	// get current stream version from in-memory checkpoint cache
	last, ok := s.lastCheckedVersion(id)
	if ok {
		if previous.String() == last {
			return nil
		}
		// coming to point where cache is refreshed but prev ver still ahead
		// means that the ver to append is not in sequence
		if previous.String() < last || cacheUpdated {
			return event.Err(event.ErrAppendEventsFailed, id.String(), "invalid chunk version, it must be next to previous record version: "+previous.String())
		}
	}

	// invalidate the cache
	if !ok || previous.String() > last {
		expr, err := expression.
			NewBuilder().
			WithProjection(expression.NamesList(
				expression.Name("_ver"),
			)).Build()
		if err != nil {
			return event.Err(event.ErrAppendEventsFailed, id.String(), err)
		}
		out, err := s.svc.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(s.table),
			Key: map[string]types.AttributeValue{
				HashKey:  &types.AttributeValueMemberS{Value: recordHashKey(id)},
				RangeKey: &types.AttributeValueMemberS{Value: recordRangeKeyWithVersion(id, previous)},
			},
			ConsistentRead:           aws.Bool(true),
			ProjectionExpression:     expr.Projection(),
			ExpressionAttributeNames: expr.Names(),
		})
		if err != nil || len(out.Item) == 0 {
			return event.Err(event.ErrAppendEventsFailed, id.String(), "previous record not found with version: %v", previous)
		}

		s.checkVersion(id, previous)
		cacheUpdated = true

		goto CHECK_PREVIOUS_RECORD
	}
	return nil
}

// doAppend works for both version and timestamp based streams.
// It appends the given record of events to the stream.
//
// Note that version param must not be nil in case of a version-based stream.
func (s *Store) doAppend(ctx context.Context, id event.StreamID, ver *event.Version, events []event.Envelope, keysFn func() (string, string), optFns ...func(*event.AppendOptions)) error {
	if len(events) == 0 {
		return nil
	}

	ses, ok := SessionFrom(ctx)
	if !ok {
		ses = NewSession(s.svc)
	}

	opt := event.AppendOptions{}
	for _, optFn := range optFns {
		if optFn == nil {
			continue
		}
		optFn(&opt)
	}

	explicitTx := false
	if !ses.HasTx() && opt.AddToTx != nil {
		explicitTx = true
		ses.StartTx()
		txItems := opt.AddToTx(ctx)
		for _, txItem := range txItems {
			var err error
			switch txi := txItem.(type) {
			case *dynamodb.PutItemInput:
				err = ses.Put(ctx, txi)
			case *dynamodb.DeleteItemInput:
				err = ses.Delete(ctx, txi)
			case *dynamodb.UpdateItemInput:
				err = ses.Update(ctx, txi)
			case *types.ConditionCheck:
				err = ses.Check(ctx, txi)
			default:
				panic(fmt.Errorf("%w: %T", event.ErrUnsupportedAppendOption, txi))
			}
			if err != nil {
				return event.Err(event.ErrAppendEventsFailed, id.String(), err)
			}
		}
	}

	b, n, err := s.Serializer.MarshalEventBatch(events)
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}
	for _, size := range n {
		if size > EventSizeLimit {
			return event.Err(event.ErrAppendEventsFailed, id.String(), "event size limit exceeded")
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
	}

	if ver != nil {
		r.Version = ver.String()
	}
	r.GID = events[0].GlobalStreamID()

	mr, err := attributevalue.MarshalMap(r)
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	expr, err := expression.
		NewBuilder().
		WithCondition(
			expression.AttributeNotExists(
				expression.Name(RangeKey),
			),
		).Build()
	if err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	if err = ses.Put(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(s.table),
		Item:                      mr,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}); err != nil {
		if IsConditionCheckFailure(err) {
			return event.Err(event.ErrAppendEventsConflict, id.String(), err)
		}
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	if explicitTx {
		if err = ses.CommitTx(ctx); err != nil {
			return event.Err(event.ErrAppendEventsFailed, id.String(), err)
		}
	}

	return nil
}

// doLoad works for both version and timestamp based streams.
// It loads and unmarshal events from the dynamodb table.
func (s *Store) doLoad(ctx context.Context, id event.StreamID, from, to string) ([]event.Envelope, error) {
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
		return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	p := dynamodb.NewQueryPaginator(s.svc, &dynamodb.QueryInput{
		TableName:                 aws.String(s.table),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
	})

	// unmarshal records
	records := []Record{}
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
		}
		rec := []Record{}
		err = attributevalue.UnmarshalListOfMaps(out.Items, &rec)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
		}
		records = append(records, rec...)
	}

	// unmarshal records' events
	envs := []event.Envelope{}
	for _, rec := range records {
		chunk, err := s.Serializer.UnmarshalEventBatch(rec.Events)
		if err != nil {
			return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
		}
		envs = append(envs, chunk...)
	}

	return envs, nil
}

func (s *Store) Replay(ctx context.Context, id event.StreamID, q event.StreamerQuery, h event.StreamerHandler) error {
	if !id.Global() {
		panic("unsupported stream type, only global stream are supported")
	}

	q.Build()
	from := q.From.Trunc()
	to := q.To

	hashKey := recordHashKey(id)

	expr, err := expression.
		NewBuilder().
		WithKeyCondition(
			expression.Key(HashKey).
				Equal(expression.Value(hashKey)).
				And(
					expression.
						Key(LocalIndexRangeKey).
						Between(expression.Value(from.String()), expression.Value(to.String()))),
		).Build()
	if err != nil {
		return event.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	indexForward := true
	if q.Order != event.StreamerReplayOrderASC {
		indexForward = false
	}
	p := dynamodb.NewQueryPaginator(s.svc, &dynamodb.QueryInput{
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
	})

	// recordCount is used to avoid processing unnecessary records found in later pages.
	recordCount := 0
PAGE_LOOP:
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return event.Err(event.ErrLoadEventFailed, id.String(), err)
		}

		records := []Record{}
		err = attributevalue.UnmarshalListOfMaps(out.Items, &records)
		if err != nil {
			return event.Err(event.ErrLoadEventFailed, id.String(), err)
		}

		for _, rec := range records {
			recordCount++
			if recordCount > int(q.RecordLimit) {
				continue PAGE_LOOP
			}

			envs, err := s.Serializer.UnmarshalEventBatch(rec.Events)
			if err != nil {
				return event.Err(event.ErrLoadEventFailed, id.String(), err)
			}
			ver, err := event.ParseVersion(rec.GVersion)
			if err != nil {
				return event.Err(event.ErrLoadEventFailed, id.String(), err)
			}

			if q.Order == event.StreamerReplayOrderDESC {
				sort.Slice(envs, func(a, b int) bool {
					return envs[a].At().Compare(envs[b].At()) >= 0
				})
			}
			l := len(envs)

			for i, env := range envs {
				rev, ok := env.(interface {
					event.Envelope
					SetGlobalVersion(v event.Version) event.Envelope
				})
				if !ok {
					panic("event envelope does not support SetGlobalVersion")
				}

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

				if err := h(ctx, event.StreamData{
					Type: event.StreamDataTypeRecord, Value: env,
				}); err != nil {
					return err
				}
			}
		}

	}

	return nil
}
