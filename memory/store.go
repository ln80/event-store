package memory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ln80/event-store/event"
	event_errors "github.com/ln80/event-store/event/errors"
	"github.com/ln80/event-store/event/sourcing"
)

// Store implements different event store interface. mainly used for testing purposes
type Store struct {
	db          map[string][]event.Envelope
	checkpoints map[string]event.Version // track global stream version
	mu          sync.RWMutex
}

// interface safe-guards
var (
	_ event.Store    = &Store{}
	_ sourcing.Store = &Store{}
	_ event.Streamer = &Store{}
)

// NewEventStore return in-memory event store implementation
func NewEventStore() *Store {
	return &Store{
		db:          make(map[string][]event.Envelope),
		checkpoints: make(map[string]event.Version),
	}
}

func (s *Store) Append(ctx context.Context, id event.StreamID, events []event.Envelope, opts ...func(*event.AppendConfig)) error {
	if len(events) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// init stream's cache db
	if _, ok := s.db[id.String()]; !ok {
		s.db[id.String()] = make([]event.Envelope, 0)
	}

	// check idempotency
	for _, evt := range events {
		for _, env := range s.db[id.String()] {
			if env.ID() == evt.ID() {
				return event.ErrAppendEventsConflict
			}
		}
	}

	// init global stream version
	if _, ok := s.checkpoints[id.GlobalID()]; !ok {
		s.checkpoints[id.GlobalID()] = event.VersionZero
	}

	// increment integer part of version for each chunk aka record

	gVer := s.checkpoints[id.GlobalID()].Incr()

	// foreach chunk event increment the fractional part of the global stream version
	l := len(events)
	for i := range events {
		if rwEnv, ok := events[i].(interface {
			event.Envelope
			SetGlobalVersion(v event.Version) event.Envelope
		}); ok {
			// mark the last event of the chunk as EOF
			if i == l-1 {
				gVer = gVer.EOF()
				rwEnv.SetGlobalVersion(gVer)
				break
			}
			// otherwise increment sequence for the next event
			rwEnv.SetGlobalVersion(gVer)
			gVer = gVer.Add(0, 1)
		}
	}

	s.db[id.String()] = append(s.db[id.String()], events...)

	s.checkpoints[id.GlobalID()] = gVer

	return nil
}

func (s *Store) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	envs, ok := s.db[id.String()]
	if !ok {
		return nil, nil
	}

	l := len(trange)

	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.TTL() > time.Duration(0) && env.At().Add(env.TTL()).Before(time.Now().UTC()) {
			continue
		}
		if l != 0 {
			if since := trange[0]; ok && env.At().Before(since) {
				continue
			}
			if l > 1 {
				if until := trange[1]; ok && env.At().After(until) {
					continue
				}
			}
		}

		fenvs = append(fenvs, env)
	}

	return fenvs, nil
}

func (s *Store) AppendToStream(ctx context.Context, chunk sourcing.Stream, opts ...func(*event.AppendConfig)) error {
	if chunk.Empty() {
		return nil
	}

	// validate the versioned chunk of events
	if err := chunk.Validate(); err != nil {
		return err
	}

	// make sure stream sequence is valid, no gaps are introduced
	lastVersion := event.VersionZero
	s.mu.Lock()
	db, ok := s.db[chunk.ID().String()]
	s.mu.Unlock()
	if ok {
		lastVersion = db[len(db)-1].Version()
	}
	firstVersion := chunk.Unwrap()[0].Version()
	if firstVersion.After(lastVersion) && !firstVersion.Next(lastVersion) {
		return event_errors.Err(
			event.ErrAppendEventsFailed, chunk.ID().String(),
			"invalid sequence "+lastVersion.String()+" "+firstVersion.String(),
		)
	}

	// perform the default append
	return s.Append(ctx, chunk.ID(), chunk.Unwrap(), opts...)
}

func (s *Store) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*sourcing.Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	envs, ok := s.db[id.String()]
	if !ok {
		return nil, nil
	}

	l := len(vrange)

	// filter stream based on range boundaries
	fenvs := make([]event.Envelope, 0)
	for _, env := range envs {
		if env.TTL() > time.Duration(0) && env.At().Add(env.TTL()).Before(time.Now().UTC()) {
			continue
		}
		if l != 0 {
			if from := vrange[0]; ok && env.Version().Before(from) {
				continue
			}
			if l > 1 {
				if to := vrange[1]; ok && env.Version().After(to) {
					continue
				}
			}
		}

		fenvs = append(fenvs, env)
	}

	return sourcing.NewStream(id, fenvs), nil
}

func (s *Store) Replay(ctx context.Context, id event.StreamID, q event.StreamerQuery, h event.StreamerHandler) error {
	// resolve sub streams
	streamIDs := []string{}
	for k := range s.db {
		if strings.HasPrefix(k, id.String()) {
			streamIDs = append(streamIDs, k)
		}
	}
	if len(streamIDs) == 0 {
		return nil
	}

	// aggregate sub streams's events in a global one
	envs := []event.Envelope{}
	for _, stmID := range streamIDs {
		envs = append(envs, s.db[stmID]...)
	}

	// default order is ASC
	// slices.SortFunc(envs, func(a, b event.Envelope) int {
	// 	return a.GlobalVersion().Compare(b.GlobalVersion())
	// })
	sort.Slice(envs, func(i, j int) bool {
		return envs[i].GlobalVersion().Compare(envs[j].GlobalVersion()) <= 0
	})

	// prepare query params
	q.Build()

	// prepare stream validation boundaries
	vb := event.ValidationBoundaries{
		From: q.From,
		To:   q.To,
	}
	vb.Build()
	vOpt := func(v *event.Validation) {
		v.GlobalStream = id.Global()
		v.SkipTimeStamp = false
		v.Boundaries = vb
	}

	cur := event.NewCursor(id.String())

	fenvs := []event.Envelope{}
	for _, env := range envs {
		ignore, err := event.ValidateEvent(env, cur, vOpt)
		if err != nil {
			return err
		}
		if ignore {
			continue
		}
		fenvs = append(fenvs, env)
	}

	if q.Order == event.StreamerReplayOrderDESC {
		// slices.SortFunc(fenvs, func(a, b event.Envelope) int {
		// 	return a.GlobalVersion().Compare(b.GlobalVersion()) * -1
		// })
		sort.Slice(fenvs, func(i, j int) bool {
			return fenvs[i].GlobalVersion().Compare(fenvs[j].GlobalVersion()) >= 0
		})
	}

	for _, env := range fenvs {
		// Trunc the stream based on order direction and record limit.
		// Note that record sequences are consecutive at the integer part level
		if q.RecordLimit > 0 {
			if q.Order == event.StreamerReplayOrderDESC {
				if env.GlobalVersion().Before(fenvs[0].GlobalVersion().Drop(uint64(q.RecordLimit), 0)) {
					break
				}
			} else {
				if env.GlobalVersion().Trunc().After(event.VersionZero.Add(uint64(q.RecordLimit), 0)) {
					break
				}
			}
		}

		// Call handler for each event
		if err := h(ctx, event.StreamData{
			Type: event.StreamDataTypeRecord, Value: env,
		}); err != nil {
			return err
		}
	}

	return nil
}
