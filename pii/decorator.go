package pii

import (
	"context"
	"time"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/event/sourcing"
	"github.com/ln80/pii"
)

// ProtectPII decorates the given event store to add some PII protections.
// It performs client-side encryption of event personal data.
func ProtectPII(store es.EventStore, f pii.Factory) *Decorator {
	return &Decorator{
		encryptor: f,
		store:     store,
	}
}

type Decorator struct {
	encryptor pii.Factory
	store     es.EventStore
}

var _ es.EventStore = &Decorator{}

// Append implements EventStore
func (s *Decorator) Append(ctx context.Context, id event.StreamID, events []event.Envelope, optFns ...func(*event.AppendOptions)) error {
	p, _ := s.encryptor.Instance(id.GlobalID())
	fn := func(ctx context.Context, ptrs ...any) error {
		return p.Encrypt(ctx, ptrs...)
	}

	if err := event.Transform(ctx, events, fn); err != nil {
		return event.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	return s.store.Append(ctx, id, events, optFns...)
}

// Load implements EventStore
func (s *Decorator) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
	p, _ := s.encryptor.Instance(id.GlobalID())
	fn := func(ctx context.Context, ptrs ...any) error {
		return p.Decrypt(ctx, ptrs...)
	}

	events, err := s.store.Load(ctx, id, trange...)
	if err != nil {
		return nil, err
	}
	if err := event.Transform(ctx, events, fn); err != nil {
		return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	return events, nil
}

// Replay implements EventStore
func (s *Decorator) Replay(ctx context.Context, stmID event.StreamID, f event.StreamerQuery, h event.StreamerHandler) error {
	p, _ := s.encryptor.Instance(stmID.GlobalID())
	fn := func(ctx context.Context, ptrs ...any) error {
		return p.Decrypt(ctx, ptrs...)
	}

	ph := func(ctx context.Context, data event.StreamData) error {
		if data.Type == event.StreamDataTypeRecord {
			events := []event.Envelope{data.Value.(event.Envelope)}
			if err := event.Transform(ctx, events, fn); err != nil {
				return err
			}
			data.Value = events[0]
		}

		return h(ctx, data)
	}

	return s.store.Replay(ctx, stmID, f, ph)
}

// AppendToStream implements EventStore
func (s *Decorator) AppendToStream(ctx context.Context, chunk sourcing.Stream, optFns ...func(*event.AppendOptions)) error {
	p, _ := s.encryptor.Instance(chunk.ID().GlobalID())
	fn := func(ctx context.Context, ptrs ...any) error {
		return p.Encrypt(ctx, ptrs...)
	}

	if err := event.Transform(ctx, chunk.Unwrap(), fn); err != nil {
		return event.Err(event.ErrAppendEventsFailed, chunk.ID().GlobalID(), err)
	}

	return s.store.AppendToStream(ctx, chunk, optFns...)
}

// LoadStream implements EventStore
func (s *Decorator) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*sourcing.Stream, error) {
	p, _ := s.encryptor.Instance(id.GlobalID())
	fn := func(ctx context.Context, ptrs ...any) error {
		return p.Decrypt(ctx, ptrs...)
	}

	stm, err := s.store.LoadStream(ctx, id, vrange...)
	if err != nil {
		return nil, err
	}
	if err := event.Transform(ctx, stm.Unwrap(), fn); err != nil {
		return nil, event.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	return stm, nil
}

var _ es.EventStore = &Decorator{}
