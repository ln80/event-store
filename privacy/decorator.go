package privacy

import (
	"context"
	"time"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/event"
	event_errors "github.com/ln80/event-store/event/errors"
	"github.com/ln80/event-store/event/sourcing"
	privacy "github.com/ln80/privacy-engine"
	sensitive "github.com/ln80/struct-sensitive"
)

// Decorate decorates the given event store to add some PII protections.
// It performs client-side encryption of event personal data.
func Decorate(store es.EventStore, f privacy.Factory) *Decorator {
	return &Decorator{
		encryptor: f,
		store:     store,
	}
}

type Decorator struct {
	encryptor privacy.Factory
	store     es.EventStore
}

var _ es.EventStore = &Decorator{}

// func makePtr(value any) any {
// 	typ := reflect.TypeOf(value)
// 	// val := reflect.ValueOf(value)
// 	if typ.Kind() == reflect.Ptr {
// 		return value
// 	}
// 	ptr := reflect.New(typ)
// 	ptr.Elem().Set(reflect.ValueOf(value))
// 	return ptr.Interface()
// }

// type PointerIndex struct {
// 	Pointer
// 	idx int
// }

// type Pointer struct {
// 	Dereference func() any
// 	Ptr         any
// }

// func ToPtr(e any) Pointer {
// 	valType := reflect2.TypeOf(e)
// 	if valType.Kind() == reflect.Pointer {
// 		return Pointer{Ptr: e}
// 	}

// 	unsafePtr := valType.UnsafeNew()
// 	valType.UnsafeSet(unsafePtr, reflect2.PtrOf(e))
// 	ptr := valType.PackEFace(unsafePtr)
// 	return Pointer{Ptr: ptr, Dereference: func() any { return valType.Indirect(ptr) }}
// }

// func referenceEvents(events []event.Envelope, cond func(env event.Envelope) bool) (evtPtrs []any, indexes map[int]PointerIndex) {
// 	evtPtrs = make([]any, 0)
// 	indexes = make(map[int]PointerIndex)
// 	idx := 0
// 	for i, env := range event.Stream(events) {
// 		if !cond(env) {
// 			continue
// 		}

// 		ptr := ToPtr(env.Event())
// 		evtPtrs = append(evtPtrs, ptr.Ptr)
// 		if ptr.Dereference == nil {
// 			continue
// 		}

// 		indexes[i] = PointerIndex{
// 			Pointer: ptr,
// 			idx:     idx,
// 		}
// 		idx++
// 	}

// 	return
// }

func (s *Decorator) Origin() es.EventStore {
	return s.store
}

// Append implements EventStore
func (s *Decorator) Append(ctx context.Context, id event.StreamID, events []event.Envelope, opts ...func(*event.AppendConfig)) error {
	p, _ := s.encryptor.Instance(id.GlobalID())

	evtPtrs, indexes := event.ReferenceEvents(events, func(env event.Envelope) bool {
		if _, ok := env.(event.Transformer); !ok {
			return false
		}
		ok, _ := sensitive.Check(env.Event())
		return ok
	})

	if err := p.Encrypt(ctx, evtPtrs...); err != nil {
		return event_errors.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	for idx, index := range indexes {
		if index.Dereference == nil {
			continue
		}
		events[idx].(event.Transformer).Transform(func(val any) (new any) {
			new = index.Dereference()
			return
		})
	}

	return s.store.Append(ctx, id, events, opts...)
}

// Load implements EventStore
func (s *Decorator) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
	p, _ := s.encryptor.Instance(id.GlobalID())

	events, err := s.store.Load(ctx, id, trange...)
	if err != nil {
		return nil, err
	}

	// Filter and extract event that contains PII from the chunk of envelopes.
	// A list of event pointers is required to encrypt/decrypt PII at struct fields level.
	evtPtrs, indexes := event.ReferenceEvents(events, func(env event.Envelope) bool {
		if _, ok := env.(event.Transformer); !ok {
			return false
		}
		ok, _ := sensitive.Check(env.Event())
		return ok
	})
	if err := p.Decrypt(ctx, evtPtrs...); err != nil {
		return nil, event_errors.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	for idx, index := range indexes {
		// Nil 'Dereference' function mains the original event value is already a pointer;
		// no need to call transform.
		if index.Dereference == nil {
			continue
		}

		// Make sure to set the new value of the event
		events[idx].(event.Transformer).Transform(func(val any) (new any) {
			new = index.Dereference()
			return
		})
	}

	return events, nil
}

// Replay implements EventStore
func (s *Decorator) Replay(ctx context.Context, stmID event.StreamID, f event.StreamerQuery, h event.StreamerHandler) error {
	p, _ := s.encryptor.Instance(stmID.GlobalID())
	// fn := func(ctx context.Context, ptrs ...any) error {
	// 	return p.Decrypt(ctx, ptrs...)
	// }

	ph := func(ctx context.Context, data event.StreamData) error {
		// if data.Type == event.StreamDataTypeRecord {
		// 	events := []event.Envelope{data.Value.(event.Envelope)}
		// 	if err := event.Transform(ctx, events, fn); err != nil {
		// 		return err
		// 	}
		// 	data.Value = events[0]
		// }

		if data.Type == event.StreamDataTypeRecord {

			env, ok := data.Value.(event.Envelope)
			if !ok {
				return h(ctx, data)
			}

			_, ok = env.(event.Transformer)
			if !ok {
				return h(ctx, data)
			}

			ptr := event.ToPtr(env.Event())

			if err := p.Decrypt(ctx, ptr.Ptr); err != nil {
				return err
			}

			if ptr.Dereference == nil {
				return h(ctx, data)
			}
			env.(event.Transformer).Transform(func(cur any) (new any) {
				new = ptr.Dereference()
				return
			})

			return h(ctx, data)
		}

		return h(ctx, data)
	}

	return s.store.Replay(ctx, stmID, f, ph)
}

// AppendToStream implements EventStore
func (s *Decorator) AppendToStream(ctx context.Context, chunk sourcing.Stream, opts ...func(*event.AppendConfig)) error {
	p, _ := s.encryptor.Instance(chunk.ID().GlobalID())
	// fn := func(ctx context.Context, ptrs ...any) error {
	// 	return p.Encrypt(ctx, ptrs...)
	// }
	// fn := func(ctx context.Context, evts ...any) ([]any, error) {
	// 	result := make([]any, len(evts))
	// 	for i, evt := range evts {
	// 		// evt := evt
	// 		evtPtr := makePtr(evt)
	// 		if err := p.Encrypt(ctx, evtPtr); err != nil {
	// 			return nil, err
	// 		}
	// 		log.Printf("encrypt ... %+v -- %T %T %+v", evt, evt, evtPtr, evtPtr)

	// 		// result[i] = evt
	// 		result[i] = reflect.ValueOf(evtPtr).Elem().Interface()
	// 	}

	// 	return result, nil
	// }

	// if err := event.Transform(ctx, chunk.Unwrap(), fn); err != nil {
	// 	return event_errors.Err(event.ErrAppendEventsFailed, chunk.ID().GlobalID(), err)
	// }

	evtPtrs, indexes := event.ReferenceEvents(chunk.Unwrap(), func(env event.Envelope) bool {
		if _, ok := env.(event.Transformer); !ok {
			return false
		}
		ok, _ := sensitive.Check(env.Event())
		return ok
	})

	if err := p.Encrypt(ctx, evtPtrs...); err != nil {
		return event_errors.Err(event.ErrAppendEventsFailed, chunk.ID().String(), err)
	}

	for idx, index := range indexes {
		if index.Dereference == nil {
			continue
		}
		chunk.Unwrap()[idx].(event.Transformer).Transform(func(val any) (new any) {
			new = index.Dereference()
			return
		})
	}

	return s.store.AppendToStream(ctx, chunk, opts...)
}

// LoadStream implements EventStore
func (s *Decorator) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*sourcing.Stream, error) {
	p, _ := s.encryptor.Instance(id.GlobalID())
	// fn := func(ctx context.Context, ptrs ...any) error {
	// 	return p.Decrypt(ctx, ptrs...)
	// }
	// fn := func(ctx context.Context, evts ...any) ([]any, error) {
	// 	result := make([]any, len(evts))
	// 	for i, evt := range evts {
	// 		evtPtr := makePtr(evt)
	// 		if err := p.Decrypt(ctx, evtPtr); err != nil {
	// 			return nil, err
	// 		}
	// 		result[i] = reflect.ValueOf(evtPtr).Elem().Interface()
	// 	}
	// 	return result, nil
	// }

	stm, err := s.store.LoadStream(ctx, id, vrange...)
	if err != nil {
		return nil, err
	}

	evtPtrs, indexes := event.ReferenceEvents(stm.Unwrap(), func(env event.Envelope) bool {
		if _, ok := env.(event.Transformer); !ok {
			return false
		}
		ok, _ := sensitive.Check(env.Event())
		return ok
	})
	if err := p.Decrypt(ctx, evtPtrs...); err != nil {
		return nil, event_errors.Err(event.ErrLoadEventFailed, id.String(), err)
	}

	for idx, index := range indexes {
		// Nil 'Dereference' function mains the original event value is already a pointer;
		// no need to call transform.
		if index.Dereference == nil {
			continue
		}

		// Make sure to set the new value of the event
		stm.Unwrap()[idx].(event.Transformer).Transform(func(val any) (new any) {
			new = index.Dereference()
			return
		})
	}

	// if err := event.Transform(ctx, stm.Unwrap(), fn); err != nil {
	// 	return nil, event_errors.Err(event.ErrLoadEventFailed, id.String(), err)
	// }

	return stm, nil
}

var _ es.EventStore = &Decorator{}
