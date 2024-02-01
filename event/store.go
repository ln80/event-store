package event

import (
	"context"
	"time"

	"github.com/ln80/event-store/event/errors"
)

var (
	ErrAppendEventsConflict    = errors.New("append events conflict")
	ErrAppendEventsFailed      = errors.New("append events failure")
	ErrUnsupportedAppendOption = errors.New("unsupported append option")
	ErrLoadEventFailed         = errors.New("load events failure")
	ErrEventSizeLimitExceeded  = errors.New("event record size limit exceeded")
)

// AppendConfig presents a generic definition of write operation options.
// the behavior might differs based on the event store implementation.
type AppendConfig struct {
	AddToTx    func(ctx context.Context) (items []any)
	AddTracing func(ctx context.Context) (traceID string)
}

// Store defines the interface of the event logging store aka timestamp-based event store.
// Check sourcing package for a version-based Store interface definition.
type Store interface {
	// Append save a chunk (aka record) of events into a stream
	Append(ctx context.Context, id StreamID, events []Envelope, optFns ...func(*AppendConfig)) error
	// Load events from a stream based on the given timestamp range
	Load(ctx context.Context, id StreamID, trange ...time.Time) ([]Envelope, error)
}
