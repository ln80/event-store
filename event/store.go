package event

import (
	"context"
	"errors"
	"time"
)

var (
	ErrAppendEventsConflict    = errors.New("append events conflict")
	ErrAppendEventsFailed      = errors.New("append events failure")
	ErrUnsupportedAppendOption = errors.New("unsupported append option")
	ErrLoadEventFailed         = errors.New("load events failure")
)

// AppendOptions presents a generic definition of write operation options.
// the behavior might differs based on the event store implementation.
type AppendOptions struct {
	AddToTx func(ctx context.Context) []any
}

// Store defines the interface of the event logging store aka timestamp-based event store.
// Check sourcing package for a version-based Store interface definition.
type Store interface {
	// Append save a chunk (aka record) of events into a stream
	Append(ctx context.Context, id StreamID, events []Envelope, optFns ...func(*AppendOptions)) error
	// Load events from a stream based on the given timestamp range
	Load(ctx context.Context, id StreamID, trange ...time.Time) ([]Envelope, error)
}
