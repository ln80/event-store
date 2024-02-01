package event

import (
	"context"

	"github.com/ln80/event-store/event/errors"
)

var (
	ErrMarshalEventFailed   = errors.New("marshal event(s) failed")
	ErrMarshalEmptyEvent    = errors.New("event to marshal is empty")
	ErrUnmarshalEventFailed = errors.New("unmarshal event(s) failed")
)

// Serializer provides a standard encoding/decoding interface for events
type Serializer interface {

	// MarshalEvent returns a binary version of the event and its size according to the supported format.
	MarshalEvent(ctx context.Context, event Envelope) ([]byte, error)

	// MarshalEventBatch returns a binary version of the given chunk of events. It also returns a slice of events' size.
	// It fails if chunk is empty.
	MarshalEventBatch(ctx context.Context, events []Envelope) ([]byte, error)

	// UnmarshalEvent returns an event envelope based on the binary/raw given event.
	// The returned envelope might be nil in case the event type if not found in the registry.
	UnmarshalEvent(ctx context.Context, b []byte) (Envelope, error)

	// UnmarshalEvent returns a slice of envelopes based on the binary given chunk of events.
	// Similarly to UnmarshalEvent, events might be nil if event type is not found in the registry.
	UnmarshalEventBatch(ctx context.Context, b []byte) ([]Envelope, error)
}
