package es

import (
	_ "unsafe"

	"github.com/ln80/event-store/event"
	event_sourcing "github.com/ln80/event-store/event/sourcing"
	_ "github.com/ln80/event-store/logger"

	"github.com/go-logr/logr"
)

// EventStore interface combines the following low-level Event Store related interfaces:
//
// - event.Store: defines an event store that support event logging pattern;
//
// - sourcing.Store: defines an event store that support event sourcing pattern;
//
// - event.Streamer: defines an event store that support streaming event from the global stream;
type EventStore interface {
	event.Store
	event_sourcing.Store
	event.Streamer
}

// SetDefaultLogger allows to override the internal default logger used by the library.
//
//go:linkname SetDefaultLogger github.com/ln80/event-store/logger.SetDefault
func SetDefaultLogger(log logr.Logger)

// DiscardLogger returns a mute logger. Pass the mute logger to 'SetDefaultLogger'
// to disable library internal logging.
//
//go:linkname DiscardLogger github.com/ln80/event-store/logger.Discard
func DiscardLogger() logr.Logger
