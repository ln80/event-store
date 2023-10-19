package es

import (
	"github.com/ln80/event-store/event"
	event_sourcing "github.com/ln80/event-store/event/sourcing"
)

// EventStore interface combines event.Store, event.Streamer, sourcing.Store interfaces
type EventStore interface {
	event.Store
	event_sourcing.Store
	event.Streamer
}
