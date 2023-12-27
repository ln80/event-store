package event

import (
	"context"
	"errors"
)

var (
	ErrPublishEventFailed = errors.New("publish events failed")
)

// Publishable presents an event that must be forwarded (in a push fashion) to some system Processors (ex: push-based Projectors)
// Note that not all events must be publishable,
// i.e poll-based Projectors may query the durable event store and replay a chunk of events in a regular basis
type Publishable interface{ EvDests() []string }

// Publisher presents the service responsible for publishing events to the given destinations
type Publisher interface {
	// Publish a chunk of events to their according destinations.
	Publish(ctx context.Context, events []Envelope) error
}

// publishDestinations resolves destinations from the given event.
// As fallback, it checks for destinations from an equivalent event from in global registry.
func publishDestinations(ctx context.Context, evt any) (dests []string) {
	pubEvt, ok := evt.(Publishable)
	dests = make([]string, 0)
	if !ok {
		gEvt, err := NewRegisterFrom(ctx).GetFromGlobal(evt)
		if err != nil {
			return
		}
		pubEvt, ok = gEvt.(Publishable)
		if !ok {
			return
		}
	}
	dests = pubEvt.EvDests()
	return
}
