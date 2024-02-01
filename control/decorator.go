package control

import (
	"context"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/event/errors"
	"github.com/ln80/event-store/event/sourcing"
)

type eventStore interface {
	event.Store
	event.Streamer
	sourcing.Store
}

type Decorator struct {
	feature FeatureToggler
	es.EventStore
}

func NewDecorator(store eventStore, feature FeatureToggler) *Decorator {
	return &Decorator{
		feature:    feature,
		EventStore: store,
	}
}

func (d *Decorator) Append(ctx context.Context, id event.StreamID, events []event.Envelope, optFns ...func(*event.AppendConfig)) error {
	toggles, err := d.feature.Get(ctx, id.GlobalID())
	if err != nil {
		return err
	}

	if err := toggles.Enabled(APPEND); err != nil {
		return errors.Err(event.ErrAppendEventsFailed, id.String(), err)
	}

	return d.EventStore.Append(ctx, id, events, optFns...)
}

func (d *Decorator) AppendToStream(ctx context.Context, chunk sourcing.Stream, optFns ...func(opt *event.AppendConfig)) error {
	toggles, err := d.feature.Get(ctx, chunk.ID().GlobalID())
	if err != nil {
		return err
	}

	if err := toggles.Enabled(APPEND); err != nil {
		return errors.Err(event.ErrAppendEventsFailed, chunk.ID().String(), err)
	}

	return d.EventStore.AppendToStream(ctx, chunk, optFns...)
}

// func (d *Decorator) Load(ctx context.Context, id event.StreamID, trange ...time.Time) ([]event.Envelope, error) {
// 	return d.EventStore.Load(ctx, id, trange...)
// }

// func (d *Decorator) LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*sourcing.Stream, error) {
// 	return d.EventStore.LoadStream(ctx, id, vrange...)
// }

// // Replay implements es.EventStore.
// func (d *Decorator) Replay(ctx context.Context, streamID event.StreamID, q event.StreamerQuery, h event.StreamerHandler) error {
// 	return d.EventStore.Replay(ctx, streamID, q, h)
// }
