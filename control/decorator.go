package control

import (
	"context"
	"log"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/event/errors"
	"github.com/ln80/event-store/event/sourcing"
)

type Decorator struct {
	feature FeatureToggler
	es.EventStore
}

func NewDecorator(store es.EventStore, feature FeatureToggler) *Decorator {
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

	log.Printf("---> toggles %+v", toggles)
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
