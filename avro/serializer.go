package avro

import (
	"context"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
	avro_registry "github.com/ln80/event-store/avro/registry"
	"github.com/ln80/event-store/event"
)

var (
	ErrReadOnlyModeEnabled = errors.New("read only mode enabled")
)

type EventSerializer struct {
	registry avro_registry.Registry

	cfg *EventSerializerConfig
}

type EventSerializerConfig struct {
	ReadOnly          bool
	Namespace         string
	SkipCurrentSchema bool
}

func NewEventSerializer(ctx context.Context, registry avro_registry.Registry, opts ...func(*EventSerializerConfig)) *EventSerializer {
	cfg := &EventSerializerConfig{
		ReadOnly: false,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	var (
		sch avro.Schema
		err error
	)
	if !cfg.SkipCurrentSchema {
		sch, err = eventSchema(registry.Client(), cfg.Namespace)
		if err != nil {
			panic(err)
		}
	}

	if err := registry.Setup(ctx, sch, func(rc *avro_registry.RegistryConfig) {
		rc.ReadOnly = cfg.ReadOnly
	}); err != nil {
		panic(err)
	}

	return &EventSerializer{
		registry: registry,
		cfg:      cfg,
	}
}

var _ event.Serializer = &EventSerializer{}

// MarshalEvent implements event.Serializer.
func (s *EventSerializer) MarshalEvent(ctx context.Context, evt event.Envelope) (b []byte, n int, err error) {
	if s.cfg.ReadOnly {
		err = ErrReadOnlyModeEnabled
		return
	}

	if evt == nil {
		err = event.ErrMarshalEmptyEvent
		return
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %v", event.ErrMarshalEventFailed, err)
		}
	}()

	var (
		avroEvt *avroEvent
	)

	avroEvt, err = convertEvent(evt)
	if err != nil {
		return
	}

	if avroEvt == nil {
		err = errors.New("failed to convert event to avro event")
		return
	}

	b, err = s.registry.Marshal(ctx, avroEvt)
	if err != nil {
		return
	}

	n = len(b)

	return
}

// MarshalEventBatch implements event.Serializer.
// Note event size per item n []int is not supported at the moment
func (s *EventSerializer) MarshalEventBatch(ctx context.Context, events []event.Envelope) (b []byte, n []int, err error) {
	if s.cfg.ReadOnly {
		err = ErrReadOnlyModeEnabled
		return
	}

	l := len(events)
	if l == 0 {
		err = event.ErrMarshalEmptyEvent
		return
	}

	n = make([]int, l)

	// normalize failure, and do not propagate infra error
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %v", event.ErrMarshalEventFailed, err)
		}
	}()

	avroEvents := make([]avroEvent, l)
	for i, evt := range events {
		var (
			avroEvt *avroEvent
			ok      bool
		)
		if avroEvt, ok = evt.(*avroEvent); !ok {
			avroEvt, err = convertEvent(evt)
			if err != nil {
				return
			}
		}
		avroEvents[i] = *avroEvt
	}

	b, err = s.registry.MarshalBatch(ctx, avroEvents)
	if err != nil {
		return
	}
	return
}

// UnmarshalEvent implements event.Serializer.
func (s *EventSerializer) UnmarshalEvent(ctx context.Context, b []byte) (event.Envelope, error) {
	avroEvt := avroEvent{}

	if err := s.registry.Unmarshal(ctx, b, &avroEvt); err != nil {
		return nil, err
	}

	return &avroEvt, nil
}

// UnmarshalEventBatch implements event.Serializer.
func (s *EventSerializer) UnmarshalEventBatch(ctx context.Context, b []byte) ([]event.Envelope, error) {
	avroEvents := []avroEvent{}

	if err := s.registry.UnmarshalBatch(ctx, b, &avroEvents); err != nil {
		return nil, err
	}

	envs := make([]event.Envelope, len(avroEvents))
	for i, avroEvt := range avroEvents {
		avroEvt := avroEvt
		envs[i] = &avroEvt
	}
	return envs, nil
}
