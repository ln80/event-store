package avro

import (
	"context"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
)

var (
	ErrReadOnlyModeEnabled = errors.New("read only mode enabled")
)

type EventSerializer struct {
	// registry Registry

	registry *registry.Registry

	cfg *EventSerializerConfig
}

type EventSerializerConfig struct {
	ReadOnly  bool
	Namespace string
	// SkipCurrentSchema disables the generation of the current schema from registered event.
	SkipCurrentSchema    bool
	PersistCurrentSchema bool
}

func NewEventSerializer(ctx context.Context, r *registry.Registry, opts ...func(*EventSerializerConfig)) *EventSerializer {
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

	log := logger.FromContext(ctx).WithName("avro").WithValues("namespace", cfg.Namespace)

	if !cfg.SkipCurrentSchema {
		sch, err = eventSchema(r.API(), cfg.Namespace)
		if err != nil {
			log.Error(err, "Failed Avro schema generation")
			panic(err)
		}
		log.V(1).Info("Generated Avro schema", "schema", sch.String())
	}

	if err := r.Setup(ctx, sch, func(rc *registry.RegistryConfig) {
		rc.PersistCurrent = cfg.PersistCurrentSchema
	}); err != nil {
		log.Error(err, "Failed to setup Avro registry")
		panic(err)
	}

	return &EventSerializer{
		registry: r,
		cfg:      cfg,
	}
}

var _ event.Serializer = &EventSerializer{}

// MarshalEvent implements event.Serializer.
func (s *EventSerializer) MarshalEvent(ctx context.Context, evt event.Envelope) (b []byte, err error) {
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

	id, schema, _, err := s.registry.GetCurrent(ctx, avroEvt)
	if err != nil {
		return
	}

	b, err = s.registry.API().Marshal(schema, avroEvt)
	if err != nil {
		return
	}

	b, err = s.registry.AppendSchemaID(b, id)
	if err != nil {
		return
	}

	return
}

// MarshalEventBatch implements event.Serializer.
// Note event size per item n []int is not supported at the moment
func (s *EventSerializer) MarshalEventBatch(ctx context.Context, events []event.Envelope) (b []byte, err error) {
	if s.cfg.ReadOnly {
		err = ErrReadOnlyModeEnabled
		return
	}

	l := len(events)
	if l == 0 {
		err = event.ErrMarshalEmptyEvent
		return
	}

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

	id, _, batchSchema, err := s.registry.GetCurrent(ctx, &avroEvents[0])
	if err != nil {
		return
	}

	b, err = s.registry.API().Marshal(batchSchema, avroEvents)
	if err != nil {
		return
	}

	b, err = s.registry.AppendSchemaID(b, id)
	if err != nil {
		return
	}

	return
}

// UnmarshalEvent implements event.Serializer.
func (s *EventSerializer) UnmarshalEvent(ctx context.Context, b []byte) (event.Envelope, error) {
	avroEvt := avroEvent{}

	id, b, err := s.registry.ExtractSchemaID(b)
	if err != nil {
		return nil, err
	}
	_, schema, _, err := s.registry.GetSchema(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := s.registry.API().Unmarshal(schema, b, &avroEvt); err != nil {
		return nil, err
	}

	avroEvt.SetAVROSchemaID(id)
	avroEvt.checkType(s.cfg.Namespace)

	return &avroEvt, nil
}

// UnmarshalEventBatch implements event.Serializer.
func (s *EventSerializer) UnmarshalEventBatch(ctx context.Context, b []byte) ([]event.Envelope, error) {
	avroEvents := []avroEvent{}

	// if err := s.registry.UnmarshalBatch(ctx, b, &avroEvents); err != nil {
	// 	return nil, err
	// }

	id, b, err := s.registry.ExtractSchemaID(b)
	if err != nil {
		return nil, err
	}
	_, _, batchSchema, err := s.registry.GetSchema(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := s.registry.API().Unmarshal(batchSchema, b, &avroEvents); err != nil {
		return nil, err
	}

	envs := make([]event.Envelope, len(avroEvents))
	for i, avroEvt := range avroEvents {
		avroEvt := avroEvt
		avroEvt.SetAVROSchemaID(id)
		avroEvt.checkType(s.cfg.Namespace)
		envs[i] = &avroEvt

	}
	return envs, nil
}
