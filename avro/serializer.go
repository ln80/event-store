package avro

import (
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/event"
)

type EventSerializer[T any] struct {
	namespace string

	schema      avro.Schema
	batchSchema avro.Schema
}

// NewEventSerializer returns a json event serializer
func NewEventSerializer[T any](namespace string) *EventSerializer[T] {
	sch, err := eventSchema[T]()
	if err != nil {
		panic(err)
	}
	batchSch, err := eventBatchSchema[T]()
	if err != nil {
		panic(err)
	}
	return &EventSerializer[T]{
		namespace:   namespace,
		schema:      sch,
		batchSchema: batchSch,
	}
}

var _ event.Serializer = &EventSerializer[any]{}

// MarshalEvent implements event.Serializer.
func (s *EventSerializer[T]) MarshalEvent(evt event.Envelope) (b []byte, n int, err error) {
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
		avroEvt *avroEvent[T]
	)

	avroEvt, err = convertEvent[T](evt)
	if err != nil {
		return
	}

	if avroEvt == nil {
		err = errors.New("failed to convert event to avro event")
		return
	}

	b, err = avro.Marshal(s.schema, *avroEvt)

	n = len(b)

	return
}

// MarshalEventBatch implements event.Serializer.
// Note event size per item n []int is not supported at the moment
func (s *EventSerializer[T]) MarshalEventBatch(events []event.Envelope) (b []byte, n []int, err error) {
	l := len(events)

	if l == 0 {
		err = event.ErrMarshalEmptyEvent
		return
	}

	// init sizes slice
	n = make([]int, l)

	// normalize failure, and do not propagate infra error
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %v", event.ErrMarshalEventFailed, err)
		}
	}()

	avroEvents := make([]avroEvent[T], l)
	for i, evt := range events {
		var (
			avroEvt *avroEvent[T]
			ok      bool
		)
		if avroEvt, ok = evt.(*avroEvent[T]); !ok {
			avroEvt, err = convertEvent[T](evt)
			if err != nil {
				return
			}
		}
		avroEvents[i] = *avroEvt
	}

	b, err = avro.Marshal(s.batchSchema, avroEvents)

	return
}

// UnmarshalEvent implements event.Serializer.
func (s *EventSerializer[T]) UnmarshalEvent(b []byte) (event.Envelope, error) {
	avroEvt := avroEvent[T]{
		namespace: s.namespace,
	}

	if err := avro.Unmarshal(s.schema, b, &avroEvt); err != nil {
		return nil, err
	}
	return &avroEvt, nil
}

// UnmarshalEventBatch implements event.Serializer.
func (s *EventSerializer[T]) UnmarshalEventBatch(b []byte) ([]event.Envelope, error) {
	avroEvents := []avroEvent[T]{}

	if err := avro.Unmarshal(s.batchSchema, b, &avroEvents); err != nil {
		return nil, err
	}

	envs := make([]event.Envelope, len(avroEvents))
	for i, avroEvt := range avroEvents {
		avroEvt := avroEvt
		avroEvt.namespace = s.namespace
		envs[i] = &avroEvt
	}
	return envs, nil
}
