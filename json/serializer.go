package json

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ln80/event-store/event"
)

// eventSerializer implements event.Serializer interface.
// it uses json serialization, and it's based on event registry
// to unmarshal envelop data aka domain event.
type eventSerializer struct {
	eventRegistry event.Register
}

// NewEventSerializer returns a json event serializer
func NewEventSerializer(namespace string) event.Serializer {
	return &eventSerializer{
		eventRegistry: event.NewRegister(namespace),
	}
}

var _ event.Serializer = &eventSerializer{}

func (s *eventSerializer) MarshalEvent(ctx context.Context, evt event.Envelope) (b []byte, n int, err error) {
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
		jsonEvt *jsonEvent
		ok      bool
	)
	if jsonEvt, ok = evt.(*jsonEvent); !ok {
		jsonEvt, err = convertEvent(evt)
		if err != nil {
			return
		}
	}
	b, err = json.Marshal(jsonEvt)

	n = len(b)

	return
}

func (s *eventSerializer) MarshalEventBatch(ctx context.Context, events []event.Envelope) (b []byte, n []int, err error) {
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

	jsonEvents := make([]jsonEvent, l)
	for i, evt := range events {
		var (
			jsonEvt *jsonEvent
		)
		jsonEvt, err = convertEvent(evt)
		if err != nil {
			return
		}
		jsonEvents[i] = *jsonEvt

		bEvt, _ := json.Marshal(jsonEvt)
		n[i] = len(bEvt)
	}

	b, err = json.Marshal(jsonEvents)
	return
}

func (s *eventSerializer) UnmarshalEvent(ctx context.Context, b []byte) (event.Envelope, error) {
	jsonEvt := jsonEvent{
		reg: s.eventRegistry,
	}
	if err := json.Unmarshal(b, &jsonEvt); err != nil {
		return nil, err
	}
	return &jsonEvt, nil
}

func (s *eventSerializer) UnmarshalEventBatch(ctx context.Context, b []byte) ([]event.Envelope, error) {
	jsonEvents := []jsonEvent{}
	if err := json.Unmarshal(b, &jsonEvents); err != nil {
		return nil, err
	}

	envs := make([]event.Envelope, len(jsonEvents))
	for i, jsonEvt := range jsonEvents {
		jsonEvt := jsonEvt
		jsonEvt.reg = s.eventRegistry
		envs[i] = &jsonEvt
	}
	return envs, nil
}
