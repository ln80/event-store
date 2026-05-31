package event

import (
	"reflect"
)

// Transformer, implemented by an event envelope, makes it capable to mutate it's original event.
// It's mainly used by event store decorators.
type Transformer interface {
	Transform(fn func(cur any) (new any))
}

// Pointer presents a pointer to an event within an envelope.
// Dereference function allows getting the pointer value when a copy
// is already made of the original event.
type Pointer struct {
	Dereference func() any
	Ptr         any
}

type PointerIndex struct {
	Pointer
}

func ToPtr(evt any) Pointer {
	v := reflect.ValueOf(evt)
	if v.Kind() == reflect.Pointer {
		return Pointer{Ptr: evt}
	}
	ptr := reflect.New(v.Type())
	ptr.Elem().Set(v)
	iface := ptr.Interface()
	return Pointer{Ptr: iface, Dereference: func() any {
		return reflect.ValueOf(iface).Elem().Interface()
	}}
}

func ReferenceEvents(events []Envelope, cond func(env Envelope) bool) (evtPtrs []any, indexes map[int]PointerIndex) {
	evtPtrs = make([]any, 0)
	indexes = make(map[int]PointerIndex)
	for i, env := range events {
		if !cond(env) {
			continue
		}

		ptr := ToPtr(env.Event())
		evtPtrs = append(evtPtrs, ptr.Ptr)
		if ptr.Dereference == nil {
			continue
		}

		indexes[i] = PointerIndex{
			Pointer: ptr,
		}
	}

	return
}
