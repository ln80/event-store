package event

import (
	"reflect"

	"github.com/modern-go/reflect2"
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
	idx int
}

func ToPtr(evt any) Pointer {
	valType := reflect2.TypeOf(evt)
	if valType.Kind() == reflect.Pointer {
		return Pointer{Ptr: evt}
	}

	unsafePtr := valType.UnsafeNew()
	valType.UnsafeSet(unsafePtr, reflect2.PtrOf(evt))
	ptr := valType.PackEFace(unsafePtr)
	return Pointer{Ptr: ptr, Dereference: func() any { return valType.Indirect(ptr) }}
}

func ReferenceEvents(events []Envelope, cond func(env Envelope) bool) (evtPtrs []any, indexes map[int]PointerIndex) {
	evtPtrs = make([]any, 0)
	indexes = make(map[int]PointerIndex)
	idx := 0
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
			idx:     idx,
		}
		idx++
	}

	return
}

// // Transform the given slice of events by replacing the data (aka domain event)
// // with the result of fn function.
// func Transform(ctx context.Context, envs []Envelope, fn func(ctx context.Context, evts ...any) ([]any, error)) error {
// 	index := make(map[int]struct{})
// 	evts := []any{}
// 	for i, env := range envs {
// 		if env == nil {
// 			continue
// 		}
// 		//ignore envelopes that don't satisfy Transformer interface
// 		if _, ok := env.(Transformer); !ok {
// 			continue
// 		}
// 		evt := env.Event()
// 		if evt == nil {
// 			continue
// 		}
// 		index[i] = struct{}{}
// 		evts = append(evts, evt)
// 	}

// 	// attempting to ensure atomicity, it might not work in all cases:
// 	// it's encouraged to use values (instead of pointers) as domain events
// 	// and keep their structures as simple as possible; avoid like-pointers and complex fields.
// 	result, err := fn(ctx, evts...)
// 	if err != nil {
// 		return err
// 	}

// 	for idx := range index {
// 		r := result[idx]
// 		envs[idx].(Transformer).Transform(func(_ any) any {
// 			return r
// 		})
// 	}

// 	return nil
// }
