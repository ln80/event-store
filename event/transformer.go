package event

import (
	"context"
)

type Transformer interface {
	Transform(fn func(cur any) (new any))
}

// Transform the given slice of events by replacing the data (aka domain event)
// with the result of fn function.
func Transform(ctx context.Context, envs []Envelope, fn func(ctx context.Context, evts ...any) ([]any, error)) error {
	index := make(map[int]struct{})
	evts := []any{}
	for i, env := range envs {
		if env == nil {
			continue
		}
		//ignore envelopes that don't satisfy Transformer interface
		if _, ok := env.(Transformer); !ok {
			continue
		}
		evt := env.Event()
		if evt == nil {
			continue
		}
		index[i] = struct{}{}
		evts = append(evts, evt)
	}

	// attempting to ensure atomicity, it might not work in all cases:
	// it's encouraged to use values (instead of pointers) as domain events
	// and keep their structures as simple as possible; avoid like-pointers and complex fields.
	result, err := fn(ctx, evts...)
	if err != nil {
		return err
	}

	for idx := range index {
		r := result[idx]
		envs[idx].(Transformer).Transform(func(_ any) any {
			return r
		})
	}

	return nil
}
