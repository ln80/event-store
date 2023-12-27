package event

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/ln80/event-store/event/testutil"
)

func TestTransformEvents(t *testing.T) {
	ctx := context.Background()

	globalID := "tenantID"

	stmID := NewStreamID(globalID, "service", "rootEntityID")

	events := []any{
		testutil.Event{
			Val: "1",
		},
		testutil.Event{
			Val: "2",
		},
		testutil.Event{
			Val: "3",
		},
	}
	envs := Wrap(ctx, stmID, events)

	t.Run("partial failure assert atomicity", func(t *testing.T) {
		wantErr := errors.New("transform fake test error")
		if err := Transform(ctx, envs, func(ctx context.Context, evts ...any) ([]any, error) {
			result := make([]any, len(evts))
			for i := range evts {
				if i%2 == 1 {
					return nil, wantErr
				}
				evt := evts[i].(testutil.Event)
				evt.Val = "0"
				result[i] = evt
			}
			return evts, nil
		}); !errors.Is(err, wantErr) {
			t.Fatalf("expect %v, %v be equals", err, wantErr)
		}
		for i, env := range envs {
			want, got := (testutil.Event{Val: strconv.Itoa(i + 1)}), env.Event()
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}
	})

	t.Run("assert transform events successfully", func(t *testing.T) {
		if err := Transform(ctx, envs, func(ctx context.Context, evts ...any) ([]any, error) {
			result := make([]any, len(evts))
			for i := 0; i < len(evts); i++ {
				evt := evts[i].(testutil.Event)
				evt.Val = "0"
				result[i] = evt
			}
			return result, nil
		}); err != nil {
			t.Fatal("expect err be nil, got", err)
		}

		for _, env := range envs {
			// t.Logf("%d: %+v %p", i, env.Event(), env.Event())
			if want, got := "0", env.Event().(testutil.Event).Val; want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}
	})
}
