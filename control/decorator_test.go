package control

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/testutil"
	"github.com/ln80/event-store/memory"
)

func TestDecorator(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	var store es.EventStore = memory.NewEventStore()

	idx := 0
	l1 := func(ctx context.Context) ([]byte, error) {
		tg := &Toggles{
			Append: false,
			Index:  true,
		}
		if idx > 1 {
			tg = &Toggles{
				Append: true,
				Index:  true,
			}
		}
		idx++
		return json.Marshal(Configuration{
			Default: tg,
		})
	}
	loader := &MockLoader{LoadFunc: l1}

	f, err := NewFeatureToggler(ctx, loader, func(ftc *FeatureToggleConfig) {
		ftc.CacheMaxAge = 50 * time.Microsecond
	})

	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	store = NewDecorator(store, f)

	streamID := event.NewStreamID(event.UID().String())

	evts := event.Wrap(ctx, streamID, testutil.GenEvents(10))
	if want, got := ErrFeatureDisabled, store.Append(ctx, streamID, evts); !errors.Is(got, want) {
		t.Fatalf("expect %v, %v be equals", want, got)
	}

	time.Sleep(f.cfg.CacheMaxAge)

	if err = store.Append(ctx, streamID, evts); err != nil {
		t.Fatal("expect err be nil, got", err)
	}
}
