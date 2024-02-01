package control

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// MockLoader is a mock implementation of the Loader interface.
type MockLoader struct {
	LoadFunc func(ctx context.Context) ([]byte, error)
}

// Load calls the LoadFunc of the MockLoader.
func (m *MockLoader) Load(ctx context.Context) ([]byte, error) {
	if m.LoadFunc != nil {
		return m.LoadFunc(ctx)
	}
	return nil, nil
}

var _ Loader = &MockLoader{}

func TestFeatureToggle(t *testing.T) {
	deadline, _ := t.Deadline()
	ctx, ctxCancel := context.WithDeadline(context.Background(), deadline)
	defer ctxCancel()

	f1 := Toggles{
		Index:   true,
		Forward: true,
		Append:  false,
	}
	f2 := Toggles{
		Index:   true,
		Forward: true,
		Append:  true,
	}
	makeLoader := func() func(context.Context) ([]byte, error) {
		idx := 0
		return func(ctx context.Context) ([]byte, error) {
			defer func() { idx++ }()
			c := Configuration{
				Default: &f1,
				Streams: []StreamToggles{
					{
						StreamID: "stm_1",
						Toggles:  f1,
					},
				},
			}

			// to make sure subscriptions will receive notifications
			if idx%2 == 0 {
				c.Streams[0].Toggles = f2

			}

			// to test the case of a new stream that wasn't found in the initial load
			if idx == 2 {
				c.Streams = append(c.Streams, StreamToggles{
					StreamID: "stm_2",
					Toggles:  f2,
				})
			}

			return json.Marshal(c)
		}
	}
	loader := &MockLoader{LoadFunc: makeLoader()}

	f, err := NewFeatureToggler(ctx, loader, func(ftc *FeatureToggleConfig) {
		ftc.CacheMaxAge = 200 * time.Microsecond
		// this one should be ignored as loader mock returns default toggles.
		ftc.Default = f2
	})

	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	tg1, _ := f.Get(ctx, "stm_1")
	if want, got := f2, tg1; want != got {
		t.Fatalf("expect %v,%v be equals", want, got)
	}
	tg2, _ := f.Get(ctx, "stm_2")
	if want, got := f1, tg2; want != got {
		t.Fatalf("expect %v,%v be equals", want, got)
	}

	sub, cancel := f.Subscribe("stm_1")

	inc := 0
	tt, _ := t.Deadline()
	tick := time.After(time.Until(tt.Add(-100 * time.Microsecond)))
	for loop := true; loop; {
		select {
		case <-tick:
			loop = false
		case _, ok := <-sub:
			if !ok {
				t.Fatal("expect channel be opened")
			}
			inc++
			if inc > 2 {
				loop = false
			}
		}
	}
	if inc <= 2 {
		t.Fatalf("expect more notification, got %d", inc)
	}

	cancel()
	if _, opened := <-sub; opened {
		t.Fatal("expect subscription channel to be closed")
	}

	// assert that stm 2 toggles have changed at the meantime != default
	tg2, _ = f.Get(ctx, "stm_2")
	if want, got := f2, tg2; want != got {
		t.Fatalf("expect %v,%v be equals", want, got)
	}
}

func TestFeatureToggle_WithLoadError(t *testing.T) {
	ctx := context.Background()

	makeLoader := func() func(context.Context) ([]byte, error) {
		idx := 0
		return func(ctx context.Context) ([]byte, error) {
			defer func() { idx++ }()
			c := Configuration{
				Streams: []StreamToggles{
					{
						StreamID: "stm_1",
					},
				},
			}
			if idx > 0 {
				return nil, errors.New("infra error")
			}
			return json.Marshal(c)
		}
	}
	loader := &MockLoader{LoadFunc: makeLoader()}

	f, err := NewFeatureToggler(ctx, loader, func(ftc *FeatureToggleConfig) {
		ftc.CacheMaxAge = 200 * time.Microsecond
	})
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	time.Sleep(f.cfg.CacheMaxAge)

	_, err = f.getRecentConfiguration(ctx)
	if want, got := ErrLoadConfigurationFailed, err; !errors.Is(got, want) {
		t.Fatalf("expect %v,%v be equals", want, got)
	}
}
