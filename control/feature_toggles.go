package control

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ln80/event-store/event/errors"
)

var (
	ErrLoadConfigurationFailed = errors.New("load configuration failed")
	ErrFeatureDisabled         = errors.New("feature disabled")
)

type Feature string

const (
	APPEND  Feature = "append"
	INDEX   Feature = "index"
	FORWARD Feature = "forward"
)

type Toggles struct {
	Index, Forward, Append bool
}

type StreamToggles struct {
	StreamID string
	Toggles
}

type Configuration struct {
	Default *Toggles
	Streams []StreamToggles
}

func (c Configuration) Get(streamID string, def ...*Toggles) Toggles {
	for _, f := range c.Streams {
		if f.StreamID == streamID {
			return f.Toggles
		}
	}
	if c.Default == nil && len(def) > 0 {
		return *def[0]
	}

	return *c.Default
}

type configurationCache struct {
	Configuration
	At time.Time
}

func (t Toggles) IsZero() bool {
	return t == Toggles{}
}
func (t Toggles) Enabled(f Feature) error {
	toggle := false
	switch f {
	case APPEND:
		toggle = t.Append
	case INDEX:
		toggle = t.Index
	case FORWARD:
		toggle = t.Forward
	}

	if !toggle {
		return ErrFeatureDisabled
	}

	return nil
}

type Cancel func()

type FeatureToggler interface {
	Subscribe(streamID string) (chan Toggles, Cancel)
	Get(ctx context.Context, streamID string) (Toggles, error)
}

type Loader interface {
	Load(ctx context.Context) ([]byte, error)
}

type FeatureToggleConfig struct {
	CacheMaxAge time.Duration
	Default     Toggles
}

type FeatureToggle struct {
	loader      Loader
	cache       atomic.Value // *configurationCache
	mu          sync.RWMutex
	subscribers map[string][]chan Toggles

	cfg *FeatureToggleConfig
}

func NewFeatureToggler(ctx context.Context, loader Loader, opts ...func(*FeatureToggleConfig)) (*FeatureToggle, error) {
	f := &FeatureToggle{
		loader:      loader,
		subscribers: make(map[string][]chan Toggles),
		cfg: &FeatureToggleConfig{
			CacheMaxAge: 30 * time.Second,
			Default: Toggles{
				Index:   true,
				Forward: true,
				Append:  true,
			},
		},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(f.cfg)
	}

	cache, err := f.load(ctx)
	if err != nil {
		return nil, err
	}

	f.cache.Store(cache)

	go f.watch(ctx)

	return f, nil
}

func (e *FeatureToggle) Get(ctx context.Context, streamID string) (Toggles, error) {
	cache, err := e.getRecentConfiguration(ctx)
	if err != nil {
		return Toggles{}, err
	}

	return cache.Get(streamID, &e.cfg.Default), nil
}

func (e *FeatureToggle) Subscribe(streamID string) (chan Toggles, Cancel) {
	cache := e.cache.Load().(*configurationCache)

	toggles := cache.Configuration.Get(streamID, &e.cfg.Default)

	e.mu.Lock()
	defer e.mu.Unlock()

	ch := make(chan Toggles, 1)
	ch <- toggles

	l := e.subscribers[streamID]
	l = append(l, ch)

	e.subscribers[streamID] = l

	cancel := func() {
		e.mu.Lock()
		defer e.mu.Unlock()

		l := make([]chan Toggles, 0, len(e.subscribers[streamID])-1)
		for _, stored := range e.subscribers[streamID] {
			if stored == ch {
				continue
			}
			l = append(l, stored)
		}
		e.subscribers[streamID] = l
		close(ch)
	}

	return ch, cancel
}

func (e *FeatureToggle) notify(streamID string, newToggles Toggles) {
	e.mu.Lock()
	defer e.mu.Unlock()

	chans := e.subscribers[streamID]

	for _, ch := range chans {
		loop := true
		for loop {
			select {
			case ch <- newToggles:
				loop = false
			default:
				// If somehow the channel is full, remove the old entry
				// and then loop and add the most recent one.
				select {
				case <-ch:
				default:
				}
			}
		}
	}
}

func (e *FeatureToggle) getRecentConfiguration(ctx context.Context) (*Configuration, error) {
	cache := e.cache.Load().(*configurationCache)

	if time.Since(cache.At) > e.cfg.CacheMaxAge {
		c, err := e.load(ctx)
		if err != nil {
			return nil, err
		}
		cache = c
		e.cache.Store(cache)
	}

	return &cache.Configuration, nil
}

func (e *FeatureToggle) load(ctx context.Context) (*configurationCache, error) {
	b, err := e.loader.Load(ctx)
	if err != nil {
		return nil, errors.Err(ErrLoadConfigurationFailed, "", err)
	}

	// an empty response means no change has occurred in config's server-side.
	if len(b) == 0 {
		return e.cache.Load().(*configurationCache), nil
	}

	config := Configuration{}
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, errors.Err(ErrLoadConfigurationFailed, "", err)
	}

	return &configurationCache{Configuration: config, At: time.Now()}, nil
}

func (e *FeatureToggle) watch(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		time.Sleep(e.cfg.CacheMaxAge)

		newConfig, err := e.load(ctx)
		if err != nil {
			return err
		}

		cache := e.cache.Load().(*configurationCache)
		seen := make(map[string]struct{}, 0)
		for _, cur := range cache.Configuration.Streams {
			newToggles := newConfig.Get(cur.StreamID, newConfig.Default)
			if newToggles != cur.Toggles {
				e.notify(cur.StreamID, newToggles)
			}
			seen[cur.StreamID] = struct{}{}
		}
		for _, n := range newConfig.Configuration.Streams {
			if _, ok := seen[n.StreamID]; ok {
				continue
			}
			if n.Toggles != *cache.Default && n.Toggles != e.cfg.Default {
				e.notify(n.StreamID, n.Toggles)
			}
		}

		e.cache.Store(newConfig)
	}
}

var _ FeatureToggler = &FeatureToggle{}
