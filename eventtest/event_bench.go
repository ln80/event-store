package eventtest

import (
	"context"
	"testing"

	"github.com/ln80/event-store/event"
)

func BenchmarkEventStore_Append(b *testing.B, store event.Store) {

	b.Run("append", func(b *testing.B) {

		ctx := context.Background()

		streamID := event.NewStreamID(event.UID().String())

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			envs := event.Wrap(ctx, streamID, GenEvents(10))
			if err := store.Append(ctx, streamID, envs); err != nil {
				b.Fatalf("expect to append events, got err: %v", err)
			}
		}
	})
}

func BenchmarkEventStore_Load(b *testing.B, store event.Store) {

	b.Run("load", func(b *testing.B) {
		ctx := context.Background()
		streamID := event.NewStreamID(event.UID().String())

		for n := 0; n < b.N; n++ {
			b.StopTimer()

			if err := store.Append(ctx, streamID, event.Wrap(ctx, streamID, GenEvents(10))); err != nil {
				b.Fatalf("expect to append events, got err: %v", err)
			}

			b.StartTimer()
			_, err := store.Load(ctx, streamID)
			if err != nil {
				b.Fatalf("expect to load events, got err: %v", err)
			}
		}
	})
}
