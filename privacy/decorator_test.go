package privacy

import (
	"context"
	"testing"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/eventtest"
	"github.com/ln80/event-store/memory"

	privacy "github.com/ln80/privacy-engine"
	privacy_memory "github.com/ln80/privacy-engine/memory"
)

func TestPrivacyProtectDecorator(t *testing.T) {
	eventtest.RegisterEvent("")

	ctx := context.Background()

	var store es.EventStore = memory.NewEventStore()

	// check: https://github.com/ln80/privacy for more details
	pf := privacy.NewFactory(func(namespace string) privacy.Protector {
		return privacy.NewProtector(namespace, privacy_memory.NewKeyEngine())
	})
	defer pf.Monitor(ctx)

	store = Decorate(store, pf)
	eventtest.TestEventLoggingStore(t, ctx, store)
	eventtest.TestEventSourcingStore(t, ctx, store)
	eventtest.TestEventStreamer(t, ctx, store)
}

func BenchmarkDecorator(b *testing.B) {
	b.Run("without decorator", func(b *testing.B) {
		eventtest.BenchmarkEventStore_Append(b, memory.NewEventStore())
		eventtest.BenchmarkEventStore_Load(b, memory.NewEventStore())
	})

	b.Run("with decorator", func(b *testing.B) {
		pf := privacy.NewFactory(func(namespace string) privacy.Protector {
			return privacy.NewProtector(namespace, privacy_memory.NewKeyEngine())
		})

		b.ResetTimer()

		eventtest.BenchmarkEventStore_Append(b, Decorate(memory.NewEventStore(), pf))
		eventtest.BenchmarkEventStore_Load(b, Decorate(memory.NewEventStore(), pf))
	})
}
