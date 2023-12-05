package pii

import (
	"context"
	"testing"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/memory"
	"github.com/ln80/event-store/testutil"

	"github.com/ln80/pii"
	pii_memory "github.com/ln80/pii/memory"
)

func TestPIIProtectWrapper(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	var store es.EventStore = memory.NewEventStore()

	// check: https://github.com/ln80/pii for more details
	pf := pii.NewFactory(func(namespace string) pii.Protector {
		return pii.NewProtector(namespace, pii_memory.NewKeyEngine())
	})
	defer pf.Monitor(ctx)

	store = ProtectPII(store, pf)

	testutil.TestEventLoggingStore(t, ctx, store)
	testutil.TestEventSourcingStore(t, ctx, store)
	testutil.TestEventStreamer(t, ctx, store)
}
