package pii

import (
	"context"
	"testing"

	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/memory"
	"github.com/ln80/event-store/testutil"

	test_suite "github.com/ln80/event-store/testutil/suite"
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

	test_suite.EventStoreTest(t, ctx, store)
	test_suite.EventSourcingStoreTest(t, ctx, store)
	test_suite.EventStreamerSuite(t, ctx, store)
}
