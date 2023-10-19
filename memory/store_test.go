package memory

import (
	"context"
	"testing"

	"github.com/ln80/event-store/testutil"
	test_suite "github.com/ln80/event-store/testutil/suite"
)

func TestEventStore(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	test_suite.EventStoreTest(t, ctx, NewEventStore())
	test_suite.EventSourcingStoreTest(t, ctx, NewEventStore())
	test_suite.EventStreamerSuite(t, ctx, NewEventStore(), func(opt *test_suite.EventStreamerSuiteOptions) {

		opt.SupportOrderDESC = true
	})
}
