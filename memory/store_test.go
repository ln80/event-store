package memory

import (
	"context"
	"testing"

	"github.com/ln80/event-store/internal/testutil"
)

func TestEventStore(t *testing.T) {
	testutil.RegisterEvent("")

	ctx := context.Background()

	testutil.TestEventLoggingStore(t, ctx, NewEventStore())
	testutil.TestEventSourcingStore(t, ctx, NewEventStore())
	testutil.TestEventStreamer(t, ctx, NewEventStore(), func(opt *testutil.TestEventStreamerOptions) {

		opt.SupportOrderDESC = true
	})
}
