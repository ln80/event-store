package memory

import (
	"context"
	"testing"

	"github.com/ln80/event-store/eventtest"
)

func TestEventStore(t *testing.T) {
	eventtest.RegisterEvent("")

	ctx := context.Background()

	eventtest.TestEventLoggingStore(t, ctx, NewEventStore())
	eventtest.TestEventSourcingStore(t, ctx, NewEventStore())
	eventtest.TestEventStreamReplayer(t, ctx, NewEventStore(), func(opt *eventtest.TestEventStreamReplayerOptions) {

		opt.SupportOrderDESC = true
	})
}
