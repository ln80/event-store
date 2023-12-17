package testutil

import (
	"context"
	"testing"

	"github.com/ln80/event-store/event"
)

func BenchmarkSerializer(b *testing.B, ser event.Serializer) {
	ctx := context.Background()
	stmID := event.NewStreamID("tenantID")

	b.ResetTimer()

	b.Run("marshal", func(b *testing.B) {
		dataSize := 0
		for n := 0; n < b.N; n++ {
			bb, _, err := ser.MarshalEvent(ctx, event.Wrap(ctx, stmID, GenEvents(1))[0])
			if err != nil {
				b.Fatalf("Error: %v", err)
			}

			dataSize += len(bb)

			_, _, err = ser.MarshalEventBatch(ctx, event.Wrap(ctx, stmID, GenEvents(100)))
			if err != nil {
				b.Fatalf("Error: %v", err)
			}
		}
		// Report the dataSize as a custom metric
		b.ReportMetric(float64(dataSize/b.N), "size/op")
	})
	b.Run("unmarshal", func(b *testing.B) {
		bb, _, err := ser.MarshalEvent(ctx, event.Wrap(ctx, stmID, GenEvents(1))[0])
		if err != nil {
			b.Fatalf("Error: %v", err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			e, err := ser.UnmarshalEvent(ctx, bb)
			if err != nil {
				b.Fatalf("Error: %v", err)
			}

			_ = e.Event()
		}
	})

}
