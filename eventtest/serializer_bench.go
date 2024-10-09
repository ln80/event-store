package eventtest

import (
	"context"
	"testing"

	"github.com/ln80/event-store/event"
)

func BenchmarkSerializer(b *testing.B, ser event.Serializer) {
	b.Helper()
	ctx := context.Background()
	stmID := event.NewStreamID("tenantID")

	b.ResetTimer()

	b.Run("marshal", func(b *testing.B) {
		b.Helper()
		dataSize := 0
		for n := 0; n < b.N; n++ {
			bb, err := ser.MarshalEvent(ctx, event.Wrap(ctx, stmID, GenEvents(1))[0])
			if err != nil {
				b.Fatalf("Error: %v", err)
			}

			dataSize += len(bb)

			_, err = ser.MarshalEventBatch(ctx, event.Wrap(ctx, stmID, GenEvents(100)))
			if err != nil {
				b.Fatalf("Error: %v", err)
			}
		}
		// Report the dataSize as a custom metric
		b.ReportMetric(float64(dataSize/b.N), "size/op")
	})

	b.Run("unmarshal", func(b *testing.B) {
		b.Helper()
		bb, err := ser.MarshalEvent(ctx, event.Wrap(ctx, stmID, GenEvents(1))[0])
		if err != nil {
			b.Fatalf("Error: %v", err)
		}

		b.ResetTimer()

		e, err := ser.UnmarshalEvent(ctx, bb)
		if err != nil {
			b.Fatalf("Error: %v", err)
		}

		_ = e.Event()
	})

	b.Run("marshal batch", func(b *testing.B) {
		b.Helper()
		for n := 0; n < b.N; n++ {
			_, err := ser.MarshalEventBatch(ctx, event.Wrap(ctx, stmID, GenEvents(100)))
			if err != nil {
				b.Fatalf("Error: %v", err)
			}
		}
	})

	b.Run("unmarshal batch", func(b *testing.B) {
		b.Helper()
		bb, err := ser.MarshalEventBatch(ctx, event.Wrap(ctx, stmID, GenEvents(100)))
		if err != nil {
			b.Fatalf("Error: %v", err)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			evts, err := ser.UnmarshalEventBatch(ctx, bb)
			if err != nil {
				b.Fatalf("Error: %v", err)
			}
			for _, evt := range evts {
				_ = evt.Event()
			}
		}
	})
}
