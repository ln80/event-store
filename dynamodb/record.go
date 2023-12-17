package dynamodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ln80/event-store/event"
)

type Record struct {
	Item
	Events []byte `dynamodbav:"_evts"`

	Since    int64  `dynamodbav:"_since"`
	Until    int64  `dynamodbav:"_until"`
	Version  string `dynamodbav:"_ver,omitempty"`
	GID      string `dynamodbav:"_gid"`
	GVersion string `dynamodbav:"_gver,omitempty"`
}

func recordHashKey(stmID event.StreamID, ps ...int) string {
	page := 1
	if len(ps) > 0 {
		page = ps[0]
	}
	return fmt.Sprintf("%s#%d", stmID.GlobalID(), page)
}

func recordRangeKeyWithTimestamp(stmID event.StreamID, t time.Time) string {
	return fmt.Sprintf("%st_%020d", strings.Join(stmID.Parts(), event.StreamIDPartsDelimiter), t.UnixNano())
}

func recordRangeKeyWithVersion(stmID event.StreamID, ver event.Version) string {
	return fmt.Sprintf("%s@v_%s", strings.Join(stmID.Parts(), event.StreamIDPartsDelimiter), ver.Trunc().String())
}

func UnmarshalRecord(ctx context.Context, r Record, serializer event.Serializer) ([]event.Envelope, error) {
	if len(r.Events) == 0 {
		return nil, nil
	}

	events, err := serializer.UnmarshalEventBatch(ctx, r.Events)
	if err != nil {
		return nil, err
	}

	gVer, err := event.ParseVersion(r.Item.LSIRangeKey)
	if err != nil {
		return nil, err
	}

	for idx, evt := range events {
		rev, ok := evt.(interface {
			event.Envelope
			SetGlobalVersion(v event.Version) event.Envelope
		})
		if !ok {
			panic("event envelope does not support SetGlobalVersion")
		}
		rev.SetGlobalVersion(gVer.Add(0, uint8(idx)))
	}

	return events, nil
}
