package dynamodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ln80/event-store/event"
)

const (
	VerAttribute  string = "ver"
	GVerAttribute string = "gver"
	GIDAttribute  string = "gid"
)

type Record struct {
	Item
	Events []byte `dynamodbav:"evts"`

	Since    int64  `dynamodbav:"since"`
	Until    int64  `dynamodbav:"until"`
	Version  string `dynamodbav:"ver,omitempty"`
	GID      string `dynamodbav:"gid"`
	GVersion string `dynamodbav:"gver,omitempty"`
}

func (rec Record) Keys() map[string]string {
	return map[string]string{
		HashKey:  rec.HashKey,
		RangeKey: rec.RangeKey,
	}
}

func recordHashKey(stmID event.StreamID, ps ...int) string {
	page := 1
	if len(ps) > 1 {
		page = ps[0]
	}
	return fmt.Sprintf("%s#%d", stmID.GlobalID(), page)
}

func recordRangeKeyWithTimestamp(stmID event.StreamID, t time.Time) string {
	return fmt.Sprintf("%s@t_%020d", strings.Join(stmID.Parts(), event.StreamIDPartsDelimiter), t.UnixNano())
}

func recordRangeKeyWithVersion(stmID event.StreamID, ver event.Version) string {
	return fmt.Sprintf("%s@v_%s", strings.Join(stmID.Parts(), event.StreamIDPartsDelimiter), ver.Trunc().String())
}

// UnpackRecord does unmarshal events contained in the record and set their respective global stream sequence.
// It fails if the record is not indexed, and it panics if an event envelope does not implement event.GlobalVersionSetter interface.
func UnpackRecord(ctx context.Context, r Record, serializer event.Serializer) ([]event.Envelope, error) {
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
		rev := event.MustGlobalVersionSetter(evt)
		rev.SetGlobalVersion(gVer.Add(0, uint8(idx)))
	}

	return events, nil
}
