package event

import (
	"context"
	"math"
)

type StreamDataType string

const (
	StreamDataTypeRecord   StreamDataType = "record"
	StreamDataTypeEnd      StreamDataType = "end"
	StreamDataTypeContinue StreamDataType = "continue"
)

// StreamData mainly contains an event record.
// It might also contain some infra-related signals such as stream termination or lagging.
// The consumer must check StreamData type and behave accordingly.
// Note that StreamData was initially introduced to deal with an old S3-based event store implementation.
type StreamData struct {
	Type  StreamDataType
	Value any
	Stm   string
}

type StreamerReplayOrder string

const (
	StreamerReplayOrderASC  StreamerReplayOrder = "ASC"
	StreamerReplayOrderDESC StreamerReplayOrder = "DESC"
)

const (
	StreamerReplayQueryDefaultLimit uint = 500
)

// StreamerHandler process the given event in the replay stream process
type StreamerHandler func(ctx context.Context, data StreamData) error

// Streamer mainly used to query global streams for event replay and projections.
type Streamer interface {
	// Replay a stream based on the given query params.
	// Replay capabilities and behavior are implementation-specific
	Replay(ctx context.Context, streamID StreamID, q StreamerQuery, h StreamerHandler) error
}

// StreamerQuery allows to filter stream based on a pre-defined range, limit, and order
type StreamerQuery struct {
	From, To    Version
	RecordLimit uint
	Order       StreamerReplayOrder
}

// Build applies filter default values if they are missing.
// In case of "To" is defined, it has to be within the range defined by "From" + "Limit"
func (q *StreamerQuery) Build() {
	if q.RecordLimit == 0 {
		q.RecordLimit = StreamerReplayQueryDefaultLimit
	}
	if q.Order == "" {
		q.Order = StreamerReplayOrderASC
	}
	if q.From.IsZero() {
		q.From = VersionMin
	}
	if q.To.IsZero() {
		q.To = VersionMax
	} else {
		if v := q.From.Trunc().Add(uint64(q.RecordLimit), math.MaxUint8); q.To.After(v) {
			q.To = v
		}
	}
}
