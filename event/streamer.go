package event

import (
	"context"
	"math"
	"time"
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

type StreamOrder string

const (
	StreamOrderASC  StreamOrder = "ASC"
	StreamOrderDESC StreamOrder = "DESC"
)

const (
	StreamerReplayQueryDefaultLimit uint = 500
)

// StreamReplayHandler process the given event in the replay stream process
type StreamReplayHandler func(ctx context.Context, data StreamData) error

// Streamer mainly used to query global streams for event replay and projections.
type StreamReplayer interface {
	// Replay a stream based on the given query params.
	// Replay capabilities and behavior are implementation-specific
	Replay(ctx context.Context, streamID StreamID, q StreamReplayQuery, h StreamReplayHandler) error
}

// StreamerQuery allows to filter stream based on a pre-defined range, limit, and order
type StreamReplayQuery struct {
	From, To    Version
	RecordLimit uint
	Order       StreamOrder
}

// Build applies filter default values if they are missing.
// In case of "To" is defined, it has to be within the range defined by "From" + "Limit"
func (q *StreamReplayQuery) Build() {
	if q.RecordLimit == 0 {
		q.RecordLimit = StreamerReplayQueryDefaultLimit
	}
	if q.Order == "" {
		q.Order = StreamOrderASC
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

// Streamer mainly used to query global streams for event replay and projections.
type StreamQuerier interface {
	// Travel a stream based on the given query params.
	// Travel capabilities and behavior are implementation-specific
	Query(ctx context.Context, id StreamID, q StreamQuery) (*StreamQueryResult, error)
}

// StreamQuery contains the parameters for a time-based travel query
type StreamQuery struct {
	From        time.Time
	To          time.Time
	RecordLimit int
	Cursor      *string

	Users   []string
	Types   []string
	IPAddrs []string

	Order StreamOrder
}

// StreamQueryResult contains the result of a time-based travel query
type StreamQueryResult struct {
	Events []Envelope
	Cursor *string
}
