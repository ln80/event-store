package sourcing

import (
	"context"

	"github.com/ln80/event-store/event"
)

type Store interface {
	AppendToStream(ctx context.Context, chunk Stream, optFns ...func(opt *event.AppendOptions)) error
	LoadStream(ctx context.Context, id event.StreamID, vrange ...event.Version) (*Stream, error)
}

func Wrap(ctx context.Context, stmID event.StreamID, curVer event.Version, events []any) Stream {
	stm := NewStream(stmID, event.Stream(
		event.Wrap(ctx, stmID, events, event.WithVersionIncr(curVer.Incr(), len(events), event.VersionSeqDiffFracPart)),
	))
	if err := stm.Validate(); err != nil {
		panic(err)
	}
	return *stm
}

type Stream struct {
	events  event.Stream
	iD      event.StreamID
	version event.Version
}

func NewStream(id event.StreamID, events event.Stream) *Stream {
	stm := &Stream{
		iD:      id,
		version: event.VersionZero,
		events:  events,
	}
	if l := len(events); l > 0 {
		stm.version = events[l-1].Version()
	}
	return stm
}

func (s *Stream) Version() event.Version {
	return s.version
}

func (s *Stream) ID() event.StreamID {
	return s.iD
}

func (s *Stream) Empty() bool {
	return s.events.Empty()
}

func (s *Stream) Validate() error {
	return s.events.Validate(func(v *event.Validation) {
		v.GlobalStream = false
		v.SkipVersion = false
		v.SkipTimeStamp = false
	})
}

func (s *Stream) Unwrap() event.Stream {
	return s.events
}
