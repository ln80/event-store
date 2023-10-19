package event

import (
	"context"
	"errors"
	"time"

	intevent "github.com/ln80/event-store/internal/event"
)

// Envelope wraps and adds meta-data to events such us timestamp, stream ID, version
type Envelope interface {
	ID() string
	Type() string
	Event() any
	At() time.Time
	StreamID() string
	Version() Version
	GlobalStreamID() string
	GlobalVersion() Version
	User() string
	Dests() []string
	TTL() time.Duration
}

// type ExpiredEnvelope interface {
// 	TTL() time.Duration
// }

type RWEnvelope interface {
	Envelope

	SetAt(t time.Time) Envelope
	SetUser(userID string) Envelope
	SetVersion(v Version) Envelope
	SetGlobalVersion(v Version) Envelope
	SetDests(dests []string) Envelope
	SetTTL(ttl time.Duration) Envelope
}

type EnvelopeOption func(env RWEnvelope)

func WithVersionIncr(startingVer Version, limit int, diff VersionSequenceDiff) EnvelopeOption {
	ver := startingVer
	count := 0
	return func(env RWEnvelope) {
		// If current event is the last one in the given record
		// then mark the fractional part of its version as EOF.
		// Otherwise, accordingly increment the version for the next event
		if count == limit-1 {
			ver = ver.EOF()
			env.SetVersion(ver)
			return
		}
		env.SetVersion(ver)

		count++
		switch diff {
		case VersionSeqDiffFracPart:
			// Note that doIncr panics if trying to increment an EOF version
			ver = ver.doIncr(VersionSeqDiffFracPart)
		case VersionSeqDiffPart:
			ver = ver.doIncr(VersionSeqDiffPart)
		default:
			panic(errors.New("invalid version sequence diff"))
		}
	}
}

func WithGlobalVersionIncr(startingVer Version, limit int, diff VersionSequenceDiff) EnvelopeOption {
	ver := startingVer
	count := 0
	return func(env RWEnvelope) {
		// If current event is the last one in the given record
		// then mark the fractional part of its version as EOF.
		// Otherwise, accordingly increment the version for the next event
		if count == limit-1 {
			ver = ver.EOF()
			env.SetGlobalVersion(ver)
			return
		}
		env.SetGlobalVersion(ver)

		count++
		switch diff {
		case VersionSeqDiffFracPart:
			// Note that doIncr panics if trying to increment an EOF version
			ver = ver.doIncr(VersionSeqDiffFracPart)
		case VersionSeqDiffPart:
			ver = ver.doIncr(VersionSeqDiffPart)
		default:
			panic(errors.New("invalid version sequence diff"))
		}
	}
}

// Envelop wraps (with options) the given events.
// By default it creates a valid timestamp-based stream chunk.
// Not that it does not set event version or global version.
func Wrap(ctx context.Context, stmID StreamID, events []any, opts ...EnvelopeOption) []Envelope {
	envs := make([]Envelope, 0)
	for _, evt := range events {
		if evt == nil {
			continue
		}
		env := &envelope{
			globalStreamID: stmID.GlobalID(),
			streamID:       stmID.String(),
			event:          evt,
			eType:          TypeOfWithContext(ctx, evt),
			eID:            UID().String(),
			at:             time.Now().UTC(),
			dests:          eventDests(ctx, evt),
		}
		if ctx.Value(ContextUserKey) != nil {
			user := ctx.Value(ContextUserKey).(string)
			env.SetUser(user)
		}
		for _, opt := range opts {
			if opt == nil {
				continue
			}
			opt(env)
		}
		envs = append(envs, env)
	}
	return envs
}

type envelope struct {
	streamID       string
	eID            string
	eType          string
	event          any
	at             time.Time
	version        Version
	user           string
	globalStreamID string
	globalVersion  Version
	dests          []string
	ttl            time.Duration
}

var _ Envelope = &envelope{}
var _ RWEnvelope = &envelope{}

// ID implements the EventID method of the Envelope interface
func (e *envelope) ID() string {
	return e.eID
}

// Type implements the EventType method of the Envelope interface.
func (e *envelope) Type() string {
	return e.eType
}

// Event implements the Event method of the envelope interface.
func (e *envelope) Event() any {
	return e.event
}

// At implements the Timestamp method of the Envelope interface.
func (e *envelope) At() time.Time {
	return e.at
}

// Version implements the Version method of the Envelope interface.
func (e *envelope) Version() Version {
	return e.version
}

// User implements the User method of the Envelope interface.
func (e *envelope) User() string {
	return e.user
}

// StreamID implements the StreamID method of the Envelope interface.
func (e *envelope) StreamID() string {
	return e.streamID
}

// GlobalStreamID implements the GlobalStreamID method of the Envelope interface.
func (e *envelope) GlobalStreamID() string {
	return e.globalStreamID
}

// GlobalVersion implements the GlobalVersion method of the Envelop interface
func (e *envelope) GlobalVersion() Version {
	return e.globalVersion
}

// Dests implements the Dests method of the Envelop interface
func (e *envelope) Dests() []string {
	return e.dests
}

// Dests implements the TTL method of the ExpiredEnvelope interface
func (e *envelope) TTL() time.Duration {
	return e.ttl
}

// SetAt implements the SetAt method of the RWEnvelope interface.
func (e *envelope) SetAt(t time.Time) Envelope {
	e.at = t
	return e
}

// SetUser implements the SetUser method of the RWEnvelope interface.
func (e *envelope) SetUser(userID string) Envelope {
	e.user = userID
	return e
}

// SetVersion implements the SetVersion method of the RWEnvelope interface.
func (e *envelope) SetVersion(v Version) Envelope {
	e.version = v
	return e
}

// SetGlobalVersion implements the SetGlobalVersion method of the RWEnvelope interface.
func (e *envelope) SetGlobalVersion(v Version) Envelope {
	e.globalVersion = v
	return e
}

// SetDests implements the SetDests method of the RWEnvelop interface
func (e *envelope) SetDests(dests []string) Envelope {
	e.dests = dests
	return e
}

// SetTTL implements the SetTTL method of the RWEnvelop interface
func (e *envelope) SetTTL(ttl time.Duration) Envelope {
	e.ttl = ttl
	return e
}

var _ intevent.Transformer = &envelope{}

func (e *envelope) Transform(fn func(any) any) {
	e.event = fn(e.event)
}
