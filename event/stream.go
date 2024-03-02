package event

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ln80/event-store/event/errors"
)

const (
	StreamIDPartsDelimiter = "#"
)

var (
	ErrInvalidStreamID = errors.New("invalid stream ID")
	ErrInvalidStream   = errors.New("invalid event stream")
	ErrCursorNotFound  = errors.New("cursor not found")
	ErrInvalidCursor   = errors.New("invalid cursor")
)

var streamIDRegex = regexp.MustCompile("^[a-zA-Z0-9-_#+@]+$")

// StreamID presents a composed event stream ID, the global part identifies the global stream ex: TenantID.
// While the other parts can identify the service, bounded context, or the root entity, etc
type StreamID struct {
	global string
	parts  []string
}

// String returns the ID of the stream
func (si StreamID) String() string {
	return strings.Join(append([]string{si.global}, si.parts...), StreamIDPartsDelimiter)
}

// Global returns true if the given stream is a global one
// A global stream ID does not have parts
func (si StreamID) Global() bool {
	return len(si.parts) == 0
}

// GlobalID returns the first part of the streamID which represents the global stream ID, e.g TenantID
func (si StreamID) GlobalID() string {
	return si.global
}

// Parts returns the event stream ID parts others than the global ID
func (si StreamID) Parts() []string {
	return si.parts
}

func validateStreamID(streamID StreamID) error {
	if !streamIDRegex.MatchString(streamID.global) {
		return fmt.Errorf("%w: %s", ErrInvalidStreamID, streamID)
	}
	for _, p := range streamID.parts {
		if !streamIDRegex.MatchString(p) {
			return fmt.Errorf("%w: %s", ErrInvalidStreamID, streamID)
		}
	}
	return nil
}

// NewStreamID returns a composed event stream ID.
// It panics if the global stream ID or parts are not alphanumeric,"-","_","+"
//
// Deprecated: use MustStreamID instead.
func NewStreamID(global string, parts ...string) StreamID {
	streamID, err := newStreamID(global, parts...)
	if err != nil {
		panic(err)
	}

	return streamID
}

// NewStreamID returns a composed event stream ID.
// It panics if the global stream ID or parts are not alphanumeric ex:"-","_","+"
func MustStreamID(global string, parts ...string) StreamID {
	streamID, err := newStreamID(global, parts...)
	if err != nil {
		panic(err)
	}

	return streamID
}

func newStreamID(global string, parts ...string) (StreamID, error) {
	streamID := StreamID{
		global: global,
		parts:  parts,
	}

	if err := validateStreamID(streamID); err != nil {
		return StreamID{}, err
	}

	return streamID, nil
}

func ParseStreamID(streamID string) (StreamID, error) {
	if streamID == "" {
		return StreamID{}, fmt.Errorf("%w: %s", ErrInvalidStreamID, streamID)
	}
	splits := strings.Split(streamID, StreamIDPartsDelimiter)

	return newStreamID(splits[0], splits[1:]...)
}

// Stream presents a collection of consecutive events
type Stream []Envelope

// Cursor is used to validate stream sequence.
type Cursor struct {
	StreamID string
	Ver      Version
	At       time.Time
}

// NewCursor returns a cursor for the given a stream
func NewCursor(streamID string) *Cursor {
	return &Cursor{
		StreamID: streamID,
	}
}

// resolveVer returns either the local or the global version of the given event.
func resolveVer(env Envelope, isGlobalStream bool) Version {
	if isGlobalStream {
		return env.GlobalVersion()
	}
	return env.Version()
}

// resolveStreamID returns either the event's local or global stream ID.
func resolveStreamID(env Envelope, isGlobalStream bool) string {
	if isGlobalStream {
		return env.GlobalStreamID()
	}
	return env.StreamID()
}

// ValidationBoundaries filters extra events contained in the result's chunks.
//
// Note that this case was relevant for an old S3-based implementation of the event store;
// It might be deprecated and removed later.
type ValidationBoundaries struct {
	Since, Until time.Time
	From, To     Version
}

// Build applies ValidationBoundaries's default values if values are zero.
func (vb *ValidationBoundaries) Build() {
	if vb.Since.IsZero() {
		vb.Since = time.Unix(0, 0)
	}
	if vb.Until.IsZero() {
		vb.Until = time.Now()
	}
	if vb.From.IsZero() {
		vb.From = VersionMin
	}
	if vb.To.IsZero() {
		vb.To = VersionMax
	}
}

// Validation presents the stream validation options
type Validation struct {
	GlobalStream  bool
	Boundaries    ValidationBoundaries
	SkipVersion   bool
	SkipTimeStamp bool
}

// ValidateEvent validates the event according to its sequence in the stream.
// It returns an error, or an 'ignored' boolean flag if the event is out of validation boundaries
func ValidateEvent(env Envelope, cur *Cursor, opts ...func(v *Validation)) (ignore bool, err error) {
	if cur == nil {
		return false, ErrCursorNotFound
	}

	// default event validation options
	v := &Validation{
		GlobalStream:  false,
		Boundaries:    ValidationBoundaries{},
		SkipVersion:   false,
		SkipTimeStamp: true,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(v)
	}
	v.Boundaries.Build()

	// Make sure the defined cursor stream ID is the same in every event envelope
	streamID := resolveStreamID(env, v.GlobalStream)
	if cur.StreamID != streamID {
		return false, errors.Err(ErrInvalidStream, cur.StreamID, "event streamID: "+streamID)
	}

	// Check event sequence based on the current cursor state
	if !v.SkipVersion {
		ver := resolveVer(env, v.GlobalStream)

		// stream version can't be zero
		if ver.IsZero() {
			return false, errors.Err(ErrInvalidStream, cur.StreamID, "invalid evt version: "+ver.String())
		}
		// return "to ignore" flag if event if out of range
		if ver.Before(v.Boundaries.From) || ver.After(v.Boundaries.To) {
			return true, nil
		}
		// Initially, the cursor does not necessarily have a specified sequence.
		// If so, set the cursor sequence based on the first event.
		// Otherwise, make sure the event is consecutively next to cursor sequence.
		if cur.Ver.IsZero() && ver.Equal(v.Boundaries.From) {
			cur.Ver = ver
		} else {
			if !ver.Next(cur.Ver) {
				return false, errors.Err(ErrInvalidStream, cur.StreamID, "invalid version sequence: "+cur.Ver.String()+","+ver.String())
			}
			cur.Ver = ver
		}
	}

	// Similarly to sequence based validation we might need to opt for a timestamp based validation.
	if !v.SkipTimeStamp {
		at := env.At()
		if at.IsZero() || at.Equal(time.Unix(0, 0)) {
			return false, errors.Err(ErrInvalidStream, cur.StreamID, "invalid evt timestamp: "+at.String())
		}
		// return "to ignore" flag if event if out of range
		if at.Before(v.Boundaries.Since) || at.After(v.Boundaries.Until) {
			return true, nil
		}
		if cur.At.IsZero() || cur.At.Equal(time.Unix(0, 0)) && at.Equal(v.Boundaries.Since) {
			cur.At = at
		} else {
			if at.Before(cur.At) {
				return false, errors.Err(ErrInvalidStream, cur.StreamID, "invalid timestamp sequence: "+cur.At.String()+","+at.String())
			}
			cur.At = at
		}
	}

	return false, nil
}

// Validate a chunk of events.
// Validate will define the validation boundaries and cursor based on the current,
// any validation boundaries set at the option-level will ignored.
func (stm Stream) Validate(opts ...func(v *Validation)) error {
	if len(stm) == 0 {
		return nil
	}

	// reduce  validation options to a single option func
	rv := &Validation{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(rv)
	}
	rv.Boundaries = ValidationBoundaries{
		From: resolveVer(stm[0], rv.GlobalStream),
		To:   resolveVer(stm[len(stm)-1], rv.GlobalStream),
	}
	rv.Boundaries.Build()

	rvOpt := func(v *Validation) {
		v.GlobalStream = rv.GlobalStream
		v.Boundaries = rv.Boundaries
		v.SkipVersion = rv.SkipVersion
		v.SkipTimeStamp = rv.SkipTimeStamp
	}

	cur := NewCursor(resolveStreamID(stm[0], rv.GlobalStream))

	for _, ev := range stm {
		if _, err := ValidateEvent(ev, cur, rvOpt); err != nil {
			return err
		}
	}
	return nil
}

func (stm Stream) Empty() bool {
	return len(stm) == 0
}

func (stm Stream) EventIDs() []string {
	ids := make([]string, len(stm))
	for i, ev := range stm {
		ids[i] = ev.ID()
	}
	return ids
}

// Events unwraps the event envelops and returns the domain events of the stream chunk
func (s Stream) Events() []any {
	events := make([]any, len(s))
	for i, e := range s {
		events[i] = e.Event()
	}
	return events
}
