package json

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ln80/event-store/event"
)

// convertEvent takes an event.Envelope an convert it to a jsonEvent type.
// It skips Versions zero values and replaces them with empty strings,
// this make the json more compact and small.
func convertEvent(evt event.Envelope) (to *jsonEvent, err error) {
	to, ok := evt.(*jsonEvent)
	if ok {
		return
	}

	var data []byte
	data, err = json.Marshal(evt.Event())
	if err != nil {
		return
	}
	to = &jsonEvent{
		FStreamID: evt.StreamID(),
		FID:       evt.ID(),
		FType:     evt.Type(),
		FRawEvent: json.RawMessage(data),
		FAt:       evt.At().UnixNano(),
		FUser:     evt.User(),
		FDests:    evt.Dests(),
	}
	if !evt.Version().Equal(event.VersionZero) {
		to.fVersion = evt.Version()
		to.FRawVersion = to.fVersion.String()
	}
	if !evt.GlobalVersion().Equal(event.VersionZero) {
		to.fGlobalVersion = evt.GlobalVersion()
		to.FRawGlobalVersion = to.fGlobalVersion.String()
	}
	return
}

type jsonEvent struct {
	FStreamID         string          `json:"StmID"`
	fGlobalStreamID   string          `json:"-"`
	FRawGlobalVersion string          `json:"GVer,omitempty"`
	fGlobalVersion    event.Version   `json:"-"`
	FRawVersion       string          `json:"Ver,omitempty"`
	fVersion          event.Version   `json:"-"`
	FID               string          `json:"ID"`
	FType             string          `json:"Type"`
	FRawEvent         json.RawMessage `json:"Data"`
	fEvent            any             `json:"-"`
	FAt               int64           `json:"At"`
	FUser             string          `json:"User,omitempty"`
	FDests            []string        `json:"Dests,omitempty"`
	FTTL              time.Duration   `json:"TTL,omitempty"`

	reg event.Register `json:"-"`
}

var _ event.Envelope = &jsonEvent{}

func (e *jsonEvent) StreamID() string {
	return e.FStreamID
}

func (e *jsonEvent) ID() string {
	return e.FID
}

func (e *jsonEvent) Type() string {
	return e.FType
}

// Event returns the original (aka domaine event) event wrapped in json event envelope.
// It may returns nil if the event type is not found in the event registry,
// or the later is not configured in the event.
// It's up to the client/caller code to tolerate (or not) the empty value.
func (e *jsonEvent) Event() any {
	if e.fEvent != nil {
		return e.fEvent
	}
	if e.reg == nil {
		return nil
	}
	evt, err := e.reg.Get(e.Type())

	if err != nil {
		log.Println("[WARNING] Unmarshal event data failed", event.Err(err, e.StreamID()))
		return nil
	}
	if err := json.Unmarshal(e.FRawEvent, evt); err != nil {
		log.Println(event.Err(fmt.Errorf("unmarshal event data failed"), e.StreamID(), err))
		return nil
	}
	e.fEvent = evt

	return e.fEvent
}

func (e *jsonEvent) At() time.Time {
	return time.Unix(0, e.FAt)
}

func (e *jsonEvent) Version() event.Version {
	// return event version if already set in the event
	if !e.fVersion.IsZero() {
		return e.fVersion
	}
	// if not then resolve the version value from the raw value
	// in case the later is not empty
	if e.FRawVersion != "" {
		e.fVersion, _ = event.Ver(e.FRawVersion)
	}
	// return zero value as a fallback
	return e.fVersion
}

func (e *jsonEvent) User() string {
	return e.FUser
}

func (e *jsonEvent) GlobalStreamID() string {
	// the global stream ID not yet set in the event then
	// parse the stream value and get global ID part.
	if e.fGlobalStreamID == "" {
		stmID, _ := event.ParseStreamID(e.FStreamID)
		e.fGlobalStreamID = stmID.GlobalID()
	}
	return e.fGlobalStreamID
}

func (e *jsonEvent) GlobalVersion() event.Version {
	// return the global version if already set in the event
	if !e.fGlobalVersion.IsZero() {
		return e.fGlobalVersion
	}
	// resolve the global stream version from the raw value if the later is not empty
	if e.FRawGlobalVersion != "" {
		e.fGlobalVersion, _ = event.Ver(e.FRawGlobalVersion)
	}
	// otherwise, return zero value
	return e.fGlobalVersion
}

func (e *jsonEvent) Dests() []string {
	return e.FDests
}

func (e *jsonEvent) TTL() time.Duration {
	return e.FTTL
}

func (e *jsonEvent) SetGlobalVersion(v event.Version) event.Envelope {
	e.fGlobalVersion = v
	e.FRawGlobalVersion = e.fGlobalVersion.String()
	return e
}

var _ event.Transformer = &jsonEvent{}

func (e *jsonEvent) Transform(fn func(any) any) {
	if e.Event() != nil {
		e.fEvent = fn(e.fEvent)
	}
}
