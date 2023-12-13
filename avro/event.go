package avro

import (
	"log"
	"time"

	"github.com/ln80/event-store/event"
)

func convertEvent(evt event.Envelope) (to *avroEvent, err error) {
	to, ok := evt.(*avroEvent)
	if ok {
		return
	}

	to = &avroEvent{
		FStreamID: evt.StreamID(),
		FID:       evt.ID(),
		FType:     evt.Type(),
		FRawEvent: evt.Event(),
		FAt:       evt.At().UnixNano(),
		FUser:     evt.User(),
		FDests:    evt.Dests(),
		FTTL:      evt.TTL(),

		fVersion:       evt.Version(),
		fGlobalVersion: evt.GlobalVersion(),
	}
	if !evt.Version().Equal(event.VersionZero) {
		to.FRawVersion = to.fVersion.String()
	}
	if !evt.GlobalVersion().Equal(event.VersionZero) {
		to.FRawGlobalVersion = to.fGlobalVersion.String()
	}
	return
}

type avroEvent struct {
	FStreamID         string `avro:"StmID"`
	fGlobalStreamID   string
	FRawGlobalVersion string `avro:"GVer"`
	fGlobalVersion    event.Version
	FRawVersion       string `avro:"Ver"`
	fVersion          event.Version
	FID               string `avro:"ID"`
	FType             string `avro:"Type"`
	fEvent            any
	FAt               int64         `avro:"At"`
	FUser             string        `avro:"User"`
	FDests            []string      `avro:"Dests"`
	FTTL              time.Duration `avro:"TTL"`

	// make sure to put data as last field for future partial decoding (using a sub-schema)
	FRawEvent any `avro:"Data" schema:"union"`

	// schemaID used by avro registries to allow re-encoding the event using the same schema.
	_schemaID string
}

var _ event.Envelope = &avroEvent{}

func (e *avroEvent) Event() any {
	if e.fEvent != nil {
		return e.fEvent
	}

	e.fEvent = e.FRawEvent
	if e.fEvent == nil {
		log.Println("[WARNING] Unmarshal event data has failed")
	}

	return e.fEvent
}

func (e *avroEvent) StreamID() string {
	return e.FStreamID
}

func (e *avroEvent) ID() string {
	return e.FID
}

func (e *avroEvent) Type() string {
	return e.FType
}

func (e *avroEvent) At() time.Time {
	return time.Unix(0, e.FAt)
}

func (e *avroEvent) Version() event.Version {
	if !e.fVersion.IsZero() {
		return e.fVersion
	}
	if e.FRawVersion != "" {
		e.fVersion, _ = event.Ver(e.FRawVersion)
	}
	return e.fVersion
}

func (e *avroEvent) User() string {
	return e.FUser
}

func (e *avroEvent) GlobalStreamID() string {
	if e.fGlobalStreamID == "" {
		stmID, _ := event.ParseStreamID(e.FStreamID)
		e.fGlobalStreamID = stmID.GlobalID()
	}
	return e.fGlobalStreamID
}

func (e *avroEvent) GlobalVersion() event.Version {
	if !e.fGlobalVersion.IsZero() {
		return e.fGlobalVersion
	}
	if e.FRawGlobalVersion != "" {
		e.fGlobalVersion, _ = event.Ver(e.FRawGlobalVersion)
	}
	return e.fGlobalVersion
}

func (e *avroEvent) Dests() []string {
	return e.FDests
}

func (e *avroEvent) TTL() time.Duration {
	return e.FTTL
}

func (e *avroEvent) SetGlobalVersion(v event.Version) event.Envelope {
	e.fGlobalVersion = v
	e.FRawGlobalVersion = e.fGlobalVersion.String()
	return e
}

var _ event.Transformer = &avroEvent{}

func (e *avroEvent) Transform(fn func(any) any) {
	if e.Event() != nil {
		e.fEvent = fn(e.fEvent)
	}
}

func (e *avroEvent) SetSchemaID(id string) {
	e._schemaID = id
}

func (e *avroEvent) SchemaID() string {
	return e._schemaID
}

func (e *avroEvent) checkType(namespace string) {
	if data := e.Event(); data != nil {
		t := event.TypeOfWithNamespace(namespace, e.Event())
		if t != e.FType {
			e.FType = t
		}
	}
}
