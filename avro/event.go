package avro

import (
	"reflect"
	"time"

	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
)

var (
	noEvent = struct{}{}
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
	avroSchemaID string
}

var _ event.Envelope = &avroEvent{}

func (e *avroEvent) Event() any {
	if e.fEvent != nil {
		if e.fEvent == noEvent {
			return nil
		}
		return e.fEvent
	}

	if e.FRawEvent != nil {
		e.fEvent = e.FRawEvent
		return e.fEvent
	}
	log := logger.Default().
		WithName("avro").
		WithValues(
			"stmID", e.StreamID(),
			"type", e.Type(),
			"ver", e.Version(),
		)
	log.V(1).Info("Unexpected empty event data")
	log.V(3).Info("Perhaps event type is no longer part of any registered Avro schemas", "schemaID", e.avroSchemaID)

	e.fEvent = noEvent
	return nil
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

func (e *avroEvent) SetAVROSchemaID(id string) {
	e.avroSchemaID = id
}

func (e *avroEvent) AVROSchemaID() string {
	return e.avroSchemaID
}

// checkType does change event type name in the envelope to match
// the current event data struct type.
func (e *avroEvent) checkType(namespace string) {
	// make sure to require namespace; this logic might change because
	// using global event registry might be error-prone.
	if namespace == "" {
		return
	}

	// event data might be empty in the case of a removed event type
	// that is not indicated as alias for a new.
	//
	// In a such case just do skip type name upgrade.
	data := e.Event()
	if data == nil {
		return
	}

	// In the case fo an empty event registry. Avro serializer use generic type i.e, a Map to represent data.
	// In such as case, do skip event type name upgrade.
	typ := reflect.TypeOf(data)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return
	}

	t := event.TypeOfWithNamespace(namespace, data)
	if t != e.FType {
		logger.Default().WithName("avro").V(1).Info("Event type name has been upgraded",
			"stmID", e.StreamID(),
			"old_type", e.FType,
			"type", t,
			"ver", e.Version())

		e.FType = t
	}
}
