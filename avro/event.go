package avro

import (
	"errors"
	"log"
	"reflect"
	"time"

	"github.com/ln80/event-store/event"
)

func convertEvent[T any](evt event.Envelope) (to *avroEvent[T], err error) {
	to, ok := evt.(*avroEvent[T])
	if ok {
		return
	}

	val := evt.Event()
	var C T
	cv := reflect.ValueOf(&C).Elem()
	if cv.Kind() != reflect.Struct {
		return nil, errors.New("invalid event container type, it must be a struct")
	}
	isSet := false
	for i := 0; i < cv.NumField(); i++ {
		field := cv.Field(i)
		if field.CanSet() {
			vv := reflect.ValueOf(val)
			if field.Type().AssignableTo(vv.Type()) {
				field.Set(vv)
				isSet = true
				break
			}
		}
	}
	if !isSet {
		err = errors.New("failed to set event in a container's field")
		return
	}

	to = &avroEvent[T]{
		FStreamID: evt.StreamID(),
		FID:       evt.ID(),
		FType:     evt.Type(),
		FRawEvent: C,
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

type avroEvent[T any] struct {
	FStreamID         string `avro:"StmID"`
	fGlobalStreamID   string `avro:"-"`
	FRawGlobalVersion string `avro:"GVer"`
	fGlobalVersion    event.Version
	FRawVersion       string `avro:"Ver"`
	fVersion          event.Version
	FID               string        `avro:"ID"`
	FType             string        `avro:"Type"`
	FRawEvent         T             `avro:"Data"`
	fEvent            any           `avro:"-"`
	FAt               int64         `avro:"At"`
	FUser             string        `avro:"User"`
	FDests            []string      `avro:"Dests"`
	FTTL              time.Duration `avro:"TTL"`

	namespace string `avro:"-"`
}

var _ event.Envelope = &avroEvent[any]{}

func (e *avroEvent[T]) Event() any {
	if e.fEvent != nil {
		return e.fEvent
	}

	// TODO add cache to avoid the recurrent reflection logic

	cv := reflect.ValueOf(e.FRawEvent)

	if cv.Kind() == reflect.Struct {
		for i := 0; i < cv.NumField(); i++ {
			field := cv.Field(i)
			if !field.CanInterface() {
				continue
			}

			fieldType := event.NormalizeTypeWithNamespace(e.namespace, field.Type())

			if fieldType == e.Type() {
				e.fEvent = field.Interface()
				break
			}
		}
	}

	if e.fEvent == nil {
		log.Println("[WARNING] Unmarshal event data has failed")
	}

	return e.fEvent
}

func (e *avroEvent[T]) StreamID() string {
	return e.FStreamID
}

func (e *avroEvent[T]) ID() string {
	return e.FID
}

func (e *avroEvent[T]) Type() string {
	return e.FType
}

func (e *avroEvent[T]) At() time.Time {
	return time.Unix(0, e.FAt)
}

func (e *avroEvent[T]) Version() event.Version {
	if !e.fVersion.IsZero() {
		return e.fVersion
	}
	if e.FRawVersion != "" {
		e.fVersion, _ = event.Ver(e.FRawVersion)
	}
	return e.fVersion
}

func (e *avroEvent[T]) User() string {
	return e.FUser
}

func (e *avroEvent[T]) GlobalStreamID() string {
	if e.fGlobalStreamID == "" {
		stmID, _ := event.ParseStreamID(e.FStreamID)
		e.fGlobalStreamID = stmID.GlobalID()
	}
	return e.fGlobalStreamID
}

func (e *avroEvent[T]) GlobalVersion() event.Version {
	if !e.fGlobalVersion.IsZero() {
		return e.fGlobalVersion
	}
	if e.FRawGlobalVersion != "" {
		e.fGlobalVersion, _ = event.Ver(e.FRawGlobalVersion)
	}
	return e.fGlobalVersion
}

func (e *avroEvent[T]) Dests() []string {
	return e.FDests
}

func (e *avroEvent[T]) TTL() time.Duration {
	return e.FTTL
}

func (e *avroEvent[T]) SetGlobalVersion(v event.Version) event.Envelope {
	e.fGlobalVersion = v
	e.FRawGlobalVersion = e.fGlobalVersion.String()
	return e
}

var _ event.Transformer = &avroEvent[any]{}

func (e *avroEvent[T]) Transform(fn func(any) any) {
	if e.Event() != nil {
		e.fEvent = fn(e.fEvent)
	}
}
