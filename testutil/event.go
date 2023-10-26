package testutil

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"time"

	"github.com/ln80/event-store/event"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func generateRandomText(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

const (
	Dest2 = "dest2"
)

type Event1 struct {
	Val string
}
type Event2 struct {
	ID         string `pii:"subjectID"`
	Val        string `pii:"data"`
	LongText   string
	MediumText string

	ExtraField1 string
	ExtraField2 string
	ExtraField3 string
}

func (e *Event2) EvDests() []string {
	return []string{Dest2}
}

var _ event.Publishable = &Event2{}

func GenEvents(count int) []any {
	events := make([]any, count)
	for i := 0; i < count; i++ {
		var evt any
		if i%2 == 0 {
			evt = &Event2{
				ID:  strconv.Itoa(i),
				Val: "val " + strconv.Itoa(i),

				LongText: generateRandomText(100),

				MediumText: generateRandomText(50),

				ExtraField1: generateRandomText(10),
				ExtraField2: generateRandomText(10),
				ExtraField3: generateRandomText(10),
			}
		} else {
			evt = &Event1{"val " + strconv.Itoa(i)}
		}

		events[i] = evt
	}
	return events
}

func FormatEnv(env event.Envelope) string {
	return fmt.Sprintf(`
		stmID: %s
		type: %s
		evtID: %s
		at: %v
		ver: %v
		gVer: %v
		user: %s
		data: %v
	`, env.StreamID(), env.Type(), env.ID(), env.At().UnixNano(), env.Version(), env.GlobalVersion(), env.User(), env.Event())
}

func CmpEnv(env1, env2 event.Envelope) bool {
	return env1.ID() == env2.ID() &&
		env1.StreamID() == env2.StreamID() &&
		env1.GlobalStreamID() == env2.GlobalStreamID() &&
		env1.User() == env2.User() &&
		env1.At().Equal(env2.At()) &&
		env1.Version().Equal(env2.Version()) &&
		reflect.DeepEqual(env1.Event(), env2.Event())
}

func RegisterEvent(namespace string) event.Register {
	return event.NewRegister(namespace).
		Set(&Event1{}).
		Set(&Event2{})
}
