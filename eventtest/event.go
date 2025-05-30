package eventtest

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
	rand.New(rand.NewSource(time.Now().UnixNano()))
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

type EventEmbed struct {
	Val string `pii:"data"`
}
type Event2 struct {
	ID       string `pii:"subjectID"`
	Val      string `pii:"data"`
	Embed    EventEmbed
	LongText string `pii:"data"`

	MediumText string `aliases:"Medium"`
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
				ID:         "id: " + strconv.Itoa(i),
				Val:        "val " + strconv.Itoa(i),
				LongText:   generateRandomText(100),
				MediumText: generateRandomText(50),
				Embed:      EventEmbed{Val: "embedded val " + strconv.Itoa(i)},
			}
		} else {
			evt = Event1{Val: "val " + strconv.Itoa(i)}
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
	metaOK := env1.ID() == env2.ID() &&
		env1.StreamID() == env2.StreamID() &&
		env1.GlobalStreamID() == env2.GlobalStreamID() &&
		env1.User() == env2.User() &&
		env1.At().Equal(env2.At()) &&
		env1.Version().Equal(env2.Version())
	if !metaOK {
		return false
	}
	return reflect.DeepEqual(event.ToPtr(env1.Event()).Ptr, event.ToPtr(env2.Event()).Ptr)
}

func RegisterEvent(namespace string) event.Register {
	return event.NewRegister(namespace).
		Set(&Event1{}).
		Set(&Event2{})
}
