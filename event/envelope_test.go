package event

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ln80/event-store/event/testutil"
)

func TestEnvelope(t *testing.T) {
	ctx := context.Background()

	globalID := "tenantID"
	stmID := NewStreamID(globalID, "service", "rootEntityID")

	t.Run("basic", func(t *testing.T) {
		events := []any{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}
		evts := Wrap(ctx, stmID, events)
		for i, evt := range evts {
			if want, val := globalID, evt.GlobalStreamID(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := stmID.String(), evt.StreamID(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := VersionZero, evt.Version(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := VersionZero, evt.GlobalVersion(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := events[i], evt.Event(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := "", evt.User(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if i%2 == 0 {
				if want, val := TypeOf(testutil.Event{}), evt.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			} else {
				if want, val := TypeOf(testutil.Event2{}), evt.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			}
			if ok := evt.At().After(time.Now().Add(-1 * time.Second)); !ok {
				t.Fatalf("expect %v be less than few second ago", evt.At())
			}
			if nowant, val := "", evt.ID(); nowant == val {
				t.Fatalf("expect %v, %v be not equals", nowant, val)
			}
		}
	})

	t.Run("with context values", func(t *testing.T) {
		user := "Joyce Pfeffer IV"
		ctx := context.WithValue(
			context.WithValue(ctx, ContextUserKey, user),
			ContextNamespaceKey, "foo")

		events := []any{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}
		evts := Wrap(ctx, stmID, events)
		for i, evt := range evts {
			if want, val := user, evt.User(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if i%2 == 0 {
				if want, val := TypeOfWithContext(ctx, testutil.Event{}), evt.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			} else {
				if want, val := TypeOfWithContext(ctx, testutil.Event2{}), evt.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			}
		}
	})

	t.Run("with options", func(t *testing.T) {
		events := []any{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}
		user := "Joyce Pfeffer IV"
		tm := time.Now().AddDate(0, 0, -1)
		tm0 := tm
		ver := NewVersion()
		ver0 := ver

		evts := Wrap(ctx, stmID, events,
			func(evt RWEnvelope) {
				evt.SetAt(tm)
				tm = tm.Add(-5 * time.Second)
			},
			func(evt RWEnvelope) {
				evt.SetUser(user)
			},
			func(evt RWEnvelope) {
				evt.SetVersion(ver)
				MustGlobalVersionSetter(evt).SetGlobalVersion(ver)
				ver = ver.Incr()
			},
			func(evt RWEnvelope) {
				MustNamespaceSetter(evt).SetNamespace("foo")
			},
			func(evt RWEnvelope) {
				evt.SetDests([]string{"dest_1"})
			},
			nil,
		)
		for i, evt := range evts {
			if want, val := user, evt.User(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := ver0.Add(uint64(i), 0), evt.Version(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := ver0.Add(uint64(i), 0), evt.GlobalVersion(); want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if i%2 == 0 {
				if want, val := TypeOfWithNamespace("foo", testutil.Event{}), evt.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			} else {
				if want, val := TypeOfWithNamespace("foo", testutil.Event2{}), evt.Type(); want != val {
					t.Fatalf("expect %v, %v be equals", want, val)
				}
			}
			if want, val := tm0.Add(-5*time.Duration(i)*time.Second), evt.At(); !val.Equal(want) {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
			if want, val := []string{"dest_1"}, evt.Dests(); !reflect.DeepEqual(want, val) {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
		}
	})

	t.Run("with version increment", func(t *testing.T) {
		t.Run("invalid", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expect to panic, got nil")
				}
			}()
			events := []any{
				&testutil.Event{
					Val: "1",
				},
				&testutil.Event2{
					Val: "2",
				},
			}
			ver0 := NewVersion()
			_ = Wrap(ctx, stmID, events,
				WithVersionIncr(ver0, len(events), 10),
			)
		})

		events := []any{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}

		// increment version's integer part
		ver0 := NewVersion()
		evts := Wrap(ctx, stmID, events,
			WithVersionIncr(ver0, len(events), VersionSeqDiffPart),
		)
		l := len(evts)
		for i, evt := range evts {
			want, val := ver0.Add(uint64(i), 0), evt.Version()
			if i == l-1 {
				want = want.EOF()
			}
			if want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
		}

		// increment version's fractional part
		ver1 := NewVersion()
		evts = Wrap(ctx, stmID, events,
			WithVersionIncr(ver0, len(events), VersionSeqDiffFracPart),
		)
		l = len(evts)
		for i, evt := range evts {
			want, val := ver1.Add(0, uint8(i)), evt.Version()
			if i == l-1 {
				want = want.EOF()
			}
			if want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
		}
	})

	t.Run("with global version increment", func(t *testing.T) {
		t.Run("invalid", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expect to panic, got nil")
				}
			}()
			events := []any{
				&testutil.Event{
					Val: "1",
				},
				&testutil.Event2{
					Val: "2",
				},
			}
			ver0 := NewVersion()
			_ = Wrap(ctx, stmID, events,
				WithGlobalVersionIncr(ver0, len(events), 10),
			)
		})

		events := []any{
			&testutil.Event{
				Val: "1",
			},
			&testutil.Event2{
				Val: "2",
			},
		}

		// increment version integer part
		ver0 := NewVersion()
		evts := Wrap(ctx, stmID, events,
			WithGlobalVersionIncr(ver0, len(events), VersionSeqDiffPart),
		)
		l := len(evts)
		for i, evt := range evts {
			want, val := ver0.Add(uint64(i), 0), evt.GlobalVersion()
			if i == l-1 {
				want = want.EOF()
			}
			if want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
		}

		// increment version's fractional part
		ver1 := NewVersion()
		evts = Wrap(ctx, stmID, events,
			WithGlobalVersionIncr(ver0, len(events), VersionSeqDiffFracPart),
		)
		l = len(evts)
		for i, evt := range evts {
			want, val := ver1.Add(0, uint8(i)), evt.GlobalVersion()
			if i == l-1 {
				want = want.EOF()
			}
			if want != val {
				t.Fatalf("expect %v, %v be equals", want, val)
			}
		}
	})
}
