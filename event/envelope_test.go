package event

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestEnvelope(t *testing.T) {
	ctx := context.Background()

	globalID := "tenantID"
	stmID := NewStreamID(globalID, "service", "rootEntityID")

	t.Run("basic", func(t *testing.T) {
		events := []any{
			&Event{
				Val: "1",
			},
			&Event2{
				Val: "2",
			},
		}
		evts := Wrap(ctx, stmID, events)
		for i, evt := range evts {
			if want, got := globalID, evt.GlobalStreamID(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := stmID.String(), evt.StreamID(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := VersionZero, evt.Version(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := VersionZero, evt.GlobalVersion(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := events[i], evt.Event(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := "", evt.User(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if i%2 == 0 {
				if want, got := TypeOf(Event{}), evt.Type(); want != got {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			} else {
				if want, got := TypeOf(Event2{}), evt.Type(); want != got {
					t.Fatalf("expect %v, %v be equals", want, got)
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
			&Event{
				Val: "1",
			},
			&Event2{
				Val: "2",
			},
		}
		evts := Wrap(ctx, stmID, events)
		for i, evt := range evts {
			if want, got := user, evt.User(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if i%2 == 0 {
				if want, got := TypeOfWithContext(ctx, Event{}), evt.Type(); want != got {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			} else {
				if want, got := TypeOfWithContext(ctx, Event2{}), evt.Type(); want != got {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			}
		}
	})

	t.Run("with options", func(t *testing.T) {
		events := []any{
			&Event{
				Val: "1",
			},
			&Event2{
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
			if want, got := user, evt.User(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := ver0.Add(uint64(i), 0), evt.Version(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := ver0.Add(uint64(i), 0), evt.GlobalVersion(); want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if i%2 == 0 {
				if want, got := TypeOfWithNamespace("foo", Event{}), evt.Type(); want != got {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			} else {
				if want, got := TypeOfWithNamespace("foo", Event2{}), evt.Type(); want != got {
					t.Fatalf("expect %v, %v be equals", want, got)
				}
			}
			if want, got := tm0.Add(-5*time.Duration(i)*time.Second), evt.At(); !got.Equal(want) {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
			if want, got := []string{"dest_1"}, evt.Dests(); !reflect.DeepEqual(want, got) {
				t.Fatalf("expect %v, %v be equals", want, got)
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
				&Event{
					Val: "1",
				},
				&Event2{
					Val: "2",
				},
			}
			ver0 := NewVersion()
			_ = Wrap(ctx, stmID, events,
				WithVersionIncr(ver0, len(events), 10),
			)
		})

		events := []any{
			&Event{
				Val: "1",
			},
			&Event2{
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
			want, got := ver0.Add(uint64(i), 0), evt.Version()
			if i == l-1 {
				want = want.EOF()
			}
			if want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}

		// increment version's fractional part
		ver1 := NewVersion()
		evts = Wrap(ctx, stmID, events,
			WithVersionIncr(ver0, len(events), VersionSeqDiffFracPart),
		)
		l = len(evts)
		for i, evt := range evts {
			want, got := ver1.Add(0, uint8(i)), evt.Version()
			if i == l-1 {
				want = want.EOF()
			}
			if want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
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
				&Event{
					Val: "1",
				},
				&Event2{
					Val: "2",
				},
			}
			ver0 := NewVersion()
			_ = Wrap(ctx, stmID, events,
				WithGlobalVersionIncr(ver0, len(events), 10),
			)
		})

		events := []any{
			&Event{
				Val: "1",
			},
			&Event2{
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
			want, got := ver0.Add(uint64(i), 0), evt.GlobalVersion()
			if i == l-1 {
				want = want.EOF()
			}
			if want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}

		// increment version's fractional part
		ver1 := NewVersion()
		evts = Wrap(ctx, stmID, events,
			WithGlobalVersionIncr(ver0, len(events), VersionSeqDiffFracPart),
		)
		l = len(evts)
		for i, evt := range evts {
			want, got := ver1.Add(0, uint8(i)), evt.GlobalVersion()
			if i == l-1 {
				want = want.EOF()
			}
			if want != got {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		}
	})
}
