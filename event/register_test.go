package event

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ln80/event-store/event/testutil"
)

func TestRegister_Get(t *testing.T) {
	namespace := "foo"
	ctx := context.WithValue(context.Background(), ContextNamespaceKey, namespace)

	reg := NewRegister(namespace)
	defer reg.Clear()
	// both events are registered in the given namespace
	reg.
		Set(&testutil.Event{}).
		Set(&testutil.Event2{})

	// get an unregistered event
	if _, err := reg.Get("foo.NoEvent"); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatalf("expect err be %v, got %v", ErrNotFoundInRegistry, err)
	}

	// successfully find Event in registry
	e, err := reg.Get(TypeOf(&testutil.Event{}))
	if err != nil {
		t.Fatal("expected err to be nil, got", err)
	}
	if _, ok := e.(*testutil.Event); !ok {
		t.Fatalf("expected casting to %s is ok, got false", TypeOf(&testutil.Event{}))
	}
	if _, err = reg.Get(TypeOfWithContext(ctx, &testutil.Event{})); err != nil {
		t.Fatal("expected err be nil, got", err)
	}

	// only Event2 is registered in global registry
	globReg := NewRegister("")
	globReg.
		Set(&testutil.Event2{})

	if _, err = globReg.Get(TypeOf(&testutil.Event{})); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatalf("expected err be %v, got %v", ErrNotFoundInRegistry, err)
	}
	_, err = reg.Get(TypeOf(&testutil.Event2{}))
	if err != nil {
		t.Fatal("expected err to be nil, got", err)
	}

	// in contrast to reg with namespace, the global registry does not force its namespace prefix in event name
	// thus, Event2 name in registry is {package name}.Event2 instead of {namespace}.Event2
	if _, err = globReg.Get(TypeOfWithContext(ctx, &testutil.Event2{})); !errors.Is(err, ErrNotFoundInRegistry) {
		t.Fatal("Expected err be nil, got", err)
	}
}

func TestRegister_GetFromGlobal(t *testing.T) {
	type Event struct{ Val string }
	type Event2 struct{ Val string }

	namespace := "testutil"

	// clear event registry before and after test
	NewRegister("").Clear()
	defer NewRegister("").Clear()

	NewRegister(namespace).
		Set(&Event{}).
		Set(&Event2{})

	NewRegister("").
		Set(&testutil.Event{})

	evt1 := Event{}

	// case 1
	globalEvt1, err := NewRegister(namespace).GetFromGlobal(evt1)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	_, ok := globalEvt1.(*testutil.Event)
	if !ok {
		t.Fatalf("expect global event type be %T, got %T", &testutil.Event{}, globalEvt1)
	}

	// case 2
	globalEvt1, err = NewRegister(namespace).GetFromGlobal(evt1)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	_, ok = globalEvt1.(*testutil.Event)
	if !ok {
		t.Fatalf("expect global event type be %T, got %T", testutil.Event{}, globalEvt1)
	}

	evt2 := Event2{}
	_, err = NewRegister(namespace).GetFromGlobal(evt2)
	if wantErr := ErrNotFoundInRegistry; !errors.Is(err, wantErr) {
		t.Fatalf("expect err be %v, got %v", wantErr, err)
	}
}

func TestRegister_All(t *testing.T) {
	type Event struct{ Val string }

	namespace := "testutil"

	NewRegister("").Clear()
	defer NewRegister("").Clear()

	evt1 := Event{Val: "1"}

	NewRegister(namespace).
		Set(&evt1, WithAliases("evt_1"), func(rep registryEntryProps) {
			rep["custom"] = "custom"
		})

	for _, entry := range NewRegister(namespace).All() {
		if want, got := "evt_1", entry.Property("aliases").([]string)[0]; want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		if want, got := TypeOfWithNamespace(namespace, evt1), entry.Name(); want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
		if want, got := "custom", entry.Property("custom").(string); want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	}

	if want, got := []string{namespace}, NewRegister("").Namespaces(); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %v,%v be equals", want, got)
	}
}
