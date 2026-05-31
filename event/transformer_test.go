package event

import (
	"context"
	"reflect"
	"testing"
)

func TestToPtr_ValueType(t *testing.T) {
	evt := Event{Val: "hello"}

	p := ToPtr(evt)

	if p.Ptr == nil {
		t.Fatal("expect Ptr to be non-nil")
	}
	if p.Dereference == nil {
		t.Fatal("expect Dereference to be non-nil for value types")
	}

	ptrVal, ok := p.Ptr.(*Event)
	if !ok {
		t.Fatalf("expect Ptr to be *Event, got %T", p.Ptr)
	}
	if ptrVal.Val != "hello" {
		t.Fatalf("expect Ptr value to be 'hello', got %q", ptrVal.Val)
	}

	ptrVal.Val = "modified"
	deref := p.Dereference()
	got, ok := deref.(Event)
	if !ok {
		t.Fatalf("expect Dereference to return Event, got %T", deref)
	}
	if got.Val != "modified" {
		t.Fatalf("expect dereferenced value to reflect mutation, got %q", got.Val)
	}
}

func TestToPtr_PointerType(t *testing.T) {
	evt := &Event{Val: "hello"}

	p := ToPtr(evt)

	if p.Ptr != evt {
		t.Fatal("expect Ptr to be the same pointer")
	}
	if p.Dereference != nil {
		t.Fatal("expect Dereference to be nil for pointer types")
	}
}

func TestReferenceEvents(t *testing.T) {
	ctx := context.Background()
	stmID := NewStreamID("tenant", "svc", "root")

	events := []any{
		Event{Val: "1"},
		Event{Val: "2"},
		Event{Val: "3"},
	}
	envs := Wrap(ctx, stmID, events)

	ptrs, indexes := ReferenceEvents(envs, func(env Envelope) bool {
		e := env.Event().(Event)
		return e.Val != "2"
	})

	if len(ptrs) != 2 {
		t.Fatalf("expect 2 pointers, got %d", len(ptrs))
	}
	if len(indexes) != 2 {
		t.Fatalf("expect 2 indexes, got %d", len(indexes))
	}

	if _, ok := indexes[0]; !ok {
		t.Fatal("expect index 0 to be present")
	}
	if _, ok := indexes[2]; !ok {
		t.Fatal("expect index 2 to be present")
	}

	for idx, index := range indexes {
		if index.Dereference == nil {
			t.Fatalf("expect Dereference to be non-nil at index %d", idx)
		}
		ptrEvt := index.Ptr.(*Event)
		ptrEvt.Val = "modified"

		envs[idx].(Transformer).Transform(func(_ any) any {
			return index.Dereference()
		})

		got := envs[idx].Event().(Event)
		if !reflect.DeepEqual(got.Val, "modified") {
			t.Fatalf("expect event at %d to be modified, got %q", idx, got.Val)
		}
	}

	unmodified := envs[1].Event().(Event)
	if unmodified.Val != "2" {
		t.Fatalf("expect event at index 1 to be unchanged, got %q", unmodified.Val)
	}
}
