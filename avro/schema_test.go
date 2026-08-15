package avro

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ln80/avro/v2"
	"github.com/ln80/event-store/event"
)

func TestEventSchema(t *testing.T) {
	ctx := context.Background()
	a := avro.Config{PartialUnionTypeResolution: false, UnionResolutionError: true}.Freeze()
	namespace := "service1"

	reg := event.NewRegister(namespace)
	defer reg.Clear()

	type ignore [0]bool

	type ValueObject1 struct {
		Uint32 uint32
	}
	type ValueObjectA struct {
		_              ignore `ev:",aliases=ValueObject1 VeryOldObject1"`
		Uint32_Changed uint32 `ev:",aliases=Uint32"`
	}

	type ValueObject2 struct {
		Time time.Time
	}

	type Event1 struct {
		Int64  int64
		String string
		Bool   bool
	}

	type Event2 struct {
		Bool  bool
		Array []string
		Obj1  ValueObject1
	}
	type EventA struct {
		Int64_Changed int64  `ev:",aliases=Int64"`
		Bytes         []byte `ev:",aliases=String"`
		Float64_New   float64
	}
	type EventB struct {
		_     ignore `ev:",aliases=Event2"`
		Bool  bool
		Array []string
		Obj1  ValueObjectA
		ValueObject2
	}

	event.NewRegister(namespace).
		Set(&Event1{}).
		Set(&Event2{})

	sch1, err := eventSchema(a, namespace)
	if err != nil {
		t.Fatal(err)
	}
	evt1 := Event1{
		Int64:  10,
		String: "foo",
		Bool:   true,
	}
	evt2 := Event2{
		Bool:  true,
		Array: []string{"foo", "bar"},
		Obj1: ValueObject1{
			Uint32: 20,
		},
	}
	evts := event.Wrap(ctx, event.NewStreamID("service1", "tenantID"), []any{
		evt1, evt2,
	},
		event.WithNameSpace(namespace),
	)
	avroEvts := make([]avroEvent, len(evts))
	for i, evt := range evts {
		avroEvt, _ := convertEvent(evt)
		avroEvts[i] = *avroEvt
	}
	b, err := a.Marshal(avro.NewArraySchema(sch1), avroEvts)
	if err != nil {
		t.Fatal(err)
	}

	// remove old events version form registry
	event.NewRegister(namespace).Clear()

	defEventA := EventA{
		Float64_New: float64(40),
	}
	defEventB := EventB{
		ValueObject2: ValueObject2{
			Time: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		},
	}
	event.NewRegister(namespace).
		Set(
			defEventA,
			event.WithAliases("Event1"),
		).
		Set(
			defEventB,
		)
	sch2, err := eventSchema(a, namespace)
	if err != nil {
		t.Fatal(err)
	}

	compat := NewCompatibilityAPI()
	r, err := compat.Resolve(sch2, sch1)
	if err != nil {
		t.Fatal(err)
	}

	resultEvts := make([]avroEvent, 0)
	err = a.Unmarshal(avro.NewArraySchema(r), b, &resultEvts)
	if err != nil {
		t.Fatal(err)
	}

	ptr1 := event.ToPtr(resultEvts[0].Event()).Ptr
	rEvt1, ok := ptr1.(*EventA)
	if !ok {
		t.Fatalf("invalid event type expect %T, got %T", EventA{}, resultEvts[0].Event())
	}
	if want, got := evt1.Int64, rEvt1.Int64_Changed; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := evt1.String, string(rEvt1.Bytes); want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := defEventA.Float64_New, rEvt1.Float64_New; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}

	ptr2 := event.ToPtr(resultEvts[1].Event()).Ptr
	rEvt2, ok := ptr2.(*EventB)
	if !ok {
		t.Fatalf("invalid event type expect %T, got %T", EventB{}, rEvt1)
	}
	if want, got := evt2.Bool, rEvt2.Bool; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := evt2.Array, rEvt2.Array; !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %+v, %+v be equals", want, got)
	}
	if want, got := evt2.Obj1.Uint32, rEvt2.Obj1.Uint32_Changed; !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %+v, %+v be equals", want, got)
	}
	if want, got := defEventB.ValueObject2, rEvt2.ValueObject2; !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %+v, %+v be equals", want, got)
	}

	if want, got := event.TypeOfWithNamespace(namespace, &Event1{}), resultEvts[0].Type(); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %+v, %+v be equals", want, got)
	}
	if want, got := event.TypeOfWithNamespace(namespace, &Event2{}), resultEvts[1].Type(); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %+v, %+v be equals", want, got)
	}

	resultEvts[0].checkType(namespace)
	resultEvts[1].checkType(namespace)

	if want, got := event.TypeOfWithNamespace(namespace, &EventA{}), resultEvts[0].Type(); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %+v, %+v be equals", want, got)
	}
	if want, got := event.TypeOfWithNamespace(namespace, &EventB{}), resultEvts[1].Type(); !reflect.DeepEqual(want, got) {
		t.Fatalf("expect %+v, %+v be equals", want, got)
	}
}

func TestPackUnpackEventSchemas(t *testing.T) {
	a := NewAPI()

	namespace := "service" + event.UID().String()

	reg := event.NewRegister(namespace)
	defer reg.Clear()

	type Event1 struct{ ID string }
	type Event2 struct{ ID string }

	reg.Set(Event1{})
	reg.Set(Event2{})

	m, err := EventSchemas(a, []string{namespace})
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	schema, ok := m[namespace]
	if !ok {
		t.Fatalf("expect to find schema for namespace '%s'", namespace)
	}

	schemas, err := UnpackEventSchemas(schema.(*avro.RecordSchema))
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}
	if n := len(schemas); n != 2 {
		t.Fatalf("expect to find two event schemas, found %v", n)
	}
	if want, got := namespace+".Event1", schemas[0].FullName(); want != got {
		t.Fatalf("expect be equals %v,%v", want, got)
	}
	if want, got := namespace+".Event2", schemas[1].FullName(); want != got {
		t.Fatalf("expect be equals %v,%v", want, got)
	}
}

// TestNestedRecordFieldZeroDefaults ensures nested records under *T and []T get
// Avro zero-value defaults, so additive nested fields stay backward-compatible.
func TestNestedRecordFieldZeroDefaults(t *testing.T) {
	a := NewAPI()
	namespace := "nest" + event.UID().String()
	reg := event.NewRegister(namespace)
	defer reg.Clear()

	// Define v1/v2 in separate scopes so both use the same Avro record names
	// (Nested / Item), matching real domain evolution that keeps type names.
	sch1 := func() *avro.RecordSchema {
		type Nested struct {
			Name string
		}
		type Item struct {
			Key string
		}
		type Ev struct {
			Payload *Nested
			Items   []Item
		}
		reg.Set(Ev{})
		s, err := eventSchema(a, namespace)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}()

	events, err := UnpackEventSchemas(sch1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expect 1 event schema, got %d", len(events))
	}
	assertFieldHasDefault(t, findNamedRecordField(t, events[0], "Payload"), "Name")
	assertFieldHasDefault(t, findArrayItemRecord(t, events[0], "Items"), "Key")

	reg.Clear()
	sch2 := func() *avro.RecordSchema {
		type Nested struct {
			Name  string
			Extra string
		}
		type Item struct {
			Key  string
			Tags []string
		}
		type Ev struct {
			Payload *Nested
			Items   []Item
		}
		event.NewRegister(namespace).Set(Ev{})
		s, err := eventSchema(a, namespace)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}()

	if err := NewCompatibilityAPI().Compatible(sch2, sch1); err != nil {
		t.Fatalf("evolved schema must be backward-compatible with v1: %v", err)
	}

	events2, err := UnpackEventSchemas(sch2)
	if err != nil {
		t.Fatal(err)
	}
	assertFieldHasDefault(t, findNamedRecordField(t, events2[0], "Payload"), "Extra")
	assertFieldHasDefault(t, findArrayItemRecord(t, events2[0], "Items"), "Tags")
}

func findNamedRecordField(t *testing.T, parent *avro.RecordSchema, fieldName string) *avro.RecordSchema {
	t.Helper()
	for _, f := range parent.Fields() {
		if f.Name() != fieldName {
			continue
		}
		typ := f.Type()
		if typ.Type() == avro.Union {
			for _, branch := range typ.(*avro.UnionSchema).Types() {
				if branch.Type() == avro.Record {
					return branch.(*avro.RecordSchema)
				}
				if branch.Type() == avro.Ref {
					return branch.(*avro.RefSchema).Schema().(*avro.RecordSchema)
				}
			}
		}
		if typ.Type() == avro.Record {
			return typ.(*avro.RecordSchema)
		}
		if typ.Type() == avro.Ref {
			return typ.(*avro.RefSchema).Schema().(*avro.RecordSchema)
		}
		t.Fatalf("field %s: expected record (possibly in union), got %s", fieldName, typ.Type())
	}
	t.Fatalf("field %s not found on %s", fieldName, parent.FullName())
	return nil
}

func findArrayItemRecord(t *testing.T, parent *avro.RecordSchema, fieldName string) *avro.RecordSchema {
	t.Helper()
	for _, f := range parent.Fields() {
		if f.Name() != fieldName {
			continue
		}
		arr, ok := f.Type().(*avro.ArraySchema)
		if !ok {
			t.Fatalf("field %s: expected array, got %s", fieldName, f.Type().Type())
		}
		items := arr.Items()
		if items.Type() == avro.Ref {
			return items.(*avro.RefSchema).Schema().(*avro.RecordSchema)
		}
		if items.Type() == avro.Record {
			return items.(*avro.RecordSchema)
		}
		t.Fatalf("field %s: expected array of records, got items %s", fieldName, items.Type())
	}
	t.Fatalf("field %s not found on %s", fieldName, parent.FullName())
	return nil
}

func assertFieldHasDefault(t *testing.T, rec *avro.RecordSchema, fieldName string) {
	t.Helper()
	for _, f := range rec.Fields() {
		if f.Name() == fieldName {
			if !f.HasDefault() {
				t.Fatalf("%s.%s: expected Avro default, got none", rec.FullName(), fieldName)
			}
			return
		}
	}
	t.Fatalf("field %s not found on %s", fieldName, rec.FullName())
}
