package avro

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
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
