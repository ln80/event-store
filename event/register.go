package event

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type registryEntryProps map[string]any

func WithEventAliases(aliases ...string) func(registryEntryProps) {
	return func(rep registryEntryProps) {
		rep["aliases"] = aliases
	}
}

type registryEntry struct {
	typ   reflect.Type
	def   any
	props registryEntryProps
}

func newRegistryEntry(typ reflect.Type,
	def any,
	props registryEntryProps) registryEntry {

	return registryEntry{typ: typ, def: def, props: props}
}

func (re registryEntry) Type() reflect.Type {
	return re.typ
}

func (re registryEntry) Default() any {
	return re.def
}

func (re registryEntry) Property(name string) any {
	prop, ok := re.props[name]
	if !ok {
		return nil
	}
	return prop
}

var (
	regMu    sync.RWMutex
	registry = make(map[string]map[string]registryEntry)
)

var (
	ErrNotFoundInRegistry = errors.New("event not found in registry")
	ErrConvertEventFailed = errors.New("convert event failed")
)

// Register defines the registry service for domain events
type Register interface {
	// Set register the given event in the registry.
	Set(event any, opts ...func(registryEntryProps)) Register

	// Get return an empty instance of the given event type.
	// Note that it returns the value type, not the pointer
	Get(name string) (any, error)

	// Convert the given event to its equivalent from the global namespace.
	// the equivalent event in the global namespace must have type named as:
	// {caller registry namespace}.{event struct name}.
	// In other words equivalent event package name is the same as caller registry namespace.
	// Note that it returns a value of the equivalent type from the global namespace not a pointer.
	Convert(evt any) (any, error)

	All() map[string]registryEntry
	// clear all namespace registries. Its mainly used in internal tests
	clear()
}

// register implement the Register interface
// it allows to have a registry per namespace, and use the global registry (i.e, empty namespace)
// to handle some fallback logics
type register struct {
	namespace string
}

// All implements Register.
func (r *register) All() map[string]registryEntry {
	regMu.Lock()
	defer regMu.Unlock()

	return registry[r.namespace]
}

var _ Register = &register{}

// NewRegisterFrom context returns a new instance of the register using the namespace found in the context.
// Otherwise, it returns an instance base on the global namespace
func NewRegisterFrom(ctx context.Context) Register {
	if namespace := ctx.Value(ContextNamespaceKey); namespace != nil {
		return NewRegister(namespace.(string))
	}
	return NewRegister("")
}

// NewRegister returns a Register instance for the given namespace.
func NewRegister(namespace string) Register {
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := registry[namespace]; !ok {
		registry[namespace] = make(map[string]registryEntry)
	}
	if _, ok := registry[""]; !ok {
		registry[""] = make(map[string]registryEntry)
	}

	return &register{namespace: namespace}
}

// Set implements Set method of the Register interface.
// It registers the given event in the current namespace registry.
// It uses TypeOfWithNamespace func to solve the event name.
// By default the event name is "{package name}.{event struct name}""
// In case of namespace exists, the event name becomes "{namespace}.{event struct name}""
func (r *register) Set(evt any, opts ...func(registryEntryProps)) Register {
	name := TypeOfWithNamespace(r.namespace, evt)
	rType, _ := resolveType(evt)

	regMu.Lock()
	defer regMu.Unlock()

	props := make(map[string]any)
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(props)
	}
	registry[r.namespace][name] = newRegistryEntry(rType, evt, props)
	return r
}

// Get implements Get method of the Register interface.
// It looks for the event in the namespace registry,
// and use the global namespace's one as fallback
func (r *register) Get(name string) (any, error) {
	regMu.Lock()
	defer regMu.Unlock()

	if r.namespace != "" {
		splits := strings.Split(name, ".")
		entry, ok := registry[r.namespace][r.namespace+"."+splits[len(splits)-1]]
		if ok {
			return reflect.New(entry.typ).Interface(), nil
		}
	}

	entry, ok := registry[r.namespace][name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFoundInRegistry, "event type: "+name)
	}

	return reflect.New(entry.typ).Interface(), nil
}

func (r *register) Convert(evt any) (convevt any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrConvertEventFailed, r)
		}
	}()

	name := TypeOfWithNamespace(r.namespace, evt)

	regMu.Lock()
	defer regMu.Unlock()

	entry, ok := registry[""][name]
	if !ok {
		err = fmt.Errorf("%w: %s", ErrNotFoundInRegistry, "during conversion to the equivalent event from global namespace: "+name)
		return
	}
	// in case event is a pointer use its value instead
	ev := evt
	if reflect.TypeOf(evt).Kind() == reflect.Ptr {
		ev = reflect.ValueOf(evt).Elem().Interface()
	}
	convevt = reflect.ValueOf(ev).Convert(entry.typ).Interface()
	return
}

// clear implements clear method of the Register interface
func (r *register) clear() {
	regMu.Lock()
	defer regMu.Unlock()

	registry = make(map[string]map[string]registryEntry)
}
