package event

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/ln80/event-store/event/errors"
)

const GlobalRegistryName = ""

var (
	ErrNotFoundInRegistry = errors.New("event not found in registry")
)

type registryEntryProps map[string]any

func WithAliases(aliases ...string) func(registryEntryProps) {
	return func(rep registryEntryProps) {
		if len(aliases) > 0 {
			rep["aliases"] = aliases
		}
	}
}

type registryEntry struct {
	name  string
	typ   reflect.Type
	def   any
	props registryEntryProps
}

func newRegistryEntry(name string, typ reflect.Type,
	def any,
	props registryEntryProps) registryEntry {

	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return registryEntry{name: name, typ: typ, def: def, props: props}
}

func (re registryEntry) Name() string {
	return re.name
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

// Register defines the registry service for domain events
type Register interface {
	// Set register the given event in the registry.
	// In case the event is pointer it register its value instead.
	Set(event any, opts ...func(registryEntryProps)) Register

	// Get returns a pointer of a zero type's value.
	Get(name string) (ptr any, err error)

	// GetFromGlobal returns a pointer to equivalent event of the given one from the global registry.
	GetFromGlobal(evt any) (ptr any, err error)

	// All returns all registered events in the current namespace. It returns both event type and
	// default value.
	All() []registryEntry

	// Clear all namespace registries. It's mainly used for internal tests
	Clear()

	Namespaces() []string
}

// register implement the Register interface
// it allows to have a registry per namespace, and use the global registry (i.e, empty namespace)
// to handle some fallback logics
type register struct {
	namespace string
}

// All implements Register.
func (r *register) All() []registryEntry {
	regMu.Lock()
	defer regMu.Unlock()

	arr := make([]registryEntry, len(registry[r.namespace]))

	idx := 0
	for _, e := range registry[r.namespace] {
		arr[idx] = e
		idx++
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Name() < arr[j].Name()
	})

	return arr
}

var _ Register = &register{}

// NewRegisterFrom context returns a new instance of the register using the namespace found in the context.
// Otherwise, it returns an instance base on the global namespace
func NewRegisterFrom(ctx context.Context) Register {
	if namespace := ctx.Value(ContextNamespaceKey); namespace != nil {
		return NewRegister(namespace.(string))
	}
	return NewRegister(GlobalRegistryName)
}

// NewRegister returns a Register instance for the given namespace.
func NewRegister(namespace string) Register {
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := registry[namespace]; !ok {
		registry[namespace] = make(map[string]registryEntry)
	}
	if _, ok := registry[GlobalRegistryName]; !ok {
		registry[GlobalRegistryName] = make(map[string]registryEntry)
	}

	return &register{namespace: namespace}
}

// Set implements Set method of the Register interface.
func (r *register) Set(evt any, opts ...func(registryEntryProps)) Register {
	name := TypeOfWithNamespace(r.namespace, evt)
	rType, _ := resolveType(evt)

	if reflect.TypeOf(evt).Kind() == reflect.Ptr {
		evt = reflect.ValueOf(evt).Elem().Interface()
	}

	regMu.Lock()
	defer regMu.Unlock()

	props := make(map[string]any)
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(props)
	}
	registry[r.namespace][name] = newRegistryEntry(name, rType, evt, props)
	return r
}

// Get implements Get method of the Register interface.
func (r *register) Get(name string) (any, error) {
	regMu.Lock()
	defer regMu.Unlock()

	if r.namespace != GlobalRegistryName {
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

// GetFromGlobal implements GetFromGlobal methods of the Register interface.
func (r *register) GetFromGlobal(evt any) (ptr any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrNotFoundInRegistry, r)
		}
	}()

	name := TypeOfWithNamespace(r.namespace, evt)

	regMu.Lock()
	defer regMu.Unlock()

	entry, ok := registry[GlobalRegistryName][name]
	if !ok {
		err = ErrNotFoundInRegistry
		return
	}

	ptr = reflect.New(entry.typ).Interface()
	return
}

// clear implements clear method of the Register interface
func (r *register) Clear() {
	regMu.Lock()
	defer regMu.Unlock()

	registry = make(map[string]map[string]registryEntry)
}

// Namespaces implements Register.
func (*register) Namespaces() []string {
	regMu.Lock()
	defer regMu.Unlock()

	arr := make([]string, 0)
	for k := range registry {
		if k == "" {
			continue
		}
		arr = append(arr, k)
	}

	return arr
}
