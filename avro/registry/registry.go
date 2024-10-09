package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
)

var (
	ErrInvalidSchemaID             = errors.New("invalid schema ID")
	ErrInvalidDataWireFormat       = errors.New("invalid data wire format")
	ErrMarshalOperationUnsupported = errors.New("marshal operation unsupported")
	ErrUnableToResolveSchema       = errors.New("unable to resolve schema")
	ErrUnableToSetupRegistry       = errors.New("unable to setup registry")
	ErrCreateOrUpdateSchemaFailed  = errors.New("failed to create or update schema")
	ErrSchemaNotFound              = errors.New("schema not found")
)

type WireFormatter interface {
	ExtractSchemaID(data []byte) (string, []byte, error)
	AppendSchemaID(data []byte, id string) ([]byte, error)
}

type Fetcher interface {
	Get(ctx context.Context, id string) (schema string, err error)
	GetByDefinition(ctx context.Context, schema avro.Schema) (id string, err error)
}

type PersistConfig struct {
	Resolver func(schema *avro.RecordSchema) (id string, version int, err error)
}

type Persister interface {
	Persist(ctx context.Context, schema *avro.RecordSchema, opts ...func(*PersistConfig)) (id string, err error)
}

type WalkConfig struct {
	Namespaces []string
}

type Walker interface {
	Walk(ctx context.Context, fn func(id string, version int64, latest bool, schema *avro.RecordSchema) error, opts ...func(*WalkConfig)) (int, error)
}

type schemaIDGetter interface{ AVROSchemaID() string }

type schemaEntry struct {
	schema      *avro.RecordSchema
	batchSchema *avro.ArraySchema
	schemaID    string
}

type RegistryConfig struct {
	Namespace      bool
	PersistCurrent bool
	ReadOnly       bool
}

type Registry struct {
	WireFormatter

	fetcher   Fetcher
	persister Persister

	cfg *RegistryConfig

	api           avro.API
	compatibility *avro.SchemaCompatibility

	current *schemaEntry
	cache   map[string]schemaEntry
	mu      sync.RWMutex
}

func New(fetcher Fetcher, persister Persister, wf WireFormatter) *Registry {
	reg := &Registry{
		WireFormatter: wf,
		fetcher:       fetcher,
		persister:     persister,
		compatibility: avro.NewSchemaCompatibility(),
		api:           avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze(),
		cache:         make(map[string]schemaEntry),
		current:       nil,
	}

	return reg
}

// GetSchemaByDefinition implements Register.
func (r *Registry) getSchemaByDefinition(ctx context.Context, schema avro.Schema) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, entry := range r.cache {
		if entry.schema.Fingerprint() == schema.Fingerprint() {
			return id, nil
		}
	}

	id, err := r.fetcher.GetByDefinition(ctx, schema)
	if err != nil {
		return "", err
	}

	if r.current != nil && r.current.schemaID != id {
		schema, err = r.compatibility.Resolve(r.current.schema, schema)
		if err != nil {
			return "", err
		}
	}
	r.cache[id] = schemaEntry{
		schema:      schema.(*avro.RecordSchema),
		batchSchema: avro.NewArraySchema(schema),
		schemaID:    id,
	}

	return id, nil
}

// API implements Register.
func (r *Registry) API() avro.API {
	return r.api
}

// Setup implements Register.
func (r *Registry) Setup(ctx context.Context, schema avro.Schema, opts ...func(*RegistryConfig)) error {
	r.cfg = &RegistryConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(r.cfg)
	}

	if schema == nil {
		return nil
	}

	if r.current != nil {
		return nil
	}

	if r.cfg.ReadOnly {
		r.current = &schemaEntry{
			schema:      schema.(*avro.RecordSchema),
			batchSchema: avro.NewArraySchema(schema),
		}
		return nil
	}

	id, err := r.getSchemaByDefinition(ctx, schema)
	if err != nil {
		if errors.Is(err, ErrSchemaNotFound) && r.cfg.PersistCurrent {
			id, err = r.persister.Persist(ctx, schema.(*avro.RecordSchema))
			if err != nil {
				return fmt.Errorf("%w: %v", ErrCreateOrUpdateSchemaFailed, err)
			}
		} else {
			return fmt.Errorf("%w: %v", ErrUnableToSetupRegistry, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = &schemaEntry{
		schemaID:    id,
		schema:      schema.(*avro.RecordSchema),
		batchSchema: avro.NewArraySchema(schema),
	}

	return nil
}

func (r *Registry) GetCurrent(ctx context.Context, v schemaIDGetter) (schemaID string, schema *avro.RecordSchema, batchSchema *avro.ArraySchema, err error) {
	if r.current != nil {
		schemaID = r.current.schemaID
		schema = r.current.schema
		batchSchema = r.current.batchSchema

		return
	}

	id := v.AVROSchemaID()
	if id == "" {
		err = ErrSchemaNotFound
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.cache[id]
	if !ok {
		err = ErrSchemaNotFound
		return
	}

	schemaID = id
	schema = entry.schema
	batchSchema = entry.batchSchema

	return
}

// GetSchema implements Register.
func (r *Registry) GetSchema(ctx context.Context, id string) (string, *avro.RecordSchema, *avro.ArraySchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current != nil && r.current.schemaID == id {
		return id, r.current.schema, r.current.batchSchema, nil
	}

	if entry, ok := r.cache[id]; ok {
		return id, entry.schema, entry.batchSchema, nil
	}

	out, err := r.fetcher.Get(ctx, id)
	if err != nil {
		return "", nil, nil, err
	}

	schema := avro.MustParse(out)
	if r.current != nil {
		// c, _ := avro.NewRecordSchema(schema.(avro.NamedSchema).Name(), schema.(avro.NamedSchema).Namespace(), r.current.schema.Fields())
		// schema, err = r.compatibility.Resolve(c, schema)
		// if err != nil {
		// 	return "", nil, nil, err
		// }
		schema, err = r.compatibility.Resolve(r.current.schema, schema)
		if err != nil {
			return "", nil, nil, err
		}
	}
	r.cache[id] = schemaEntry{
		schemaID:    id,
		schema:      schema.(*avro.RecordSchema),
		batchSchema: avro.NewArraySchema(schema),
	}

	return id, r.cache[id].schema, r.cache[id].batchSchema, nil
}
