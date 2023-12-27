package memory

import (
	"context"
	"fmt"
	"sync"

	avro "github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
)

type schemaEntry struct {
	schema      *avro.RecordSchema
	batchSchema *avro.ArraySchema
	schemaID    string
}

type Registry struct {
	api           avro.API
	compatibility *avro.SchemaCompatibility

	current schemaEntry
	cache   map[string]schemaEntry
	mu      sync.RWMutex
}

var _ registry.Registry = &Registry{}

func NewRegistry() *Registry {
	reg := &Registry{
		compatibility: avro.NewSchemaCompatibility(),
		cache:         make(map[string]schemaEntry),
		api:           avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze(),
	}

	return reg
}

// Setup implements avro.Registry.
func (r *Registry) Setup(ctx context.Context, schema avro.Schema, opts ...func(*registry.RegistryConfig)) error {
	if r.current.schema != nil && r.current.schemaID != "" {
		return nil
	}
	if schema == nil {
		return nil
	}
	r.current.schema = schema.(*avro.RecordSchema)
	fingerprint := schema.Fingerprint()
	r.current.schemaID = string(fingerprint[:])
	r.current.batchSchema = avro.NewArraySchema(r.current.schema)

	return nil
}

// Client implements avro.Registry.
func (r *Registry) API() avro.API {
	return r.api
}

// Marshal implements avro.Registry.
func (r *Registry) Marshal(ctx context.Context, v any) ([]byte, error) {
	if r.current.schema == nil {
		return nil, registry.ErrUnableToResolveSchema
	}
	b, err := r.api.Marshal(r.current.schema, v)
	if err != nil {
		return nil, err
	}
	b, err = r.appendSchemaID(b, []byte(r.current.schemaID))
	if err != nil {
		return nil, err
	}

	return b, nil
}

// MarshalBatch implements avro.Registry.
func (r *Registry) MarshalBatch(ctx context.Context, v any) ([]byte, error) {
	if r.current.batchSchema == nil {
		return nil, registry.ErrUnableToResolveSchema
	}

	b, err := r.api.Marshal(r.current.batchSchema, v)
	if err != nil {
		return nil, err
	}
	b, err = r.appendSchemaID(b, []byte(r.current.schemaID))
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Unmarshal implements avro.Registry.
func (r *Registry) Unmarshal(ctx context.Context, b []byte, v any) error {
	id, b, err := r.extractSchemaID(b)
	if err != nil {
		return err
	}

	_, schema, _, err := r.getSchema(ctx, id)
	if err != nil {
		return err
	}

	if err := r.api.Unmarshal(schema, b, v); err != nil {
		return err
	}

	return nil
}

// UnmarshalBatch implements avro.Registry.
func (r *Registry) UnmarshalBatch(ctx context.Context, b []byte, v any) error {
	id, b, err := r.extractSchemaID(b)
	if err != nil {
		return err
	}

	_, _, batchSchema, err := r.getSchema(ctx, id)
	if err != nil {
		return err
	}

	if err := r.api.Unmarshal(batchSchema, b, v); err != nil {
		return err
	}

	return nil
}

func (r *Registry) getSchema(ctx context.Context, id string) (string, *avro.RecordSchema, *avro.ArraySchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if id == r.current.schemaID {
		return id, r.current.schema, r.current.batchSchema, nil
	}

	entry, ok := r.cache[id]
	if !ok {
		return "", nil, nil, fmt.Errorf("%w: %v", registry.ErrUnableToResolveSchema, id)
	}

	return id, entry.schema, entry.batchSchema, nil
}

func (r *Registry) appendSchemaID(data []byte, id []byte) ([]byte, error) {
	return append(id[:], data...), nil
}

func (r *Registry) extractSchemaID(data []byte) (string, []byte, error) {
	if len(data) < 64 {
		return "", nil, fmt.Errorf("data too short")
	}

	return string(data[0:32]), data[32:], nil
}
