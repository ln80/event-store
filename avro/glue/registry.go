package glue

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithytime "github.com/aws/smithy-go/time"
	smithywaiter "github.com/aws/smithy-go/waiter"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
)

type schemaEntry struct {
	schema      *avro.RecordSchema
	batchSchema *avro.ArraySchema
	schemaID    string
}

type Registry struct {
	registryName string
	client       ClientAPI

	api           avro.API
	compatibility *avro.SchemaCompatibility

	current *schemaEntry
	cache   map[string]schemaEntry

	mu sync.RWMutex

	cfg *registry.RegistryConfig
}

var _ registry.Registry = &Registry{}

func NewRegistry(registryName string, client ClientAPI) *Registry {
	reg := &Registry{
		registryName:  registryName,
		compatibility: avro.NewSchemaCompatibility(),
		client:        client,
		cache:         make(map[string]schemaEntry),
		api:           avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze(),
	}

	return reg
}

func (r *Registry) Setup(ctx context.Context, schema avro.Schema, opts ...func(*registry.RegistryConfig)) error {
	r.cfg = &registry.RegistryConfig{}
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

	id, _, err := r.getSchemaByDefinition(ctx, schema)
	if err != nil {
		var tce *types.EntityNotFoundException
		if errors.As(err, &tce) {
			id, err = r.upsertSchema(ctx, schema)
			if err != nil {
				return fmt.Errorf("%w: %v", registry.ErrCreateOrUpdateSchemaFailed, err)
			}
		} else {
			return fmt.Errorf("%w: %v", registry.ErrUnableToSetupSchema, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = &schemaEntry{
		schema:      schema.(*avro.RecordSchema),
		batchSchema: avro.NewArraySchema(schema),
		schemaID:    id,
	}

	return nil
}

func (r *Registry) API() avro.API {
	return r.api
}

func (r *Registry) Marshal(ctx context.Context, v any) ([]byte, error) {
	if r.cfg.ReadOnly {
		return nil, fmt.Errorf("%w: read only enabled", registry.ErrMarshalOperationUnsupported)
	}

	var (
		schema   avro.Schema
		schemaID string
	)

	if r.current != nil {
		schema = r.current.schema
		schemaID = r.current.schemaID
	} else {
		vv, ok := v.(registry.AVROSchemaGetter)
		if !ok {
			pv := reflect.New(reflect.TypeOf(v))
			pv.Elem().Set(reflect.ValueOf(v))
			vv, ok = pv.Interface().(registry.AVROSchemaGetter)
		}
		if ok {
			schemaID = vv.AVROSchemaID()
			r.mu.Lock()
			entry, ok := r.cache[schemaID]
			r.mu.Unlock()
			if ok {
				schema = entry.schema
			}
		}
	}
	if schema == nil {
		return nil, fmt.Errorf("%w: for id : %v", registry.ErrUnableToResolveSchema, schemaID)
	}
	b, err := r.api.Marshal(schema, v)
	if err != nil {
		return nil, err
	}
	b, err = r.appendSchemaID(b, schemaID)
	if err != nil {
		return nil, err
	}

	return b, nil
}

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

	vv, ok := v.(registry.AVROSchemaSetter)
	if !ok {
		pv := reflect.New(reflect.TypeOf(v))
		pv.Elem().Set(reflect.ValueOf(v))
		vv, ok = pv.Interface().(registry.AVROSchemaSetter)
	}
	if ok {
		vv.SetAVROSchemaID(id)
	}

	return nil
}

func (r *Registry) MarshalBatch(ctx context.Context, v any) ([]byte, error) {
	if r.cfg.ReadOnly {
		return nil, fmt.Errorf("%w: read only enabled", registry.ErrMarshalOperationUnsupported)
	}

	var (
		schema   avro.Schema
		schemaID string
	)

	if r.current != nil {
		schema = r.current.batchSchema
		schemaID = r.current.schemaID
	} else {
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Pointer {
			val = val.Elem()
		}
		if val.Kind() == reflect.Slice {
			elem := val.Index(0)
			if elem.CanAddr() {
				if vv, ok := elem.Addr().Interface().(registry.AVROSchemaGetter); ok {
					id := vv.AVROSchemaID()
					r.mu.Lock()
					entry, ok := r.cache[id]
					r.mu.Unlock()
					if ok {
						schema = entry.batchSchema
						schemaID = id
					}

				}
			}
		}
		if schema == nil {
			return nil, fmt.Errorf("%w: for id : %v", registry.ErrUnableToResolveSchema, schemaID)
		}
	}

	b, err := r.api.Marshal(schema, v)
	if err != nil {
		return nil, err
	}
	b, err = r.appendSchemaID(b, schemaID)
	if err != nil {
		return nil, err
	}

	return b, nil
}

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

	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Pointer || val.Elem().Kind() != reflect.Slice {
		return nil
	}

	val = val.Elem()
	for i := 0; i < val.Len(); i++ {
		elem := val.Index(i)
		if elem.CanAddr() {
			v, ok := elem.Addr().Interface().(registry.AVROSchemaSetter)
			if ok {
				v.SetAVROSchemaID(id)
			}
		}
	}

	return nil
}

func (r *Registry) upsertSchema(ctx context.Context, schema avro.Schema) (string, error) {
	id, err := r.updateSchema(ctx, schema)
	if err != nil {
		var tce *types.EntityNotFoundException
		if errors.As(err, &tce) {
			id, err = r.createSchema(ctx, schema)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}

	return id, err
}

func (r *Registry) createSchema(ctx context.Context, schema avro.Schema) (string, error) {
	def, _ := schema.(*avro.RecordSchema).MarshalJSON()
	out, err := r.client.CreateSchema(ctx, &glue.CreateSchemaInput{
		DataFormat: types.DataFormatAvro,
		SchemaName: aws.String(schema.(*avro.RecordSchema).FullName()),

		Compatibility: types.CompatibilityBackwardAll,
		RegistryId: &types.RegistryId{
			RegistryName: aws.String(r.registryName),
		},
		SchemaDefinition: aws.String(string(def)),
		Tags:             map[string]string{},
	})
	if err != nil {
		return "", err
	}

	id := aws.ToString(out.SchemaVersionId)
	if out.SchemaVersionStatus != types.SchemaVersionStatusAvailable {
		out, err := r.waitForSchema(ctx, id)
		if err != nil {
			return "", fmt.Errorf("failed to create schema %v: %w", id, err)
		}
		if out.Status != types.SchemaVersionStatusAvailable {
			return "", fmt.Errorf("failed to create schema: %v", id)
		}
	}
	return id, nil
}

func (r *Registry) updateSchema(ctx context.Context, schema avro.Schema) (string, error) {
	def, _ := schema.(*avro.RecordSchema).MarshalJSON()

	out, err := r.client.RegisterSchemaVersion(ctx, &glue.RegisterSchemaVersionInput{
		SchemaDefinition: aws.String(string(def)),
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(r.registryName),
			SchemaName:   aws.String(schema.(*avro.RecordSchema).FullName()),
		},
	})
	if err != nil {
		return "", err
	}

	id := aws.ToString(out.SchemaVersionId)
	if out.Status != types.SchemaVersionStatusAvailable {
		out, err := r.waitForSchema(ctx, id)
		if err != nil {
			return "", fmt.Errorf("failed to update schema %v: %w", id, err)
		}
		if out.Status != types.SchemaVersionStatusAvailable {
			return "", fmt.Errorf("failed to update schema %v", id)
		}
	}
	return id, nil
}

func (r *Registry) waitForSchema(ctx context.Context, id string) (*glue.GetSchemaVersionOutput, error) {
	minDelay, maxDelay, maxWaitDur := 1*time.Second, 5*time.Second, 25*time.Second

	ctx, cancelFn := context.WithTimeout(ctx, maxWaitDur)
	defer cancelFn()

	remainingTime := maxWaitDur
	var attempt int64
	for {
		attempt++
		start := time.Now()
		out, err := r.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
			SchemaVersionId: &id,
		})
		if err != nil {
			return nil, err
		}
		retryable := out.Status == types.SchemaVersionStatusPending
		if !retryable {
			return out, nil
		}

		remainingTime -= time.Since(start)
		if remainingTime < minDelay || remainingTime <= 0 {
			break
		}

		delay, err := smithywaiter.ComputeDelay(
			attempt, minDelay, maxDelay, remainingTime,
		)
		if err != nil {
			return nil, fmt.Errorf("error computing waiter delay, %w", err)
		}

		remainingTime -= delay
		// sleep for the delay amount before invoking a request
		if err := smithytime.SleepWithContext(ctx, delay); err != nil {
			return nil, fmt.Errorf("request cancelled while waiting, %w", err)
		}
	}
	return nil, fmt.Errorf("exceeded max wait time for GetSchemaVersion waiter")
}

func (r *Registry) getSchemaByDefinition(ctx context.Context, schema avro.Schema) (string, avro.Schema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for id, entry := range r.cache {
		if entry.schema.Fingerprint() == schema.Fingerprint() {
			return id, schema, nil
		}
	}

	def, _ := schema.(*avro.RecordSchema).MarshalJSON()
	out, err := r.client.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{
		SchemaDefinition: aws.String(string(def)),
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(r.registryName),
			SchemaName:   aws.String(schema.(*avro.RecordSchema).FullName()),
		},
	})
	if err != nil {
		return "", nil, err
	}

	id := aws.ToString(out.SchemaVersionId)
	if out.Status != types.SchemaVersionStatusAvailable {
		out, err := r.waitForSchema(ctx, id)
		if err != nil {
			return "", nil, fmt.Errorf("failed to get schema %v: %w", id, err)
		}
		if out.Status != types.SchemaVersionStatusAvailable {
			return "", nil, fmt.Errorf("failed to get schema: %v", id)
		}
	}

	if r.current != nil && r.current.schemaID != id {
		schema, err = r.compatibility.Resolve(r.current.schema, schema)
		if err != nil {
			return "", nil, err
		}
	}
	r.cache[id] = schemaEntry{
		schema:      schema.(*avro.RecordSchema),
		batchSchema: avro.NewArraySchema(schema),
		schemaID:    id,
	}

	return id, schema, nil
}

func (r *Registry) getSchema(ctx context.Context, id string) (string, *avro.RecordSchema, *avro.ArraySchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current != nil && r.current.schemaID == id {
		return id, r.current.schema, r.current.batchSchema, nil
	}

	if entry, ok := r.cache[id]; ok {
		return id, entry.schema, entry.batchSchema, nil
	}
	out, err := r.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaVersionId: aws.String(id),
	})
	if err != nil {
		return "", nil, nil, err
	}
	schema := avro.MustParse(aws.ToString(out.SchemaDefinition))
	if r.current != nil {
		schema, err = r.compatibility.Resolve(r.current.schema, schema)
		if err != nil {
			return "", nil, nil, err
		}
	}
	r.cache[id] = schemaEntry{
		schema:      schema.(*avro.RecordSchema),
		batchSchema: avro.NewArraySchema(schema),
		schemaID:    id,
	}

	return id, r.cache[id].schema, r.cache[id].batchSchema, nil
}

func (r *Registry) extractSchemaID(data []byte) (string, []byte, error) {
	if len(data) < 18 {
		return "", nil, fmt.Errorf("%w: data too short", registry.ErrInvalidDataWireFormat)
	}

	if data[0] != 3 {
		return "", nil, fmt.Errorf("%w: invalid first byte", registry.ErrInvalidDataWireFormat)
	}

	if data[1] != 0 {
		return "", nil, fmt.Errorf("%w: invalid second byte", registry.ErrInvalidDataWireFormat)
	}

	return uuid.UUID(data[2:18]).String(), data[18:], nil
}

func (r *Registry) appendSchemaID(data []byte, id string) ([]byte, error) {
	bid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("%w: %v (%v)", registry.ErrInvalidSchemaID, err, id)
	}
	if len(bid) > 16 {
		return nil, fmt.Errorf("%w: invalid length", registry.ErrInvalidSchemaID)
	}

	return append([]byte{3, 0}, append(bid[:], data...)...), nil
}
