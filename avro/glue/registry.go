package glue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithytime "github.com/aws/smithy-go/time"
	smithywaiter "github.com/aws/smithy-go/waiter"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
)

var (
	ErrInvalidSchemaID       = errors.New("invalid schema ID")
	ErrInvalidDataWireFormat = errors.New("invalid data wire format")
)

type schemaEntry struct {
	schema      *avro.RecordSchema
	batchSchema *avro.ArraySchema
	schemaID    string
}

type Registry struct {
	registryName string
	schemaName   string
	client       *glue.Client

	api           avro.API
	compatibility *avro.SchemaCompatibility

	current schemaEntry
	cache   map[string]schemaEntry

	mu sync.RWMutex
}

func NewRegistry(registryName, schemaName string, client *glue.Client) *Registry {
	reg := &Registry{
		registryName:  registryName,
		schemaName:    schemaName,
		compatibility: avro.NewSchemaCompatibility(),
		client:        client,
		cache:         make(map[string]schemaEntry),
		api:           avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze(),
	}

	return reg
}

func (r *Registry) Setup(ctx context.Context, schema avro.Schema) error {
	if r.current.schema != nil && r.current.schemaID != "" {
		return nil
	}

	id, _, err := r.getSchemaByDefinition(ctx, schema)
	if err != nil {
		var tce *types.EntityNotFoundException
		if errors.As(err, &tce) {
			id, err = r.updateSchema(ctx, schema)
			if err != nil {
				var tce *types.EntityNotFoundException
				if errors.As(err, &tce) {
					id, err = r.createSchema(ctx, schema)
					if err != nil {
						return err
					}
				} else {
					return err
				}
			}
		} else {
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.current.schema = schema.(*avro.RecordSchema)
	r.current.schemaID = id
	r.current.batchSchema = avro.NewArraySchema(r.current.schema)

	return nil
}

func (r *Registry) Marshal(ctx context.Context, v any) ([]byte, error) {
	b, err := r.api.Marshal(r.current.schema, v)
	if err != nil {
		return nil, err
	}
	b, err = r.appendSchemaID(b, r.current.schemaID)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (r *Registry) Client() avro.API {
	return r.api
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

	return nil
}

func (r *Registry) MarshalBatch(ctx context.Context, v any) ([]byte, error) {
	b, err := r.api.Marshal(r.current.batchSchema, v)
	if err != nil {
		return nil, err
	}
	b, err = r.appendSchemaID(b, r.current.schemaID)
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

	return nil
}

func (r *Registry) createSchema(ctx context.Context, schema avro.Schema) (string, error) {
	def, _ := schema.(*avro.RecordSchema).MarshalJSON()
	out, err := r.client.CreateSchema(ctx, &glue.CreateSchemaInput{
		DataFormat: types.DataFormatAvro,
		SchemaName: aws.String(r.schemaName),

		Compatibility: types.CompatibilityBackwardAll,
		// Description:      new(string),
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
			SchemaName:   aws.String(r.schemaName),
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
			SchemaName:   aws.String(r.schemaName),
		},
	})
	if err != nil {
		return "", nil, err
	}

	id := aws.ToString(out.SchemaVersionId)
	if r.current.schema != nil && r.current.schemaID != id {
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

	if id == r.current.schemaID {
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

	schema, err := r.compatibility.Resolve(r.current.schema, avro.MustParse(aws.ToString(out.SchemaDefinition)))
	if err != nil {
		return "", nil, nil, err
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
		return "", nil, fmt.Errorf("%w: data too short", ErrInvalidDataWireFormat)
	}

	if data[0] != 3 {
		return "", nil, fmt.Errorf("%w: invalid first byte", ErrInvalidDataWireFormat)
	}

	if data[1] != 0 {
		return "", nil, fmt.Errorf("%w: invalid second byte", ErrInvalidDataWireFormat)
	}

	return uuid.UUID(data[2:18]).String(), data[18:], nil
}

func (r *Registry) appendSchemaID(data []byte, id string) ([]byte, error) {
	bid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSchemaID, err)
	}
	if len(bid) > 16 {
		return nil, fmt.Errorf("%w: invalid length", ErrInvalidSchemaID)
	}

	return append([]byte{3, 0}, append(bid[:], data...)...), nil
}
