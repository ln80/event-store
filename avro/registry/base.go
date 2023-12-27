package registry

import (
	"context"
	"errors"

	"github.com/hamba/avro/v2"
)

var (
	ErrInvalidSchemaID             = errors.New("invalid schema ID")
	ErrInvalidDataWireFormat       = errors.New("invalid data wire format")
	ErrMarshalOperationUnsupported = errors.New("marshal operation unsupported")
	ErrUnableToResolveSchema       = errors.New("unable to resolve schema")
	ErrUnableToSetupSchema         = errors.New("unable to setup schema")
	ErrCreateOrUpdateSchemaFailed  = errors.New("failed to create or update schema")
)

type RegistryConfig struct {
	ReadOnly bool
}

// Registry presents Avro schema registry used by event serializer to handle schema resolution.
type Registry interface {
	Setup(ctx context.Context, schema avro.Schema, opts ...func(*RegistryConfig)) error
	API() avro.API
	Marshal(ctx context.Context, v any) ([]byte, error)
	MarshalBatch(ctx context.Context, v any) ([]byte, error)
	Unmarshal(ctx context.Context, b []byte, v any) error
	UnmarshalBatch(ctx context.Context, b []byte, v any) error
}

type AVROSchemaGetter interface{ AVROSchemaID() string }

type AVROSchemaSetter interface{ SetAVROSchemaID(id string) }
