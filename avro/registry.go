package avro

import (
	"context"

	"github.com/hamba/avro/v2"
	avro_glue "github.com/ln80/event-store/avro/glue"
	avro_memory "github.com/ln80/event-store/avro/glue"
)

// Registry presents Avro schema registry used by event serializer to handle schema resolution.
type Registry interface {
	Setup(ctx context.Context, schema avro.Schema) error
	Client() avro.API
	Marshal(ctx context.Context, v any) ([]byte, error)
	MarshalBatch(ctx context.Context, v any) ([]byte, error)
	Unmarshal(ctx context.Context, b []byte, v any) error
	UnmarshalBatch(ctx context.Context, b []byte, v any) error
}

var _ Registry = &avro_glue.Registry{}
var _ Registry = &avro_memory.Registry{}
