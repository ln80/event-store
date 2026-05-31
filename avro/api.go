package avro

import "github.com/ln80/avro/v2"

func NewAPI() avro.API {
	return avro.Config{PartialUnionTypeResolution: true, UnionResolutionError: false}.Freeze()
}

func NewCompatibilityAPI() *avro.SchemaCompatibility {
	return avro.NewSchemaCompatibility()
}
