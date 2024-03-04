package avro

import (
	"io/fs"

	avro_fs "github.com/ln80/event-store/avro/fs"
	avro_glue "github.com/ln80/event-store/avro/glue"

	avro_registry "github.com/ln80/event-store/avro/registry"
)

func NewFSRegistry(f fs.FS, dir string) *avro_registry.Registry {
	svc := avro_fs.NewAdapter(f, dir)
	return avro_registry.New(svc, svc, avro_fs.NewWireFormatter())
}

func NewGlueRegistry(registryName string, client avro_glue.ClientAPI) *avro_registry.Registry {
	svc := avro_glue.NewAdapter(registryName, client)
	return avro_registry.New(svc, svc, avro_glue.NewWireFormatter())
}
