package avro

import (
	"io/fs"

	avro_fs "github.com/ln80/event-store/avro/fs"

	avro_registry "github.com/ln80/event-store/avro/registry"
)

type FSRegistryConfig struct {
	PersistDir    string
	WireFormatter avro_registry.WireFormatter
}

func NewFSRegistry(f fs.FS, opts ...func(*FSRegistryConfig)) *avro_registry.Registry {
	cfg := &FSRegistryConfig{
		PersistDir:    "",
		WireFormatter: avro_fs.NewWireFormatter(),
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt(cfg)
	}

	fetcher := avro_fs.NewAdapter(f, "")
	var persister avro_registry.Persister = nil
	if cfg.PersistDir != "" {
		persister = avro_fs.NewAdapter(f, cfg.PersistDir)
	}

	return avro_registry.New(fetcher, persister, cfg.WireFormatter)
}

// func NewGlueRegistry(registryName string, client avro_glue.ClientAPI) *avro_registry.Registry {
// 	svc := avro_glue.NewAdapter(registryName, client)
// 	return avro_registry.New(svc, svc, avro_glue.NewWireFormatter())
// }
