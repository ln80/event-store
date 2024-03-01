package fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"

	avro "github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
)

// Get implements avro.Fetcher.
func (f *Adapter) Get(ctx context.Context, id string) (string, error) {
	matches, err := fs.Glob(f.fs, "*/*@"+id+".json")
	if err != nil {
		return "", err
	}

	if len(matches) == 0 {
		return "", fmt.Errorf("%w: %s", registry.ErrSchemaNotFound, id)
	}

	return matches[0], nil
}

// GetByDefinition implements avro.Fetcher.
func (f *Adapter) GetByDefinition(ctx context.Context, schema avro.Schema) (string, error) {
	matches, err := fs.Glob(f.fs, "*/*@*.json")
	if err != nil {
		return "", err
	}

	if len(matches) == 0 {
		return "", registry.ErrSchemaNotFound
	}

	for _, path := range matches {
		file, err := f.fs.Open(path)
		if err != nil {
			return "", err
		}

		b, err := io.ReadAll(file)
		if err != nil {
			return "", err
		}
		sch, err := avro.ParseBytes(b)
		if err != nil {
			return "", err
		}

		if sch.Fingerprint() == schema.Fingerprint() {
			splits := strings.Split(path, "@")
			id := strings.Split(splits[len(splits)-1], ".json")[0]
			if len(id) == 0 {
				continue
			}

			return id, nil
		}

	}

	return "", registry.ErrSchemaNotFound
}
