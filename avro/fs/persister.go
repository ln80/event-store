package fs

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	avro "github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
)

func (p *Adapter) Persist(ctx context.Context, schema *avro.RecordSchema, opts ...func(*registry.PersistConfig)) (string, error) {
	if _, err := os.Stat(p.dir); os.IsNotExist(err) {
		return "", err
	}

	namespace := schema.Namespace()
	dir := p.dir + "/" + namespace

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return "", err
		}
	}

	cfg := &registry.PersistConfig{
		Resolver: func(schema *avro.RecordSchema) (id string, version int, err error) {
			fingerprint, _ := schema.FingerprintUsing(avro.CRC64Avro)
			id = hex.EncodeToString(fingerprint[:])
			version = 1

			var matches []string
			matches, err = fs.Glob(p.fs, schema.Namespace()+"/*@*"+".json")
			if err != nil {
				return
			}
			if len(matches) > 0 {
				sort.Strings(matches)
				latest := matches[len(matches)-1]
				splits := strings.Split(latest, "/")
				splits = strings.Split(splits[len(splits)-1], "@")
				prev, _ := strconv.Atoi(splits[0])
				version = prev + 1
			}

			return
		},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	id, version, err := cfg.Resolver(schema)
	if err != nil {
		return "", err
	}

	path := filepath.Clean(filepath.Join(dir, strconv.Itoa(version)+"@"+id+".json"))
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	b, err := schema.MarshalJSON()
	if err != nil {
		return "", err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, b, "", "\t"); err != nil {
		return "", err
	}

	if _, err := file.Write(pretty.Bytes()); err != nil {
		return "", err
	}

	return id, nil
}

var _ registry.Persister = &Adapter{}
