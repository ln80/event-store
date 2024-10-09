package fs

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	avro "github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
)

type Adapter struct {
	fs  fs.FS
	dir string
}

func NewAdapter(f fs.FS, dir string) *Adapter {
	a := &Adapter{
		fs:  f,
		dir: dir,
	}

	return a
}

func NewDirAdapter(dir string) *Adapter {
	return NewAdapter(os.DirFS(dir), dir)
}

func parseSchemaPath(path string) (version int64, namespace, id string, err error) {
	splits := strings.Split(path, "/")
	if len(splits) < 2 {
		err = fmt.Errorf("invalid path: '%s'", path)
		return
	}
	namespace = strings.TrimSpace(splits[len(splits)-2])

	splits = strings.Split(splits[len(splits)-1], "@")
	version, _ = strconv.ParseInt(splits[0], 10, 64)
	id = strings.TrimSpace(strings.ReplaceAll(splits[1], ".json", ""))
	return
}

// Persist implements registry.Persister
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
				// sort.Strings(matches)
				// latest := matches[len(matches)-1]

				// var prev int64
				// prev, _, _, err = parseSchemaPath(latest)
				// if err != nil {
				// 	return
				// }

				// version = int(prev) + 1
				var prev int64 = 0
				var latest string = ""
				for _, path := range matches {
					var v int64
					v, _, _, err = parseSchemaPath(path)
					if err != nil {
						return
					}
					if v > prev {
						prev = v
						latest = path
					}
				}
				prev, _, _, err = parseSchemaPath(latest)
				if err != nil {
					return
				}

				version = int(prev) + 1
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

// Get implements registry.Fetcher.
func (f *Adapter) Get(ctx context.Context, id string) (string, error) {
	matches, err := fs.Glob(f.fs, "*/*@"+id+".json")
	if err != nil {
		return "", err
	}

	if len(matches) == 0 {
		return "", fmt.Errorf("%w: %s", registry.ErrSchemaNotFound, id)
	}

	file, err := f.fs.Open(matches[0])
	if err != nil {
		return "", err
	}

	b, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// GetByDefinition implements registry.Fetcher.
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

// Walk implements registry.Walker.
func (f *Adapter) Walk(ctx context.Context, fn func(id string, version int64, latest bool, schema *avro.RecordSchema) error, opts ...func(*registry.WalkConfig)) (int, error) {
	matches, err := fs.Glob(f.fs, "*/*@*.json")
	if err != nil {
		return 0, err
	}
	sort.Strings(matches)

	cfg := &registry.WalkConfig{
		Namespaces: make([]string, 0),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	foundNamespaces := len(cfg.Namespaces) > 0
	filter := func(n string) bool {
		if !foundNamespaces {
			return true
		}
		for _, nn := range cfg.Namespaces {
			if nn == n {
				return true
			}
		}
		return false
	}
	n := 0
	l := len(matches)
	for i, path := range matches {
		version, namespace, id, err := parseSchemaPath(path)
		if err != nil {
			return 0, err
		}

		if !filter(namespace) {
			continue
		}

		file, err := f.fs.Open(path)
		if err != nil {
			return 0, err
		}
		b, err := io.ReadAll(file)
		if err != nil {
			return 0, err
		}
		sch, err := avro.ParseBytes(b)
		if err != nil {
			return 0, err
		}

		latest := true
		if i < l-1 {
			nextPath := matches[i+1]
			_, n, _, err := parseSchemaPath(nextPath)
			if err != nil {
				return 0, err
			}
			if n == namespace {
				latest = false
			}
		}
		if err := fn(id, version, latest, sch.(*avro.RecordSchema)); err != nil {
			return 0, err
		}

		n++
	}

	return n, nil
}

var _ registry.Walker = &Adapter{}

type WireFormatter struct{}

// AppendSchemaID implements registry.WireFormatter.
func (*WireFormatter) AppendSchemaID(data []byte, id string) ([]byte, error) {
	bid := []byte(id)

	if len(bid) > 16 {
		return nil, fmt.Errorf("%w: invalid length %d", registry.ErrInvalidSchemaID, len(bid))
	}

	return append(bid[:], data...), nil
}

// ExtractSchemaID implements registry.WireFormatter.
func (*WireFormatter) ExtractSchemaID(data []byte) (string, []byte, error) {
	if len(data) < 16 {
		return "", nil, fmt.Errorf("%w: data too short", registry.ErrInvalidDataWireFormat)
	}

	return string(data[0:16]), data[16:], nil
}

func NewWireFormatter() *WireFormatter {
	return &WireFormatter{}
}

var _ registry.WireFormatter = &WireFormatter{}
