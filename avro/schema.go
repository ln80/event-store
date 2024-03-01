package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/event"
)

type SchemaMap map[string]avro.Schema

func UnpackEventSchemas(schema *avro.RecordSchema) ([]*avro.RecordSchema, error) {
	schemas := make([]*avro.RecordSchema, 0)
	for _, f := range schema.Fields() {
		if f.Name() == "Data" {
			sc := f.Type().(*avro.UnionSchema)
			for _, t := range sc.Types() {
				if t.Type() == avro.Null {
					continue
				}
				tt, ok := t.(*avro.RecordSchema)
				if !ok {
					return nil, fmt.Errorf("event schema type must be a record, got %T", t)
				}
				schemas = append(schemas, tt)
			}
		}
	}

	return schemas, nil
}

func EventSchemas(a avro.API, namespaces []string) (SchemaMap, error) {
	if len(namespaces) == 0 {
		namespaces = event.NewRegister("").Namespaces()
	}

	currents := make(map[string]avro.Schema)
	for _, n := range namespaces {
		s, err := eventSchema(a, n)
		if err != nil {
			return nil, fmt.Errorf("failed to generate avro schema for '%s' events, err: %w", n, err)
		}
		currents[n] = s
	}

	return currents, nil
}

func eventSchema(a avro.API, namespace string) (*avro.RecordSchema, error) {
	schemas := make([]avro.Schema, 0)
	cache := make(seenCache)

	entries := event.NewRegister(namespace).All()
	if len(entries) == 0 {
		return nil, fmt.Errorf("events not found in '%s' registry", namespace)
	}
	for _, entry := range event.NewRegister(namespace).All() {
		mapDef := map[string]any{}
		b, err := json.Marshal(entry.Default())
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(b, &mapDef)
		if err != nil {
			return nil, err
		}
		aliases := make([]string, 0)
		if as := entry.Property("aliases"); as != nil {
			aliases = append(aliases, as.([]string)...)
		}
		aliases = structTypeAliases(entry.Type(), aliases)

		sch, err := schemaOf(entry.Type(), func(sc *schemaConfig) {
			sc.name = entry.Name()
			sc.namespace = namespace
			if len(mapDef) > 0 {
				sc.def = mapDef
			}
			sc.aliases = aliases
			sc.cache = cache
		})
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, sch)

		a.Register(entry.Name(), entry.Default())
	}
	schemas = append([]avro.Schema{avro.NewPrimitiveSchema(avro.Null, nil)}, schemas...)

	unionSch, err := avro.NewUnionSchema(schemas)
	if err != nil {
		return nil, err
	}

	sc, err := schemaOf(reflect.TypeOf(avroEvent{}), func(sc *schemaConfig) {
		sc.inject["union"] = unionSch
		sc.namespace = namespace
		if sc.namespace == "" {
			sc.namespace = "_"
		}
		sc.name = "events"
	})
	if err != nil {
		return nil, err
	}

	schema, ok := sc.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("event schema type must be a record, got %T", sc)
	}

	return schema, nil
}

type seenCache map[string]avro.NamedSchema

type schemaConfig struct {
	name       string
	namespace  string
	inject     map[string]avro.Schema
	avroTagKey string
	def        any
	aliases    []string
	cache      seenCache
}

var typeOfBytes = reflect.TypeOf([]byte(nil))

func schemaOf(t reflect.Type, opts ...func(*schemaConfig)) (avro.Schema, error) {
	cfg := &schemaConfig{
		inject:     make(map[string]avro.Schema),
		avroTagKey: "avro",
		def:        nil,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	childOpt := func(sc *schemaConfig) {
		sc.avroTagKey = cfg.avroTagKey
		sc.inject = cfg.inject
		sc.namespace = cfg.namespace
		sc.cache = cfg.cache
	}

	avroOpts := make([]avro.SchemaOption, 0)
	if len(cfg.aliases) > 0 {
		avroOpts = append(avroOpts, avro.WithAliases(cfg.aliases))
	}

	if t == typeOfBytes {
		return avro.NewPrimitiveSchema(avro.Bytes, nil, avroOpts...), nil
	}

	switch t.Kind() {
	case reflect.String:
		return avro.NewPrimitiveSchema(avro.String, nil, avroOpts...), nil

	case reflect.Bool:
		return avro.NewPrimitiveSchema(avro.Boolean, nil, avroOpts...), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return avro.NewPrimitiveSchema(avro.Int, nil, avroOpts...), nil

	case reflect.Int64, reflect.Uint32:
		if strings.Contains(strings.ToLower(t.String()), "duration") {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.Duration)), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, nil, avroOpts...), nil

	case reflect.Uint64:
		return avro.NewPrimitiveSchema(avro.Fixed, nil, avroOpts...), nil

	case reflect.Float32:
		return avro.NewPrimitiveSchema(avro.Float, nil, avroOpts...), nil

	case reflect.Float64:
		return avro.NewPrimitiveSchema(avro.Double, nil, avroOpts...), nil

	case reflect.Slice, reflect.Array:
		es, err := schemaOf(t.Elem(), childOpt)
		if err != nil {
			return nil, err
		}
		return avro.NewArraySchema(es, avroOpts...), nil

	case reflect.Map:
		es, err := schemaOf(t.Elem(), childOpt)
		if err != nil {
			return nil, err
		}
		return avro.NewMapSchema(es, avroOpts...), nil

	case reflect.Pointer:
		n := avro.NewPrimitiveSchema(avro.Null, nil)
		es, err := schemaOf(t.Elem(), childOpt)
		if err != nil {
			return nil, err
		}
		return avro.NewUnionSchema([]avro.Schema{n, es})

	case reflect.Struct:
		if t.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis), avroOpts...), nil
		}
		fields := make([]*avro.Field, 0)
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}

			if f.Anonymous {
				ft := f.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					as, err := schemaOf(ft, childOpt)
					if err != nil {
						return nil, err
					}
					switch s := as.(type) {
					case *avro.RecordSchema:
						fields = append(fields, s.Fields()...)
					case *avro.RefSchema:
						fields = append(fields, s.Schema().(*avro.RecordSchema).Fields()...)
					default:
						return nil, errors.New("invalid anonymous field type schema")
					}
					continue
				}

				return nil, errors.New("invalid anonymous field type")
			}

			evName, evOpts := event.ParseTag(f.Tag)

			var fs avro.Schema
			if inject, ok := evOpts["inject"]; ok && len(inject) > 0 && inject[0] != "" {
				if sch, ok := cfg.inject[inject[0]]; ok {
					fs = sch
				}
			}

			if fs == nil {
				aliases := structTypeAliases(f.Type, nil)
				var err error
				fs, err = schemaOf(f.Type, childOpt, func(sc *schemaConfig) {
					if len(aliases) > 0 {
						sc.aliases = aliases
					}
				})
				if err != nil {
					return nil, err
				}
			}

			var fName string
			if tag, ok := f.Tag.Lookup(cfg.avroTagKey); ok {
				fName = tag
			} else {
				if evName != "" {
					fName = evName
				} else {
					fName = f.Name
				}
			}

			avroFieldOpts := make([]avro.SchemaOption, 0)

			var fDef any
			if def, ok := cfg.def.(map[string]any); ok {
				d, ok := def[fName]
				if ok {
					fDef = d
				}
			}
			if fDef != nil {
				avroFieldOpts = append(avroFieldOpts, avro.WithDefault(fDef))
			}
			if aliases, ok := evOpts["aliases"]; ok {
				if len(aliases) > 0 {
					avroFieldOpts = append(avroFieldOpts, avro.WithAliases(aliases))
				}
			}
			ff, err := avro.NewField(fName, fs, avroFieldOpts...)
			if err != nil {
				return nil, err
			}
			fields = append(fields, ff)
		}

		var name string
		name = cfg.name
		if name == "" {
			name = strings.ReplaceAll(strings.Split(t.Name(), "[")[0], ".", "_")
		}

		fullName := name
		if cfg.namespace != "" {
			fullName = cfg.namespace + "." + fullName
		}

		if cfg.cache != nil {
			if s, ok := cfg.cache[fullName]; ok {
				return avro.NewRefSchema(s), nil
			}
		}

		sh, err := avro.NewRecordSchema(name, cfg.namespace, fields, avroOpts...)
		if err != nil {
			return nil, err
		}

		if cfg.cache != nil {
			cfg.cache[sh.FullName()] = sh
		}

		return sh, nil
	default:
		return nil, fmt.Errorf("event schema: unknown type %s %v", t.Kind().String(), t)
	}
}

func structTypeAliases(t reflect.Type, defAliases []string) []string {
	aliases := append([]string{}, defAliases...)
	if t.Kind() != reflect.Struct {
		return aliases
	}

	blankF, ok := t.FieldByName("_")
	if !ok {
		return aliases
	}
	_, evOpts := event.ParseTag(blankF.Tag)
	evAls, ok := evOpts["aliases"]
	if !ok {
		return aliases

	}

	dedup := strings.Join(aliases, ",")
	for _, a := range evAls {
		if strings.Contains(dedup, a) {
			continue
		}
		aliases = append(aliases, a)
	}

	return aliases
}
