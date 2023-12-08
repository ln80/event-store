package avro

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/event"
	"github.com/mitchellh/mapstructure"
)

// eventSchema returns the avro schema of the event defined in the avro package.
func eventSchema(a avro.API, namespace string) (avro.Schema, error) {
	schemas := []avro.Schema{avro.NewPrimitiveSchema(avro.Null, nil)}
	for t, entry := range event.NewRegister(namespace).All() {
		def := entry.Default()
		mapDef := map[string]any{}
		if !reflect.ValueOf(def).IsZero() {
			decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
				TagName: "avro",
				Result:  &mapDef,
			})
			if err != nil {
				return nil, err
			}
			err = decoder.Decode(def)
			if err != nil {
				return nil, err
			}
		}
		sch, err := schemaOf(entry.Type(), func(sc *schemaConfig) {
			sc.name = t
			if len(mapDef) > 0 {
				sc.def = mapDef
			}
			if aliases := entry.Property("aliases"); aliases != nil {
				sc.aliases = aliases.([]string)
			}
		})
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, sch)

		a.Register(t, reflect.New(entry.Type()).Interface())
	}

	unionSch, err := avro.NewUnionSchema(schemas)
	if err != nil {
		return nil, err
	}

	return schemaOf(reflect.TypeOf(avroEvent{}), func(sc *schemaConfig) {
		sc.inject["union"] = unionSch
		sc.namespace = namespace
		if sc.namespace == "" {
			sc.namespace = "global"
		}
		sc.name = "events"
	})
}

type schemaConfig struct {
	name       string
	namespace  string
	inject     map[string]avro.Schema
	avroTagKey string
	def        any
	aliases    []string
}

// schemaOf convert a golang type, mainly a struct, to an avro schema
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
	}

	avroOpts := make([]avro.SchemaOption, 0)
	if len(cfg.aliases) > 0 {
		avroOpts = append(avroOpts, avro.WithAliases(cfg.aliases))
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
			var fs avro.Schema
			if tag, ok := f.Tag.Lookup("schema"); ok {
				if sch, ok := cfg.inject[tag]; ok {
					fs = sch
				}
			}
			if fs == nil {
				var err error
				fs, err = schemaOf(f.Type, childOpt)
				if err != nil {
					return nil, err
				}
			}
			var fName string
			if tag, ok := f.Tag.Lookup(cfg.avroTagKey); ok {
				fName = tag
			} else {
				fName = f.Name
			}
			var fDef any
			if def, ok := cfg.def.(map[string]any); ok {
				if d, ok := def[fName]; ok && !reflect.ValueOf(d).IsZero() {
					fDef = d
				}
			}
			avroFieldOpts := make([]avro.SchemaOption, 0)

			var aliases []string
			if tag, ok := f.Tag.Lookup("aliases"); ok {
				aliases = strings.Split(tag, " ")
				if len(aliases) > 0 {
					avroFieldOpts = append(avroFieldOpts, avro.WithAliases(aliases))
				}
			}

			if fDef != nil {
				avroFieldOpts = append(avroFieldOpts, avro.WithDefault(fDef))
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
		return avro.NewRecordSchema(name, cfg.namespace, fields, avroOpts...)

	default:
		return nil, fmt.Errorf("unknown type %s %v", t.Kind().String(), t)
	}
}
