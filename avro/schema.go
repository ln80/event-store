package avro

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"golang.org/x/crypto/sha3"

	sensitive "github.com/ln80/struct-sensitive"

	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/helper"
)

// SchemaMap presents a map of schemas
type SchemaMap map[string]avro.Schema

// UnpackEventSchemas expects to receive the schema of the event envelope
// and returns the wrapped event types' schemas.
//
// It fails if the given schema type and sub-types are not as expected
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

// EventSchemas generates schema for each given namespace and returns a map of schemas per namespace.
//
// In the case of an empty namespaces slice it registers all found namespace in the event registry.
func EventSchemas(a avro.API, namespaces []string) (SchemaMap, error) {
	if len(namespaces) == 0 {
		namespaces = event.NewRegister("").Namespaces()
	}

	currents := make(map[string]avro.Schema)
	for _, n := range namespaces {
		s, err := eventSchema(a, n)
		if err != nil {
			return nil, fmt.Errorf("failed to generate AVRO schema for '%s' events, err: %w", n, err)
		}

		// validate the generate schema fo the namespace event envelope
		b, _ := s.MarshalJSON()
		if _, err = avro.ParseBytes(b); err != nil {
			return nil, fmt.Errorf("generate invalid AVRO schema for '%s' events, err: %w", n, err)
		}

		currents[n] = s
	}

	return currents, nil
}

// registerEventTypes does register event types as AVRO type
func registerEventTypes(a avro.API, namespace string) {
	for _, entry := range event.NewRegister(namespace).All() {
		a.Register(entry.Name(), entry.Default())
		// make sure to accept both struct value and pointer as valid AVRO type
		// because some event serializers unmarshal events as a slice of type pointer.
		a.Register(entry.Name(), event.ToPtr(entry.Default()).Ptr)
	}
}

// eventSchema generate the AVRO schema of the given event
func eventSchema(a avro.API, namespace string) (*avro.RecordSchema, error) {
	schemas := make([]avro.Schema, 0)
	cache := make(seenCache)

	entries := event.NewRegister(namespace).All()
	if len(entries) == 0 {
		return nil, fmt.Errorf("events not found in '%s' registry", namespace)
	}

	registerEventTypes(a, namespace)

	c := map[string]any{}
	for _, entry := range event.NewRegister(namespace).All() {
		def, err := defaultOf(entry.Default())
		if err != nil {
			return nil, err
		}

		// aliases can be defined at the registry-level as well as at
		// the struct-level (using the blank "_" with tag)
		aliases := make([]string, 0)
		if as := entry.Property("aliases"); as != nil {
			aliases = append(aliases, as.([]string)...)
		}
		aliases = structTypeAliases(entry.Type(), aliases)
		aliases = helper.Unique(aliases)

		schema, err := schemaOf(entry.Type(), func(sc *schemaConfig) {
			sc.name = entry.Name()
			sc.namespace = namespace
			sc.def = def
			sc.aliases = aliases
			sc.cache = cache
			sc.isEvent = true
			sc.ctx = c
		})
		if err != nil {
			return nil, fmt.Errorf("event '%s': %w", entry.Name(), err)
		}

		// validate the generate schema mainly the default configuration
		data, err := a.Marshal(schema, map[string]any{})
		if err != nil {
			return nil, fmt.Errorf("event '%s' invalid generated defaults: %w", entry.Name(), err)
		}
		data2, err := a.Marshal(schema, entry.Default())
		if err != nil {
			return nil, fmt.Errorf("event '%s' invalid generated schema: %w", entry.Name(), err)
		}
		if !reflect.DeepEqual(data, data2) {
			return nil, fmt.Errorf("event '%s': invalid generated defaults", entry.Name())
		}

		schemas = append(schemas, schema)
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
		sc.root = true

		sc.ctx = c
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
	// name forces the name of the struct record schema.
	// It's mainly used in the case of root envelope.
	name string
	// typeRenames similarly to name it forces the renaming of the struct record schema
	// using the first element in the list. It adds the rest of elements as schema name aliases.
	// the rest of elements present a log of previous values given to 'typeRenames',
	// this behavior is required to ensure backward compatibility as any change to the event schema
	// is immutable at the model-level.
	//
	// Note that 'typeRenames' is ignored if 'name' is presents.
	// typeRenames []string
	// namespace of given to the struct record schema.
	namespace string
	// inject allows to inject a given schema directly for a specific set of fields.
	// mainly used in the case of the root envelope.
	inject map[string]avro.Schema
	// the avro tag key "avro" by default.
	// mainly used in the case of the root envelope.
	avroTagKey string
	// the default value to consider for the resolved schema.
	def any
	// aliases to add to the resolved schema.
	aliases []string
	// a cache context used mainly to deduplicate generated schemas.
	cache seenCache
	// isEvent a flag to identify record schemas that represent an event.
	// This flag is translated into a schema custom property.
	isEvent bool
	// root is a flag that separates the root schemas from the nested ones.
	root bool
	// ctx is used to collect details while walking through the struct tree.
	// ctx is mainly used to generate 'defaults' and 'sensitives' fingerprint fields at the envelope-level.
	ctx map[string]any
}

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

	// the basic schemaConfig to pass down to complex child fields
	childOpt := func(sc *schemaConfig) {
		sc.avroTagKey = cfg.avroTagKey
		sc.inject = cfg.inject
		sc.namespace = cfg.namespace
		sc.cache = cfg.cache
		sc.root = false
		sc.ctx = cfg.ctx
	}

	avroOpts := make([]avro.SchemaOption, 0)

	props := map[string]any{}
	if cfg.isEvent {
		props["isEvent"] = true
	}
	if len(props) > 0 {
		avroOpts = append(avroOpts, avro.WithProps(props))
	}

	if len(cfg.aliases) > 0 {
		avroOpts = append(avroOpts, avro.WithAliases(cfg.aliases))
	}

	primitiveSchema, err := schemaOfPrimitives(t, avroOpts)
	if err != nil {
		return nil, err
	}
	if primitiveSchema != nil {
		return primitiveSchema, nil
	}

	switch t.Kind() {
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
		return schemaOfStruct(t, avroOpts, cfg, childOpt)

	default:
		return nil, fmt.Errorf("event schema: unknown type %s %v", t.Kind().String(), t)
	}
}

var byteType = reflect.TypeFor[[]byte]()

// schemaOfPrimitives generates schema for primitives Go type.
// it returns nil schema if the type is not considered as primitive.
func schemaOfPrimitives(t reflect.Type, avroOpts []avro.SchemaOption) (avro.Schema, error) {
	if t == byteType {
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
		if strings.Contains(strings.ToLower(t.String()), "time.duration") {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros)), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, nil, avroOpts...), nil

	case reflect.Uint64:
		return avro.NewPrimitiveSchema(avro.Fixed, nil, avroOpts...), nil

	case reflect.Float32:
		return avro.NewPrimitiveSchema(avro.Float, nil, avroOpts...), nil

	case reflect.Float64:
		return avro.NewPrimitiveSchema(avro.Double, nil, avroOpts...), nil

	case reflect.Struct:
		if t.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis), avroOpts...), nil
		}
	}

	return nil, nil
}

// schemaOfStruct generate the schema of the give struct type
func schemaOfStruct(t reflect.Type, avroOpts []avro.SchemaOption, cfg *schemaConfig, childOpt func(*schemaConfig), opts ...func(*schemaConfig)) (avro.Schema, error) {
	var name string
	name = cfg.name
	// If the name is not provided as config then extract it from struct type name.
	if name == "" {
		// In the case of generic type do ignore the type param part
		name = strings.Split(t.String(), "[")[0]

		// In the case of namespace provided, do ensure the fullName validity
		// by removing the package name to replace with the given namespace
		if cfg.namespace != "" {
			parts := strings.Split(name, ".")
			name = parts[len(parts)-1]
		}
	}

	fullName := name
	if cfg.namespace != "" {
		fullName = cfg.namespace + "." + fullName
	}

	fields := make([]*avro.Field, 0)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		// extract event options from `ev:` tag
		_, evOpts := event.ParseTag(f.Tag)

		if f.Anonymous {
			ft := f.Type

			// TBD: in the case of anonymous struct pointer field
			// do not follow the logic of nullable union.
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				// the resolve the schema of the nested struct using the same default value as parent
				anonymousSchema, err := schemaOf(ft, childOpt, func(sc *schemaConfig) {
					sc.def = cfg.def
					sc.ctx = cfg.ctx
					sc.cache = cfg.cache
				})
				if err != nil {
					return nil, err
				}

				switch s := anonymousSchema.(type) {

				// In the case of a record do simply append fields to the parent schema
				case *avro.RecordSchema:
					fields = append(fields, s.Fields()...)

				// In the case of RefSchema make sure to append fields to the parent
				// while adjusting default values.
				//
				// Reminder: facing a RefSchema mains that the schema was resolved based on a previous event
				// and added to cache. Per consequences, defaults belongs to another event.
				case *avro.RefSchema:
					for _, rf := range s.Schema().(*avro.RecordSchema).Fields() {
						refFieldOpts := append(make([]avro.SchemaOption, 0), avro.WithAliases(rf.Aliases()), avro.WithOrder(rf.Order()), avro.WithProps(rf.Props()))

						// Override the field default value extracted from ref if parent has any.
						var fieldDef any
						if rf.HasDefault() {
							fieldDef = rf.Default()
						}
						def, found := fieldDefault(cfg.def, rf.Name())
						if found {
							fieldDef = def
						}
						refFieldOpts = append(refFieldOpts, avro.WithDefault(fieldDef))

						field, err := avro.NewField(rf.Name(), deduplicateType(cfg.cache, rf.Type()), refFieldOpts...)
						if err != nil {
							return nil, err
						}
						fields = append(fields, field)
					}
				default:
					return nil, errors.New("invalid anonymous field AVRO schema type")
				}
				continue
			}

			return nil, errors.New("invalid anonymous field type. only struct or struct pointer are supported")
		}

		fieldName := f.Name

		// avro tag name is only considered at in the case of event envelope
		if cfg.root && !cfg.isEvent {
			if tag, ok := f.Tag.Lookup(cfg.avroTagKey); ok {
				fieldName = tag
			}
		}

		var fieldSchema avro.Schema

		// limit the 'inject' capability to the root (the event envelope) as I'm not sure
		// about the drawbacks in general cases
		if cfg.root && !cfg.isEvent {
			if inject, ok := evOpts["inject"]; ok && len(inject) > 0 && inject[0] != "" {
				if sch, ok := cfg.inject[inject[0]]; ok {
					fieldSchema = sch
				}
			}
		}

		// Do extract the field default value from the parent.
		// His is required in the case of a field of type struct
		// which is resolved as a nested record schema that requires a default as well.
		//
		// That said, default values are defined and duplicated at multiple levels.
		fieldDef, fieldDefFound := fieldDefault(cfg.def, fieldName)

		// resolve field schema
		if fieldSchema == nil {
			var err error
			fieldSchema, err = schemaOf(f.Type, childOpt, func(sc *schemaConfig) {
				// resolve field type aliases from a blank field defined at the field struct-level.
				// Note that this is different from field name direct aliases.
				if typeAliases := structTypeAliases(f.Type, nil); len(typeAliases) > 0 {
					sc.aliases = typeAliases
				}

				// in the case 'renames' option which forces the renaming of the type,
				// do append 'renames' older values and the original name as aliases as well.
				// the first renames value will be used as the schema type name.
				// if renames, ok := evOpts["renames"]; ok {
				// 	sc.typeRenames = renames
				// 	sc.aliases = append(sc.aliases, renames[1:]...)
				// }

				if fieldDefFound {
					sc.def = fieldDef
				}
			})
			if err != nil {
				return nil, err
			}
		}

		avroFieldOpts := make([]avro.SchemaOption, 0)

		if fieldDef != nil {
			avroFieldOpts = append(avroFieldOpts, avro.WithDefault(fieldDef))
			// append the value to context fingerprint
			if cfg.ctx != nil && !cfg.root {
				appendDefaultToFingerprint(cfg.ctx, fieldDef)
			}
		}

		// only accept a nil default field value in case of a nullable union type.
		if fieldDef == nil {
			if fieldSchema.Type() == avro.Union && fieldSchema.(*avro.UnionSchema).Types()[0].Type() == avro.Null {
				avroFieldOpts = append(avroFieldOpts, avro.WithDefault(fieldDef))
				if cfg.ctx != nil && !cfg.root {
					appendDefaultToFingerprint(cfg.ctx, fieldDef)
				}
			}
		}

		// handle field name aliases
		if aliases, ok := evOpts["aliases"]; ok && len(aliases) > 0 {
			avroFieldOpts = append(avroFieldOpts, avro.WithAliases(aliases))
		}

		tagPayload := sensitive.ParseTag(f.Tag)
		if tagPayload != nil {
			avroFieldOpts = append(avroFieldOpts, avro.WithProps(map[string]any{
				"sensitive": tagPayload,
			}))
			if cfg.ctx != nil {
				appendSensitiveToFingerprint(cfg.ctx, f.Type.Name()+"."+f.Name+"."+tagPayload.Marshal())
			}
		}
		field, err := avro.NewField(fieldName, fieldSchema, avroFieldOpts...)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	if cfg.root {
		defF := sensitiveFingerprintToField(cfg.ctx)
		if defF != nil {
			fields = append(fields, defF)
		}

		defPII := defaultFingerprintToField(cfg.ctx)
		if defPII != nil {
			fields = append(fields, defPII)
		}
	}

	// if len(cfg.typeRenames) > 0 {
	// 	name = cfg.typeRenames[0]
	// }
	//
	if !cfg.root && cfg.cache != nil {
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
}

func appendDefaultToFingerprint(ctx map[string]any, def any) {
	v, ok := ctx["def"]
	if !ok {
		v = make([]byte, 0)
	}
	if m, ok := def.(map[string]any); ok {
		arr := make([][2]any, 0)
		for k, v := range m {
			arr = append(arr, [2]any{k, v})
		}
		slices.SortStableFunc(arr, func(a, b [2]any) int {
			return bytes.Compare([]byte(a[0].(string)), []byte(b[0].(string)))
		})
		def = arr
	}
	vv := v.([]byte)
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(def)
	vv = append(vv, buf.Bytes()...)

	ctx["def"] = vv
}

func appendSensitiveToFingerprint(ctx map[string]any, data string) {
	v, ok := ctx["sensitive"]
	if !ok {
		v = make([]string, 0)
	}
	vv := v.([]string)
	vv = append(vv, data)
	ctx["sensitive"] = vv
}

func defaultFingerprintToField(ctx map[string]any) *avro.Field {
	v, ok := ctx["def"]
	if !ok {
		return nil
	}
	vv := v.([]byte)
	slices.Sort(vv)

	hash := sha3.Sum256(vv)
	fingerprint := hex.EncodeToString(hash[:])
	fff, _ := avro.NewField("defxyz"+fingerprint, avro.NewPrimitiveSchema(avro.String, nil), avro.WithDefault(""))

	return fff
}

func sensitiveFingerprintToField(ctx map[string]any) *avro.Field {
	v, ok := ctx["sensitive"]
	if !ok {

		return nil
	}
	vv := v.([]string)
	slices.Sort(vv)

	hash := sha3.Sum256([]byte(strings.Join(vv, ".")))
	fingerprint := hex.EncodeToString(hash[:])
	fff, _ := avro.NewField("sensitivexyz"+fingerprint, avro.NewPrimitiveSchema(avro.String, nil), avro.WithDefault(""))

	return fff
}

// deduplicateType replaces a type (or subtype) with a ref if it already exists in the cache.
func deduplicateType(cache seenCache, schema avro.Schema) avro.Schema {
	if schema.Type() == avro.Ref {
		return schema
	}

	if schema.Type() == avro.Record {
		sch := schema.(*avro.RecordSchema)
		if s, ok := cache[sch.FullName()]; ok {
			return avro.NewRefSchema(s)
		}
		return sch
	}

	if schema.Type() == avro.Array {
		return avro.NewArraySchema(deduplicateType(cache, schema.(*avro.ArraySchema).Items()))
	}
	if schema.Type() == avro.Map {
		return avro.NewMapSchema(deduplicateType(cache, schema.(*avro.MapSchema).Values()))
	}

	if schema.Type() == avro.Union {
		schemas := make([]avro.Schema, 0)
		for _, s := range schema.(*avro.UnionSchema).Types() {
			schemas = append(schemas, deduplicateType(cache, s))
		}
		s, _ := avro.NewUnionSchema(schemas)
		return s
	}

	return schema
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

	for _, a := range evAls {
		found := false
		for _, aa := range aliases {
			if aa == a {
				found = true
				break
			}
		}
		if !found {
			aliases = append(aliases, a)
		}
	}

	return aliases
}
