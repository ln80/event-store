package avro

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
)

// eventSchema returns the avro schema of the event defined in the avro package.
func eventSchema[T any]() (avro.Schema, error) {
	return schemaOf(reflect.TypeOf(avroEvent[T]{}))
}

// eventBatchSchema returns the avro schema of a batch of events (aka slice of events)
func eventBatchSchema[T any]() (avro.Schema, error) {
	sch, err := eventSchema[T]()
	if err != nil {
		return nil, err
	}

	return avro.NewArraySchema(sch), nil
}

// schemaOf convert a golang type, mainly a struct, to an avro schema
func schemaOf(t reflect.Type) (avro.Schema, error) {
	switch t.Kind() {
	case reflect.String:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case reflect.Bool:
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return avro.NewPrimitiveSchema(avro.Int, nil), nil
	case reflect.Int64, reflect.Uint32:
		if strings.Contains(strings.ToLower(t.String()), "duration") {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.Duration)), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case reflect.Uint64:
		return avro.NewPrimitiveSchema(avro.Fixed, nil), nil
	case reflect.Float32:
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case reflect.Float64:
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case reflect.Slice, reflect.Array:
		es, err := schemaOf(t.Elem())
		if err != nil {
			return nil, err
		}
		return avro.NewArraySchema(es), nil
	case reflect.Map:
		es, err := schemaOf(t.Elem())
		if err != nil {
			return nil, err
		}
		return avro.NewMapSchema(es), nil
	case reflect.Pointer:
		n := avro.NewPrimitiveSchema(avro.Null, nil)
		es, err := schemaOf(t.Elem())
		if err != nil {
			return nil, err
		}
		return avro.NewUnionSchema([]avro.Schema{n, es})
	case reflect.Struct:
		if t.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis)), nil
		}
		fields := make([]*avro.Field, 0)
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			fs, err := schemaOf(f.Type)
			if err != nil {
				return nil, err
			}

			var fName string
			if tag, ok := f.Tag.Lookup("avro"); ok {
				fName = tag
			} else {
				fName = f.Name
			}
			// fName := f.Tag.Get("avro")
			// if fName == "" {
			// 	fName = f.Name
			// }
			ff, err := avro.NewField(fName, fs)
			if err != nil {
				return nil, err
			}
			fields = append(fields, ff)
		}

		// the generic struct type name has the following format structName[full/path/typeParameter]
		// which does not compatible with avro naming spec.
		name := strings.ReplaceAll(strings.Split(t.Name(), "[")[0], ".", "_")
		return avro.NewRecordSchema(name, "", fields)
	default:
		return nil, fmt.Errorf("unknown type %s %v", t.Kind().String(), t)
	}
}
