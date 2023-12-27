package glue

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
)

const schema0 = `{
	"type": "record",
	"name": "event",
	"fields" : [
		{"name": "type", "type": "string"},
		{
			"name": "data", 
			"type": [
				"null", 
				{ 
					"type": "record",
					"name": "eventA",
					"fields": [
						{"name": "a0", "type": "string"},
						{"name": "old", "type": "string"}
					]
				}
			]
		}
	]
}`

const schema = `{
	"type": "record",
	"name": "event",
	"fields" : [
		{"name": "type", "type": "string"},
		{
			"name": "data", 
			"type": [
				"null", 
				{ 
					"type": "record",
					"name": "eventA",
					"fields": [
						{"name": "a", "type": "string", "aliases":["a0"]}
					]
				},
				{ 
					"type": "record",
					"name": "eventB",
					"fields": [
						{"name": "b", "type": "string"}
					]
				}
			]
		}
	]
}`

type eventA struct {
	A string `avro:"a"`
}
type eventB struct {
	A string `avro:"b"`
}

type envelope struct {
	Type     string `avro:"type"`
	Data     any    `avro:"data"`
	schemaID string
}

func (e *envelope) SetAVROSchemaID(id string) {
	e.schemaID = id
}

func (e *envelope) AVROSchemaID() string {
	return e.schemaID
}

func TestRegistry_Setup(t *testing.T) {
	ctx := context.Background()
	name := "registryName"

	tcs := []struct {
		client ClientAPI
		ok     bool
		err    error
	}{
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return nil, &types.EntityNotFoundException{}
				},
				RegisterSchemaVersionFunc: func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
					return nil, &types.EntityNotFoundException{}
				},

				CreateSchemaFunc: func(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
					return &glue.CreateSchemaOutput{
						SchemaVersionId:     aws.String("schemaID"),
						SchemaVersionStatus: types.SchemaVersionStatusAvailable,
					}, nil
				},
			},
			ok: true,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return nil, &types.EntityNotFoundException{}
				},
				RegisterSchemaVersionFunc: func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
					return nil, &types.EntityNotFoundException{}
				},

				CreateSchemaFunc: func(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
					return &glue.CreateSchemaOutput{
						SchemaVersionId:     aws.String("schemaID"),
						SchemaVersionStatus: types.SchemaVersionStatusPending,
					}, nil
				},
				GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					return &glue.GetSchemaVersionOutput{
						Status:          types.SchemaVersionStatusAvailable,
						SchemaVersionId: aws.String("schemaID"),
					}, nil
				},
			},
			ok: true,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return &glue.GetSchemaByDefinitionOutput{
						SchemaVersionId: aws.String("schemaID"),
						Status:          types.SchemaVersionStatusAvailable,
					}, nil
				},
			},
			ok: true,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return &glue.GetSchemaByDefinitionOutput{
						SchemaVersionId: aws.String("schemaID"),
						Status:          types.SchemaVersionStatusPending,
					}, nil
				},
				GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					return &glue.GetSchemaVersionOutput{
						Status:          types.SchemaVersionStatusAvailable,
						SchemaVersionId: aws.String("schemaID"),
					}, nil
				},
			},
			ok: true,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return &glue.GetSchemaByDefinitionOutput{
						SchemaVersionId: aws.String("schemaID"),
						Status:          types.SchemaVersionStatusPending,
					}, nil
				},
				GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					return &glue.GetSchemaVersionOutput{
						Status:          types.SchemaVersionStatusFailure,
						SchemaVersionId: aws.String("schemaID"),
					}, nil
				},
			},
			ok:  false,
			err: registry.ErrUnableToSetupSchema,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return nil, &types.EntityNotFoundException{}
				},
				RegisterSchemaVersionFunc: func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
					return &glue.RegisterSchemaVersionOutput{
						SchemaVersionId: aws.String("schemaID"),
						Status:          types.SchemaVersionStatusAvailable,
					}, nil
				},
			},
			ok: true,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return nil, errors.New("unexpected error")
				},
				RegisterSchemaVersionFunc: func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
					return &glue.RegisterSchemaVersionOutput{
						SchemaVersionId: aws.String("schemaID"),
						Status:          types.SchemaVersionStatusAvailable,
					}, nil
				},
			},
			ok:  false,
			err: registry.ErrUnableToSetupSchema,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return nil, &types.EntityNotFoundException{}
				},
				RegisterSchemaVersionFunc: func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
					return &glue.RegisterSchemaVersionOutput{
						SchemaVersionId: aws.String("schemaID"),
						Status:          types.SchemaVersionStatusPending,
					}, nil
				},

				GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					return &glue.GetSchemaVersionOutput{
						Status:          types.SchemaVersionStatusAvailable,
						SchemaVersionId: aws.String("schemaID"),
					}, nil
				},
			},
			ok: true,
		},
		{
			client: &MockClientAPI{
				GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
					return nil, &types.EntityNotFoundException{}
				},
				RegisterSchemaVersionFunc: func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
					return &glue.RegisterSchemaVersionOutput{
						SchemaVersionId: aws.String("schemaID"),
						Status:          types.SchemaVersionStatusPending,
					}, nil
				},

				GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					return &glue.GetSchemaVersionOutput{
						Status:          types.SchemaVersionStatusFailure,
						SchemaVersionId: aws.String("schemaID"),
					}, nil
				},
			},
			ok:  false,
			err: registry.ErrCreateOrUpdateSchemaFailed,
		},
	}

	for i, tc := range tcs {
		t.Run("tc:"+strconv.Itoa(i), func(t *testing.T) {
			registry := NewRegistry(name, tc.client)
			err := registry.Setup(ctx, avro.MustParse(schema))
			if tc.ok {
				if err != nil {
					t.Fatal("expect err be nil, got", err)
				}
			} else {
				if err == nil {
					t.Fatal("expect err not to be nil")
				}

				if tc.err != nil {
					if !errors.Is(err, tc.err) {
						t.Fatalf("expect err to be %v, got %v", tc.err, err)
					}
				}
			}
		})
	}
}

func TestRegistry_MarshalUnmarshal(t *testing.T) {
	ctx := context.Background()
	name := "registryName"
	schemaID := uuid.New().String()

	client := &MockClientAPI{
		GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
			return nil, &types.EntityNotFoundException{}
		},
		RegisterSchemaVersionFunc: func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
			return nil, &types.EntityNotFoundException{}
		},

		CreateSchemaFunc: func(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
			return &glue.CreateSchemaOutput{
				SchemaVersionId:     aws.String(schemaID),
				SchemaVersionStatus: types.SchemaVersionStatusAvailable,
			}, nil
		},
	}

	registry := NewRegistry(name, client)

	registry.API().Register("eventA", eventA{})
	registry.API().Register("eventB", eventB{})

	err := registry.Setup(ctx, avro.MustParse(schema))
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	t.Run("simple", func(t *testing.T) {
		value := map[string]any{
			"type": "eventA",
			"data": eventA{A: "value a"},
		}
		b, err := registry.Marshal(ctx, value)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}

		result := map[string]any{}
		err = registry.Unmarshal(ctx, b, &result)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}
		if !reflect.DeepEqual(value, result) {
			t.Fatalf("expect %+v, %+v be equals", value, result)
		}
	})

	t.Run("batch", func(t *testing.T) {
		values := []map[string]any{
			{
				"type": "eventA",
				"data": eventA{A: "value a"},
			},
			{
				"type": "eventB",
				"data": eventA{A: "value b"},
			},
		}
		b, err := registry.MarshalBatch(ctx, values)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}

		result := []map[string]any{}
		err = registry.UnmarshalBatch(ctx, b, &result)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}
		if !reflect.DeepEqual(values, result) {
			t.Fatalf("expect %+v, %+v be equals", values, result)
		}
	})
}

func TestRegistry_MarshalUnmarshal_WithoutCurrent(t *testing.T) {
	ctx := context.Background()
	name := "registryName"

	t.Run("simple", func(t *testing.T) {
		client := &MockClientAPI{
			GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
				return &glue.GetSchemaVersionOutput{
					SchemaDefinition: aws.String(schema),
				}, nil
			},
		}

		reg := NewRegistry(name, client)

		schemaID := "358b785a-0c78-4e9c-8586-3ebb23bf7751"

		b := []byte{0x3, 0x0, 0x35, 0x8b, 0x78, 0x5a, 0xc, 0x78, 0x4e, 0x9c, 0x85, 0x86, 0x3e, 0xbb, 0x23, 0xbf, 0x77, 0x51, 0xc, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x41, 0x2, 0xe, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x20, 0x61}

		err := reg.Setup(ctx, nil)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}

		result := envelope{}
		err = reg.Unmarshal(ctx, b, &result)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}
		if want, got := schemaID, result.AVROSchemaID(); want != got {
			t.Fatalf("expect %s, %s be equals", want, got)
		}

		b2, err := reg.Marshal(ctx, result)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}
		if want, got := b, b2; !reflect.DeepEqual(want, got) {
			t.Fatalf("expect %s, %s be equals", want, got)
		}

		// envelop doesn't contain an embedded schemaID
		_, err = reg.Marshal(ctx, envelope{Type: "eventA", Data: eventA{}})
		if want, got := registry.ErrUnableToResolveSchema, err; !errors.Is(got, want) {
			t.Fatalf("expect %s, %s be equals", want, got)
		}
	})
	t.Run("batch", func(t *testing.T) {
		client := &MockClientAPI{
			GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
				return &glue.GetSchemaVersionOutput{
					SchemaDefinition: aws.String(schema),
				}, nil
			},
		}

		reg := NewRegistry(name, client)

		schemaID := "1c931318-7070-4b96-86f6-dd76a6a2191e"
		b := []byte{0x3, 0x0, 0x1c, 0x93, 0x13, 0x18, 0x70, 0x70, 0x4b, 0x96, 0x86, 0xf6, 0xdd, 0x76, 0xa6, 0xa2, 0x19, 0x1e, 0x3, 0x40, 0xc, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x41, 0x2, 0xe, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x20, 0x61, 0xc, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x42, 0x2, 0xe, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x20, 0x62, 0x0}

		// ignore current schema
		err := reg.Setup(ctx, nil)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}

		result := []envelope{}
		err = reg.UnmarshalBatch(ctx, b, &result)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}

		for _, e := range result {
			if want, got := schemaID, e.AVROSchemaID(); want != got {
				t.Fatalf("expect %s, %s be equals", want, got)
			}
		}

		b2, err := reg.MarshalBatch(ctx, result)
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}
		if want, got := b, b2; !reflect.DeepEqual(want, got) {
			t.Fatalf("expect %s, %s be equals", want, got)
		}

		_, err = reg.MarshalBatch(ctx, []envelope{{Type: "eventA", Data: eventA{}}, {Type: "eventB", Data: eventB{}}})
		if want, got := registry.ErrUnableToResolveSchema, err; !errors.Is(got, want) {
			t.Fatalf("expect %s, %s be equals", want, got)
		}
	})
}

func TestRegistry_ReadOnly(t *testing.T) {
	ctx := context.Background()
	name := "registryName"

	client := &MockClientAPI{
		GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
			return &glue.GetSchemaVersionOutput{
				SchemaDefinition: aws.String(schema),
			}, nil
		},
	}

	reg := NewRegistry(name, client)

	// schemaID := "1c931318-7070-4b96-86f6-dd76a6a2191e"
	b := []byte{0x3, 0x0, 0x1c, 0x93, 0x13, 0x18, 0x70, 0x70, 0x4b, 0x96, 0x86, 0xf6, 0xdd, 0x76, 0xa6, 0xa2, 0x19, 0x1e, 0x3, 0x40, 0xc, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x41, 0x2, 0xe, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x20, 0x61, 0xc, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x42, 0x2, 0xe, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x20, 0x62, 0x0}

	// ignore current schema
	err := reg.Setup(ctx, nil, func(rc *registry.RegistryConfig) {
		rc.ReadOnly = true
	}, nil)
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	result := []envelope{}
	err = reg.UnmarshalBatch(ctx, b, &result)
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	_, err = reg.MarshalBatch(ctx, result)

	if want, got := registry.ErrMarshalOperationUnsupported, err; !errors.Is(got, want) {
		t.Fatalf("expect %s, %s be equals", want, got)
	}
}

func TestRegistry_WithSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	name := "registryName"
	schema0ID := uuid.New().String()
	schemaID := uuid.New().String()

	// encode data using old schema
	reg0 := NewRegistry(name, &MockClientAPI{
		GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
			return &glue.GetSchemaByDefinitionOutput{
				SchemaVersionId: aws.String(schema0ID),
				Status:          types.SchemaVersionStatusAvailable,
			}, nil
		}},
	)
	err := reg0.Setup(ctx, avro.MustParse(schema0))
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}
	old := envelope{
		Type: "eventA",
		Data: map[string]any{
			"eventA": map[string]any{
				"a0":  "value a0",
				"old": "value old",
			},
		},
	}
	b, err := reg0.Marshal(ctx, old)
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	// prepare new registry with new schema version
	reg := NewRegistry(name, &MockClientAPI{
		GetSchemaByDefinitionFunc: func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
			return &glue.GetSchemaByDefinitionOutput{
				SchemaVersionId: aws.String(schemaID),
				Status:          types.SchemaVersionStatusAvailable,
			}, nil
		},
		GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
			return &glue.GetSchemaVersionOutput{
				SchemaDefinition: aws.String(schema0),
				Status:           types.SchemaVersionStatusAvailable,
			}, nil
		},
	})
	err = reg.Setup(ctx, avro.MustParse(schema))
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}
	reg.API().Register("eventA", eventA{})

	var result envelope
	err = reg.Unmarshal(ctx, b, &result)
	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	if want, got := old.Type, result.Type; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
	if want, got := old.Data.(map[string]any)["eventA"].(map[string]any)["a0"], result.Data.(eventA).A; want != got {
		t.Fatalf("expect %v, %v be equals", want, got)
	}
}
