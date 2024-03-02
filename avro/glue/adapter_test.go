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
			err: registry.ErrUnableToSetupRegistry,
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
			err: registry.ErrUnableToSetupRegistry,
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
			svc := NewAdapter(name, tc.client)
			r := registry.New(svc, svc, NewWireFormatter())
			err := r.Setup(ctx, avro.MustParse(schema), func(rc *registry.RegistryConfig) {
				rc.PersistCurrent = true
			})
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

func TestWalker(t *testing.T) {
	ctx := context.Background()

	t.Run("with success", func(t *testing.T) {
		schemaID := "550e8400-e29b-41d4-a716-446655440000"
		client := &MockClientAPI{
			ListSchemasFunc: func(ctx context.Context, params *glue.ListSchemasInput, optFns ...func(*glue.Options)) (*glue.ListSchemasOutput, error) {
				return &glue.ListSchemasOutput{
					Schemas: []types.SchemaListItem{
						{
							SchemaName: aws.String("service1.events"),
						},
						{
							SchemaName: aws.String("service2.events"),
						},
					},
				}, nil
			},
			ListSchemaVersionsFunc: func(ctx context.Context, params *glue.ListSchemaVersionsInput, optFns ...func(*glue.Options)) (*glue.ListSchemaVersionsOutput, error) {
				return &glue.ListSchemaVersionsOutput{
					Schemas: []types.SchemaVersionListItem{
						{
							SchemaVersionId: aws.String(schemaID),
							Status:          types.SchemaVersionStatusAvailable,
							VersionNumber:   aws.Int64(1),
						},
					},
				}, nil
			},
			GetSchemaVersionFunc: func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
				return &glue.GetSchemaVersionOutput{
					SchemaDefinition: aws.String(schema),
					SchemaVersionId:  aws.String(schemaID),
					Status:           types.SchemaVersionStatusAvailable,
					VersionNumber:    aws.Int64(1),
				}, nil
			},
		}

		svc := NewAdapter("", client)

		n, err := svc.Walk(ctx, func(id string, number int64, latest bool, schema *avro.RecordSchema) error {
			return nil
		}, func(wc *registry.WalkConfig) {
			wc.Namespaces = append(wc.Namespaces, "service1")
		})
		if err != nil {
			t.Fatal("expect err be nil, got", err)
		}
		if want, got := 1, n; want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})

	t.Run("with error", func(t *testing.T) {
		infraErr := errors.New("infra error")
		client := &MockClientAPI{
			ListSchemasFunc: func(ctx context.Context, params *glue.ListSchemasInput, optFns ...func(*glue.Options)) (*glue.ListSchemasOutput, error) {
				return &glue.ListSchemasOutput{
					Schemas: []types.SchemaListItem{
						{
							SchemaName: aws.String("service1.events"),
						},
						{
							SchemaName: aws.String("service2.events"),
						},
					},
				}, nil
			},
			ListSchemaVersionsFunc: func(ctx context.Context, params *glue.ListSchemaVersionsInput, optFns ...func(*glue.Options)) (*glue.ListSchemaVersionsOutput, error) {
				return nil, infraErr
			},
		}

		svc := NewAdapter("", client)

		n, err := svc.Walk(ctx, func(id string, number int64, latest bool, schema *avro.RecordSchema) error {
			return nil
		}, func(wc *registry.WalkConfig) {
			wc.Namespaces = append(wc.Namespaces, "service1")
		})
		if !errors.Is(err, infraErr) {
			t.Fatalf("expect err be %v, got %v", infraErr, err)
		}
		if want, got := 0, n; want != got {
			t.Fatalf("expect %v, %v be equals", want, got)
		}
	})
}

func TestWireFormatter(t *testing.T) {
	wf := NewWireFormatter()

	b := []byte{0, 1, 2}
	tcs := []struct {
		ok bool
		id string
	}{
		{
			ok: false,
			id: "",
		},
		{
			ok: false,
			id: "too short",
		},
		{
			ok: true,
			id: uuid.NewString(),
		},
	}

	for i, tc := range tcs {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			bb, err := wf.AppendSchemaID(b, tc.id)
			if tc.ok {
				if err != nil {
					t.Fatal("expect err be nil, got", err)
				}
				id2, b2, err := wf.ExtractSchemaID(bb)
				if err != nil {
					t.Fatal("expect err be nil, got", err)
				}
				if want, got := tc.id, id2; want != got {
					t.Fatalf("expect %v,%v be equals", want, got)
				}
				if want, got := b, b2; !reflect.DeepEqual(want, got) {
					t.Fatalf("expect %v,%v be equals", want, got)
				}
			} else {
				if err == nil {
					t.Fatal("expect err be not nil, got nil")
				}
			}
		})
	}
}
