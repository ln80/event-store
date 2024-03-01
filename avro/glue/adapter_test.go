package glue

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
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
