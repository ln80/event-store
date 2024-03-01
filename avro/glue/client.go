package glue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

type ClientAPI interface {
	CreateSchema(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error)
	RegisterSchemaVersion(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error)
	GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
	GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error)
	ListSchemas(ctx context.Context, params *glue.ListSchemasInput, optFns ...func(*glue.Options)) (*glue.ListSchemasOutput, error)
	ListSchemaVersions(ctx context.Context, params *glue.ListSchemaVersionsInput, optFns ...func(*glue.Options)) (*glue.ListSchemaVersionsOutput, error)
}

var _ ClientAPI = glue.New(glue.Options{})

func NewClient(cfg aws.Config) ClientAPI {
	return glue.NewFromConfig(cfg)
}
