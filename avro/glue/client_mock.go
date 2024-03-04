package glue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/glue"
)

type MockClientAPI struct {
	CreateSchemaFunc          func(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error)
	RegisterSchemaVersionFunc func(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error)
	GetSchemaVersionFunc      func(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
	GetSchemaByDefinitionFunc func(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error)
	ListSchemasFunc           func(ctx context.Context, params *glue.ListSchemasInput, optFns ...func(*glue.Options)) (*glue.ListSchemasOutput, error)
	ListSchemaVersionsFunc    func(ctx context.Context, params *glue.ListSchemaVersionsInput, optFns ...func(*glue.Options)) (*glue.ListSchemaVersionsOutput, error)
}

func (m *MockClientAPI) CreateSchema(ctx context.Context, params *glue.CreateSchemaInput, optFns ...func(*glue.Options)) (*glue.CreateSchemaOutput, error) {
	return m.CreateSchemaFunc(ctx, params, optFns...)
}

func (m *MockClientAPI) RegisterSchemaVersion(ctx context.Context, params *glue.RegisterSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.RegisterSchemaVersionOutput, error) {
	return m.RegisterSchemaVersionFunc(ctx, params, optFns...)
}

func (m *MockClientAPI) GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	return m.GetSchemaVersionFunc(ctx, params, optFns...)
}

func (m *MockClientAPI) GetSchemaByDefinition(ctx context.Context, params *glue.GetSchemaByDefinitionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaByDefinitionOutput, error) {
	return m.GetSchemaByDefinitionFunc(ctx, params, optFns...)
}

func (m *MockClientAPI) ListSchemaVersions(ctx context.Context, params *glue.ListSchemaVersionsInput, optFns ...func(*glue.Options)) (*glue.ListSchemaVersionsOutput, error) {
	return m.ListSchemaVersionsFunc(ctx, params, optFns...)
}

func (m *MockClientAPI) ListSchemas(ctx context.Context, params *glue.ListSchemasInput, optFns ...func(*glue.Options)) (*glue.ListSchemasOutput, error) {
	return m.ListSchemasFunc(ctx, params, optFns...)
}

var _ ClientAPI = &MockClientAPI{}
