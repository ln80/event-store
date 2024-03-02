package glue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/uuid"
	avro "github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/registry"
)

type Adapter struct {
	registryName string
	client       ClientAPI
}

func NewAdapter(registryName string, client ClientAPI) *Adapter {
	a := &Adapter{
		registryName: registryName,
		client:       client,
	}

	return a
}

func (r *Adapter) List(ctx context.Context, namespaces []string) ([]string, error) {
	input := &glue.ListSchemasInput{
		MaxResults: aws.Int32(25),
		RegistryId: &types.RegistryId{
			RegistryName: aws.String(r.registryName),
		},
	}

	getToken := func(output *glue.ListSchemasOutput) *string {
		return output.NextToken
	}

	setToken := func(input *glue.ListSchemasInput, token *string) *glue.ListSchemasInput {
		input.NextToken = token

		return input
	}

	query := func(ctx context.Context, input *glue.ListSchemasInput) (*glue.ListSchemasOutput, error) {
		return r.client.ListSchemas(ctx, input)
	}

	result := make([]string, 0)

	p := newPaginator(
		input,
		getToken,
		setToken,
		query,
	)

	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, s := range out.Schemas {
			splits := strings.Split(aws.ToString(s.SchemaName), ".")
			if len(splits) != 2 || splits[1] != "events" || splits[0] == "" {
				continue
			}

			namespace := splits[0]

			found := len(namespaces) == 0
			for _, n := range namespaces {
				if n == namespace {
					found = true
					break
				}
			}
			if found {
				result = append(result, aws.ToString(s.SchemaName))
			}
		}
	}

	return result, nil
}

var _ registry.Walker = &Adapter{}

func (r *Adapter) Walk(ctx context.Context, fn func(id string, number int64, latest bool, schema *avro.RecordSchema) error, opts ...func(*registry.WalkConfig)) (int, error) {
	cfg := &registry.WalkConfig{
		Namespaces: make([]string, 0),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(cfg)
	}

	list, err := r.List(ctx, cfg.Namespaces)
	if err != nil {
		return 0, err
	}

	getToken := func(output *glue.ListSchemaVersionsOutput) *string {
		return output.NextToken
	}

	setToken := func(input *glue.ListSchemaVersionsInput, token *string) *glue.ListSchemaVersionsInput {
		input.NextToken = token

		return input
	}

	query := func(ctx context.Context, input *glue.ListSchemaVersionsInput) (*glue.ListSchemaVersionsOutput, error) {
		return r.client.ListSchemaVersions(ctx, input)
	}

	n := 0
	for _, name := range list {
		input := &glue.ListSchemaVersionsInput{
			SchemaId: &types.SchemaId{
				RegistryName: aws.String(r.registryName),
				SchemaName:   aws.String(name),
			},
			MaxResults: aws.Int32(50),
		}

		p := newPaginator(
			input,
			getToken,
			setToken,
			query,
		)

		schemas := make([]types.SchemaVersionListItem, 0)

		for p.HasMorePages() {
			out, err := p.NextPage(ctx)
			if err != nil {
				return 0, err
			}
			for _, v := range out.Schemas {
				if v.Status != types.SchemaVersionStatusAvailable {
					continue
				}
				schemas = append(schemas, v)
			}
		}

		sort.Slice(schemas, func(i, j int) bool {
			return *schemas[i].VersionNumber < *schemas[j].VersionNumber
		})

		l := len(schemas)
		for i, v := range schemas {
			id := aws.ToString(v.SchemaVersionId)

			schema, err := r.Get(ctx, id)
			if err != nil {
				return 0, err
			}
			number := aws.ToInt64(v.VersionNumber)

			if err := fn(id, number, i == l-1, avro.MustParse(schema).(*avro.RecordSchema)); err != nil {
				return 0, err
			}

			n++
		}
	}

	return n, nil
}

var _ registry.Fetcher = &Adapter{}

// Get implements avro.Fetcher.
func (f *Adapter) Get(ctx context.Context, id string) (string, error) {
	out, err := f.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaVersionId: aws.String(id),
	})
	if err != nil {
		return "", err
	}

	return aws.ToString(out.SchemaDefinition), nil
}

// GetByDefinition implements avro.Fetcher.
func (f *Adapter) GetByDefinition(ctx context.Context, schema avro.Schema) (string, error) {
	def, _ := schema.(*avro.RecordSchema).MarshalJSON()
	out, err := f.client.GetSchemaByDefinition(ctx, &glue.GetSchemaByDefinitionInput{
		SchemaDefinition: aws.String(string(def)),
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(f.registryName),
			SchemaName:   aws.String(schema.(*avro.RecordSchema).FullName()),
		},
	})
	if err != nil {
		var tce *types.EntityNotFoundException
		if errors.As(err, &tce) {
			err = fmt.Errorf("%w: %v", registry.ErrSchemaNotFound, err)
		}

		return "", err
	}

	// sch := avro.ParseBytes([]byte(aws.ToString(out.)))
	id := aws.ToString(out.SchemaVersionId)
	if out.Status != types.SchemaVersionStatusAvailable {
		get := func(ctx context.Context) (*glue.GetSchemaVersionOutput, error) {
			return f.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
				SchemaVersionId: &id,
			})
		}
		retryable := func(out *glue.GetSchemaVersionOutput) bool {
			return out.Status == types.SchemaVersionStatusPending
		}

		out, err := wait(ctx, get, retryable)
		if err != nil {
			return "", fmt.Errorf("failed to get schema %v: %w", id, err)
		}
		if out.Status != types.SchemaVersionStatusAvailable {
			return "", fmt.Errorf("failed to get schema: %v", id)
		}
	}

	return id, nil
}

var _ registry.Persister = &Adapter{}

// Persist implements avro.Persister.
func (p *Adapter) Persist(ctx context.Context, schema *avro.RecordSchema, opts ...func(*registry.PersistConfig)) (string, error) {
	id, err := p.updateSchema(ctx, schema)
	if err != nil {
		var tce *types.EntityNotFoundException
		if errors.As(err, &tce) {
			id, err = p.createSchema(ctx, schema)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}

	return id, err
}

func (p *Adapter) createSchema(ctx context.Context, schema avro.Schema) (string, error) {
	def, _ := schema.(*avro.RecordSchema).MarshalJSON()
	out, err := p.client.CreateSchema(ctx, &glue.CreateSchemaInput{
		DataFormat: types.DataFormatAvro,
		SchemaName: aws.String(schema.(*avro.RecordSchema).FullName()),

		Compatibility: types.CompatibilityBackwardAll,
		RegistryId: &types.RegistryId{
			RegistryName: aws.String(p.registryName),
		},
		SchemaDefinition: aws.String(string(def)),
		Tags:             map[string]string{},
	})
	if err != nil {
		return "", err
	}

	id := aws.ToString(out.SchemaVersionId)

	if out.SchemaVersionStatus != types.SchemaVersionStatusAvailable {

		get := func(ctx context.Context) (*glue.GetSchemaVersionOutput, error) {
			return p.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
				SchemaVersionId: &id,
			})
		}

		retryable := func(out *glue.GetSchemaVersionOutput) bool {
			return out.Status == types.SchemaVersionStatusPending
		}

		out, err := wait(ctx, get, retryable)
		if err != nil {
			return "", fmt.Errorf("failed to create schema %v: %w", id, err)
		}
		if out.Status != types.SchemaVersionStatusAvailable {
			return "", fmt.Errorf("failed to create schema: %v", id)
		}
	}
	return id, nil
}

func (p *Adapter) updateSchema(ctx context.Context, schema avro.Schema) (string, error) {
	def, _ := schema.(*avro.RecordSchema).MarshalJSON()
	out, err := p.client.RegisterSchemaVersion(ctx, &glue.RegisterSchemaVersionInput{
		SchemaDefinition: aws.String(string(def)),
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(p.registryName),
			SchemaName:   aws.String(schema.(*avro.RecordSchema).FullName()),
		},
	})
	if err != nil {
		return "", err
	}

	id := aws.ToString(out.SchemaVersionId)

	if out.Status != types.SchemaVersionStatusAvailable {
		get := func(ctx context.Context) (*glue.GetSchemaVersionOutput, error) {
			return p.client.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
				SchemaVersionId: &id,
			})
		}
		retryable := func(out *glue.GetSchemaVersionOutput) bool {
			return out.Status == types.SchemaVersionStatusPending
		}

		out, err := wait(ctx, get, retryable)
		if err != nil {
			return "", fmt.Errorf("failed to update schema %v: %w", id, err)
		}
		if out.Status != types.SchemaVersionStatusAvailable {
			return "", fmt.Errorf("failed to update schema %v: %s", id, out.Status)
		}
	}
	return id, nil
}

type WireFormatter struct{}

func NewWireFormatter() *WireFormatter {
	return &WireFormatter{}
}

var _ registry.WireFormatter = &WireFormatter{}

// AppendSchemaID implements registry.WireFormatter.
func (*WireFormatter) AppendSchemaID(data []byte, id string) ([]byte, error) {
	bid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("%w: %v (%v)", registry.ErrInvalidSchemaID, err, id)
	}
	if len(bid) > 16 {
		return nil, fmt.Errorf("%w: invalid length", registry.ErrInvalidSchemaID)
	}

	return append([]byte{3, 0}, append(bid[:], data...)...), nil
}

// ExtractSchemaID implements registry.WireFormatter.
func (*WireFormatter) ExtractSchemaID(data []byte) (string, []byte, error) {
	if len(data) < 18 {
		return "", nil, fmt.Errorf("%w: data too short", registry.ErrInvalidDataWireFormat)
	}

	if data[0] != 3 {
		return "", nil, fmt.Errorf("%w: invalid first byte", registry.ErrInvalidDataWireFormat)
	}

	if data[1] != 0 {
		return "", nil, fmt.Errorf("%w: invalid second byte", registry.ErrInvalidDataWireFormat)
	}

	return uuid.UUID(data[2:18]).String(), data[18:], nil
}
