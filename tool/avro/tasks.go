package avro_tool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/format"
	"html/template"
	"sort"
	"strings"

	_avro "github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/gen"
	"github.com/ln80/event-store/avro"
	"github.com/ln80/event-store/avro/registry"
	internal "github.com/ln80/event-store/tool/internal"
)

// Avro Schema Supported tasks
const (
	GenerateSchemas    = "GenerateSchemas"
	CheckCompatibility = "CheckCompatibility"
	PersistSchemas     = "PersistSchemas"
	EmbedSchemas       = "EmbedSchemas"
)

// score presents the order of tasks execution.
var score = map[string]int{
	GenerateSchemas:    0,
	CheckCompatibility: 1,
	PersistSchemas:     2,
	EmbedSchemas:       3,
}

type GenerateSchemasTask struct {
	internal.Task

	namespaces []string
	schemas    avro.SchemaMap
}

type CheckCompatibilityTask struct {
	internal.Task

	walker registry.Walker
}

type PersistSchemasTask struct {
	internal.Task

	persister registry.Persister
	fetcher   registry.Fetcher
}

type EmbedSchemasTask_EmbedDestination struct {
	Out    string
	Module string
}

type EmbedSchemasTask struct {
	internal.Task

	dest EmbedSchemasTask_EmbedDestination

	persister registry.Persister

	walker registry.Walker
}

type JobExecuter struct {
	tasks   []internal.Task
	done    map[string]bool
	printer internal.TaskPrinter
}

func NewJobExecuter(printer internal.TaskPrinter) *JobExecuter {
	return &JobExecuter{
		tasks:   make([]internal.Task, 0),
		done:    make(map[string]bool),
		printer: printer,
	}
}

func (e *JobExecuter) GenerateSchemas(namespaces ...string) *JobExecuter {
	if t := internal.TaskFrom[*GenerateSchemasTask](e.tasks); t != nil {
		return e
	}

	tt := &GenerateSchemasTask{
		Task:       internal.NewTask(GenerateSchemas),
		namespaces: append([]string{}, namespaces...),
		schemas:    make(avro.SchemaMap),
	}

	e.tasks = append(e.tasks, tt)

	return e
}

func (e *JobExecuter) CheckCompatibility(walker registry.Walker) *JobExecuter {
	if t := internal.TaskFrom[*CheckCompatibilityTask](e.tasks); t != nil {
		return e
	}

	tt := &CheckCompatibilityTask{
		Task:   internal.NewTask(CheckCompatibility),
		walker: walker,
	}
	e.tasks = append(e.tasks, tt)
	return e
}

func (e *JobExecuter) PersistSchemas(fetcher registry.Fetcher, persister registry.Persister) *JobExecuter {
	if t := internal.TaskFrom[*PersistSchemasTask](e.tasks); t != nil {
		return e
	}

	tt := &PersistSchemasTask{
		Task:      internal.NewTask(PersistSchemas),
		persister: persister,
		fetcher:   fetcher,
	}
	e.tasks = append(e.tasks, tt)
	return e
}
func (e *JobExecuter) EmbedSchemas(walker registry.Walker, persister registry.Persister, out, module string) *JobExecuter {
	if t := internal.TaskFrom[*EmbedSchemasTask](e.tasks); t != nil {
		return e
	}

	tt := &EmbedSchemasTask{
		Task:      internal.NewTask(EmbedSchemas),
		walker:    walker,
		persister: persister,
		dest: EmbedSchemasTask_EmbedDestination{
			Out:    out,
			Module: module,
		},
	}
	e.tasks = append(e.tasks, tt)
	return e
}

func (e *JobExecuter) Execute(ctx context.Context) error {
	sort.Slice(e.tasks, func(i, j int) bool {
		return score[e.tasks[i].Name()] <= score[e.tasks[j].Name()]
	})

	e.printer.Message("\nAbout to run:\n", nil)
	for i, t := range e.tasks {
		e.printer.Task(i+1, t)
	}
	e.printer.Message("\n", nil)

	for i, t := range e.tasks {
		t := t
		if e.done[t.Name()] {
			continue
		}

		e.printer.Task(i+1, t)

		e.printer.Message("Started...", nil)
		if err := e.executeTask(ctx, t); err != nil {
			e.printer.Error(err, &t)
			return err
		}
		e.printer.Message("Done\n\n", nil)
	}

	e.printer.Message("The job is done", nil)

	return nil
}

func (e *JobExecuter) executeTask(ctx context.Context, t internal.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("run task panicked: %s", r)
			return
		}
	}()

	for _, d := range t.DependsOn() {
		if _, ok := e.done[d]; !ok {
			err = fmt.Errorf("task %s depends on %s", t, d)
			return
		}
	}

	switch tt := t.(type) {
	case *GenerateSchemasTask:
		if err = tt.Run(ctx); err != nil {
			return
		}

	case *CheckCompatibilityTask:
		t0 := internal.TaskFrom[*GenerateSchemasTask](e.tasks)
		if err = tt.Run(ctx, t0.schemas); err != nil {
			return
		}

	case *PersistSchemasTask:
		t0 := internal.TaskFrom[*GenerateSchemasTask](e.tasks)
		if err = tt.Run(ctx, t0.schemas); err != nil {
			return
		}

	case *EmbedSchemasTask:
		if err = tt.Run(ctx); err != nil {
			return
		}
	}

	e.done[t.Name()] = true

	return
}

func (tt *GenerateSchemasTask) Run(ctx context.Context, deps ...any) error {
	curs, err := avro.EventSchemas(avro.NewAPI(), tt.namespaces)
	if err != nil {
		return err
	}
	if len(curs) > 0 {
		tt.schemas = curs
	}

	return nil
}

func (tt *CheckCompatibilityTask) Run(ctx context.Context, deps ...any) error {
	schemas := deps[0].(avro.SchemaMap)

	compat := avro.NewCompatibilityAPI()

	_, err := tt.walker.Walk(ctx, func(id string, version int64, latest bool, schema *_avro.RecordSchema) error {

		n := schema.Namespace()

		cur, ok := schemas[schema.Namespace()]
		if !ok {
			return nil
		}

		if err := compat.Compatible(cur, schema); err != nil {
			return fmt.Errorf("namespace: %s, incompatible schema with version: %d", n, version)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (tt *PersistSchemasTask) Run(ctx context.Context, deps ...any) error {
	schemas := deps[0].(avro.SchemaMap)

	for _, s := range schemas {
		_, err := tt.fetcher.GetByDefinition(ctx, s)
		if err != nil {
			if errors.Is(err, registry.ErrSchemaNotFound) {
				if _, err := tt.persister.Persist(ctx, s.(*_avro.RecordSchema)); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	return nil
}

func (tt *EmbedSchemasTask) Run(ctx context.Context, deps ...any) error {
	if err := internal.CheckDir(tt.dest.Out); err != nil {
		return err
	}

	splits := strings.Split(tt.dest.Out, "/")
	packageName := splits[len(splits)-1]

	if m := tt.dest.Module; m != "" && !strings.HasSuffix(m, packageName) {
		return fmt.Errorf("packageName '%s' must be the same as module suffix '%s'", packageName, m)
	}

	n, err := tt.walker.Walk(ctx, func(id string, version int64, latest bool, schema *_avro.RecordSchema) error {
		opt := func(pc *registry.PersistConfig) {
			pc.Resolver = func(_ *_avro.RecordSchema) (string, int, error) {
				return id, int(version), nil
			}
		}
		if _, err := tt.persister.Persist(ctx, schema, opt); err != nil {
			return err
		}

		if !latest {
			return nil
		}

		namespace := schema.Namespace()
		dir := tt.dest.Out + "/" + namespace

		// generate event types from the latest
		g := gen.NewGenerator(schema.Namespace(), nil, gen.WithFullName(false), gen.WithEncoders(false))
		types := make([]string, 0)

		schemas, err := avro.UnpackEventSchemas(schema)
		if err != nil {
			return err
		}
		for _, sc := range schemas {
			types = append(types, sc.Name())
			g.Parse(sc)
		}

		var buf bytes.Buffer
		if err = g.Write(&buf); err != nil {
			return err
		}
		pretty, err := format.Source(buf.Bytes())
		if err != nil {
			return err
		}
		if err := internal.WriteToFile(dir+"/"+"events.go", pretty); err != nil {
			return err
		}

		// register generated types in event registry
		data := struct {
			PackageName string
			Events      []string
		}{
			PackageName: namespace,
			Events:      types,
		}
		b, err := internal.RenderCode(registerEventTmpl, data)
		if err != nil {
			return err
		}
		if err := internal.WriteToFile(dir+"/"+"init.go", b); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	if n == 0 {
		return nil
	}

	// generate o code that exposes the embedded file system
	data := struct {
		PackageName string
	}{
		PackageName: packageName,
	}
	b, err := internal.RenderCode(embedSchemaTmpl, data)
	if err != nil {
		return err
	}
	if err := internal.WriteToFile(tt.dest.Out+"/"+"init.go", b); err != nil {
		return err
	}

	// setup go module
	if tt.dest.Module != "" {
		if err := internal.SetupModule(tt.dest.Module, tt.dest.Out); err != nil {
			return err
		}
	}

	return nil
}

var registerEventTmpl = template.Must(template.New("register-event-tmpl").Parse(`
/*
 * Code Generated by ln80/event-store. DO NOT EDIT.
 */
package {{.PackageName}}

import "github.com/ln80/event-store/event"

func init() { {{range .Events}}
		event.NewRegister("").Set({{ . }}{}){{end}}
}
`))

var embedSchemaTmpl = template.Must(template.New("embed-schema-tmpl").Parse(`
/*
 * Code Generated by ln80/event-store. DO NOT EDIT.
 */
package {{.PackageName}}

import "embed"

var (
	//go:embed */*.json
	schemas embed.FS
)

func EmbedFS() embed.FS {
	return schemas
}
`))
