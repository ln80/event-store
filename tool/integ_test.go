//go:build integ

package tool_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/hamba/avro/v2"
	"github.com/ln80/event-store/avro/fs"
	"github.com/ln80/event-store/avro/glue"
	"github.com/ln80/event-store/avro/registry"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/tool"
)

func TestAvroTasks(t *testing.T) {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(
		ctx,
	)
	if err != nil {
		t.Fatal(err, "failed to load AWS config")
	}

	registryName := os.Getenv("SCHEMA_REGISTRY_NAME")
	if registryName == "" {
		t.Fatal(err, "'SCHEMA_REGISTRY_NAME' env var not found")
	}

	svc := glue.NewAdapter(
		registryName,
		glue.NewClient(cfg),
	)

	var (
		walker    registry.Walker    = svc
		persister registry.Persister = svc
		fetcher   registry.Fetcher   = svc
	)

	p := tool.NewPalette()

	p.SetPrinter(&tool.DefaultPrinter{Output: os.Stdout, Err: os.Stdout})

	type Event1 struct {
		ID   string
		Desc string
	}

	type Event2 struct {
		Name  string
		Title string
	}

	namespace := "example" + event.UID().String()
	reg := event.NewRegister(namespace)

	reg.
		Set(Event1{}).
		Set(Event2{})

	out := t.TempDir() + "/events"
	t.Log("temp dir: ", out)
	// out := "./testdata" + "/events"

	var dirPersister registry.Persister = fs.NewDirAdapter(out)

	avroJob := p.Avro()

	err = avroJob.
		// Generate current schemas based on the registered events in the event.Registry.
		GenerateSchemas().
		// Check the current schemas compatibility against all the previous registered versions
		CheckCompatibility(walker).
		// Persist the current schemas in the remote registry
		PersistSchemas(fetcher, persister).
		// Fetch all schemas versions and embed them in the code (embed.FS).
		// Generate go types based on the latest version.
		EmbedSchemas(walker, dirPersister, out, "").
		// Execute the job.
		Execute(ctx)

	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	var embedWalker registry.Walker = fs.NewAdapter(os.DirFS(out), "")

	n, err := embedWalker.Walk(ctx, func(id string, version int64, latest bool, schema *avro.RecordSchema) error {
		return nil
	}, func(wc *registry.WalkConfig) {
		wc.Namespaces = append(wc.Namespaces, namespace)
	})

	if err != nil {
		t.Fatal("expect err be nil, got", err)
	}

	if n != 1 {
		t.Fatal("expect to walk through one schema, got", n)
	}
}
