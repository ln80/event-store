package tool_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	avro_fs "github.com/ln80/event-store/avro/fs"
	"github.com/ln80/event-store/avro/glue"
	"github.com/ln80/event-store/avro/registry"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/tool"
	"github.com/ln80/event-store/tool/internal"
)

func TestXxx(t *testing.T) {

	ctx := context.Background()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	cfg, err := config.LoadDefaultConfig(
		ctx,
	)
	if err != nil {
		log.Fatal(err, "failed to load AWS config")
	}

	svc := glue.NewAdapter(
		"local-test-registry",
		glue.NewClient(cfg),
	)

	var (
		walker    registry.Walker    = svc
		persister registry.Persister = svc
		fetcher   registry.Fetcher   = svc
	)

	p := tool.NewPalette()

	p.SetPrinter(&internal.DefaultPrinter{Output: os.Stdout, Err: os.Stdout})

	type AccountCreated3 struct {
		_      [0]string `ev:",aliases=AccountCreated AccountCreated2"`
		ID     string
		Desc   string
		Status string
		Titre  string
		X      string
	}

	event.NewRegister("accounting").Set(AccountCreated3{
		Status: "pending",
	})

	var dirPersister registry.Persister = avro_fs.NewDirAdapter("./repo2")

	_ = p.
		Avro().
		GenerateSchemas("accounting").
		CheckCompatibility(walker).
		PersistSchemas(fetcher, persister).
		EmbedSchemas(walker, dirPersister, "./repo2", "").
		Execute(ctx)
}
