package avro_test

import (
	"context"
	"os"
	"testing"

	"github.com/hamba/avro/v2"
	_avro "github.com/ln80/event-store/avro"
	"github.com/ln80/event-store/avro/fs"
	bank_v1 "github.com/ln80/event-store/avro/testdata/v1"
	bank_v2 "github.com/ln80/event-store/avro/testdata/v2"
	"github.com/ln80/event-store/event"
)

func TestInteg(t *testing.T) {
	ctx := context.Background()

	// Prepare event schema FS registry directory
	dirPath, err := os.MkdirTemp("testdata", "tmp_schemas")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dirPath)
	})
	dirFS := os.DirFS(dirPath)

	// Mimic schema evolution by using two separate package with the same name v1, v2.
	// Encode an event from package v1 and decode it using package v2.
	// Make sure the event schema evolution is handled as it should be
	bank_v1.Register()

	//
	ser_v1 := _avro.NewEventSerializer(ctx,
		_avro.NewFSRegistry(dirFS, func(fc *_avro.FSRegistryConfig) {
			fc.PersistDir = dirPath
		}),
		func(esc *_avro.EventSerializerConfig) {
			esc.PersistCurrentSchema = true
			esc.Namespace = "bank"
		})

	accountOpened_v1 := bank_v1.AccountOpened{
		Owner: bank_v1.User{
			ID:    "abc",
			Email: "tom.bill@example.com",
		},
	}
	envelope_v1 := event.Wrap(ctx, event.NewStreamID("bank"), []any{accountOpened_v1})[0]

	encoded, err := ser_v1.MarshalEvent(ctx, envelope_v1)
	if err != nil {
		t.Fatal(err)
	}
	// Upgrade event schemas by clearing the package v1 registry
	// and registering events from package v2.
	bank_v1.Clear()

	bank_v2.Register()
	defer bank_v2.Clear()

	ser_v2 := _avro.NewEventSerializer(
		ctx,
		_avro.NewFSRegistry(dirFS, func(fc *_avro.FSRegistryConfig) {
			fc.PersistDir = dirPath
		}),
		func(esc *_avro.EventSerializerConfig) {
			esc.PersistCurrentSchema = true
			esc.Namespace = "bank"
		},
	)

	// walk through schema folder and make sure we got two schema versions.
	schemaVersionCount := 0
	_, _ = fs.NewAdapter(dirFS, dirPath).Walk(ctx, func(id string, version int64, latest bool, schema *avro.RecordSchema) error {
		schemaVersionCount++
		return nil
	})
	if schemaVersionCount != 2 {
		t.Fatal("expect 2 schema versions, got", schemaVersionCount)
	}

	// unmarshal encoded event using the new version and make sure the event evolution is done accordingly.
	envelope_v2, err := ser_v2.UnmarshalEvent(ctx, encoded)
	if err != nil {
		t.Fatal(err)
	}

	accountOpened_v2 := envelope_v2.Event().(*bank_v2.AccountOpened)
	if want, got := accountOpened_v2.Client.Email, accountOpened_v1.Owner.Email; want != got {
		t.Fatalf("want %v, got %v", want, got)
	}
	if want, got := bank_v2.DEFAULT_BIRTH_DAY, accountOpened_v2.Client.BirthDay; want != got {
		t.Fatalf("want %v, got %v", want, got)
	}
}
