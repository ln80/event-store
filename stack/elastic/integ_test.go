//go:build integ

package elastic

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-xray-sdk-go/instrumentation/awsv2"
	"github.com/aws/aws-xray-sdk-go/xray"
	es "github.com/ln80/event-store"
	"github.com/ln80/event-store/avro"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/internal/logger"
	"github.com/ln80/event-store/internal/testutil"
	"github.com/ln80/event-store/stack/elastic/shared"
)

func init() {
	testutil.RegisterEvent("")

	logger.SetDefault(logger.Discard())
}

func TestIntegration(t *testing.T) {
	ctx, err := xray.ContextWithConfig(
		context.Background(),
		xray.Config{
			DaemonAddr:     "127.0.0.1:2000", // default
			ServiceVersion: "3.3.10",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, seg := xray.BeginSegment(ctx, "Elastic::IntegrationTest")
	defer seg.Close(nil)

	logger.SetDefault(logger.New().WithValues("x-traceID", seg.TraceID))

	stackName := os.Getenv("STACK_NAME")
	if stackName == "" {
		t.Fatalf("missed env param stackName")
	}
	t.Log("env params", stackName)

	// init aws config
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("eu-west-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	awsv2.AWSV2Instrumentor(&cfg.APIOptions)

	output, err := cloudformation.NewFromConfig(cfg).DescribeStacks(ctx, &cloudformation.DescribeStacksInput{
		StackName: &stackName,
	})
	if err != nil {
		t.Fatalf("failed to describe stack: %v", err)
	}
	if len(output.Stacks) == 0 {
		t.Fatalf("Stack not found: %s\n", stackName)
	}
	stack := output.Stacks[0]
	var (
		table, queueUrl, registryName string
	)
	for _, output := range stack.Outputs {
		switch *output.OutputKey {
		case "EventTable":
			table = *output.OutputValue
		case "ConsumerQueueUrl":
			queueUrl = *output.OutputValue
		case "SchemaRegistryName":
			registryName = *output.OutputValue
		}
	}
	if table == "" || queueUrl == "" || registryName == "" {
		t.Fatal("missed stack params", table, queueUrl, registryName)
	}
	t.Log("stack params", table, queueUrl, registryName)

	serializer := avro.NewEventSerializer(
		ctx,
		avro.NewGlueRegistry(
			registryName,
			shared.InitGlueClient(cfg),
		),
		func(esc *avro.EventSerializerConfig) {
			esc.Namespace = ""
			esc.PersistCurrentSchema = true
		},
	)

	store := es.NewElasticStore(
		table,
		func(esc *es.ElasticStoreConfig) {
			esc.DynamodbClient = shared.InitDynamodbClient(cfg)
			esc.AddDynamodbStoreOption(func(sc *dynamodb.StoreConfig) {
				sc.Serializer = serializer
			})
		})

	testutil.TestEventLoggingStore(t, ctx, store)

	testutil.TestEventSourcingStore(t, ctx, store)

	testutil.TestEventStreamer(t, ctx, store, func(opt *testutil.TestEventStreamerOptions) {
		// wait for global stream indexing (asynchronous)
		opt.PostAppend = func(id event.StreamID) {
			time.Sleep(1 * time.Second)
		}
	})

	// assert that a sample of events were forwarded to the queue.
	if err := retry(2, time.Second, func() error {
		output, err := sqs.NewFromConfig(cfg).ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &queueUrl,
			MaxNumberOfMessages: 10,
			// AttributeNames: []types.QueueAttributeName{
			// 	"AWSTraceHeader",
			// },
		})

		if err != nil {
			t.Fatalf("failed to receive messages: %v", err)
		}
		if len(output.Messages) == 0 {
			return errors.New("empty messages result")
		}

		t.Logf("%d message received", len(output.Messages))

		for _, msg := range output.Messages {
			b, err := base64.StdEncoding.DecodeString(*msg.Body)
			if err != nil {
				return fmt.Errorf("decode bas64 failed: %v", err)
			}
			evt, err := serializer.UnmarshalEvent(ctx, b)
			if err != nil {
				return fmt.Errorf("unmarshal received event failed: %v", err)
			}
			t.Logf("received evt %+v", testutil.FormatEnv(evt))
		}

		return nil
	}); err != nil {
		t.Fatalf("expect err be nil, got: %v", err)
	}
}

func retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {
		if attempts--; attempts > 0 {
			time.Sleep(sleep)
			return retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}
