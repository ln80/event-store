// go:build integ

package main

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ln80/event-store/avro"
	"github.com/ln80/event-store/avro/glue"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/stack/elastic/utils"
	"github.com/ln80/event-store/testutil"
)

func init() {
	testutil.RegisterEvent("")
}

func TestIntegration(t *testing.T) {
	ctx := context.Background()

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

	output, err := cloudformation.NewFromConfig(cfg).DescribeStacks(ctx, &cloudformation.DescribeStacksInput{
		StackName: &stackName,
		// StackName: aws.String("elastic-event-store-integ-test-1"),
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

	// init dynamodb event store client

	serializer := avro.NewEventSerializer(
		ctx,
		glue.NewRegistry(
			registryName,
			utils.InitGlueClient(cfg),
		),
		func(esc *avro.EventSerializerConfig) {
			esc.Namespace = ""
		},
	)

	store := dynamodb.NewEventStore(utils.InitDynamodbClient(cfg), table, func(sc *dynamodb.StoreConfig) {
		sc.Serializer = serializer
	})

	// test event-logging use cases
	testutil.TestEventLoggingStore(t, ctx, store)

	// test event-sourcing use cases
	testutil.TestEventSourcingStore(t, ctx, store)

	// test replay the global stream use cases
	testutil.TestEventStreamer(t, ctx, store, func(opt *testutil.TestEventStreamerOptions) {
		// wait for global stream indexing (asynchronous)
		opt.PostAppend = func(id event.StreamID) {
			time.Sleep(1 * time.Second)
		}
	})

	// previous tests, for sure, added events to different streams
	// the following test must receive messages from the integration test's SQS queue
	// retry logic allows to deal with the asynchronous nature of the publishing process
	if err := retry(2, time.Second, func() error {
		output, err := sqs.NewFromConfig(cfg).ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &queueUrl,
			MaxNumberOfMessages: 10,
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
				return errors.New("decode bas64 failed")
			}

			evt, err := serializer.UnmarshalEvent(ctx, b)
			if err != nil {
				return errors.New("unmarshal received event failed")
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
