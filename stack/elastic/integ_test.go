//go:build integ

package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/event"
	"github.com/ln80/event-store/stack/elastic/utils"
	"github.com/ln80/event-store/testutil"
	test_suite "github.com/ln80/event-store/testutil/suite"
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
	)
	if err != nil {
		t.Fatal(err)
	}

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
		table, queueUrl string
	)
	for _, output := range stack.Outputs {
		switch *output.OutputKey {
		case "EventTable":
			table = *output.OutputValue
		case "ConsumerQueueUrl":
			queueUrl = *output.OutputValue
		}
	}
	if table == "" || queueUrl == "" {
		t.Fatal("missed stack params", table, queueUrl)
	}
	t.Log("stack params", table, queueUrl)

	// init dynamodb event store client
	store := dynamodb.NewEventStore(utils.InitDynamodbClient(cfg), table)

	// test event-logging use cases
	test_suite.EventStoreTest(t, ctx, store)

	// test event-sourcing use cases
	test_suite.EventSourcingStoreTest(t, ctx, store)

	// test replay the global stream use cases
	test_suite.EventStreamerSuite(t, ctx, store, func(opt *test_suite.EventStreamerSuiteOptions) {
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
