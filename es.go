package es

import (
	"context"

	_ "unsafe"

	_ "github.com/ln80/event-store/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-logr/logr"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/event"
	event_sourcing "github.com/ln80/event-store/event/sourcing"
	"github.com/ln80/event-store/xray"
)

// EventStore interface combines the following low-level Event Store related interfaces:
//
// - event.Store: defines an event store that support event logging pattern;
//
// - sourcing.Store: defines an event store that support event sourcing pattern;
//
// - event.Streamer: defines an event store that support streaming event from the global stream;
type EventStore interface {
	event.Store
	event_sourcing.Store
	event.Streamer
}

// ElasticStoreConfig presents the config used by The Elastic Event Store client.
// It mainly serves as facade for the specific infra implementation of the event store.
type ElasticStoreConfig struct {
	// LoadAWSConfig is used as fallback solution to get the needed AWS config
	// to initiate AWS service clients.
	LoadAWSConfig func() (aws.Config, error)

	// DynamodbClient presents the AWS SDK V2 client for Dynamodb service.
	// If not found the library tries to initiate it from the resolved AWS Config.
	DynamodbClient dynamodb.ClientAPI

	// DynamodbStoreOptions presents a set of functional options for
	// the dynamodb event store implementation.
	dynamodbStoreOptions []func(*dynamodb.StoreConfig)

	// EnableTracing is enabled by default. By default it enable X-Ray tracing
	// at dynamodb event store level.
	EnableTracing bool
}

func (esc *ElasticStoreConfig) AddDynamodbStoreOption(opt func(*dynamodb.StoreConfig)) {
	esc.dynamodbStoreOptions = append(esc.dynamodbStoreOptions, opt)
}

// NewElasticStore returns a client for the Elastic Event Store,
// which is built on Dynamodb, supporting event-sourcing, and event-logging patterns.
//
// If underlying infra SDK clients are not found in the config, it will attempt to initiate them
// using the default environment configuration.
//
// Note that tracing is enabled by default, and AWS X-Ray service is used under the hood.
func NewElasticStore(dynamodbTable string, opts ...func(*ElasticStoreConfig)) EventStore {
	cfg := ElasticStoreConfig{
		LoadAWSConfig: func() (aws.Config, error) {
			return config.LoadDefaultConfig(context.Background())
		},
		dynamodbStoreOptions: make([]func(*dynamodb.StoreConfig), 0),
		EnableTracing:        true,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&cfg)
	}
	if cfg.DynamodbClient == nil {
		cc, err := cfg.LoadAWSConfig()
		if err != nil {
			panic(`elastic store client: 
			missed dynamodb client or couldn't load AWS config, 
			err: ` + err.Error())
		}
		cfg.DynamodbClient = dynamodb.NewClient(cc)
	}
	if cfg.EnableTracing {
		cfg.AddDynamodbStoreOption(func(sc *dynamodb.StoreConfig) {
			sc.AppendEventOptions = append(sc.AppendEventOptions, xray.AppendEventWithTracing())
		})
	}

	cfg.AddDynamodbStoreOption(func(sc *dynamodb.StoreConfig) {
		sc.RecordSizeLimit = 250 * 1024 // 6KB safety margin
	})

	return dynamodb.NewEventStore(cfg.DynamodbClient, dynamodbTable, cfg.dynamodbStoreOptions...)
}

// SetDefaultLogger allows to override the internal default logger used by the library.
//
//go:linkname SetDefaultLogger github.com/ln80/event-store/logger.SetDefault
func SetDefaultLogger(log logr.Logger)

// DiscardLogger returns a mute logger. Pass the mute logger to 'SetDefaultLogger'
// to disable library internal logging.
//
//go:linkname DiscardLogger github.com/ln80/event-store/logger.Discard
func DiscardLogger() logr.Logger
