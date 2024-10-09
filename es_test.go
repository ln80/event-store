package es_test

import (
	"testing"

	es "github.com/ln80/event-store"
)

// func TestNewElasticStore(t *testing.T) {
// 	table := "table-name"
// 	var store es.EventStore

// 	func() {
// 		defer func() {
// 			if r := recover(); r == nil {
// 				t.Fatal("expect to panic")
// 			}
// 		}()
// 		_ = es.NewElasticStore(table, func(esc *es.ElasticStoreConfig) {
// 			esc.LoadAWSConfig = func() (aws.Config, error) {
// 				return aws.Config{}, errors.New("unexpected error")
// 			}
// 		})
// 	}()

// 	if store = es.NewElasticStore(table); store == nil {
// 		t.Fatal("expect store not be nil")
// 	}

// 	if store = es.NewElasticStore(table, func(esc *es.ElasticStoreConfig) {
// 		esc.LoadAWSConfig = func() (aws.Config, error) {
// 			return *aws.NewConfig(), nil
// 		}
// 	}); store == nil {
// 		t.Fatal("expect store not be nil")
// 	}

// 	if store = es.NewElasticStore(table, func(esc *es.ElasticStoreConfig) {
// 		esc.DynamodbClient = dynamodb.NewClient(*aws.NewConfig())
// 	}); store == nil {
// 		t.Fatal("expect store not be nil")
// 	}
// }

func TestLoggerBinding(t *testing.T) {
	// Capture regressions related to the binding of logging functions to
	// the internal logger package.
	// In such scenarios, the test will report a failure with the message:
	// "[build failed]"

	log := es.DiscardLogger()
	es.SetDefaultLogger(log)

}
