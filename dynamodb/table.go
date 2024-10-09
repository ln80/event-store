package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	HashKey            string = "_pk"
	RangeKey           string = "_sk"
	TTLAttribute       string = "_ttl"
	LocalIndexRangeKey string = "_lsik"
	LocalIndex         string = "_lsi"
	TraceIDAttribute   string = "_traceID"
)

type Item struct {
	HashKey     string `dynamodbav:"_pk"`
	RangeKey    string `dynamodbav:"_sk"`
	LSIRangeKey string `dynamodbav:"_lsik,omitempty" localIndex:"_lsi,range"`
	TTL         int64  `dynamodbav:"_ttl,omitempty"`
	TraceID     string `dynamodbav:"_traceID,omitempty"`
}

func StoreCreateTableInput(table string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(HashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(RangeKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(LocalIndexRangeKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(HashKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(RangeKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(table),
		BillingMode: types.BillingModePayPerRequest,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(LocalIndex),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(HashKey),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(LocalIndexRangeKey),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
	}
}
func CreateTable(ctx context.Context, svc AdminAPI, table string) error {
	_, err := svc.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(HashKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(RangeKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(LocalIndexRangeKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(HashKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(RangeKey),
				KeyType:       types.KeyTypeRange,
			},
		},
		TableName:   aws.String(table),
		BillingMode: types.BillingModePayPerRequest,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(LocalIndex),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(HashKey),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(LocalIndexRangeKey),
						KeyType:       types.KeyTypeRange,
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
	})
	if err != nil {
		return err
	}

	if err = waitForTable(ctx, svc, table); err != nil {
		return err
	}

	if _, err = svc.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String(TTLAttribute),
		},
	}); err != nil {
		return err
	}

	return nil

}

func DeleteTable(ctx context.Context, svc AdminAPI, table string) error {
	if _, err := svc.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(table),
	}); err != nil {
		return err
	}
	return nil
}

func waitForTable(ctx context.Context, svc AdminAPI, table string) error {
	w := dynamodb.NewTableExistsWaiter(svc)
	if err := w.Wait(ctx,
		&dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		},
		2*time.Minute,
		func(o *dynamodb.TableExistsWaiterOptions) {
			o.MaxDelay = 5 * time.Second
			o.MinDelay = 1 * time.Second
		}); err != nil {
		return fmt.Errorf("timed out while waiting for table to become active: %w", err)
	}
	return nil
}
