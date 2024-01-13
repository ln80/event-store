package dynamodb

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// IsConditionCheckFailure checks if the given error is an aws error that expresses a conditional failure exception.
// It works seamlessly in both single write and within a transaction operation.
func IsConditionCheckFailure(err error) bool {
	// if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
	// 	return true
	// }
	// var oe *smithy.OperationError
	// if errors.As(err, &oe) {
	// var re *http.ResponseError
	// 	if errors.As(err, &re) {
	// 		var tce *types.TransactionCanceledException
	// 		if errors.As(err, &tce) {
	// 			for _, reason := range tce.CancellationReasons {
	// 				if *reason.Code == "ConditionalCheckFailed" {
	// 					return true
	// 				}

	// 			}
	// 		}
	// 	}
	// }

	var cce *types.ConditionalCheckFailedException
	if errors.As(err, &cce) {
		return true
	}

	var tce *types.TransactionCanceledException
	if errors.As(err, &tce) {
		for _, reason := range tce.CancellationReasons {
			if *reason.Code == "ConditionalCheckFailed" {
				return true
			}
		}
	}
	return false
}

func IsConditionCheckFailureWithItem(err error, hashKey, rangeKey string) (bool, bool) {
	var cce *types.ConditionalCheckFailedException
	if errors.As(err, &cce) {
		item := cce.Item
		if len(item) > 0 {
			hk := item[HashKey]
			rk := item[RangeKey]
			if hk.(*types.AttributeValueMemberS).Value == hashKey && rk.(*types.AttributeValueMemberS).Value == rangeKey {
				return true, true
			}
		}

		return true, false
	}

	var tce *types.TransactionCanceledException
	if errors.As(err, &tce) {
		flag := false
		items := []map[string]types.AttributeValue{}
		for _, reason := range tce.CancellationReasons {
			if *reason.Code == "ConditionalCheckFailed" {
				flag = true
				items = append(items, reason.Item)
			}
		}
		for _, item := range items {
			hk := item[HashKey]
			rk := item[RangeKey]
			if hk.(*types.AttributeValueMemberS).Value == hashKey && rk.(*types.AttributeValueMemberS).Value == rangeKey {
				return flag, true
			}
		}
		return flag, false
	}

	return false, false
}
