package xray

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-xray-sdk-go/instrumentation/awsv2"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/ln80/event-store/event"
)

func AppendEventWithTracing() func(*event.AppendConfig) {
	return func(eao *event.AppendConfig) {
		eao.AddTracing = func(ctx context.Context) (traceID string) {
			if seg := xray.GetSegment(ctx); seg != nil {
				traceID = seg.DownstreamHeader().String()
			}
			return
		}
	}
}

func Instrument(cfg aws.Config) {
	awsv2.AWSV2Instrumentor(&cfg.APIOptions)
}
