package shared

import (
	"context"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-xray-sdk-go/header"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/ln80/event-store/dynamodb"
	"github.com/ln80/event-store/internal/logger"
)

var (
	traceServiceName      string = "EventStore"
	traceServiceNamespace string = "ln80"
)

func WithTracing(ctx context.Context, traceHeader, name string, keyAndValues ...any) (context.Context, *xray.Segment) {
	h := header.FromString(traceHeader)

	ctx, seg := xray.NewSegmentFromHeader(ctx, traceServiceName+"::"+name, nil, h)
	seg.GetAWS()["region"] = middleware.GetRegion(ctx)
	seg.GetAWS()["operation"] = name

	if seg != nil {
		seg.Namespace = traceServiceNamespace
		for i := 0; i < len(keyAndValues); i += 2 {
			_ = xray.AddAnnotation(ctx, keyAndValues[i].(string), keyAndValues[i+1])
		}
	}
	return ctx, seg
}

func InitLogger(name string) {
	log := logger.New(func(o *logger.Options) {
		o.Output = os.Stdout
		if v := os.Getenv("VERBOSITY_LEVEL"); v != "" {
			if level, err := strconv.Atoi(v); err != nil {
				o.VerbosityLevel = level
			}
		}
	}).
		WithName(name).
		WithValues("function", os.Getenv("AWS_LAMBDA_FUNCTION_NAME"))

	logger.SetDefault(log)
}

func WithRecordContext(ctx context.Context, ev events.DynamoDBEventRecord) (context.Context, func(error)) {
	raw := ev.Change.NewImage

	log := logger.WithStream(logger.Default(), raw[dynamodb.GIDAttribute].String()).
		WithValues("record", RecordKeys(ev.Change.Keys))

	close := func(error) {}

	if t, ok := raw[dynamodb.TraceIDAttribute]; ok {
		var (
			seg *xray.Segment
		)
		ctx, seg = WithTracing(ctx, t.String(), "Forwarder",
			"gstmID", raw[dynamodb.GIDAttribute].String(),
			"gver", raw[dynamodb.GVerAttribute].String(),
		)
		if seg != nil {
			log = logger.WithTrace(log, seg.TraceID)
			close = func(err error) {
				seg.Close(err)
			}
		}
	}
	ctx, cancel := context.WithCancel(logger.NewContext(ctx, log))

	return ctx, func(err error) {
		close(err)
		cancel()
	}
}
