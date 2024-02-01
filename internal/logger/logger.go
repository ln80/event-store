package logger

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
)

var (
	defaultLogger logr.Logger
	mu            sync.RWMutex
	defaultLevel  = 1 // info and debug
)

func init() {
	SetDefault(New())
}

type Options struct {
	Output           io.Writer
	VerbosityLevel   int
	IncludeFileLine  bool
	IncludeTimestamp bool
}

func New(optFns ...func(*Options)) logr.Logger {
	opts := &Options{
		Output:           os.Stderr,
		VerbosityLevel:   defaultLevel,
		IncludeTimestamp: true,
		IncludeFileLine:  false,
	}
	for _, fn := range optFns {
		if fn == nil {
			continue
		}
		fn(opts)
	}

	zerolog.TimeFieldFormat = time.RFC3339
	zerologr.SetMaxV(opts.VerbosityLevel)
	zlc := zerolog.New(opts.Output).With()
	if opts.IncludeFileLine {
		zlc = zlc.Caller()
	}
	if opts.IncludeTimestamp {
		zlc = zlc.Timestamp()
	}
	zl := zlc.Logger()

	return zerologr.New(&zl)
}

func SetDefault(log logr.Logger) {
	mu.Lock()
	defer mu.Unlock()

	defaultLogger = log
}

func Discard() logr.Logger {
	return logr.Discard()
}

func Default() logr.Logger {
	return defaultLogger
}

func FromContext(ctx context.Context) logr.Logger {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return Default()
	}
	return logger
}

func NewContext(ctx context.Context, logger logr.Logger) context.Context {
	return logr.NewContext(ctx, logger)
}

func WithStream(logger logr.Logger, gstmID string) logr.Logger {
	return logger.WithValues("gstmID", gstmID)
}

func WithTrace(logger logr.Logger, traceID string) logr.Logger {
	return logger.WithValues("x-traceID", traceID)
}
