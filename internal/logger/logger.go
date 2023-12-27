package logger

import (
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

func Default() logr.Logger {
	return defaultLogger
}
