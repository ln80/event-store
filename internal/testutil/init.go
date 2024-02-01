package testutil

import (
	"github.com/ln80/event-store/internal/logger"
)

func init() {
	logger.SetDefault(logger.Discard())
}
