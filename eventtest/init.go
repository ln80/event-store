package eventtest

import (
	"github.com/ln80/event-store/logger"
)

func init() {
	logger.SetDefault(logger.Discard())
}
