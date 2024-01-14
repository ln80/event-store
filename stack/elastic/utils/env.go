package utils

import (
	"errors"
	"os"

	"github.com/ln80/event-store/internal/logger"
)

func MustGetenv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		logger.Default().Error(errors.New("missed env param: "+name), "")
		os.Exit(1)
	}

	return v
}
