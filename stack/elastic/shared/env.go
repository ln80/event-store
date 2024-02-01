package shared

import (
	"errors"
	"os"
	"strings"

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

func MustParseConfigPathEnv() (found bool, application string, environment string, configuration string) {
	path := os.Getenv("FEATURE_TOGGLES_CONFIG_PATH")
	if path == "" {
		return
	}
	params := strings.Split(path, "/")
	if len(params) != 6 {
		panic("invalid feature toggles AppConfig path: " + path)
	}

	found = true
	application = params[1]
	environment = params[3]
	configuration = params[5]

	return
}
