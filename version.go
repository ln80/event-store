package es

import (
	"github.com/Masterminds/semver/v3"
)

type Version string

// VERSION is the current version of the Go Module.
const ES_VERSION Version = "v0.0.1"

// Semver parses and returns semver struct.
func (v Version) Semver() *semver.Version {
	return semver.MustParse(string(v))
}
