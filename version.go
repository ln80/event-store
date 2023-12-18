package es

import (
	"github.com/Masterminds/semver/v3"
)

type Version string

// Semver parses and returns semver struct.
func (v Version) Semver() *semver.Version {
	return semver.MustParse(string(v))
}

// MODULE_VERSION is the current version of the Go Module.
const MODULE_VERSION Version = "v0.0.1"

// ELASTIC_VERSION is the current version of the elastic stack.
const ELASTIC_VERSION = "v0.0.1"
