package event

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
)

var (
	ErrVersionLimitExceeded = errors.New("version limit exceeded")
	ErrVersionMalformed     = errors.New("version malformed")
	ErrInvalidSequenceIncr  = errors.New("invalid sequence increment")
	ErrVersionEOFReached    = errors.New("version EOF reached")
)

const (
	VersionPartZeroStr = "00000000000000000000"
	VersionFracZeroStr = "000"
)

const (
	VersionSuffixEOF    = "e"
	VersionSuffixNotEOF = "~"
)

// VersionSequenceDiff defines the difference between two consecutive versions.
// Two consecutive events within the same record have a fractional difference of .1.
// Two consecutive events belonging to separate records have a difference of 1.
type VersionSequenceDiff int

const (
	VersionSeqDiffFracPart VersionSequenceDiff = iota
	VersionSeqDiffPart
)

var (
	VersionMax = Version{
		p: math.MaxUint64,
		f: math.MaxUint8,
	}

	VersionMin = Version{
		p: 1,
		f: 0,
	}

	VersionZero = Version{
		p: 0,
		f: 0,
	}
)

type Version struct {
	// integer parts
	p uint64
	// fractional part
	f uint8
	// end of fractional part
	eof bool
}

// Ver does parse the given version (the first string arguments),
// or returns a new one with minimal value
func Ver(str ...string) (Version, error) {
	if len(str) > 0 {
		return ParseVersion(str[0])
	}
	return VersionMin, nil
}

// NewVersion returns new version with minimal value.
// It fails if version is malformed
func NewVersion() Version {
	return VersionMin
}

// ParseVersion the given string version
func ParseVersion(str string) (Version, error) {
	l := len(str)
	if l == 0 {
		return VersionZero, nil
	}

	v := Version{}

	if l != 25 {
		return v, fmt.Errorf("%w: %s", ErrVersionMalformed, str)
	}

	p := str[0:20]
	sep := str[l-5 : l-4]
	f := str[l-4 : l-1]
	eof := str[l-1 : l]

	if sep != "." || eof != VersionSuffixEOF && eof != VersionSuffixNotEOF {
		return v, fmt.Errorf("%w: %s", ErrVersionMalformed, str)
	}

	if f != VersionFracZeroStr {
		i, err := strconv.ParseUint(f, 10, 8)
		if err != nil {
			return v, fmt.Errorf("%w: %v", ErrVersionMalformed, err)
		}
		v.f = uint8(i)
	}

	if p != VersionPartZeroStr {
		i, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return v, fmt.Errorf("%w: %v", ErrVersionMalformed, err)
		}
		v.p = uint64(i)
	}

	if eof == VersionSuffixEOF {
		v.eof = true
	}

	return v, nil
}

// Incr increments the version integer part while removing the fractional part from the returned version
func (v Version) Incr() Version {
	return v.doIncr(VersionSeqDiffPart)
}

// Decr decrements the version integer part while removing the fractional part from the returned version
func (v Version) Decr() Version {
	v.eof = false
	if v.p > 0 {
		v.p--
		v.f = 0
	} else {
		panic(ErrVersionLimitExceeded)
	}
	return v
}

func (v Version) doIncr(diff VersionSequenceDiff) Version {
	v.eof = false
	switch diff {
	case VersionSeqDiffFracPart:
		if v.eof {
			panic(ErrVersionEOFReached)
		}
		if v.f < math.MaxUint8-uint8(1) {
			v.f += 1
			return v
		} else {
			panic(ErrVersionLimitExceeded)
		}
	case VersionSeqDiffPart:
		if v.p < math.MaxUint64 {
			v.p++
			v.f = 0
		} else {
			panic(ErrVersionLimitExceeded)
		}
		return v
	default:
		panic(fmt.Errorf("%w: %d", ErrInvalidSequenceIncr, diff))
	}
}

// Add increments the version using the given values for both integer and fractional parts
func (v Version) Add(p uint64, d uint8) Version {
	v.eof = false
	if p > 0 {
		if v.p <= math.MaxUint64-p {
			v.p += p
		} else {
			panic(ErrVersionLimitExceeded)
		}
	}
	if d > 0 {
		if v.f <= math.MaxUint8-d {
			v.f += d
		} else {
			panic(ErrVersionLimitExceeded)
		}
	}
	return v
}

// Drop decrements the version using the given values for both integer and fractional parts
func (v Version) Drop(p uint64, d uint8) Version {
	v.eof = false
	if p > 0 {
		if v.p >= p {
			v.p -= p
		} else {
			v.p = 0
		}
	}
	if d > 0 {
		if v.f >= d {
			v.f -= d
		} else {
			v.f = 0
		}
	}
	return v
}

// EOF mark the current version as "End OF Fractional Part"
func (v Version) EOF() Version {
	v.eof = true
	return v
}

// Trunc truncates the fractional part and returns a new version
func (v Version) Trunc() Version {
	v.f = 0
	v.eof = false
	return v
}

// String serializes the version and returns a sorted string value
func (v Version) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("%020d", v.p))
	buf.WriteString(fmt.Sprintf(".%03d", v.f))
	if v.eof {
		buf.WriteString(VersionSuffixEOF)
	} else {
		buf.WriteString(VersionSuffixNotEOF)
	}

	return buf.String()
}

func (v Version) IsZero() bool {
	return v == VersionZero
}

func (v Version) Equal(ov Version) bool {
	v.eof = false
	ov.eof = false
	return v == ov
}

func (v Version) Next(to Version) bool {
	diff := uint64(0)
	diff += v.p - to.p
	if diff > 1 {
		return false
	}

	if diff == 1 {
		return (v.f == 0 && (to.f == 0 || to.eof))
	}

	return v.f-to.f == 1
}

func (v Version) Before(ov Version) bool {
	if v.p == ov.p {
		return v.f < ov.f
	}
	return v.p < ov.p
}

func (v Version) After(ov Version) bool {
	return !v.Before(ov) && !v.Equal(ov)
}

func (v Version) Between(v1, v2 Version) bool {
	return (v.After(v1) || v.Equal(v1)) && (v.Before(v2) || v.Equal(v2))
}

func (v Version) Compare(ov Version) int {
	if v.After(ov) {
		return 1
	}
	if v.Before(ov) {
		return -1
	}
	return 0
}
