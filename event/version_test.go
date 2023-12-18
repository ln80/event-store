package event

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

const (
	VerZeroStr = "00000000000000000000.000~"
	VerMinStr  = "00000000000000000001.000~"
	VerMaxStr  = "18446744073709551615.255~"
)

func TestVersion_Basic(t *testing.T) {
	ver := NewVersion()
	if ver != VersionMin {
		t.Fatalf("expect versions %v, %v be equals", ver, VersionMin)
	}

	if VersionMin.String() != VerMinStr {
		t.Fatalf("expect ver strings %v, %v be equals", VersionMin, VerMinStr)
	}
	if VersionMax.String() != VerMaxStr {
		t.Fatalf("expect ver strings %v, %v be equals", VersionMax, VerMaxStr)
	}
	if VersionZero.String() != VerZeroStr {
		t.Fatalf("expect ver strings %v, %v be equals", VersionZero, VerZeroStr)
	}
	if !VersionZero.IsZero() {
		t.Fatal("expect true, got false")
	}

	if ver.String() != VerMinStr {
		t.Fatalf("expect ver strings %v, %v be equals", ver, VerMinStr)
	}
	if ver.IsZero() {
		t.Fatal("expect false, got true")
	}
	if ver.String() > VersionMax.String() {
		t.Fatalf("expect ver %v lthe %v", ver, VersionMax)
	}
	if ver.String() < VersionMin.String() {
		t.Fatalf("expect ver %v gthe %v", ver, VersionMax)
	}
	if ver.String() <= VersionZero.String() {
		t.Fatalf("expect ver %v gth %v", ver, VersionMax)
	}

	ver, err := Ver("")
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if ver != VersionZero {
		t.Fatalf("expect versions %v, %v be equals", ver, VersionZero)
	}

	_, err = Ver("invalid")
	if !errors.Is(err, ErrVersionMalformed) {
		t.Fatalf("expect err be: %v, got nil", ErrVersionMalformed)
	}

	ver, err = Ver()
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if ver != VersionMin {
		t.Fatalf("expect versions %v, %v be equals", ver, VersionMin)
	}

	ver, err = Ver(VerMinStr)
	if err != nil {
		t.Fatalf("expect err be nil, got %v", err)
	}
	if ver != VersionMin {
		t.Fatalf("expect versions %v, %v be equals", ver, VersionMin)
	}
}
func TestVersion_Parse(t *testing.T) {
	tcs := []struct {
		str string
		ok  bool
		err error
	}{

		{
			str: "invalid version",
			ok:  false,
			err: ErrVersionMalformed,
		},
		{
			str: "00000000000000000000.00000000000000000000",
			ok:  false,
			err: ErrVersionMalformed,
		},
		{
			str: "000000000000.0000000000",
			ok:  false,
			err: ErrVersionMalformed,
		},
		{
			str: "9994674407370955161500006744073709551615.9994967295",
			ok:  false,
			err: ErrVersionMalformed,
		},
		{
			str: "00000000000000000301.O05~",
			ok:  false,
			err: ErrVersionMalformed,
		},
		{
			str: "0000000000000000A301.005~",
			ok:  false,
			err: ErrVersionMalformed,
		},
		{
			str: "184467440737095516151844674407370955161518446744073709551615.0000007295",
			ok:  false,
			err: ErrVersionMalformed,
		},
		{
			str: "",
			ok:  true,
		},
		{
			str: VerZeroStr,
			ok:  true,
		},
		{
			str: VerMinStr,
			ok:  true,
		},
		{
			str: VerMaxStr,
			ok:  true,
		},
		{
			str: "00000000000000000301.005~",
			ok:  true,
		},
		{
			str: "00000000000000000301.005e",
			ok:  true,
		},
		{
			str: "18446744073709551615.255~",
			ok:  true,
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("test parse ver %d:", i+1), func(t *testing.T) {
			ver, err := ParseVersion(tc.str)
			if tc.ok {
				if err != nil {
					t.Fatalf("expect parse version, got err %v", err)
				}
				zeroCaseCond := tc.str == "" && ver.String() == VerZeroStr
				if ver.String() != tc.str && !zeroCaseCond {
					t.Fatalf("expect ver strings %v, %v be equals", ver, tc.str)
				}
			} else {
				if !errors.Is(err, tc.err) {
					t.Fatalf(
						"expect parse err: %v, got: %v", ErrVersionMalformed, err)
				}
			}
		})
	}
}

func TestVersion_Incr(t *testing.T) {
	assertDiff := func(t *testing.T, ver, ver1 Version, r1 uint64, r3 uint8) {
		if d1, d3 := ver1.p-ver.p, ver1.f-ver.f; d1 != r1 || d3 != r3 {
			t.Fatalf("expect vers parts diffs be %d,%d, got %d,%d", r1, r3, d1, d3)
		}
	}

	assertPanic := func(t *testing.T, fn func(), err error) {
		func() {
			defer func() {
				if wanterr, r := err, recover(); r == nil || !strings.Contains(fmt.Sprintf("%v", r), err.Error()) {
					t.Fatalf("expect panic with err %v, got %v", wanterr, r)
				}
			}()
			fn()
		}()
	}

	ver := NewVersion()

	ver1 := ver.doIncr(VersionSeqDiffPart).Incr()
	assertDiff(t, ver, ver1, 2, 0)

	ver1 = ver1.Decr()
	assertDiff(t, ver, ver1, 1, 0)

	ver1 = ver1.Decr()
	assertDiff(t, ver, ver1, 0, 0)

	var addmax uint64 = math.MaxUint64 - 1

	ver1 = ver1.Add(addmax, 0)
	assertDiff(t, ver, ver1, addmax, 0)

	assertPanic(t, func() { ver1 = ver1.Incr().Incr() }, ErrVersionLimitExceeded)
	assertPanic(t, func() { ver1.Add(addmax, 0) }, ErrVersionLimitExceeded)

	ver1 = ver1.doIncr(VersionSeqDiffFracPart).doIncr(VersionSeqDiffFracPart)
	assertDiff(t, ver, ver1, addmax, 2)

	assertPanic(t, func() { ver1 = ver1.Trunc().doIncr(VersionSeqDiffFracPart).Add(0, math.MaxUint8) }, ErrVersionLimitExceeded)
	assertPanic(t, func() { ver1 = ver1.Trunc().Add(0, math.MaxUint8).doIncr(VersionSeqDiffFracPart) }, ErrVersionLimitExceeded)
	assertPanic(t, func() { ver1 = ver1.Trunc().doIncr(999) }, ErrInvalidSequenceIncr)

	ver1 = NewVersion().Decr()
	assertDiff(t, ver1, VersionZero, 0, 0)
	assertPanic(t, func() { ver1 = ver1.Decr() }, ErrVersionLimitExceeded)

	ver1 = VersionMax.Drop(10, 0)
	assertDiff(t, ver1, VersionMax, 10, 0)
}

func TestVersion_Compare(t *testing.T) {
	ver := NewVersion().Add(10, 0)
	type Tc struct {
		v1, v2 Version
		cmp    int  // -1, 0, 1
		next   bool // v1 is next to v2
	}
	tcs := []Tc{
		{
			v1:  ver,
			v2:  ver,
			cmp: 0,
		},
		{
			v1:  ver.Decr(),
			v2:  ver,
			cmp: -1,
		},
		{
			v1:   ver.Incr(),
			v2:   ver,
			cmp:  1,
			next: true,
		},
		func() Tc {
			v := NewVersion().Add(1, 20)
			return Tc{
				v1:   v.Add(1, 0).Trunc(),
				v2:   v,
				cmp:  1,
				next: false,
			}
		}(),
		func() Tc {
			v := NewVersion().Add(1, 20)
			return Tc{
				v1:   v.Add(1, 0).Trunc(),
				v2:   v.EOF(),
				cmp:  1,
				next: true,
			}
		}(),
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("test compare ver %d:", i+1), func(t *testing.T) {
			if want, got := tc.cmp, tc.v1.Compare(tc.v2); want != got {
				t.Fatalf("expect %v and %v be equals", want, got)
			}
			switch tc.cmp {
			case -1:
				if !tc.v1.Before(tc.v2) {
					t.Fatalf("expect %v lh %v", tc.v1, tc.v2)
				}
			case 0:
				if !tc.v1.Equal(ver) {
					t.Fatalf("expect %v and %v be equals", tc.v1, tc.v2)
				}
			case 1:
				if !tc.v1.After(tc.v2) {
					t.Fatalf("expect %v gh %v", tc.v1, tc.v2)
				}
			}
			if tc.next {
				if !tc.v1.Next(tc.v2) {
					t.Fatalf("expect %v is next to %v", tc.v1, tc.v2)
				}
			} else {
				if tc.v1.Next(tc.v2) {
					t.Fatalf("expect %v is not next to %v", tc.v1, tc.v2)
				}
			}
		})
	}
}
