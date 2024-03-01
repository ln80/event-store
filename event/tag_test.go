package event

import (
	"fmt"
	"reflect"
	"testing"
)

func TestParseTag(t *testing.T) {
	tcs := []struct {
		evt  any
		name string
		opts TagOptions
	}{
		{
			evt: struct {
				Val string `ev:""`
			}{},
			name: "",
			opts: map[string][]string{},
		},
		{
			evt: struct {
				Val string `ev:"val"`
			}{},
			name: "val",
			opts: map[string][]string{},
		},
		{
			evt: struct {
				Val string `ev:"val,"`
			}{},
			name: "val",
			opts: map[string][]string{},
		},
		{
			evt: struct {
				Val string `ev:"val,aliases=v v2"`
			}{},
			name: "val",
			opts: map[string][]string{
				"aliases": {"v", "v2"},
			},
		},
		{
			evt: struct {
				Val string `ev:"val,aliases="`
			}{},
			name: "val",
			opts: map[string][]string{
				"aliases": {},
			},
		},
		{
			evt: struct {
				Val string `ev:"val,aliases"`
			}{},
			name: "val",
			opts: map[string][]string{
				"aliases": {},
			},
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("test parse ver %d:", i+1), func(t *testing.T) {
			field, _ := reflect.TypeOf(tc.evt).FieldByName("Val")
			name, opts := ParseTag(field.Tag)

			if want, got := tc.name, name; want != got {
				t.Fatalf("expect %s, %s be equals", want, got)
			}
			if want, got := tc.opts, opts; !reflect.DeepEqual(want, got) {
				t.Fatalf("expect %v, %v be equals", want, got)
			}
		})
	}
}
