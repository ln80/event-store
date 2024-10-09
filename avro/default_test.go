package avro

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestDefaultOf(t *testing.T) {
	type tc struct {
		val  any
		want any
	}
	tcs := []tc{
		{
			val: func() any {
				type T struct {
				}
				return &T{}
			}(),
			want: nil,
		},
		{
			val: func() any {
				type T struct {
				}
				return T{}
			}(),
			want: map[string]any{},
		},
		{
			val: func() any {
				type T struct {
					A string
				}
				return T{A: "a"}
			}(),
			want: map[string]any{"A": "a"},
		},
		{
			val: func() any {
				type T struct {
					A string
					B []byte
				}
				return T{A: "a", B: []byte("b")}
			}(),
			want: map[string]any{"A": "a", "B": "b"},
		},
		{
			val: func() any {
				type T struct {
					A string
					B []byte
					C uint16
				}
				return T{A: "a", B: []byte("b"), C: uint16(10)}
			}(),
			want: map[string]any{"A": "a", "B": "b", "C": int(10)},
		},
		func() tc {
			type T struct {
				A string
				B []byte
				C uint16
				D float64
				T time.Time
			}
			tt := time.Now()
			return tc{
				val:  T{A: "a", B: []byte("b"), C: uint16(10), D: 1.5, T: tt},
				want: map[string]any{"A": "a", "B": "b", "C": int(10), "D": float64(1.5), "T": tt.UnixMilli()},
			}
		}(),
		func() tc {
			type A struct {
				A string
			}
			type T struct {
				A A
				B string
			}
			return tc{
				val:  T{A: A{A: "a"}, B: "b"},
				want: map[string]any{"A": map[string]any{"A": "a"}, "B": "b"},
			}
		}(),
		func() tc {
			type A struct {
				A string
			}
			type T struct {
				A
				B string
			}
			return tc{
				val:  T{A: A{A: "a"}, B: "b"},
				want: map[string]any{"A": "a", "B": "b"},
			}
		}(),
		func() tc {
			type A struct {
				A string
			}
			type T struct {
				A
				b string
			}
			return tc{
				val:  T{A: A{A: "a"}, b: "b"},
				want: map[string]any{"A": "a"},
			}
		}(),
		func() tc {
			type A struct {
				A string
			}
			type T struct {
				*A
				B string
			}
			return tc{
				val:  T{A: &A{A: "a"}, B: "b"},
				want: map[string]any{"A": "a", "B": "b"},
			}
		}(),
		func() tc {
			type T struct {
				A []string
			}
			return tc{
				val:  T{A: []string{"a", "b"}},
				want: map[string]any{"A": []any{"a", "b"}},
			}
		}(),
		func() tc {
			type T struct {
				A map[string]string
			}
			return tc{
				val:  T{A: map[string]string{"a": "b"}},
				want: map[string]any{"A": map[string]any{"a": "b"}},
			}
		}(),
	}

	for i, tc := range tcs {
		t.Run("tc: "+strconv.Itoa(i), func(t *testing.T) {
			result, err := defaultOf(tc.val)
			if err != nil {
				t.Fatal("expect err be nil, got", err)
			}
			if want, got := tc.want, result; !reflect.DeepEqual(want, got) {
				t.Fatalf("want %+#v, got %+#v", want, got)
			}
		})
	}
}
