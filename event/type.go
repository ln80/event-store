package event

import (
	"context"
	"reflect"
	"strings"
)

func normalizeType(t reflect.Type) (reflect.Type, string) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t, t.String()
}

// resolveType of the given value which is mainly an event.
// It accepts both pointers and values. Pointers type are replaced with their values type.
func resolveType(v any) (reflect.Type, string) {
	rType := reflect.TypeOf(v)

	return normalizeType(rType)
}

// TypeOf returns the type of a value or its pointer
func TypeOf(v any) (vType string) {
	_, vType = resolveType(v)
	return
}

// TypeOfWithNamespace returns the type of the value using the given namespace.
// By default the type format is {package name}.{value type name}.
// The return is changed to {namespace}.{value type name} if namespace is not empty
func TypeOfWithNamespace(namespace string, v any) string {
	rType := reflect.TypeOf(v)
	return NormalizeTypeWithNamespace(namespace, rType)
}

func NormalizeTypeWithNamespace(namespace string, t reflect.Type) string {
	_, str := normalizeType(t)
	if namespace != "" {
		splits := strings.Split(str, ".")
		return namespace + "." + splits[len(splits)-1]
	}
	return str
}

// TypeOfWithContext uses TypeOfWithNamespace under the hood and looks for the namespace value from the context.
func TypeOfWithContext(ctx context.Context, v any) string {
	if ctx.Value(ContextNamespaceKey) != nil {
		return TypeOfWithNamespace(ctx.Value(ContextNamespaceKey).(string), v)
	}
	return TypeOf(v)
}
