package event

import (
	"context"
	"reflect"
	"strings"
)

// resolveType of the given value which is mainly an event.
// It accepts both pointers and values. Pointers type are replaced with their values type.
func resolveType(v any) (reflect.Type, string) {
	rType := reflect.TypeOf(v)
	if rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType, rType.String()
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
	t := TypeOf(v)
	if namespace != "" {
		splits := strings.Split(t, ".")
		return namespace + "." + splits[len(splits)-1]
	}
	return t
}

// TypeOfWithContext uses TypeOfWithNamespace under the hood and looks for the namespace value from the context.
func TypeOfWithContext(ctx context.Context, v any) string {
	if ctx.Value(ContextNamespaceKey) != nil {
		return TypeOfWithNamespace(ctx.Value(ContextNamespaceKey).(string), v)
	}
	return TypeOf(v)
}
