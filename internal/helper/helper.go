package helper

import "slices"

func Unique[T comparable](slice []T) []T {
	seen := make(map[T]struct{})

	unique := slices.DeleteFunc(slice, func(elem T) bool {
		if _, exists := seen[elem]; exists {
			return true
		}
		seen[elem] = struct{}{}
		return false
	})

	return unique
}
