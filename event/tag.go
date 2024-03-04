package event

import (
	"reflect"
	"strings"
)

var (
	// TagID is the default event tag ID.
	TagID = "ev"
)

// TagOptions presents a map of the event's field options. Each option may contain multiple values.
type TagOptions map[string][]string

// ParseTag returns the event field name and a map of optional options (ex, aliases).
// Tag options are not pre-defined at the 'event' package level but can be defined in
// other packages for different purposes.
func ParseTag(tag reflect.StructTag) (name string, opts TagOptions) {
	opts = make(map[string][]string)
	tagStr := tag.Get(TagID)
	if tagStr == "" {
		return
	}
	raw := strings.Split(tagStr, ",")
	name = strings.TrimSpace(raw[0])
	for _, o := range raw[1:] {
		splits := strings.Split(o, "=")

		optName := strings.TrimSpace(splits[0])
		if optName == "" {
			continue
		}
		optValues := []string{}
		if len(splits) > 1 {
			for _, vv := range strings.Split(strings.TrimSpace(splits[1]), " ") {
				vv := strings.TrimSpace(vv)
				if vv == "" {
					continue
				}
				optValues = append(optValues, strings.TrimSpace(vv))
			}
		}
		opts[optName] = optValues
	}

	return
}
