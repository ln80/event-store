package fs

import (
	"fmt"

	"github.com/ln80/event-store/avro/registry"
)

type WireFormatter struct{}

// AppendSchemaID implements registry.WireFormatter.
func (*WireFormatter) AppendSchemaID(data []byte, id string) ([]byte, error) {
	bid := []byte(id)

	if len(bid) > 16 {
		return nil, fmt.Errorf("%w: invalid length %d", registry.ErrInvalidSchemaID, len(bid))
	}

	return append(bid[:], data...), nil
}

// ExtractSchemaID implements registry.WireFormatter.
func (*WireFormatter) ExtractSchemaID(data []byte) (string, []byte, error) {
	if len(data) < 16 {
		return "", nil, fmt.Errorf("%w: data too short", registry.ErrInvalidDataWireFormat)
	}

	return string(data[0:16]), data[16:], nil
}

func NewWireFormatter() *WireFormatter {
	return &WireFormatter{}
}

var _ registry.WireFormatter = &WireFormatter{}
