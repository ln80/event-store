package event

import (
	"github.com/rs/xid"
)

// ID presents the interface responsible for generating UID.
// It serves as an abstraction to avoid strong coupling to a specific library/algorithm
type ID interface {
	String() string
}

func UID() ID {
	return xid.New()
}
