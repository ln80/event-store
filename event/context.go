package event

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

var (
	ContextNamespaceKey = ContextKey("namespace")
	ContextUserKey      = ContextKey("user")
	ContextIPAddrKey    = ContextKey("ip_addr")
)
