package event

import "context"

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

var (
	ContextNamespaceKey = ContextKey("namespace")
	ContextUserKey      = ContextKey("user")
	ContextIPAddrKey    = ContextKey("ip_addr")
)

type ContextValues struct {
	User   string
	IPAddr string
}

func ContextWith(ctx context.Context, values ContextValues) context.Context {
	if values.User != "" {
		ctx = context.WithValue(ctx, ContextUserKey, values.User)
	}
	if values.IPAddr != "" {
		ctx = context.WithValue(ctx, ContextIPAddrKey, values.IPAddr)
	}
	return ctx
}
