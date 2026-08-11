package graph

import (
	"github.com/odysseia-greek/ionia/herodotos/gateway"
)

type Resolver struct {
	Handler *gateway.HerodotosHandler
}

func value(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
