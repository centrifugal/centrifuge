package centrifuge

import (
	"context"
)

// SessionResolveMeta ...
type SessionResolveMeta struct{}

// SessionReply ...
type SessionReply struct {
	Disconnect *Disconnect
	Data       Raw
}

// SessionResolver allows to resolve connection Session data in custom way.
type SessionResolver interface {
	Resolve(context.Context, Transport, SessionResolveMeta) SessionReply
}
