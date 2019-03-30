package centrifuge

import (
	"context"
)

// SessionResolveMeta contains additional data for SessionResolver.
type SessionResolveMeta struct{}

// SessionReply is a reply of SessionResover with instructions to library.
// Disconnect if provived allows to close connection, Data is a custom data
// to send in Session push.
type SessionReply struct {
	Disconnect *Disconnect
	Data       Raw
}

// SessionResolver allows to resolve connection Session data in custom way.
type SessionResolver interface {
	Resolve(context.Context, Transport, SessionResolveMeta) SessionReply
}
