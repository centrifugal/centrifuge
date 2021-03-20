package clientproto

import (
	"github.com/centrifugal/protocol"
)

// NewMessagePush returns initialized async push message.
func NewMessagePush(data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type: protocol.PushType_PUSH_TYPE_MESSAGE,
		Data: data,
	}
}

// NewPublicationPush returns initialized async publication message.
func NewPublicationPush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushType_PUSH_TYPE_PUBLICATION,
		Channel: ch,
		Data:    data,
	}
}

// NewJoinPush returns initialized async join message.
func NewJoinPush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushType_PUSH_TYPE_JOIN,
		Channel: ch,
		Data:    data,
	}
}

// NewLeavePush returns initialized async leave message.
func NewLeavePush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushType_PUSH_TYPE_LEAVE,
		Channel: ch,
		Data:    data,
	}
}

// NewUnsubscribePush returns initialized async unsubscribe message.
func NewUnsubscribePush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushType_PUSH_TYPE_UNSUBSCRIBE,
		Channel: ch,
		Data:    data,
	}
}

// NewSubscribePush returns initialized async subscribe message.
func NewSubscribePush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushType_PUSH_TYPE_SUBSCRIBE,
		Channel: ch,
		Data:    data,
	}
}

// NewConnPush returns initialized async connect message.
func NewConnectPush(data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type: protocol.PushType_PUSH_TYPE_CONNECT,
		Data: data,
	}
}

// NewDisconnectPush returns initialized async disconnect message.
func NewDisconnectPush(data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type: protocol.PushType_PUSH_TYPE_DISCONNECT,
		Data: data,
	}
}
