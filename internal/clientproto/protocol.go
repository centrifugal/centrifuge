package clientproto

import (
	"github.com/centrifugal/protocol"
)

// NewMessagePush returns initialized async push message.
func NewMessagePush(data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type: protocol.PushTypeMessage,
		Data: data,
	}
}

// NewPublicationPush returns initialized async publication message.
func NewPublicationPush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushTypePublication,
		Channel: ch,
		Data:    data,
	}
}

// NewJoinPush returns initialized async join message.
func NewJoinPush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushTypeJoin,
		Channel: ch,
		Data:    data,
	}
}

// NewLeavePush returns initialized async leave message.
func NewLeavePush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushTypeLeave,
		Channel: ch,
		Data:    data,
	}
}

// NewUnsubscribePush returns initialized async unsubscribe message.
func NewUnsubscribePush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushTypeUnsubscribe,
		Channel: ch,
		Data:    data,
	}
}

// NewSubscribePush returns initialized async subscribe message.
func NewSubscribePush(ch string, data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type:    protocol.PushTypeSubscribe,
		Channel: ch,
		Data:    data,
	}
}

// NewConnPush returns initialized async connect message.
func NewConnectPush(data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type: protocol.PushTypeConnect,
		Data: data,
	}
}

// NewDisconnectPush returns initialized async disconnect message.
func NewDisconnectPush(data protocol.Raw) *protocol.Push {
	return &protocol.Push{
		Type: protocol.PushTypeDisconnect,
		Data: data,
	}
}
