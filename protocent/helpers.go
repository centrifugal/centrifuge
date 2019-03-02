package protocent

import "github.com/centrifugal/centrifuge/internal/proto"

// NewPublicationPush returns initialized async publication message.
func NewPublicationPush(ch string, data proto.Raw) *Push {
	return &Push{
		Type:    PushTypePublication,
		Channel: ch,
		Data:    data,
	}
}

// NewJoinPush returns initialized async join message.
func NewJoinPush(ch string, data proto.Raw) *Push {
	return &Push{
		Type:    PushTypeJoin,
		Channel: ch,
		Data:    data,
	}
}

// NewLeavePush returns initialized async leave message.
func NewLeavePush(ch string, data proto.Raw) *Push {
	return &Push{
		Type:    PushTypeLeave,
		Channel: ch,
		Data:    data,
	}
}
