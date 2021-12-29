package prepared

import (
	"sync"

	"github.com/centrifugal/protocol"
)

// Reply is a structure for encoding reply only once.
type Reply struct {
	Reply     *protocol.Reply
	protoType protocol.Type
	data      []byte
	once      sync.Once
	pushData  []byte
	pushOnce  sync.Once
}

// NewReply initializes Reply.
func NewReply(reply *protocol.Reply, protoType protocol.Type) *Reply {
	return &Reply{
		Reply:     reply,
		protoType: protoType,
	}
}

// Data returns encoded Reply.
func (r *Reply) Data() []byte {
	r.once.Do(func() {
		encoder := protocol.GetReplyEncoder(r.protoType)
		data, _ := encoder.Encode(r.Reply)
		r.data = data
	})
	return r.data
}

// PushData returns encoded Push part of Reply.
func (r *Reply) PushData() []byte {
	r.pushOnce.Do(func() {
		encoder := protocol.GetPushEncoder(r.protoType)
		pushData, _ := encoder.Encode(r.Reply.Push)
		r.pushData = pushData
	})
	return r.pushData
}
