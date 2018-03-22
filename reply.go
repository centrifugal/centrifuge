package centrifuge

import (
	"sync"

	"github.com/centrifugal/centrifuge/internal/proto"
)

// preparedReply is structure for encoding reply only once.
type preparedReply struct {
	Enc   proto.Encoding
	Reply *proto.Reply
	data  []byte
	once  sync.Once
}

// newPreparedReply initializes PreparedReply.
func newPreparedReply(reply *proto.Reply, enc proto.Encoding) *preparedReply {
	return &preparedReply{
		Reply: reply,
		Enc:   enc,
	}
}

// Data returns data associated with reply which is only calculated once.
func (r *preparedReply) Data() []byte {
	r.once.Do(func() {
		encoder := proto.GetReplyEncoder(r.Enc)
		encoder.Encode(r.Reply)
		data := encoder.Finish()
		proto.PutReplyEncoder(r.Enc, encoder)
		r.data = data
	})
	return r.data
}
