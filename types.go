package centrifuge

import (
	"github.com/centrifugal/protocol"
)

// Publication contains Data sent to channel subscribers.
// In channels with recover option on it also has incremental Offset.
// If Publication sent from client side it can also have ClientInfo (otherwise nil).
type Publication struct {
	Offset uint64
	Data   []byte
	Info   *ClientInfo
}

func publicationFromProto(pp *protocol.Publication) *Publication {
	pub := Publication{
		Offset: pp.Offset,
		Data:   pp.Data,
	}
	if pp.GetInfo() != nil {
		pub.Info = clientInfoFromProto(pp.GetInfo())
	}
	return &pub
}

// ClientInfo contains information about client connection.
// This is returned in presence response, sent in Join/Leave messages,
// can also be attached to Publication.
type ClientInfo struct {
	User     string
	Client   string
	ConnInfo []byte
	ChanInfo []byte
}

func clientInfoFromProto(pi *protocol.ClientInfo) *ClientInfo {
	return &ClientInfo{
		User:     pi.User,
		Client:   pi.Client,
		ConnInfo: pi.ConnInfo,
		ChanInfo: pi.ChanInfo,
	}
}
