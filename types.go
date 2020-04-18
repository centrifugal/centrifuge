package centrifuge

import "github.com/centrifugal/protocol"

// Define some type aliases to protocol types.
//
// Some reasoning to use aliases here is to prevent user's code to directly import
// centrifugal/protocol package.
//
// Theoretically we could provide wrappers to protocol types for centrifuge public API
// but copying protocol types to wrapper types introduces additional overhead so we
// decided to be low-level here for now. This is a subject to think about for future
// releases though since users that need maximum performance can use Engine methods
// directly.
type (
	// Publication contains Data sent to channel subscribers.
	// In channels with recover option on it also has incremental Offset.
	// If Publication sent from client side it can also have Info (otherwise nil).
	Publication = protocol.Publication
	// ClientInfo contains information about client connection.
	ClientInfo = protocol.ClientInfo
	// Raw represents raw untouched bytes.
	Raw = protocol.Raw
)

//// Publication contains Data sent to channel subscribers.
//// In channels with recover option on it also has incremental Offset.
//// If Publication sent from client side it can also have ClientInfo (otherwise nil).
//type Publication struct {
//	Offset uint64
//	Data   []byte
//	Info   *ClientInfo
//}
//
//func publicationFromProto(pp *protocol.Publication) *Publication {
//	pub := Publication{
//		Offset: pp.Offset,
//		Data:   pp.Data,
//	}
//	if pp.GetInfo() != nil {
//		pub.Info = clientInfoFromProto(pp.GetInfo())
//	}
//	return &pub
//}
//
//// ClientInfo contains information about client connection.
//// This is returned in presence response, sent in Join/Leave messages,
//// can also be attached to Publication.
//type ClientInfo struct {
//	User     string
//	Client   string
//	ConnInfo []byte
//	ChanInfo []byte
//}
//
//func clientInfoFromProto(pi *protocol.ClientInfo) *ClientInfo {
//	return &ClientInfo{
//		User:     pi.User,
//		Client:   pi.Client,
//		ConnInfo: pi.ConnInfo,
//		ChanInfo: pi.ChanInfo,
//	}
//}
