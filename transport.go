package centrifuge

import "github.com/centrifugal/protocol"

// ProtocolType represents client connection transport encoding format.
type ProtocolType string

func (t ProtocolType) toProto() protocol.Type {
	return protocol.Type(t)
}

const (
	// ProtocolTypeJSON means JSON protocol - i.e. data encoded in
	// JSON-streaming format.
	ProtocolTypeJSON ProtocolType = "json"
	// ProtocolTypeProtobuf means protobuf protocol - i.e. data encoded
	// as length-delimited protobuf messages.
	ProtocolTypeProtobuf ProtocolType = "protobuf"
)

// EncodingType represents client payload encoding format.
type EncodingType string

const (
	// EncodingTypeJSON means JSON payload.
	EncodingTypeJSON EncodingType = "json"
	// EncodingTypeBinary means binary payload.
	EncodingTypeBinary EncodingType = "binary"
)

//type WriteType int
//
//// Known write types.
//const (
//	// WriteTypeEncodedReplyData means protocol.Reply encoded into Centrifuge protocol.
//	WriteTypeEncodedReplyData WriteType = iota + 1
//	// WriteTypeEncodedPushData means protocol.Push encoded into Centrifuge protocol.
//	WriteTypeEncodedPushData
//	// WriteTypeReply means raw protocol.Reply.
//	WriteTypeReply
//	// WriteTypePush means raw protocol.Push
//	WriteTypePush
//)

// TransportInfo has read-only transport description methods.
type TransportInfo interface {
	// Name returns a name of transport used for client connection.
	Name() string
	// Protocol returns underlying transport protocol type used.
	// At moment this can be for example a JSON streaming based protocol
	// or Protobuf length-delimited protocol.
	Protocol() ProtocolType
	// Encoding returns payload encoding type used by client. By default
	// server assumes that payload passed as JSON.
	Encoding() EncodingType
	// Unidirectional returns whether transport is unidirectional.
	Unidirectional() bool
	//// WriteType defines what transport expects as input for writing.
	//WriteType() WriteType
}

// Transport abstracts a connection transport between server and client.
// It does not contain Read method as reading can be handled by connection
// handler code (for example by WebsocketHandler.ServeHTTP).
type Transport interface {
	TransportInfo
	// Write writes data encoded using Centrifuge protocol to a connection.
	Write([]byte) error
	//// WriteReply writes protocol.Reply to a connection.
	//WriteReply(*protocol.Reply) error
	//// WritePush writes protocol.Push to a connection.
	//WritePush(*protocol.Push) error
	// Close closes transport.
	Close(*Disconnect) error
}
