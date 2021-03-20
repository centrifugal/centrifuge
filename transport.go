package centrifuge

import "github.com/centrifugal/protocol"

// ProtocolType represents client connection transport encoding format.
type ProtocolType string

func (t ProtocolType) toProto() protocol.Type {
	return protocol.Type(t)
}

const (
	// ProtocolTypeJSON means JSON-based protocol.
	ProtocolTypeJSON ProtocolType = "json"
	// ProtocolTypeProtobuf means protobuf protocol.
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

// It's possible to disable certain types of pushes to be sent to a client connection
// by using ClientConfig.DisabledPushFlags.
const (
	PushFlagConnect uint64 = 1 << iota
	PushFlagDisconnect
	PushFlagSubscribe
	PushFlagJoin
	PushFlagLeave
	PushFlagUnsubscribe
	PushFlagPublication
	PushFlagMessage
)

// TransportInfo has read-only transport description methods.
type TransportInfo interface {
	// Name returns a name of transport.
	Name() string
	// Protocol returns an underlying transport protocol type used.
	// JSON or Protobuf types are supported.
	Protocol() ProtocolType
	// Encoding returns payload encoding type used by client. By default
	// server assumes that payloads passed in JSON format. But it's possible
	// to give a tip to Centrifuge that payload is binary.
	Encoding() EncodingType
	// Unidirectional returns whether transport is unidirectional. For
	// unidirectional transports Centrifuge uses Push protobuf messages
	// without additional wrapping into Reply protocol message.
	Unidirectional() bool
	// DisabledPushFlags returns a disabled push flags for specific transport.
	DisabledPushFlags() uint64
}

// Transport abstracts a connection transport between server and client.
// It does not contain Read method as reading can be handled by connection
// handler code (for example by WebsocketHandler.ServeHTTP).
type Transport interface {
	TransportInfo
	// Write should write data into a connection. Every byte slice here is a
	// single Reply (or Push for unidirectional transport) encoded according
	// transport ProtocolType.
	Write(...[]byte) error
	// Close must close a transport. Transport implementation can optionally
	// handle Disconnect passed here. For example builtin WebSocket transport
	// sends Disconnect as part of websocket.CloseMessage.
	Close(*Disconnect) error
}
