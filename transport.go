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
	// Unidirectional returns whether transport is unidirectional. For
	// unidirectional transports Centrifuge uses Push protobuf messages
	// without additional wrapping into Reply protocol message.
	Unidirectional() bool
	// DisabledPushFlags returns a disabled push flags for specific transport.
	// For example this allows to disable sending Disconnect push in case of
	// bidirectional WebSocket implementation since disconnect data sent inside
	// Close frame.
	DisabledPushFlags() uint64
}

// Transport abstracts a connection transport between server and client.
// It does not contain Read method as reading can be handled by connection
// handler code (for example by WebsocketHandler.ServeHTTP).
type Transport interface {
	TransportInfo
	// Write should write single push data into a connection. Every byte slice
	// here is a single Reply (or Push for unidirectional transport) encoded
	// according transport ProtocolType.
	Write([]byte) error
	// WriteMany should write data into a connection. Every byte slice here is a
	// single Reply (or Push for unidirectional transport) encoded according
	// transport ProtocolType. The reason why we have both Write and WriteMany
	// here is to have a path without any additional allocations for massive
	// broadcasts (since variadic args cause allocation).
	WriteMany(...[]byte) error
	// Close must close a transport. Transport implementation can optionally
	// handle Disconnect passed here. For example builtin WebSocket transport
	// sends Disconnect as part of websocket.CloseMessage.
	Close(*Disconnect) error
}
