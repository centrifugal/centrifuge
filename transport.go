package centrifuge

import (
	"time"

	"github.com/centrifugal/protocol"
)

// ProtocolType represents client connection transport encoding format.
type ProtocolType string

func (t ProtocolType) toProto() protocol.Type {
	return protocol.Type(t)
}

const (
	// ProtocolTypeJSON means JSON-based protocol.
	ProtocolTypeJSON ProtocolType = "json"
	// ProtocolTypeProtobuf means Protobuf protocol.
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

// ProtocolVersion defines protocol behavior.
type ProtocolVersion uint8

const (
	// ProtocolVersion1 is the legacy client protocol format. It will be
	// removed at some point in the future.
	ProtocolVersion1 ProtocolVersion = 1
	// ProtocolVersion2 is the current stable client protocol.
	ProtocolVersion2 ProtocolVersion = 2
)

// AppLevelPing is a configuration that describes desired application
// level ping-pong behavior. AppLevelPing applied only to clients that
// use protocol >= ProtocolVersion2.
type AppLevelPing struct {
	// PingInterval tells how often to issue application-level server-to-client pings.
	// Zero value will disable pings.
	PingInterval time.Duration
	// PongTimeout sets time for application-level pong check after issuing ping.
	// Pong only checked for bidirectional clients as unidirectional clients can't send
	// pong back. PongTimeout must be less than PingInterval – this is a limitation of
	// current implementation (should be fine for most cases though). Zero value disables
	// pong checks.
	PongTimeout time.Duration
}

// TransportInfo has read-only transport description methods. Some of these methods
// can modify the behaviour of Client.
type TransportInfo interface {
	// Name returns a name of transport.
	Name() string
	// Protocol returns an underlying transport protocol type used by transport.
	// JSON or Protobuf protocol types are supported by Centrifuge. Message encoding
	// happens of Client level.
	Protocol() ProtocolType
	// ProtocolVersion returns protocol version used by transport.
	ProtocolVersion() ProtocolVersion
	// Unidirectional returns whether transport is unidirectional. For
	// unidirectional transports Client writes Push protobuf messages
	// without additional wrapping pushes into Reply type.
	Unidirectional() bool
	// Emulation must return true for transport that uses Centrifuge emulation layer.
	// See EmulationHandler for more details.
	Emulation() bool
	// DisabledPushFlags returns a disabled push flags for specific transport.
	// For example this allows to disable sending Disconnect push in case of
	// bidirectional WebSocket implementation since disconnect data sent inside
	// Close frame.
	DisabledPushFlags() uint64
	// AppLevelPing returns application-level server-to-client ping
	// configuration.
	AppLevelPing() AppLevelPing
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
	// transport ProtocolType.
	// The reason why we have both Write and WriteMany here is to have a path
	// without extra allocations for massive broadcasts (since variadic args cause
	// allocation).
	WriteMany(...[]byte) error
	// Close must close transport. Transport implementation can optionally
	// handle Disconnect passed here. For example builtin WebSocket transport
	// sends Disconnect as part of websocket.CloseMessage.
	Close(Disconnect) error
}
