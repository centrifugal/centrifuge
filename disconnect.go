package centrifuge

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// Make sure disconnectError implements error interface.
var _ error = (*disconnectError)(nil)

type disconnectError struct {
	Disconnect
}

func (d disconnectError) Error() string {
	return fmt.Sprintf("code: %d, reason: %s", d.Code, d.Reason)
}

// NewDisconnectError allows to create a special error from Disconnect object
// which results into a client disconnect with corresponding Code and Reason.
func NewDisconnectError(d Disconnect) error {
	return &disconnectError{Disconnect: d}
}

// Disconnect allows configuring how client will be disconnected from a server.
// A server can provide a Disconnect.Code and Disconnect.Reason to a client. Clients
// can execute some custom logic based on a certain Disconnect.Code. Code is also
// used for metric collection. Disconnect.Reason is optional and exists mostly for
// human-readable description of returned code – i.e. for logging, debugging etc.
//
// The important note is that Disconnect.Reason must be less than 127 bytes
// due to WebSocket protocol limitations.
//
// Codes have some rules which should be followed by a client connector implementation.
// These rules described below.
//
// Codes in range 0-2999 should not be used by a Centrifuge library user. Those are
// reserved for the client-side and transport specific needs. Codes in range >=5000
// should not be used also. Those are reserved by Centrifuge.
//
// Client should reconnect upon receiving code in range 3000-3499, 4000-4499, >=5000.
// For codes <3000 reconnect behavior can be adjusted for specific transport.
//
// Codes in range 3500-3999 and 4500-4999 are application terminal codes, no automatic
// reconnect should be made by a client implementation.
//
// Library users supposed to use codes in range 4000-4999 for creating custom
// disconnects.
type Disconnect struct {
	// Code is a disconnect code.
	Code uint32 `json:"code,omitempty"`
	// Reason is a short description of disconnect code for humans.
	Reason string `json:"reason"`

	// Reconnect is only used for compatibility with ProtocolVersion1.
	// Ignore this field if all your clients are using ProtocolVersion2.
	// Deprecated.
	Reconnect bool `json:"reconnect"`
}

// String representation.
func (d Disconnect) String() string {
	return fmt.Sprintf("code: %d, reason: %s", d.Code, d.Reason)
}

func (d *Disconnect) isReconnect(protoVersion ProtocolVersion) bool {
	if protoVersion == ProtocolVersion1 {
		return d.Reconnect
	}
	return d.Code < 3500 || d.Code >= 5000 || (d.Code >= 4000 && d.Code < 4500)
}

// This is a temporary close text cache for ProtocolVersion1 compatibility.
var closeTextCacheMu sync.RWMutex
var closeTextCache map[uint32]string

func init() {
	closeTextCache = map[uint32]string{}
}

// CloseText allows building disconnect advice sent inside Close frame.
// At moment, we don't encode Code here to not duplicate information
// since it is sent separately as Code of WebSocket/SockJS Close Frame.
func (d *Disconnect) CloseText(protoVersion ProtocolVersion) string {
	if protoVersion >= ProtocolVersion2 {
		return d.Reason
	}
	closeTextCacheMu.RLock()
	closeText, ok := closeTextCache[d.Code]
	closeTextCacheMu.RUnlock()
	if !ok {
		buf := strings.Builder{}
		buf.WriteString(`{"reason":`)
		reason, _ := json.Marshal(d.Reason)
		buf.Write(reason)
		buf.WriteString(`,"reconnect":`)
		var reconnect = d.isReconnect(ProtocolVersion1)
		if reconnect {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
		buf.WriteString(`}`)
		closeText = buf.String()
		closeTextCacheMu.Lock()
		closeTextCache[d.Code] = closeText
		closeTextCacheMu.Unlock()
	}
	return closeText
}

// DisconnectConnectionClosed is a special Disconnect object used when
// client connection was closed without any advice from a server side.
// This can be a clean disconnect, or temporary disconnect of the client
// due to internet connection loss. Server can not distinguish the actual
// reason of disconnect.
var DisconnectConnectionClosed = Disconnect{
	Code:   3000,
	Reason: "connection closed",
}

// Some predefined non-terminal disconnect structures used by
// the library internally.
var (
	// DisconnectShutdown issued when node is going to shut down.
	DisconnectShutdown = Disconnect{
		Code:      3001,
		Reason:    "shutdown",
		Reconnect: true,
	}
	// DisconnectServerError issued when internal error occurred on server.
	DisconnectServerError = Disconnect{
		Code:      3004,
		Reason:    "internal server error",
		Reconnect: true,
	}
	// DisconnectExpired issued when client connection expired.
	DisconnectExpired = Disconnect{
		Code:      3005,
		Reason:    "connection expired",
		Reconnect: true,
	}
	// DisconnectSubExpired issued when client subscription expired.
	DisconnectSubExpired = Disconnect{
		Code:      3006,
		Reason:    "subscription expired",
		Reconnect: true,
	}
	// DisconnectSlow issued when client can't read messages fast enough.
	DisconnectSlow = Disconnect{
		Code:      3008,
		Reason:    "slow",
		Reconnect: true,
	}
	// DisconnectWriteError issued when an error occurred while writing to
	// client connection.
	DisconnectWriteError = Disconnect{
		Code:      3009,
		Reason:    "write error",
		Reconnect: true,
	}
	// DisconnectInsufficientState issued when server detects wrong client
	// position in channel Publication stream. Disconnect allows client
	// to restore missed publications on reconnect.
	DisconnectInsufficientState = Disconnect{
		Code:      3010,
		Reason:    "insufficient state",
		Reconnect: true,
	}
	// DisconnectForceReconnect issued when server disconnects connection.
	DisconnectForceReconnect = Disconnect{
		Code:      3011,
		Reason:    "force reconnect",
		Reconnect: true,
	}
	// DisconnectNoPong may be issued when server disconnects bidirectional
	// connection due to no pong received to application-level server-to-client
	// pings in a configured time.
	DisconnectNoPong = Disconnect{
		Code:      3012,
		Reason:    "no pong",
		Reconnect: true,
	}
)

// The codes below are built-in terminal codes.
var (
	// DisconnectInvalidToken issued when client came with invalid token.
	DisconnectInvalidToken = Disconnect{
		Code:   3500,
		Reason: "invalid token",
	}
	// DisconnectBadRequest issued when client uses malformed protocol
	// frames or wrong order of commands.
	DisconnectBadRequest = Disconnect{
		Code:   3501,
		Reason: "bad request",
	}
	// DisconnectStale issued to close connection that did not become
	// authenticated in configured interval after dialing.
	DisconnectStale = Disconnect{
		Code:   3502,
		Reason: "stale",
	}
	// DisconnectForceNoReconnect issued when server disconnects connection
	// and asks it to not reconnect again.
	DisconnectForceNoReconnect = Disconnect{
		Code:   3503,
		Reason: "force disconnect",
	}
	// DisconnectConnectionLimit can be issued when client connection exceeds a
	// configured connection limit (per user ID or due to other rule).
	DisconnectConnectionLimit = Disconnect{
		Code:   3504,
		Reason: "connection limit",
	}
	// DisconnectChannelLimit can be issued when client connection exceeds a
	// configured channel limit.
	DisconnectChannelLimit = Disconnect{
		Code:   3505,
		Reason: "channel limit",
	}
)
