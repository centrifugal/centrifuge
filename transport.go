package centrifuge

import (
	"net/http"
)

// TransportInfo contains extended transport description.
type TransportInfo struct {
	// Request contains initial HTTP request sent by client. Can be nil in case of
	// non-HTTP based transports. Though both Websocket and SockjS we currently
	// support use HTTP on start so this field will present.
	Request *http.Request
}

// Transport abstracts a connection transport between server and client.
type Transport interface {
	// Name returns a name of transport used for client connection.
	Name() string
	// Protocol returns underlying transport protocol type used.
	// At moment this can be for example a JSON streaming based protocol
	// or Protobuf length-delimited protocol.
	Protocol() ProtocolType
	// Encoding() returns payload encoding type used by client. By default
	// server assumes that payload passed as JSON.
	Encoding() EncodingType
	// Info returns transport information.
	Info() TransportInfo
}

type transport interface {
	Transport
	// Send sends data to session.
	Write([]byte) error
	// Close closes transport.
	Close(*Disconnect) error
}
