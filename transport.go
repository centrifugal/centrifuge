package centrifuge

import (
	"net/http"
)

// TransportMeta contains extended transport description.
// Depending on transport implementation some fields here can be missing.
type TransportMeta struct {
	// Request contains initial HTTP request sent by client. Can be nil in case of
	// non-HTTP based transports. Though both Websocket and SockjS we currently
	// support use HTTP on start so this field will present.
	Request *http.Request
}

// TransportDetails has read-only transport description methods.
type TransportDetails interface {
	// Name returns a name of transport used for client connection.
	Name() string
	// Encoding returns transport encoding used.
	Encoding() Encoding
	// Info returns transport information.
	Meta() TransportMeta
}

// Transport abstracts a connection transport between server and client.
// It does not contain Read method as reading can be handled by connection
// handler code.
type Transport interface {
	TransportDetails
	// Send sends data encoded using Centrifuge protocol to session.
	Write([]byte) error
	// Close closes transport.
	Close(*Disconnect) error
}
