package centrifuge

import (
	"github.com/centrifugal/centrifuge/internal/proto"
)

// TransportInspector allows to inspect connection transport state.
type TransportInspector interface {
	// Name returns a name of transport used for client connection.
	Name() string
	// Encoding returns transport encoding used.
	Encoding() proto.Encoding
}

// Transport abstracts a connection transport between server and client.
type Transport interface {
	TransportInspector
	// Send sends data to session.
	Send(*proto.PreparedReply) error
	// Close closes the session with provided code and reason.
	Close(*proto.Disconnect) error
}
