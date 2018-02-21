package centrifuge

import (
	"github.com/centrifugal/centrifuge/internal/proto"
	"github.com/centrifugal/centrifuge/internal/proto/apiproto"
)

// Publication can be sent into channel and delivered to all channel subscribers.
type Publication = proto.Publication

// Disconnect allows to configure how client will be disconnected from server.
type Disconnect = proto.Disconnect

// ClientInfo is short information about client connection.
type ClientInfo = proto.ClientInfo

// Info represents summary of Centrifuge node cluster.
type Info = apiproto.InfoResult
