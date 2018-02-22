package centrifuge

import (
	"github.com/centrifugal/centrifuge/internal/proto"
	"github.com/centrifugal/centrifuge/internal/proto/apiproto"
)

// Raw represents raw bytes.
type Raw = proto.Raw

// Publication can be sent into channel and delivered to all channel subscribers.
type Publication = proto.Publication

// Error represents client reply error.
type Error = proto.Error

// ClientInfo is short information about client connection.
type ClientInfo = proto.ClientInfo

// Info represents summary of Centrifuge node cluster.
type Info = apiproto.InfoResult
