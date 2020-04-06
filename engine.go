package centrifuge

import (
	"context"
	"time"

	"github.com/centrifugal/protocol"
)

// PresenceStats represents a short presence information for channel.
type PresenceStats struct {
	NumClients int
	NumUsers   int
}

// BrokerEventHandler can handle messages received from PUB/SUB system.
type BrokerEventHandler interface {
	// Publication must register callback func to handle Publications received.
	HandlePublication(ch string, pub *Publication) error
	// Join must register callback func to handle Join messages received.
	HandleJoin(ch string, join *protocol.Join) error
	// Leave must register callback func to handle Leave messages received.
	HandleLeave(ch string, leave *protocol.Leave) error
	// Control must register callback func to handle Control data received.
	HandleControl([]byte) error
}

// HistoryFilter allows to filter history according to fields set.
type HistoryFilter struct {
	// Since used to recover missed messages since provided RecoveryPosition.
	Since *StreamPosition
	// Limit number of publications to return.
	Limit int
}

// StreamPosition contains fields to rely in stream recovery process. More info
// about stream recovery in docs: https://centrifugal.github.io/centrifugo/server/recover/
type StreamPosition struct {
	// Seq defines publication incremental sequence.
	Seq uint32
	// Gen defines publication sequence generation. The reason why we use both Seq and
	// Gen fields is the fact that Javascript can't properly work with big numbers. As
	// we not only support JSON but also Protobuf protocol format decision was made to
	// be effective in serialization size and not pass sequences as strings.
	Gen uint32
	// Epoch of sequence and generation. Allows to handle situations when storage
	// lost stream entirely for some reason (expired or lost after restart) and we
	// want to track this fact to prevent successful recovery from another stream.
	// I.e. for example we have stream [1, 2, 3], then it's lost and new stream
	// contains [1, 2, 3, 4], client that recovers from position 3 will only receive
	// publication 4 missing 1, 2, 3 from new stream. With epoch we can tell client
	// that correct recovery is not possible.
	Epoch string
}

// Closer is an interface that Broker, HistoryManager and PresenceManager can
// optionally implement if they need to close any resources on Centrifuge node
// shutdown.
type Closer interface {
	// Close when called should clean up used resources.
	Close(ctx context.Context) error
}

// Broker is responsible for PUB/SUB mechanics.
type Broker interface {
	// Run called once on start when broker already set to node. At
	// this moment node is ready to process broker events.
	Run(BrokerEventHandler) error

	// Subscribe node on channel to listen all messages coming from channel.
	Subscribe(ch string) error
	// Unsubscribe node from channel to stop listening messages from it.
	Unsubscribe(ch string) error

	// Publish allows to send Publication Push into channel. Publications should
	// be delivered to all clients subscribed on this channel at moment on
	// any Centrifuge node. The returned value is channel in which we will
	// send error as soon as engine finishes publish operation.
	Publish(ch string, pub *Publication, opts *ChannelOptions) error
	// PublishJoin publishes Join Push message into channel.
	PublishJoin(ch string, join *protocol.Join, opts *ChannelOptions) error
	// PublishLeave publishes Leave Push message into channel.
	PublishLeave(ch string, leave *protocol.Leave, opts *ChannelOptions) error
	// PublishControl allows to send control command data to all running nodes.
	PublishControl(data []byte) error

	// Channels returns slice of currently active channels (with one or more
	// subscribers) on all running nodes. This is possible with Redis but can
	// be much harder in other PUB/SUB system. Anyway this information can only
	// be used for admin needs to better understand state of system. So it's not
	// a big problem if another Broker implementation won't support this method.
	Channels() ([]string, error)
}

// HistoryManager is responsible for dealing with channel history management.
type HistoryManager interface {
	// History returns a slice of publications published into channel.
	// HistoryFilter allows to set several filtering options.
	// Returns slice of Publications with Seq and Gen properly set, current
	// stream top position and error.
	History(ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error)
	// AddHistory adds Publication to channel history. Storage should
	// automatically maintain history size and lifetime according to
	// channel options if needed.
	// The returned value is Publication ready to be published to
	// Broker (with Seq and Gen properly set if needed).
	// If returned Publication is nil then node will not try to publish
	// it to Broker at all. This is useful for situations when engine can
	// atomically save Publication to history and publish it to channel.
	AddHistory(ch string, pub *Publication, opts *ChannelOptions) (*Publication, error)
	// RemoveHistory removes history from channel. This is in general not
	// needed as history expires automatically (based on history_lifetime)
	// but sometimes can be useful for application logic.
	RemoveHistory(ch string) error
}

// PresenceManager is responsible for channel presence management.
type PresenceManager interface {
	// Presence returns actual presence information for channel.
	Presence(ch string) (map[string]*ClientInfo, error)
	// PresenceStats returns short stats of current presence data
	// suitable for scenarios when caller does not need full client
	// info returned by presence method.
	PresenceStats(ch string) (PresenceStats, error)
	// AddPresence sets or updates presence information in channel
	// for connection with specified identifier. Engine should have a
	// property to expire client information that was not updated
	// (touched) after some configured time interval.
	AddPresence(ch string, clientID string, info *ClientInfo, expire time.Duration) error
	// RemovePresence removes presence information for connection
	// with specified identifier.
	RemovePresence(ch string, clientID string) error
}

// Engine is responsible for PUB/SUB mechanics, channel history and
// presence information.
type Engine interface {
	Broker
	HistoryManager
	PresenceManager
}
