package centrifuge

import (
	"context"
	"time"
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
	HandleJoin(ch string, join *Join) error
	// Leave must register callback func to handle Leave messages received.
	HandleLeave(ch string, leave *Leave) error
	// Control must register callback func to handle Control data received.
	HandleControl([]byte) error
}

// HistoryFilter allows to filter history according to fields set.
type HistoryFilter struct {
	// Since used to recover missed messages since provided RecoveryPosition.
	Since *RecoveryPosition
	// Limit number of publications to return.
	Limit int
}

// RecoveryPosition contains fields to rely in recovery process.
type RecoveryPosition struct {
	// Seq defines publication sequence.
	Seq uint32
	// Gen defines publication generation. The reason why we use both seq and gen
	// is the fact that Javascript can't properly work with big numbers.
	Gen uint32
	// Epoch of sequence and generation. Allows to handle situations when storage
	// lost seq and gen for some reason and we don't want to improperly decide
	// that publications were successfully recovered.
	Epoch string
}

// Closer ...
type Closer interface {
	// Close when called should clean up used resources if needed.
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
	// Channels returns slice of currently active channels (with
	// one or more subscribers) on all running nodes.
	Channels() ([]string, error)

	// Publish allows to send Publication into channel. This message should
	// be delivered to all clients subscribed on this channel at moment on
	// any Centrifugo node. The returned value is channel in which we will
	// send error as soon as engine finishes publish operation. Also this
	// method must maintain history for channels if enabled in channel options.
	Publish(ch string, pub *Publication, opts *ChannelOptions) <-chan error
	// PublishJoin publishes Join message into channel.
	PublishJoin(ch string, join *Join, opts *ChannelOptions) <-chan error
	// PublishLeave publishes Leave message into channel.
	PublishLeave(ch string, leave *Leave, opts *ChannelOptions) <-chan error
	// PublishControl allows to send control command data to all running nodes.
	PublishControl(data []byte) <-chan error
}

// HistoryManager is responsible for dealing with channel history management.
type HistoryManager interface {
	// History returns a slice of publications published into channel.
	// HistoryFilter allows to set several filtering options.
	History(ch string, filter HistoryFilter) ([]*Publication, RecoveryPosition, error)
	// AddHistory adds Publication to channel history. Storage should
	// automatically maintain history size and lifetime according to
	// channel options if needed.
	AddHistory(ch string, pub *Publication, opts *ChannelOptions, onDone func(seq uint64, err error)) <-chan error
	// RemoveHistory removes history from channel. This is in general not
	// needed as history expires automatically (based on history_lifetime)
	// but sometimes can be useful for application logic.
	RemoveHistory(ch string) error
}

// PresenceManager is responsible for channel presence management.
type PresenceManager interface {
	// Presence returns actual presence information for channel.
	Presence(ch string) (map[string]*ClientInfo, error)
	// PresenseStats returns short stats of current presence data
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
