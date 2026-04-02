package centrifuge

// PresenceStats represents a short presence information for channel.
type PresenceStats struct {
	// NumClients is a number of client connections in channel.
	NumClients int
	// NumUsers is a number of unique users in channel.
	NumUsers int
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
	// for connection with specified identifier. PresenceManager should
	// have a property to expire client information that was not updated
	// (touched) after some configured time interval.
	AddPresence(ch string, clientID string, info *ClientInfo) error
	// AddPresenceBatch sets or updates presence information for multiple channels
	// in a single batch operation. This is more efficient than calling AddPresence
	// multiple times as it reduces network round trips (e.g. using Redis pipeline).
	// The items slice contains channel, clientID, and info for each presence update.
	AddPresenceBatch(items []PresenceBatchItem) error
	// RemovePresence removes presence information for connection
	// with specified client and user identifiers.
	RemovePresence(ch string, clientID string, userID string) error
	// SupportsBatchPresence reports whether this implementation can execute
	// AddPresenceBatch correctly and efficiently.
	//
	// When false, the node skips AddPresenceBatch and falls back to
	// calling AddPresence once per item. Implementations must return false
	// whenever batch execution would be semantically incorrect or would offer
	// no performance benefit. A typical example is Redis Cluster mode, where a
	// pipelined batch whose EVALSHA commands reference keys in different hash
	// slots is rejected by Redis with:
	//
	//	CROSSSLOT Keys in request don't hash to the same slot
	//
	// Because each presence update targets a distinct channel that may hash to a
	// different slot, batch execution is fundamentally incompatible with Redis
	// Cluster. See RedisPresenceManager.SupportsBatchPresence for the full list
	// of cluster-specific constraints.
	SupportsBatchPresence() bool
}

// PresenceBatchItem represents a single presence update in a batch operation.
type PresenceBatchItem struct {
	Channel  string
	ClientID string
	Info     *ClientInfo
}
