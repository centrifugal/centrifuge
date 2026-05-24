package centrifuge

import "sync"

// keyedChannelOptions are generic options for any keyed channel.
type keyedChannelOptions struct {
	// MaxTrackedPerConnection limits how many keys a single connection
	// can track in this channel. Zero value means 5000.
	MaxTrackedPerConnection int
}

// keyedManager manages keyed channel state: per-channel reverse index
// (key→subscribers) and keyed hub for per-key fan-out.
type keyedManager struct {
	node     *Node
	mu       sync.RWMutex
	channels map[string]*keyedChannelState
}

type keyedChannelState struct {
	hub  *keyedHub
	opts keyedChannelOptions
}

func newKeyedManager(node *Node) *keyedManager {
	return &keyedManager{
		node:     node,
		channels: make(map[string]*keyedChannelState),
	}
}

func (m *keyedManager) getOrCreateChannel(channel string, opts keyedChannelOptions) *keyedChannelState {
	m.mu.Lock()
	s, ok := m.channels[channel]
	if !ok {
		s = &keyedChannelState{
			hub:  newKeyedHub(),
			opts: opts,
		}
		m.channels[channel] = s
	}
	m.mu.Unlock()
	return s
}

func (m *keyedManager) getHub(channel string) *keyedHub {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return s.hub
}

func (m *keyedManager) removeChannel(channel string) {
	m.mu.Lock()
	delete(m.channels, channel)
	m.mu.Unlock()
}

// addSubscribers ensures a keyedChannelState exists for the channel and
// atomically adds the given client as a subscriber to each key. The
// "create state + add subscriber" sequence runs under m.mu so a concurrent
// removeChannelIfEmpty cannot delete the state between creation and the
// first addSubscriber — that race would otherwise leave the client in an
// orphaned hub that future broadcasts (via getHub) no longer reach.
func (m *keyedManager) addSubscribers(channel string, keys []string, c *Client, opts keyedChannelOptions) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.channels[channel]
	if !ok {
		s = &keyedChannelState{
			hub:  newKeyedHub(),
			opts: opts,
		}
		m.channels[channel] = s
	}
	for _, key := range keys {
		s.hub.addSubscriber(key, c)
	}
}

// removeChannelIfEmpty deletes the channel from the manager only when its
// hub has no subscribers. Used by sharedPollChannelState.finalizeShutdown
// instead of the unconditional removeChannel, so a new client whose track
// has just added itself to the hub via addSubscribers is not orphaned by
// an in-flight shutdown from an older sharedPollChannelState.
func (m *keyedManager) removeChannelIfEmpty(channel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.channels[channel]
	if !ok {
		return
	}
	if s.hub.numKeys() > 0 {
		return
	}
	delete(m.channels, channel)
}

const defaultMaxTrackedPerConnection = 5000

func (m *keyedManager) maxTrackedPerConnection(channel string) int {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return defaultMaxTrackedPerConnection
	}
	limit := s.opts.MaxTrackedPerConnection
	if limit <= 0 {
		return defaultMaxTrackedPerConnection
	}
	return limit
}
