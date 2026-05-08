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
