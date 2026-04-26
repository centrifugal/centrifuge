package centrifuge

import "sync"

// KeyedChannelOptions are generic options for any keyed channel.
type KeyedChannelOptions struct {
	// MaxTrackedPerConnection limits how many keys a single connection
	// can track in this channel. Zero value means 5000.
	MaxTrackedPerConnection int
}

// KeyedManager manages keyed channel state: per-channel reverse index
// (key→subscribers) and keyed hub for per-key fan-out.
type KeyedManager struct {
	node     *Node
	mu       sync.RWMutex
	channels map[string]*keyedChannelState
}

type keyedChannelState struct {
	hub  *keyedHub
	opts KeyedChannelOptions
}

func newKeyedManager(node *Node) *KeyedManager {
	return &KeyedManager{
		node:     node,
		channels: make(map[string]*keyedChannelState),
	}
}

func (m *KeyedManager) getOrCreateChannel(channel string, opts KeyedChannelOptions) *keyedChannelState {
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

func (m *KeyedManager) getHub(channel string) *keyedHub {
	m.mu.RLock()
	s, ok := m.channels[channel]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return s.hub
}

func (m *KeyedManager) removeChannel(channel string) {
	m.mu.Lock()
	delete(m.channels, channel)
	m.mu.Unlock()
}

const defaultMaxTrackedPerConnection = 5000

func (m *KeyedManager) maxTrackedPerConnection(channel string) int {
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
