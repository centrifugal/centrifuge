package centrifuge

import (
	"sync"

	"github.com/centrifugal/protocol"
)

// keyedHub is a per-channel reverse index: key → set of subscriber clients.
// Used by shared poll subscriptions for per-key fan-out.
type keyedHub struct {
	mu    sync.RWMutex
	items map[string]map[string]*Client // key → clientUID → *Client
}

func newKeyedHub() *keyedHub {
	return &keyedHub{
		items: make(map[string]map[string]*Client),
	}
}

func (h *keyedHub) addSubscriber(key string, c *Client) {
	h.mu.Lock()
	subs, ok := h.items[key]
	if !ok {
		subs = make(map[string]*Client)
		h.items[key] = subs
	}
	subs[c.uid] = c
	h.mu.Unlock()
}

func (h *keyedHub) removeSubscriber(key string, c *Client) (keyEmpty bool) {
	h.mu.Lock()
	subs, ok := h.items[key]
	if ok {
		delete(subs, c.uid)
		if len(subs) == 0 {
			delete(h.items, key)
			keyEmpty = true
		}
	}
	h.mu.Unlock()
	return keyEmpty
}

func (h *keyedHub) subscribers(key string) []*Client {
	h.mu.RLock()
	subs, ok := h.items[key]
	if !ok {
		h.mu.RUnlock()
		return nil
	}
	targets := make([]*Client, 0, len(subs))
	for _, c := range subs {
		targets = append(targets, c)
	}
	h.mu.RUnlock()
	return targets
}

func (h *keyedHub) allKeys() []string {
	h.mu.RLock()
	keys := make([]string, 0, len(h.items))
	for k := range h.items {
		keys = append(keys, k)
	}
	h.mu.RUnlock()
	return keys
}

func (h *keyedHub) subscriberCount(key string) int {
	h.mu.RLock()
	count := len(h.items[key])
	h.mu.RUnlock()
	return count
}

func (h *keyedHub) hasSubscriber(key string, c *Client) bool {
	h.mu.RLock()
	subs, ok := h.items[key]
	if !ok {
		h.mu.RUnlock()
		return false
	}
	_, has := subs[c.uid]
	h.mu.RUnlock()
	return has
}

func (h *keyedHub) removeAllSubscribers(key string) {
	h.mu.Lock()
	delete(h.items, key)
	h.mu.Unlock()
}

func (h *keyedHub) numKeys() int {
	h.mu.RLock()
	n := len(h.items)
	h.mu.RUnlock()
	return n
}

// collectAllClients returns the deduplicated set of *Client refs subscribed
// to any key on this hub. Used to enumerate every client of a shared-poll
// channel — for epoch-flip-driven unsubscribe in particular.
//
// Holds h.mu.RLock during enumeration only; the returned slice is safe to
// iterate without holding hub or channel-state locks.
func (h *keyedHub) collectAllClients() []*Client {
	h.mu.RLock()
	if len(h.items) == 0 {
		h.mu.RUnlock()
		return nil
	}
	seen := make(map[string]*Client)
	for _, subs := range h.items {
		for uid, c := range subs {
			if _, ok := seen[uid]; ok {
				continue
			}
			seen[uid] = c
		}
	}
	h.mu.RUnlock()

	out := make([]*Client, 0, len(seen))
	for _, c := range seen {
		out = append(out, c)
	}
	return out
}

// broadcastToKey sends a publication to all subscribers of a key.
// Each subscriber's per-connection version is checked — only clients
// with a version lower than pubVersion receive the publication.
// Must be called WITHOUT holding sharedPollChannelState.mu.
func (h *keyedHub) broadcastToKey(channel string, key string, pubVersion uint64, pub *protocol.Publication, prep preparedData) {
	targets := h.subscribers(key)
	for _, c := range targets {
		c.keyedWritePublication(channel, key, pubVersion, pub, prep)
	}
}

// broadcastRemoval sends a removal publication to all subscribers of a key.
// Must be called WITHOUT holding sharedPollChannelState.mu.
func (h *keyedHub) broadcastRemoval(channel string, key string) {
	targets := h.subscribers(key)
	if len(targets) == 0 {
		return
	}
	pub := &protocol.Publication{Key: key, Removed: true}
	for _, c := range targets {
		c.keyedWriteRemoval(channel, key, pub)
	}
}

// broadcastRemovalToUsers sends removal publications only to connections
// belonging to the specified users (or excluding specified users).
func (h *keyedHub) broadcastRemovalToUsers(channel string, key string, users []string, excludeUsers []string) {
	targets := h.subscribers(key)
	if len(targets) == 0 {
		return
	}

	var userSet map[string]struct{}
	var excludeSet map[string]struct{}

	if len(users) > 0 {
		userSet = make(map[string]struct{}, len(users))
		for _, u := range users {
			userSet[u] = struct{}{}
		}
	}
	if len(excludeUsers) > 0 {
		excludeSet = make(map[string]struct{}, len(excludeUsers))
		for _, u := range excludeUsers {
			excludeSet[u] = struct{}{}
		}
	}

	pub := &protocol.Publication{Key: key, Removed: true}
	for _, c := range targets {
		uid := c.UserID()
		if userSet != nil {
			if _, ok := userSet[uid]; !ok {
				continue
			}
		}
		if excludeSet != nil {
			if _, ok := excludeSet[uid]; ok {
				continue
			}
		}
		c.keyedWriteRemoval(channel, key, pub)
	}
}

// removeSubscribersForUsers removes subscribers matching user/exclude filters.
func (h *keyedHub) removeSubscribersForUsers(key string, users []string, excludeUsers []string) {
	var userSet map[string]struct{}
	var excludeSet map[string]struct{}

	if len(users) > 0 {
		userSet = make(map[string]struct{}, len(users))
		for _, u := range users {
			userSet[u] = struct{}{}
		}
	}
	if len(excludeUsers) > 0 {
		excludeSet = make(map[string]struct{}, len(excludeUsers))
		for _, u := range excludeUsers {
			excludeSet[u] = struct{}{}
		}
	}

	h.mu.Lock()
	subs, ok := h.items[key]
	if !ok {
		h.mu.Unlock()
		return
	}
	for uid, c := range subs {
		user := c.UserID()
		if userSet != nil {
			if _, ok := userSet[user]; !ok {
				continue
			}
		}
		if excludeSet != nil {
			if _, ok := excludeSet[user]; ok {
				continue
			}
		}
		delete(subs, uid)
	}
	if len(subs) == 0 {
		delete(h.items, key)
	}
	h.mu.Unlock()
}
