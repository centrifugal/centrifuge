package main

import (
	"fmt"
	"sync"

	"github.com/centrifugal/centrifuge"
)

// pollStore is the backend the shared poll handler reads. Scenarios move a key's
// version here and then assert the update reached every connection tracking it,
// which is the only way to check the refresh loop end to end: the server asks
// this store what a key looks like now, and the answer must reach the client.
var pollStore = &keyStore{items: map[string]map[string]keyValue{}}

type keyValue struct {
	version uint64
	data    string
	removed bool
}

type keyStore struct {
	mu    sync.RWMutex
	items map[string]map[string]keyValue
}

// set bumps a key to the given version with fresh data.
func (s *keyStore) set(channel, key string, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.items[channel]
	if !ok {
		ch = map[string]keyValue{}
		s.items[channel] = ch
	}
	ch[key] = keyValue{version: version, data: fmt.Sprintf(`{"key":%q,"v":%d}`, key, version)}
}

// remove marks a key removed, which the refresh loop must deliver as a removal.
func (s *keyStore) remove(channel, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.items[channel]
	if !ok {
		ch = map[string]keyValue{}
		s.items[channel] = ch
	}
	cur := ch[key]
	ch[key] = keyValue{version: cur.version + 1, removed: true}
}

// poll answers a refresh cycle: every key the server asks about is reported at
// its current version, or as removed when the store never had it.
func (s *keyStore) poll(e centrifuge.SharedPollEvent) centrifuge.SharedPollResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ch := s.items[e.Channel]
	items := make([]centrifuge.SharedPollRefreshItem, 0, len(e.Items))
	for _, it := range e.Items {
		val, ok := ch[it.Key]
		if !ok || val.removed {
			items = append(items, centrifuge.SharedPollRefreshItem{Key: it.Key, Removed: true, Version: val.version})
			continue
		}
		items = append(items, centrifuge.SharedPollRefreshItem{
			Key:     it.Key,
			Data:    []byte(val.data),
			Version: val.version,
		})
	}
	return centrifuge.SharedPollResult{Items: items}
}
