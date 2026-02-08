package centrifuge

import (
	"sync"
	"time"
)

const numDebounceShards = 4096

type debounceKey struct {
	channel string
	key     string
}

type debounceEntry[T any] struct {
	timer *time.Timer
	value T
}

type debounceShard[T any] struct {
	mu      sync.Mutex
	entries map[debounceKey]*debounceEntry[T]
}

// debouncer is a generic sharded debouncer keyed by (channel, key).
// It coalesces rapid updates and fires the flush callback with the latest value.
type debouncer[T any] struct {
	shards [numDebounceShards]debounceShard[T]
	flush  func(channel, key string, value T)
}

func newDebouncer[T any](flush func(channel, key string, value T)) *debouncer[T] {
	return &debouncer[T]{flush: flush}
}

// Debounce stores the value and starts a timer if one isn't already pending.
// If a timer is already pending, only the value is updated (the timer is NOT
// reset). This gives throttle-with-trailing behavior: the flush callback fires
// on the original schedule with the latest value, ensuring events are delivered
// periodically even during continuous updates.
func (d *debouncer[T]) Debounce(channel, key string, duration time.Duration, value T) {
	idx := index(channel+"\x00"+key, numDebounceShards)
	shard := &d.shards[idx]
	dk := debounceKey{channel: channel, key: key}

	shard.mu.Lock()
	if e, ok := shard.entries[dk]; ok {
		e.value = value
		shard.mu.Unlock()
		return
	}

	if shard.entries == nil {
		shard.entries = make(map[debounceKey]*debounceEntry[T])
	}

	e := &debounceEntry[T]{value: value}
	e.timer = time.AfterFunc(duration, func() {
		d.fire(shard, dk)
	})
	shard.entries[dk] = e
	shard.mu.Unlock()
}

func (d *debouncer[T]) fire(shard *debounceShard[T], dk debounceKey) {
	shard.mu.Lock()
	e, ok := shard.entries[dk]
	if !ok {
		shard.mu.Unlock()
		return
	}
	value := e.value
	delete(shard.entries, dk)
	shard.mu.Unlock()
	d.flush(dk.channel, dk.key, value)
}

// Cancel stops and removes a pending debounced entry. Returns true if an
// entry was found and cancelled.
func (d *debouncer[T]) Cancel(channel, key string) bool {
	idx := index(channel+"\x00"+key, numDebounceShards)
	shard := &d.shards[idx]
	dk := debounceKey{channel: channel, key: key}

	shard.mu.Lock()
	e, ok := shard.entries[dk]
	if ok {
		e.timer.Stop()
		delete(shard.entries, dk)
	}
	shard.mu.Unlock()
	return ok
}

// Close stops all pending timers and clears all entries.
func (d *debouncer[T]) Close() {
	for i := range d.shards {
		shard := &d.shards[i]
		shard.mu.Lock()
		for dk, e := range shard.entries {
			e.timer.Stop()
			delete(shard.entries, dk)
		}
		shard.mu.Unlock()
	}
}
