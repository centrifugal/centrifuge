package centrifuge

import (
	"sync"
)

// offsetWatermarks detect offsets a Redis server reused after losing writes.
//
// A Redis broker assigns a publication's offset and publishes it in one
// script, and a node handles the PUB/SUB messages of a channel in order, so
// within an epoch the offsets a node receives over PUB/SUB only grow. An offset
// at or below the last one received means the server gave out the same offsets
// again: it lost writes (a failover to a replica that missed them, a restart
// from an older snapshot) without the stream getting a new epoch. Clients
// positioned past that offset would skip the new publications as duplicates.
//
// The epoch is then lost for the channel on this node: every publication of
// it, starting with the one that revealed the loss, reaches local subscribers
// under a different epoch, so they resync, and the broker resets the stream in
// Redis, so that recovering from the lost epoch fails for everyone.
//
// Only PUB/SUB deliveries count, never history reads: a subscriber's position
// can legitimately be ahead of PUB/SUB right after it subscribed.
type offsetWatermarks struct {
	shards [64]watermarkShard
}

type watermarkShard struct {
	mu sync.Mutex
	m  map[string]watermark
}

type watermark struct {
	epoch  string
	offset uint64
	lost   bool
}

// lostEpochSuffix is appended to the epoch of a lost stream's publications:
// a client holding the epoch sees a mismatch and resyncs.
const lostEpochSuffix = ":lost"

func newOffsetWatermarks() *offsetWatermarks {
	w := &offsetWatermarks{}
	for i := range w.shards {
		w.shards[i].m = make(map[string]watermark)
	}
	return w
}

func (w *offsetWatermarks) shard(ch string) *watermarkShard {
	return &w.shards[index(ch, len(w.shards))]
}

// observe records a publication received over PUB/SUB. It returns the epoch to
// deliver it with, and true when this publication revealed the loss (the
// caller resets the stream once).
func (w *offsetWatermarks) observe(ch string, epoch string, offset uint64, track func() bool) (string, bool) {
	if offset == 0 || epoch == "" {
		return epoch, false
	}
	s := w.shard(ch)
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[ch]
	if !ok || cur.epoch != epoch {
		if !ok && !track() {
			// No local subscribers: nothing to protect, and no entry to leak.
			return epoch, false
		}
		s.m[ch] = watermark{epoch: epoch, offset: offset}
		return epoch, false
	}
	if cur.lost {
		return epoch + lostEpochSuffix, false
	}
	if offset <= cur.offset {
		cur.lost = true
		s.m[ch] = cur
		return epoch + lostEpochSuffix, true
	}
	cur.offset = offset
	s.m[ch] = cur
	return epoch, false
}

// forget drops a channel the node no longer subscribes to.
func (w *offsetWatermarks) forget(channels ...string) {
	for _, ch := range channels {
		s := w.shard(ch)
		s.mu.Lock()
		delete(s.m, ch)
		s.mu.Unlock()
	}
}
