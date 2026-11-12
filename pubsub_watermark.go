package centrifuge

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/centrifugal/protocol"
	"github.com/redis/rueidis"
)

// offsetWatermarks detect offsets a Redis server reused after losing writes.
//
// A Redis broker assigns a publication's offset and publishes it in one
// script, and a node handles the PUB/SUB messages of a channel in order, so
// within an epoch the offsets a node receives over PUB/SUB normally only grow.
// An offset at or below the last one received is a suspect: the server may
// have given out the same offsets again because it lost writes (a failover to
// a replica that missed them, a restart from an older snapshot) without the
// stream getting a new epoch - clients positioned past that offset would skip
// the new publications as duplicates. Or it is only late: with classic PUB/SUB
// in Redis Cluster, messages of a channel published by two servers during a
// slot migration travel separate links and may arrive out of order.
//
// So a suspect is verified in Redis (see verifyReusedOffset): the history
// entry at the highest offset received must still be the publication received
// (its time). If it is, nothing was lost. If it isn't, or the stream went
// below it, the server lost writes: the epoch is lost for the channel on this
// node - its later publications reach local subscribers under a different
// epoch, so they resync - and the broker reacts (with the gate, the server's
// lineage rotates).
//
// Only PUB/SUB deliveries count, never history reads: a subscriber's position
// can legitimately be ahead of PUB/SUB right after it subscribed.
type offsetWatermarks struct {
	shards [64]watermarkShard
	// checks and losses count suspects verified and losses confirmed.
	checks atomic.Int64
	losses atomic.Int64
}

type watermarkShard struct {
	mu sync.Mutex
	m  map[string]watermark
}

type watermark struct {
	epoch  string
	offset uint64
	// time of the publication at offset (Publication.Time, milliseconds).
	time      int64
	lost      bool
	verifying bool
}

// watermarkCheck is a suspect to verify: the highest offset received for the
// channel's epoch and the time of its publication.
type watermarkCheck struct {
	ch     string
	epoch  string
	offset uint64
	time   int64
	got    uint64 // the offset that made it a suspect
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
// deliver it with, and a check when the offset is a suspect (one at a time per
// channel).
func (w *offsetWatermarks) observe(ch string, epoch string, offset uint64, pubTime int64, track func() bool) (string, *watermarkCheck) {
	if offset == 0 || epoch == "" {
		return epoch, nil
	}
	s := w.shard(ch)
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[ch]
	if !ok || cur.epoch != epoch {
		if !ok && !track() {
			// No local subscribers: nothing to protect, and no entry to leak.
			return epoch, nil
		}
		s.m[ch] = watermark{epoch: epoch, offset: offset, time: pubTime}
		return epoch, nil
	}
	if cur.lost {
		return epoch + lostEpochSuffix, nil
	}
	if offset <= cur.offset {
		if cur.verifying {
			return epoch, nil
		}
		cur.verifying = true
		s.m[ch] = cur
		return epoch, &watermarkCheck{ch: ch, epoch: epoch, offset: cur.offset, time: cur.time, got: offset}
	}
	cur.offset = offset
	cur.time = pubTime
	s.m[ch] = cur
	return epoch, nil
}

// settle ends a check: lost marks the epoch lost for the channel.
func (w *offsetWatermarks) settle(c *watermarkCheck, lost bool) {
	s := w.shard(c.ch)
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[c.ch]
	if !ok || cur.epoch != c.epoch {
		return
	}
	cur.verifying = false
	w.checks.Add(1)
	if lost {
		cur.lost = true
		w.losses.Add(1)
	}
	s.m[c.ch] = cur
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

// verifyReusedOffset tells whether a suspect (see offsetWatermarks) is a loss:
// the stream's history no longer holds the publication received at c.offset.
// It is not a loss when that entry is still there with the same publication
// (a late message), or when it can't tell: the epoch changed meanwhile, or the
// history was trimmed past the offset.
func verifyReusedOffset(ctx context.Context, client rueidis.Client, streamKey, metaKey string, c *watermarkCheck) (bool, error) {
	meta, err := client.Do(ctx, client.B().Hmget().Key(metaKey).Field("e", "s").Build()).ToArray()
	if err != nil {
		return false, err
	}
	if len(meta) < 2 {
		return false, nil
	}
	epoch, _ := meta[0].ToString()
	if pubsubEpoch(epoch) != c.epoch {
		return false, nil // reset meanwhile: nothing left to protect
	}
	top, _ := meta[1].AsUint64()
	if top < c.offset {
		return true, nil // the stream went back below what was received
	}
	id := strconv.FormatUint(c.offset, 10) + "-0"
	entries, err := client.Do(ctx, client.B().Xrange().Key(streamKey).Start(id).End(id).Build()).AsXRange()
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		first, err := client.Do(ctx, client.B().Xrange().Key(streamKey).Start("-").End("+").Count(1).Build()).AsXRange()
		if err != nil {
			return false, err
		}
		if len(first) == 0 {
			return false, nil
		}
		firstOffset, _ := strconv.ParseUint(strings.SplitN(first[0].ID, "-", 2)[0], 10, 64)
		// Trimmed past it: can't tell. Otherwise the entry is gone from a range
		// the stream still holds: lost.
		return firstOffset <= c.offset, nil
	}
	data, ok := entries[0].FieldValues["d"]
	if !ok {
		return false, nil
	}
	var pub protocol.Publication
	if err := pub.UnmarshalCF([]byte(data)); err != nil {
		return false, nil
	}
	return pub.Time != c.time, nil
}

// resyncLocalSubscribers tells the node's subscribers of a channel that their
// stream state is insufficient: they resubscribe and recover (or reload).
func (n *Node) resyncLocalSubscribers(ch string) {
	for _, c := range n.hub.subscriberClients(ch) {
		c.mu.RLock()
		chCtx, ok := c.channels[ch]
		c.mu.RUnlock()
		if !ok {
			continue
		}
		go c.handleInsufficientState(ch, channelHasFlag(chCtx.flags, flagServerSide))
	}
}
