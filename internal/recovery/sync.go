package recovery

import (
	"sync"
	"sync/atomic"

	"github.com/centrifugal/protocol"
)

// PubSubSync synchronizes the recovery of a subscription with publications which
// come from PUB/SUB while the client is subscribing. T is what the client queues to
// write a publication later.
//
// A channel the client subscribes to goes through three phases:
//
//  1. From StartBuffering until ReadBuffered (the sync point) publications are
//     collected. ReadBuffered returns them, and the subscriber merges them with
//     what it read from history into its subscribe result, unless one of them is
//     from another epoch than that history.
//  2. From ReadBuffered until StopBuffering publications are queued: the
//     subscription is not committed yet, and its result is not written.
//  3. StopBuffering is called by the subscriber once its result is written. It
//     writes the queued publications in order, and lets the following ones through.
//
// SyncPublication never waits for the subscriber. A publication is broadcast with
// its channel's hub shard lock held, and the subscriber may need that lock (or wait
// for something which does) before it gets to StopBuffering.
//
// The zero value is ready to use.
type PubSubSync[T any] struct {
	// numBuffers is the number of buffers in the map, so that broadcasts to a
	// client which is not subscribing don't take the mutex.
	numBuffers atomic.Int32
	mu         sync.RWMutex
	buffers    map[string]*Buffer[T] // Made on first use: most clients never need it.
}

type bufferPhase uint8

const (
	phaseCollecting bufferPhase = iota
	phaseQueueing
	phaseStopped
)

// Buffer is the state of one subscribe attempt to a channel. A nil *Buffer is valid
// in all PubSubSync methods and means nothing is buffered.
type Buffer[T any] struct {
	channel      string
	maxQueueSize int
	mu           sync.Mutex
	phase        bufferPhase
	pubs         []*protocol.Publication
	// epoch of the collected publications, and whether they have more than one.
	epoch       string
	mixedEpochs bool
	// queue holds the items of phase 2 in chunks, so that growing it never copies
	// them. Chunks double in size up to maxQueueChunk items.
	queue      [][]T
	queueSize  int
	overflowed bool
}

const (
	minQueueChunk = 4
	maxQueueChunk = 64
)

func (b *Buffer[T]) enqueue(item T) {
	n := len(b.queue)
	if n == 0 || len(b.queue[n-1]) == cap(b.queue[n-1]) {
		size := minQueueChunk
		if n > 0 {
			size = min(2*cap(b.queue[n-1]), maxQueueChunk)
		}
		b.queue = append(b.queue, make([]T, 0, size))
		n++
	}
	b.queue[n-1] = append(b.queue[n-1], item)
}

// StartBuffering starts phase 1 for the channel. It must be called before the
// subscription is added to the hub, so that no publication for it is missed.
// maxQueueSize limits the size of the data of the publications queued in phase 2,
// zero means no limit.
func (s *PubSubSync[T]) StartBuffering(channel string, maxQueueSize int) *Buffer[T] {
	b := &Buffer[T]{channel: channel, maxQueueSize: maxQueueSize}
	s.mu.Lock()
	if s.buffers == nil {
		s.buffers = make(map[string]*Buffer[T])
	}
	if _, ok := s.buffers[channel]; !ok {
		s.numBuffers.Add(1)
	}
	s.buffers[channel] = b
	s.mu.Unlock()
	return b
}

// Buffering reports whether any channel is buffering. It lets the broadcast path
// skip SyncPublication (and building its queued item) for clients which are not
// subscribing. A subscription gets into the hub after StartBuffering, so a
// publication broadcast to it always sees its buffer.
func (s *PubSubSync[T]) Buffering() bool {
	return s.numBuffers.Load() > 0
}

// SyncPublication takes a publication of the given epoch into the channel's buffer
// if the channel is buffering: in phase 1 pub is collected, in phase 2 item is
// queued, and size counts towards the queue limit. It returns false if the channel
// isn't buffering, and then the caller writes the publication itself. It never
// blocks for long.
func (s *PubSubSync[T]) SyncPublication(channel string, pub *protocol.Publication, epoch string, size int, item T) bool {
	s.mu.RLock()
	b, ok := s.buffers[channel]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.phase {
	case phaseCollecting:
		if len(b.pubs) == 0 {
			b.epoch = epoch
		} else if epoch != b.epoch {
			b.mixedEpochs = true
		}
		b.pubs = append(b.pubs, pub)
		return true
	case phaseQueueing:
		if b.overflowed {
			return true
		}
		b.queueSize += size
		if b.maxQueueSize > 0 && b.queueSize > b.maxQueueSize {
			// Too much to keep. The subscriber learns about it from StopBuffering.
			b.overflowed = true
			b.queue = nil
			return true
		}
		b.enqueue(item)
		return true
	default:
		// Stopped: everything queued has been written already.
		return false
	}
}

// ReadBuffered is the sync point: it returns the publications collected in phase 1
// and starts phase 2. epoch is the epoch of the history the subscriber read: ok is
// false if a collected publication is from another one, then they can't be merged
// with it. They are compared with an empty epoch (the read knew nothing about the
// stream, a lagging replica) only among themselves: they must all be from one.
func (s *PubSubSync[T]) ReadBuffered(b *Buffer[T], epoch string) (pubs []*protocol.Publication, ok bool) {
	if b == nil {
		return nil, true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.phase != phaseCollecting {
		return nil, true
	}
	pubs = b.pubs
	ok = len(pubs) == 0 || (!b.mixedEpochs && (epoch == "" || b.epoch == epoch))
	b.pubs = nil
	b.phase = phaseQueueing
	return pubs, ok
}

// StopBuffering writes the items queued in phase 2 with write, in order, and lets
// the following publications through. It must be called after the subscription is
// committed and its result is written. It returns true if the queue overflowed:
// then publications were dropped, and the caller must make the client resubscribe.
// Calls after the first one (and after CancelBuffering) do nothing.
func (s *PubSubSync[T]) StopBuffering(b *Buffer[T], write func(T)) (overflowed bool) {
	if b == nil {
		return false
	}
	for {
		b.mu.Lock()
		if b.phase == phaseStopped {
			b.mu.Unlock()
			return false
		}
		if len(b.queue) == 0 {
			// Only once the queue is empty: a publication which comes while the
			// previous batch is written must be queued behind it.
			overflowed = b.overflowed
			b.stopLocked()
			b.mu.Unlock()
			s.remove(b)
			return overflowed
		}
		queue := b.queue
		b.queue = nil
		b.mu.Unlock()
		for _, chunk := range queue {
			for _, item := range chunk {
				write(item)
			}
		}
	}
}

// CancelBuffering drops everything buffered for a subscribe attempt which failed.
// Calls after the first one (and after StopBuffering) do nothing.
func (s *PubSubSync[T]) CancelBuffering(b *Buffer[T]) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.phase == phaseStopped {
		b.mu.Unlock()
		return
	}
	b.stopLocked()
	b.mu.Unlock()
	s.remove(b)
}

func (b *Buffer[T]) stopLocked() {
	b.phase = phaseStopped
	b.pubs = nil
	b.queue = nil
}

// remove deletes b from the map, unless a newer subscribe attempt to the channel
// has replaced it.
func (s *PubSubSync[T]) remove(b *Buffer[T]) {
	s.mu.Lock()
	if s.buffers[b.channel] == b {
		delete(s.buffers, b.channel)
		s.numBuffers.Add(-1)
	}
	s.mu.Unlock()
}
