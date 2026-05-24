// Package queue provides an unbounded MPSC-friendly queue of Item with
// dynamic grow/shrink, doorbell-based blocking Wait, and atomic-load fast
// paths for Len/Size/Cap/Closed.
package queue

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/protocol"
)

type Item struct {
	Data      []byte
	Channel   string
	Key       string
	FrameType protocol.FrameType
}

// Queue is an unbounded queue of Item. It is goroutine safe and tuned for
// multi-producer / single-consumer use (which matches the per-connection
// writer in centrifuge). External behavior matches the previous mutex+cond
// implementation; the difference is purely operational:
//
//   - Len / Size / Cap / Closed are lock-free atomic loads.
//   - Wait is a select on a chan doorbell + closeCh (no cond, no mutex
//     acquired by Wait itself unless there is nothing to read).
//   - Add / Remove still take the ring mutex (the ring grows/shrinks
//     dynamically), but the critical sections are tight and free of
//     defers on the hot paths.
//
// Inspired by http://blog.dubbelboer.com/2015/04/25/go-faster-queue.html (MIT).
type Queue struct {
	// Hot-path atomics. Read without holding mu. Writes happen under mu
	// so atomic readers see values consistent with a recent mutation.
	cnt    atomic.Int64
	size   atomic.Int64
	cp     atomic.Int64
	closed atomic.Bool

	// notify is a buffered (cap 1) doorbell. Producers send on enqueue;
	// a parked Wait consumes one. Multiple items accumulate while the
	// chan is full — Wait performs a "baton pass" send on wake-up so a
	// second waiter still gets a turn.
	notify chan struct{}

	// closeCh is closed by Close / CloseRemaining and broadcast-wakes
	// any parked Wait.
	closeCh chan struct{}

	// mu protects the ring (nodes, head, tail, initCap, shrinkTimer).
	mu          sync.Mutex
	nodes       []Item
	head, tail  int
	initCap     int
	shrinkTimer *time.Timer
}

// New returns a new Item queue with initial capacity.
func New(initialCapacity int) *Queue {
	q := &Queue{
		initCap: initialCapacity,
		nodes:   make([]Item, initialCapacity),
		notify:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	q.cp.Store(int64(initialCapacity))
	return q
}

// resize must be called with q.mu held. It rotates the ring to a new
// capacity and updates the atomic Cap mirror.
func (q *Queue) resize(n int) {
	nodes := make([]Item, n)
	cnt := int(q.cnt.Load())
	if cnt == 0 {
		q.head = 0
		q.tail = 0
		q.nodes = nodes
		q.cp.Store(int64(n))
		return
	}
	if q.head < q.tail {
		copy(nodes, q.nodes[q.head:q.tail])
	} else {
		c := copy(nodes, q.nodes[q.head:])
		copy(nodes[c:], q.nodes[:q.tail])
	}
	q.tail = cnt % n
	q.head = 0
	q.nodes = nodes
	q.cp.Store(int64(n))
}

// wake performs a non-blocking send on the doorbell. If the doorbell
// is already full, the wake is coalesced — the existing pending wake
// will serve the next reader.
func (q *Queue) wake() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Add an Item to the back of the queue. Returns false if the queue is
// closed. In that case the Item is dropped.
func (q *Queue) Add(i Item) bool {
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return false
	}
	if int(q.cnt.Load()) == len(q.nodes) {
		q.resize(len(q.nodes) * 2)
	}
	q.nodes[q.tail] = i
	q.tail = (q.tail + 1) % len(q.nodes)
	q.size.Add(int64(len(i.Data)))
	q.cnt.Add(1)
	q.mu.Unlock()
	q.wake()
	return true
}

// AddMany items to the back of the queue. Returns false if the queue
// is closed (no items added in that case). An empty items slice on an
// open queue is a successful no-op (returns true); on a closed queue
// it still returns false — matching the original implementation's
// reject-everything-when-closed semantics.
func (q *Queue) AddMany(items ...Item) bool {
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return false
	}
	if len(items) == 0 {
		q.mu.Unlock()
		return true
	}
	have := int(q.cnt.Load())
	need := have + len(items)
	if need > len(q.nodes) {
		newCap := len(q.nodes)
		if newCap == 0 {
			newCap = q.initCap
		}
		for newCap < need {
			newCap *= 2
		}
		q.resize(newCap)
	}
	var added int64
	for _, it := range items {
		q.nodes[q.tail] = it
		q.tail = (q.tail + 1) % len(q.nodes)
		added += int64(len(it.Data))
	}
	q.size.Add(added)
	q.cnt.Add(int64(len(items)))
	q.mu.Unlock()
	q.wake()
	return true
}

// Close the queue and discard all entries in the queue. All goroutines
// in Wait will return.
func (q *Queue) Close() {
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return
	}
	q.closed.Store(true)
	q.cnt.Store(0)
	q.size.Store(0)
	q.cp.Store(0)
	q.nodes = nil
	if q.shrinkTimer != nil {
		q.shrinkTimer.Stop()
		q.shrinkTimer = nil
	}
	q.mu.Unlock()
	close(q.closeCh)
}

// CloseRemaining will close the queue and return all entries in the
// queue. All goroutines in Wait will return.
func (q *Queue) CloseRemaining() []Item {
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return []Item{}
	}
	cnt := int(q.cnt.Load())
	rem := make([]Item, 0, cnt)
	for i := 0; i < cnt; i++ {
		rem = append(rem, q.nodes[q.head])
		q.nodes[q.head] = Item{}
		q.head = (q.head + 1) % len(q.nodes)
	}
	q.closed.Store(true)
	q.cnt.Store(0)
	q.size.Store(0)
	q.cp.Store(0)
	q.nodes = nil
	if q.shrinkTimer != nil {
		q.shrinkTimer.Stop()
		q.shrinkTimer = nil
	}
	q.mu.Unlock()
	close(q.closeCh)
	return rem
}

// Closed returns true if the queue has been closed. Lock-free.
func (q *Queue) Closed() bool {
	return q.closed.Load()
}

// Wait for a message to be added. If there are items on the queue Wait
// will return immediately. Returns false if the queue is closed.
func (q *Queue) Wait() bool {
	// Fast path: items already present.
	if q.cnt.Load() > 0 {
		return true
	}
	if q.closed.Load() {
		return false
	}
	for {
		select {
		case <-q.notify:
			if q.cnt.Load() > 0 {
				return true
			}
			if q.closed.Load() {
				return false
			}
			// Spurious wake (e.g. a stray notify left after a
			// closed-but-not-yet-observed window). Re-arm and loop.
		case <-q.closeCh:
			return false
		}
	}
}

// Remove will remove an Item from the queue. If false is returned, it
// either means 1) there were no items on the queue or 2) the queue is
// closed.
func (q *Queue) Remove() (Item, bool) {
	q.mu.Lock()
	if q.cnt.Load() == 0 {
		q.mu.Unlock()
		return Item{}, false
	}
	it := q.nodes[q.head]
	q.nodes[q.head] = Item{}
	q.head = (q.head + 1) % len(q.nodes)
	q.cnt.Add(-1)
	q.size.Add(-int64(len(it.Data)))

	if n := len(q.nodes) / 2; n >= q.initCap && int(q.cnt.Load()) <= n {
		q.resize(n)
	}
	q.mu.Unlock()
	return it, true
}

// RemoveMany removes up to maxItems items from the queue. If maxItems
// is -1, it removes all available items. Returns the slice of removed
// items and a boolean indicating whether at least one item was removed
// (false means no messages were available).
func (q *Queue) RemoveMany(maxItems int) ([]Item, bool) {
	q.mu.Lock()
	cnt := int(q.cnt.Load())
	if cnt == 0 {
		q.mu.Unlock()
		return nil, false
	}
	count := cnt
	if maxItems != -1 && count > maxItems {
		count = maxItems
	}
	items := make([]Item, count)
	var removedBytes int64
	for i := 0; i < count; i++ {
		items[i] = q.nodes[q.head]
		q.nodes[q.head] = Item{}
		q.head = (q.head + 1) % len(q.nodes)
		removedBytes += int64(len(items[i].Data))
	}
	q.cnt.Add(-int64(count))
	q.size.Add(-removedBytes)

	n := -1
	k := len(q.nodes) / 2
	rem := int(q.cnt.Load())
	for k >= q.initCap && rem <= k {
		n = k
		k /= 2
	}
	if n != -1 {
		q.resize(n)
	}
	q.mu.Unlock()
	return items, true
}

// RemoveManyInto removes up to maxItems items from the queue into the
// provided buffer. If maxItems is -1, it removes all available items.
// Returns the number of items actually removed and a boolean indicating
// whether at least one item was removed (false means no messages were
// available). The caller must provide a buffer with sufficient capacity.
// This method does not perform shrinking — that's deferred to FinishCollect.
func (q *Queue) RemoveManyInto(buf []Item, maxItems int) (int, bool) {
	q.mu.Lock()
	cnt := int(q.cnt.Load())
	if cnt == 0 {
		q.mu.Unlock()
		return 0, false
	}
	count := cnt
	if maxItems != -1 && count > maxItems {
		count = maxItems
	}
	if count > len(buf) {
		count = len(buf)
	}
	var removedBytes int64
	for i := 0; i < count; i++ {
		buf[i] = q.nodes[q.head]
		q.nodes[q.head] = Item{}
		q.head = (q.head + 1) % len(q.nodes)
		removedBytes += int64(len(buf[i].Data))
	}
	q.cnt.Add(-int64(count))
	q.size.Add(-removedBytes)
	if q.cnt.Load() == 0 {
		q.head = 0
		q.tail = 0
	}
	q.mu.Unlock()
	return count, true
}

// Cap returns the current allocated capacity. Lock-free.
func (q *Queue) Cap() int {
	return int(q.cp.Load())
}

// Len returns the current length of the queue. Lock-free.
func (q *Queue) Len() int {
	return int(q.cnt.Load())
}

// Size returns the current sum of Data byte lengths in the queue. Lock-free.
func (q *Queue) Size() int {
	return int(q.size.Load())
}

// FinishCollect marks the end of batch collection and schedules delayed
// shrinking. If shrinkDelay is 0, shrinks immediately. Otherwise schedules
// shrink after delay. Under load the timer keeps resetting, keeping the
// queue at working-set size.
func (q *Queue) FinishCollect(shrinkDelay time.Duration) {
	q.mu.Lock()
	if shrinkDelay == 0 {
		q.doShrinkLocked()
		q.mu.Unlock()
		return
	}
	if q.shrinkTimer == nil {
		q.shrinkTimer = time.AfterFunc(shrinkDelay, func() {
			q.mu.Lock()
			q.doShrinkLocked()
			q.mu.Unlock()
		})
	} else {
		q.shrinkTimer.Reset(shrinkDelay)
	}
	q.mu.Unlock()
}

// doShrinkLocked performs the actual shrinking. Must be called with q.mu held.
func (q *Queue) doShrinkLocked() {
	if q.closed.Load() {
		return
	}
	if q.cnt.Load() == 0 {
		q.head = 0
		q.tail = 0
	}
	n := -1
	k := len(q.nodes) / 2
	cnt := int(q.cnt.Load())
	for k >= q.initCap && cnt <= k {
		n = k
		k /= 2
	}
	if n != -1 {
		q.resize(n)
	}
}
