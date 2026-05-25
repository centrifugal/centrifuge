// Package queue provides an unbounded MPSC-friendly generic queue with
// dynamic grow/shrink, doorbell-based blocking Wait, and atomic-load
// fast paths for Len / Size / Cap / Closed.
package queue

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/protocol"
)

// Item is the per-message payload used by the connection writer queue.
// Kept in this package so callers don't import an extra one just for
// the type. Other queue users may parameterise Queue with any T.
type Item struct {
	Data      []byte
	Channel   string
	Key       string
	FrameType protocol.FrameType
}

// ItemSize is the default Size function for a Queue[Item] — returns
// the byte length of the Data payload.
func ItemSize(i Item) int { return len(i.Data) }

// Queue is an unbounded queue of T. It is goroutine safe and tuned for
// multi-producer / single-consumer use (which matches the per-connection
// writer in centrifuge). The implementation is a dynamic ring buffer
// guarded by a mutex on Add / Remove, with atomic counters and a chan
// doorbell so Len / Size / Cap / Closed and Wait can run without
// acquiring the ring mutex.
//
// Inspired by http://blog.dubbelboer.com/2015/04/25/go-faster-queue.html (MIT).
type Queue[T any] struct {
	// Hot-path atomics. Read without holding mu. Writes happen under mu
	// so atomic readers see values consistent with a recent mutation.
	cnt    atomic.Int64
	size   atomic.Int64
	cp     atomic.Int64
	closed atomic.Bool

	// notify is a buffered (cap 1) doorbell. Producers send on enqueue;
	// a parked Wait consumes one.
	notify chan struct{}

	// closeCh is closed by Close / CloseRemaining and broadcast-wakes
	// any parked Wait.
	closeCh chan struct{}

	// sizeFn maps an item to its bookkept byte size. Must not be nil.
	sizeFn func(T) int

	// mu protects the ring (nodes, head, tail, initCap, shrinkTimer).
	mu          sync.Mutex
	nodes       []T
	head, tail  int
	initCap     int
	shrinkTimer *time.Timer
}

// New returns a new Queue[T] with the given initial capacity. sizeFn
// is used to compute the per-item byte size tracked by Size(); pass a
// function returning 0 if size tracking isn't needed.
func New[T any](initialCapacity int, sizeFn func(T) int) *Queue[T] {
	if sizeFn == nil {
		sizeFn = func(T) int { return 0 }
	}
	q := &Queue[T]{
		initCap: initialCapacity,
		nodes:   make([]T, initialCapacity),
		notify:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
		sizeFn:  sizeFn,
	}
	q.cp.Store(int64(initialCapacity))
	return q
}

// resize must be called with q.mu held. It rotates the ring to a new
// capacity and updates the atomic Cap mirror.
func (q *Queue[T]) resize(n int) {
	nodes := make([]T, n)
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
// is already full, the wake is coalesced.
func (q *Queue[T]) wake() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Add an item to the back of the queue. Returns false if the queue is
// closed. In that case the item is dropped.
func (q *Queue[T]) Add(i T) bool {
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
	q.size.Add(int64(q.sizeFn(i)))
	q.cnt.Add(1)
	q.mu.Unlock()
	q.wake()
	return true
}

// AddMany items to the back of the queue. Returns false if the queue
// is closed (no items added in that case). An empty items slice on an
// open queue is a successful no-op (returns true); on a closed queue
// it still returns false — matching the closed-queue-rejects-all
// semantics of the original implementation.
func (q *Queue[T]) AddMany(items ...T) bool {
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
		added += int64(q.sizeFn(it))
	}
	q.size.Add(added)
	q.cnt.Add(int64(len(items)))
	q.mu.Unlock()
	q.wake()
	return true
}

// Close the queue and discard all entries in the queue. All goroutines
// in Wait will return.
func (q *Queue[T]) Close() {
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
func (q *Queue[T]) CloseRemaining() []T {
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return []T{}
	}
	cnt := int(q.cnt.Load())
	rem := make([]T, 0, cnt)
	var zero T
	for i := 0; i < cnt; i++ {
		rem = append(rem, q.nodes[q.head])
		q.nodes[q.head] = zero
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
func (q *Queue[T]) Closed() bool {
	return q.closed.Load()
}

// Wait for an item to be added. If there are items on the queue Wait
// will return immediately. Returns false if the queue is closed.
func (q *Queue[T]) Wait() bool {
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
			// Spurious wake — claim was consumed by a racing reader
			// or a stale notify left from a prior cycle. Re-park.
		case <-q.closeCh:
			return false
		}
	}
}

// Remove will remove an item from the queue. If false is returned, it
// either means 1) there were no items on the queue or 2) the queue is
// closed.
func (q *Queue[T]) Remove() (T, bool) {
	var zero T
	q.mu.Lock()
	if q.cnt.Load() == 0 {
		q.mu.Unlock()
		return zero, false
	}
	it := q.nodes[q.head]
	q.nodes[q.head] = zero
	q.head = (q.head + 1) % len(q.nodes)
	q.cnt.Add(-1)
	q.size.Add(-int64(q.sizeFn(it)))

	if n := len(q.nodes) / 2; n >= q.initCap && int(q.cnt.Load()) <= n {
		q.resize(n)
	}
	q.mu.Unlock()
	return it, true
}

// RemoveMany removes up to maxItems items from the queue. If maxItems
// is -1, it removes all available items. Returns the slice of removed
// items and a boolean indicating whether at least one item was removed
// (false means no items were available).
func (q *Queue[T]) RemoveMany(maxItems int) ([]T, bool) {
	var zero T
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
	items := make([]T, count)
	var removedBytes int64
	for i := 0; i < count; i++ {
		items[i] = q.nodes[q.head]
		q.nodes[q.head] = zero
		q.head = (q.head + 1) % len(q.nodes)
		removedBytes += int64(q.sizeFn(items[i]))
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
// whether at least one item was removed. The caller must provide a
// buffer with sufficient capacity. This method does not perform
// shrinking — that's deferred to FinishCollect.
func (q *Queue[T]) RemoveManyInto(buf []T, maxItems int) (int, bool) {
	var zero T
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
		q.nodes[q.head] = zero
		q.head = (q.head + 1) % len(q.nodes)
		removedBytes += int64(q.sizeFn(buf[i]))
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
func (q *Queue[T]) Cap() int { return int(q.cp.Load()) }

// Len returns the current length of the queue. Lock-free.
func (q *Queue[T]) Len() int { return int(q.cnt.Load()) }

// Size returns the current bookkept byte total. Lock-free.
func (q *Queue[T]) Size() int { return int(q.size.Load()) }

// FinishCollect marks the end of batch collection and schedules
// delayed shrinking. If shrinkDelay is 0, shrinks immediately;
// otherwise schedules a shrink after the delay. Under load the timer
// keeps resetting, keeping the queue at working-set size.
func (q *Queue[T]) FinishCollect(shrinkDelay time.Duration) {
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
func (q *Queue[T]) doShrinkLocked() {
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
