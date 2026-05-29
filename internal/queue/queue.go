// Package queue provides an unbounded MPSC-friendly generic queue with
// dynamic grow/shrink, sync.Cond-based blocking Wait, and atomic-load
// fast paths for Cap / Closed. Byte-size bookkeeping lives under
// the queue mutex; Add / AddMany return the post-insert byte size so the
// hot enqueue path needs no separate Size() call.
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
// guarded by a mutex on Add / Remove, with sync.Cond for blocking Wait. The length and byte-size totals are kept under the mutex (plain
// int64) rather than as atomics: an atomic read-modify-write per
// enqueue/dequeue would bounce its cache line across all producer cores
// on every operation (independently of the mutex line), which dominates
// throughput under producer contention. Add / AddMany return the
// post-insert size so the hot enqueue path (writer.enqueue's
// MaxQueueSize check) reads it under the lock it already holds instead
// of taking a second synchronization. cp (capacity mirror) and closed
// stay atomic because they are written only on resize / close, so
// reading them lock-free costs no per-operation contention.
//
// Inspired by http://blog.dubbelboer.com/2015/04/25/go-faster-queue.html (MIT).
type Queue[T any] struct {
	// Rarely-written atomics, safe to read lock-free without per-op
	// cache-line contention. cp changes only on resize, closed once.
	cp     atomic.Int64
	closed atomic.Bool

	// sizeFn maps an item to its bookkept byte size. Must not be nil.
	sizeFn func(T) int

	// mu protects the ring (nodes, head, tail, initCap, shrinkTimer) and
	// the length / byte-size totals. cond (on mu) is the blocking wakeup
	// for Wait: a plain sync.Mutex + sync.Cond beats a channel doorbell
	// here — channel send/recv take the runtime hchan lock on every
	// park/unpark, which dominates when the single consumer parks often
	// (large / unlimited drains), while keeping sync.Mutex (lighter than
	// RWMutex) preserves the win under short-hold producer contention.
	mu    sync.Mutex
	cond  *sync.Cond
	nodes []T
	// sizes is a ring parallel to nodes holding each item's byte size,
	// so the consumer drains by summing plain int64s instead of calling
	// sizeFn per item under the lock — shortening the drain critical
	// section for large frames under producer contention.
	sizes       []int64
	head, tail  int
	cnt         int64
	size        int64
	initCap     int
	shrinkTimer *time.Timer
	// waiting is true while the (single) consumer is parked in Wait.
	// Producers call cond.Signal only when it is set, skipping the
	// wakeup when the consumer is draining or absent.
	waiting bool
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
		sizeFn:  sizeFn,
	}
	q.cond = sync.NewCond(&q.mu)
	q.cp.Store(int64(initialCapacity))
	return q
}

// resize must be called with q.mu held. It rotates the ring to a new
// capacity and updates the atomic Cap mirror.
func (q *Queue[T]) resize(n int) {
	nodes := make([]T, n)
	cnt := int(q.cnt)
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

// Add an item to the back of the queue. Returns the queue's byte size
// after the insert and true on success, or (0, false) if the queue is
// closed (in which case the item is dropped). The returned size lets the
// caller's MaxQueueSize check avoid a separate Size() call.
func (q *Queue[T]) Add(i T) (int, bool) {
	// Compute the per-item size outside the critical section: sizeFn is
	// an indirect call and must not lengthen the contended mutex hold.
	sz := int64(q.sizeFn(i))
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return 0, false
	}
	if int(q.cnt) == len(q.nodes) {
		q.resize(len(q.nodes) * 2)
	}
	q.nodes[q.tail] = i
	q.tail = (q.tail + 1) % len(q.nodes)
	q.size += sz
	size := int(q.size)
	q.cnt++
	if q.waiting { // wake the parked consumer, if any
		q.cond.Signal()
	}
	q.mu.Unlock()
	return size, true
}

// AddMany items to the back of the queue. Returns the queue's byte size
// after the insert and true on success, or (0, false) if the queue is
// closed (no items added in that case). An empty items slice on an open
// queue is a successful no-op (returns the current size and true); on a
// closed queue it still returns (0, false) — matching the
// closed-queue-rejects-all semantics of the original implementation.
func (q *Queue[T]) AddMany(items ...T) (int, bool) {
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return 0, false
	}
	if len(items) == 0 {
		size := int(q.size)
		q.mu.Unlock()
		return size, true
	}
	have := int(q.cnt)
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
		sz := int64(q.sizeFn(it))
		q.nodes[q.tail] = it
		q.tail = (q.tail + 1) % len(q.nodes)
		added += sz
	}
	q.size += added
	size := int(q.size)
	q.cnt += int64(len(items))
	if q.waiting { // wake the parked consumer, if any
		q.cond.Signal()
	}
	q.mu.Unlock()
	return size, true
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
	q.cnt = 0
	q.size = 0
	q.cp.Store(0)
	q.nodes = nil
	if q.shrinkTimer != nil {
		q.shrinkTimer.Stop()
		q.shrinkTimer = nil
	}
	q.cond.Broadcast()
	q.mu.Unlock()
}

// CloseRemaining will close the queue and return all entries in the
// queue. All goroutines in Wait will return.
func (q *Queue[T]) CloseRemaining() []T {
	q.mu.Lock()
	if q.closed.Load() {
		q.mu.Unlock()
		return []T{}
	}
	cnt := int(q.cnt)
	rem := make([]T, 0, cnt)
	var zero T
	for i := 0; i < cnt; i++ {
		rem = append(rem, q.nodes[q.head])
		q.nodes[q.head] = zero
		q.head = (q.head + 1) % len(q.nodes)
	}
	q.closed.Store(true)
	q.cnt = 0
	q.size = 0
	q.cp.Store(0)
	q.nodes = nil
	if q.shrinkTimer != nil {
		q.shrinkTimer.Stop()
		q.shrinkTimer = nil
	}
	q.cond.Broadcast()
	q.mu.Unlock()
	return rem
}

// Closed returns true if the queue has been closed. Lock-free.
func (q *Queue[T]) Closed() bool {
	return q.closed.Load()
}

// Wait for an item to be added. If there are items on the queue Wait
// will return immediately. Returns false if the queue is closed.
//
// cnt is read under the mutex (briefly) rather than atomically: that
// keeps it plain on the contended Add/Remove path. The mutex is never
// held while parked — the doorbell is a buffered (cap 1) chan, so an Add
// that happens between the cnt check and the park still delivers a token
// the parked select consumes, hence no lost wakeup.
//
// The waiting flag (set under the lock just before cond.Wait) lets Add
// skip cond.Signal whenever no consumer is parked — i.e. while the single
// consumer is busy draining, or when there is none. Because cnt++ and the
// waiting read happen under the same lock the consumer uses to check cnt
// and set waiting, a producer either sees the item (consumer returns
// without parking) or sees waiting==true (and signals): no lost wakeup.
func (q *Queue[T]) Wait() bool {
	q.mu.Lock()
	for q.cnt == 0 {
		if q.closed.Load() {
			q.waiting = false
			q.mu.Unlock()
			return false
		}
		q.waiting = true
		q.cond.Wait()
	}
	q.waiting = false
	q.mu.Unlock()
	return true
}

// Remove will remove an item from the queue. If false is returned, it
// either means 1) there were no items on the queue or 2) the queue is
// closed.
func (q *Queue[T]) Remove() (T, bool) {
	var zero T
	q.mu.Lock()
	if q.cnt == 0 {
		q.mu.Unlock()
		return zero, false
	}
	it := q.nodes[q.head]
	q.nodes[q.head] = zero
	q.head = (q.head + 1) % len(q.nodes)
	q.cnt--
	q.size -= int64(q.sizeFn(it))

	if n := len(q.nodes) / 2; n >= q.initCap && int(q.cnt) <= n {
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
	cnt := int(q.cnt)
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
	q.cnt -= int64(count)
	q.size -= removedBytes

	n := -1
	k := len(q.nodes) / 2
	rem := int(q.cnt)
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
	cnt := int(q.cnt)
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
	q.cnt -= int64(count)
	q.size -= removedBytes
	if q.cnt == 0 {
		q.head = 0
		q.tail = 0
	}
	q.mu.Unlock()
	return count, true
}

// Cap returns the current allocated capacity. Lock-free.
func (q *Queue[T]) Cap() int { return int(q.cp.Load()) }

// Len returns the current length of the queue. Takes the queue mutex
// (the counter lives under it); only the single consumer calls this on
// its drain path, so it does not contend with itself.
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	n := q.cnt
	q.mu.Unlock()
	return int(n)
}

// Size returns the current bookkept byte total. Unlike Len / Cap /
// Closed this takes the queue mutex, because the byte total lives under
// it. The hot enqueue path should use the size returned by Add /
// AddMany instead of calling this.
func (q *Queue[T]) Size() int {
	q.mu.Lock()
	s := q.size
	q.mu.Unlock()
	return int(s)
}

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
	if q.cnt == 0 {
		q.head = 0
		q.tail = 0
	}
	n := -1
	k := len(q.nodes) / 2
	cnt := int(q.cnt)
	for k >= q.initCap && cnt <= k {
		n = k
		k /= 2
	}
	if n != -1 {
		q.resize(n)
	}
}
