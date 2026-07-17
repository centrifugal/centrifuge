package centrifuge

import (
	"sync"
	"sync/atomic"
	"time"
)

var timerEntryPool = sync.Pool{
	New: func() interface{} {
		return new(TimerEntry)
	},
}

// timerHandlePool is a global pool for reusing TimerHandle objects.
var timerHandlePool = sync.Pool{
	New: func() interface{} {
		return &TimerHandle{}
	},
}

// TimerEntry represents an individual timer scheduled in a wheel.
type TimerEntry struct {
	rotations int    // Number of full wheel rotations remaining.
	callback  func() // Function to call when the timer fires.
	cancelled bool   // Marks whether the timer has been cancelled.
	// generation guards against ABA reuse of a pooled entry. It is bumped
	// every time the entry is (re)scheduled and every time it fires or is
	// cancelled, so a TimerHandle that captured an older generation can detect
	// that "its" entry has since been recycled for a different timer and must
	// not be touched. It is atomic because the entry pool is shared across all
	// ShardedTimerWheel shards: a fired entry can be recycled by another shard's
	// AddTimer (writing generation under that shard's lock) while a stale handle
	// from the original shard reads generation under its own lock.
	generation atomic.Uint64
}

// TimerHandle implements TimerCanceler and is returned by TimerWheel.AddTimer.
type TimerHandle struct {
	wheel       *TimerWheel
	bucketIndex int
	entry       *TimerEntry
	generation  uint64     // Generation of entry captured when this handle was created.
	mu          sync.Mutex // Protects access to the entry.
}

// Cancel immediately cancels the timer and removes it from its bucket.
func (th *TimerHandle) Cancel() {
	th.mu.Lock()
	defer th.mu.Unlock()

	// If already cancelled or executed, nothing to do.
	if th.entry == nil {
		return
	}

	// Lock the wheel to safely remove the timer from its bucket.
	th.wheel.mu.Lock()
	// Guard against ABA reuse: if the entry has already fired (or was cancelled)
	// its generation was bumped, and it may since have been recycled from the
	// pool for an unrelated timer. Cancelling by pointer identity in that case
	// would silently drop another client's live timer, so bail out. This is the
	// normal centrifuge lifecycle: onTimerOp fires, then scheduleNextTimer calls
	// Cancel on the just-fired handle.
	if th.entry.generation.Load() != th.generation {
		th.wheel.mu.Unlock()
		th.entry = nil
		th.wheel = nil
		th.bucketIndex = 0
		timerHandlePool.Put(th)
		return
	}
	bucket := th.wheel.buckets[th.bucketIndex]
	newBucket := bucket[:0]
	for _, timer := range bucket {
		if timer == th.entry {
			// Mark the timer as cancelled and do not retain it. Bump the
			// generation first so any other stale handle to this entry bails.
			timer.cancelled = true
			timer.generation.Add(1)
			timerEntryPool.Put(timer)
			continue
		}
		newBucket = append(newBucket, timer)
	}
	th.wheel.buckets[th.bucketIndex] = newBucket
	th.entry = nil // Clear the entry
	th.wheel.mu.Unlock()

	// Clear the handle's state.
	th.entry = nil
	th.wheel = nil
	th.bucketIndex = 0

	// Return the handle to the pool for reuse.
	timerHandlePool.Put(th)
}

// newTimerHandle obtains a TimerHandle from the pool and initializes it.
func newTimerHandle(tw *TimerWheel, bucketIndex int, entry *TimerEntry) *TimerHandle {
	handle := timerHandlePool.Get().(*TimerHandle)
	handle.wheel = tw
	handle.bucketIndex = bucketIndex
	handle.entry = entry
	handle.generation = entry.generation.Load()
	// Note: The sync.Mutex is zero-value usable and should not require explicit reset.
	return handle
}

// TimerWheel is a circular timing wheel managing timers in buckets.
type TimerWheel struct {
	tickInterval  time.Duration   // The base tick interval.
	wheelSize     int             // Number of buckets in the wheel.
	buckets       [][]*TimerEntry // Buckets for scheduling timers.
	currentBucket int             // Index of the current bucket.
	maxBatchSize  int             // Batch size for executing callbacks.
	ticker        *time.Ticker    // Ticker that drives the wheel.
	stopCh        chan struct{}   // Channel to signal when to stop the wheel.
	mu            sync.Mutex      // Protects wheel state.
}

// NewTimerWheel creates a new TimerWheel with the specified number of buckets and tick interval.
func NewTimerWheel(wheelSize int, tickInterval time.Duration, maxBatchSize int) *TimerWheel {
	if maxBatchSize == 0 {
		maxBatchSize = 1
	}
	buckets := make([][]*TimerEntry, wheelSize)
	for i := range buckets {
		buckets[i] = make([]*TimerEntry, 0)
	}
	return &TimerWheel{
		tickInterval:  tickInterval,
		wheelSize:     wheelSize,
		buckets:       buckets,
		currentBucket: 0,
		maxBatchSize:  maxBatchSize,
		stopCh:        make(chan struct{}),
	}
}

// Start begins processing ticks on the TimerWheel.
func (tw *TimerWheel) Start() {
	tw.ticker = time.NewTicker(tw.tickInterval)
	go func() {
		for {
			select {
			case <-tw.ticker.C:
				tw.tick()
			case <-tw.stopCh:
				tw.ticker.Stop()
				return
			}
		}
	}()
}

// Stop halts the TimerWheel.
func (tw *TimerWheel) Stop() {
	close(tw.stopCh)
}

//// tick advances the wheel one bucket and processes all timers scheduled in that bucket.
//func (tw *TimerWheel) tick() {
//	tw.mu.Lock()
//	bucketIdx := tw.currentBucket
//	currentBucket := tw.buckets[bucketIdx]
//
//	var remaining []*TimerEntry
//	for _, timer := range currentBucket {
//		if timer.cancelled {
//			// Skip cancelled timers.
//			continue
//		}
//		if timer.rotations > 0 {
//			timer.rotations--
//			remaining = append(remaining, timer)
//		} else {
//			// Timer is due; execute its callback in its own goroutine.
//			go timer.callback()
//			timerEntryPool.Put(timer)
//		}
//	}
//	// Replace the processed bucket with any remaining timers.
//	tw.buckets[bucketIdx] = remaining
//
//	// Move the current bucket pointer.
//	tw.currentBucket = (tw.currentBucket + 1) % tw.wheelSize
//	tw.mu.Unlock()
//}

// tick advances the wheel one bucket and processes all timers scheduled in that bucket.
func (tw *TimerWheel) tick() {
	tw.mu.Lock()
	bucketIdx := tw.currentBucket
	currentBucket := tw.buckets[bucketIdx]

	var remaining []*TimerEntry
	var callbacks []func()

	for _, timer := range currentBucket {
		if timer.cancelled {
			// Skip cancelled timers.
			continue
		}
		if timer.rotations > 0 {
			timer.rotations--
			remaining = append(remaining, timer)
		} else {
			if tw.maxBatchSize == 1 {
				// Execute immediately in its own goroutine (original behavior).
				go timer.callback()
			} else {
				// Collect callback for batch execution.
				callbacks = append(callbacks, timer.callback)
			}
			// Bump the generation before pooling so any outstanding handle to
			// this fired entry (e.g. the one Cancel()ed by scheduleNextTimer)
			// detects the entry has been recycled and does not touch it.
			timer.generation.Add(1)
			timerEntryPool.Put(timer)
		}
	}

	// If batching is enabled, run callbacks in chunks of at most maxBatchSize per
	// goroutine. Chunking amortizes the goroutine spawn while bounding how many
	// unrelated callbacks a single slow callback can delay: a callback may block
	// (centrifuge presence ticks and refresh handlers do network I/O), and
	// putting a whole bucket on one goroutine lets one connection stall every
	// other connection due in that bucket.
	// callbacks is a fresh slice per tick, so it can be sliced without copying.
	if tw.maxBatchSize > 1 && len(callbacks) > 0 {
		for start := 0; start < len(callbacks); start += tw.maxBatchSize {
			end := min(start+tw.maxBatchSize, len(callbacks))
			batch := callbacks[start:end]
			go func() {
				for _, cb := range batch {
					cb()
				}
			}()
		}
	}

	// Replace the processed bucket with any remaining timers.
	tw.buckets[bucketIdx] = remaining

	// Move the current bucket pointer.
	tw.currentBucket = (tw.currentBucket + 1) % tw.wheelSize
	tw.mu.Unlock()
}

// AddTimer schedules a new timer to execute after the specified delay and returns a TimerCanceler.
func (tw *TimerWheel) AddTimer(delay time.Duration, callback func()) *TimerHandle {
	if delay < 0 {
		delay = 0
	}

	// Calculate the number of ticks required.
	ticks := int(delay / tw.tickInterval)
	if delay%tw.tickInterval > 0 {
		ticks++
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()

	bucketIndex := (tw.currentBucket + ticks) % tw.wheelSize
	rotations := ticks / tw.wheelSize

	entry := timerEntryPool.Get().(*TimerEntry)
	entry.rotations = rotations
	entry.callback = callback
	entry.cancelled = false
	// Bump the generation on (re)scheduling so a handle from a previous life of
	// this pooled entry object cannot match the new timer.
	entry.generation.Add(1)

	tw.buckets[bucketIndex] = append(tw.buckets[bucketIndex], entry)

	// Use the newTimerHandle helper to obtain a pooled TimerHandle.
	return newTimerHandle(tw, bucketIndex, entry)
}

// ShardedTimerWheel manages multiple TimerWheel shards and implements TimerScheduler.
type ShardedTimerWheel struct {
	shards       []*TimerWheel
	numShards    int
	currentShard int64
}

// NewShardedTimerWheel creates a new ShardedTimerWheel with the specified number of shards.
// Each shard is a TimerWheel with wheelSize buckets and the given tick interval.
func NewShardedTimerWheel(numShards, wheelSize int, tickInterval time.Duration, maxBatchSize int) *ShardedTimerWheel {
	shards := make([]*TimerWheel, numShards)
	for i := 0; i < numShards; i++ {
		tw := NewTimerWheel(wheelSize, tickInterval, maxBatchSize)
		tw.Start()
		shards[i] = tw
	}
	return &ShardedTimerWheel{
		shards:    shards,
		numShards: numShards,
	}
}

// getNextTimerWheel selects the next TimerWheel shard using round-robin.
func (stw *ShardedTimerWheel) getNextTimerWheel() *TimerWheel {
	idx := atomic.AddInt64(&stw.currentShard, 1) % int64(stw.numShards)
	return stw.shards[idx]
}

// ScheduleTimer schedules a callback for later execution and returns a TimerCanceler.
// This method makes ShardedTimerWheel conform to the TimerScheduler interface.
func (stw *ShardedTimerWheel) ScheduleTimer(duration time.Duration, callback func()) TimerCanceler {
	tw := stw.getNextTimerWheel()
	return tw.AddTimer(duration, callback)
}

// Stop stops all TimerWheel shards.
func (stw *ShardedTimerWheel) Stop() {
	for _, tw := range stw.shards {
		tw.Stop()
	}
}
