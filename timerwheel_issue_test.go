package centrifuge

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWheel_SlowCallbackBlastRadius measures how many unrelated callbacks a
// single slow callback delays. Serializing callbacks *within* a batch is by
// design — batching is the point. What matters is that the damage is bounded:
// a callback may block (centrifuge presence ticks do PresenceManager network
// round trips), so one connection must not be able to stall every other
// connection due in the same bucket.
//
// Uses 1 shard so all timers with equal delay land in the same bucket
// deterministically (ScheduleTimer round-robins across shards).
func TestWheel_SlowCallbackBlastRadius(t *testing.T) {
	const numVictims = 40
	const maxBatchSize = 4
	const slowCallbackDuration = 400 * time.Millisecond
	const dueAt = 100 * time.Millisecond

	wheel := NewShardedTimerWheel(1, 256, 10*time.Millisecond, maxBatchSize)
	defer wheel.Stop()

	var wg sync.WaitGroup
	wg.Add(numVictims + 1)
	var delayed atomic.Int64
	scheduledAt := time.Now()

	// Scheduled first so it sits at the head of the bucket and runs first.
	wheel.ScheduleTimer(dueAt, func() {
		time.Sleep(slowCallbackDuration) // e.g. a presence loop doing Redis RTTs
		wg.Done()
	})
	for i := 0; i < numVictims; i++ {
		wheel.ScheduleTimer(dueAt, func() {
			// A victim is "delayed" if it ran well after it was due.
			if time.Since(scheduledAt) > dueAt+slowCallbackDuration/2 {
				delayed.Add(1)
			}
			wg.Done()
		})
	}
	wg.Wait()

	t.Logf("%d unrelated callbacks due at %v alongside one %v callback (maxBatchSize=%d)",
		numVictims, dueAt, slowCallbackDuration, maxBatchSize)
	t.Logf("delayed by it: %d (bounded by maxBatchSize-1=%d; whole bucket would be %d)",
		delayed.Load(), maxBatchSize-1, numVictims)

	require.LessOrEqual(t, delayed.Load(), int64(maxBatchSize-1),
		"a slow callback delayed more than its batch — blast radius is the whole bucket")
}

// TestWheel_MaxBatchSizeChunks proves whether maxBatchSize actually bounds how
// many callbacks share a goroutine. maxBatchSize is documented/named as a batch
// size, and PRO constructs the wheel with 4.
//
// With N callbacks each sleeping D in one bucket:
//   - chunked into groups of 4 -> wall time ~= 4*D
//   - single goroutine for all -> wall time ~= N*D
func TestWheel_MaxBatchSizeChunks(t *testing.T) {
	const numCallbacks = 20
	const maxBatchSize = 4
	const sleep = 20 * time.Millisecond

	wheel := NewShardedTimerWheel(1, 256, 10*time.Millisecond, maxBatchSize)
	defer wheel.Stop()

	var wg sync.WaitGroup
	wg.Add(numCallbacks)
	var maxConcurrent, current atomic.Int64

	start := time.Now()
	for i := 0; i < numCallbacks; i++ {
		wheel.ScheduleTimer(100*time.Millisecond, func() {
			c := current.Add(1)
			for {
				m := maxConcurrent.Load()
				if c <= m || maxConcurrent.CompareAndSwap(m, c) {
					break
				}
			}
			time.Sleep(sleep)
			current.Add(-1)
			wg.Done()
		})
	}
	wg.Wait()
	elapsed := time.Since(start) - 100*time.Millisecond // subtract the scheduled delay

	t.Logf("%d callbacks, maxBatchSize=%d, each sleeping %v", numCallbacks, maxBatchSize, sleep)
	t.Logf("peak concurrent callbacks: %d (expect ~%d if chunked, 1 if all serial)",
		maxConcurrent.Load(), numCallbacks/maxBatchSize)
	t.Logf("elapsed: %v (expect ~%v if chunked by %d, ~%v if fully serial)",
		elapsed, time.Duration(maxBatchSize)*sleep, maxBatchSize, numCallbacks*sleep)

	require.Greater(t, maxConcurrent.Load(), int64(1),
		"maxBatchSize=4 did not chunk: all callbacks ran on a single goroutine")
}
