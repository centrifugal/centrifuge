package centrifuge

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
)

// This file is the single, self-contained benchmark suite for the
// per-connection writer queue. It replaces the old BenchmarkWriteMerge /
// BenchmarkWriteMergeDisabled benchmarks and covers every writer code
// path: all flush strategies (immediate, merged, delay-batched,
// timer-driven), every maxMessagesInFrame regime (1, N, unlimited), both
// enqueue entry points (enqueue / enqueueMany), the shrink scheduling
// path, and producer concurrency (sequential vs concurrent broadcast
// fan-in).
//
// It deliberately uses ONLY the writer API (newWriter / run / enqueue /
// enqueueMany / close) plus w.messages.Len(), all of which are identical
// across branches, so the exact same file can be dropped onto master to
// compare. Run with:
//
//	go test -run='^$' -bench=BenchmarkWriterQueue -benchmem -benchtime=300ms -count=10 .
//
// The suite models the real path: many messages are produced into the
// queue and the writer transforms them into transport calls. The
// transport callbacks just count deliveries (cheap, non-zero), so the
// consumer keeps up and the queue stays bounded — the numbers reflect
// enqueue + drain + dispatch cost, not backlog/resize cost. A light
// depth gate (checked rarely) protects against unbounded growth if the
// consumer ever falls behind. ns/op is always per produced message.

// benchSink keeps encodeCost results live so the compiler can't elide them.
var benchSink uint64

// encodeCost models the per-message work a real producer does before
// enqueuing — protocol marshaling of the payload. Real broadcast producers
// are NOT infinitely-fast enqueue loops; this pacing keeps the consumer able
// to keep up so the queue stays shallow (frames of ~tens–128 messages, as in
// production) rather than backing up to thousands, which never happens for a
// connection that isn't already being disconnected as slow.
func encodeCost(b []byte) uint64 {
	var h uint64 = 1469598103934665603
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

type writerBenchCase struct {
	name        string
	writeDelay  time.Duration
	frame       int // maxMessagesInFrame: 1, N, or -1 for unlimited
	useTimer    bool
	shrinkDelay time.Duration
	// itemsPerCall is how many messages each producer call submits:
	// 1 uses enqueue (single), >1 uses enqueueMany (batch).
	itemsPerCall int
}

var writerBenchCases = []writerBenchCase{
	// Dedicated-goroutine, no write delay.
	{"Simple", 0, 1, false, 0, 1},     // one message per write, no merge
	{"Batched16", 0, 16, false, 0, 1}, // merge up to 16 per frame
	{"Batched64", 0, 64, false, 0, 1}, // merge up to 64 per frame
	{"Unlimited", 0, -1, false, 0, 1}, // drain everything each cycle
	// Dedicated-goroutine, delay-batched (timer per drain cycle).
	{"DelayBatch16", 100 * time.Microsecond, 16, false, 0, 1},
	// Timer-driven (non-blocking, AfterFunc flush).
	{"Timer16", 100 * time.Microsecond, 16, true, 0, 1},
	{"TimerUnlimited", 100 * time.Microsecond, -1, true, 0, 1},
	// Timer-driven with delayed shrink scheduling (exercises FinishCollect).
	{"TimerShrink16", 100 * time.Microsecond, 16, true, 10 * time.Millisecond, 1},
	// enqueueMany (batch submit) entry point.
	{"BatchedMany8", 0, 16, false, 0, 8},
	{"TimerMany8", 100 * time.Microsecond, 16, true, 0, 8},
}

func runWriterQueueBench(b *testing.B, producers int, c writerBenchCase) {
	const depthCap = 8192 // backpressure high-water mark (rarely hit)

	var delivered int64
	target := int64(b.N)
	done := make(chan struct{})
	var closeOnce sync.Once

	// Model the real network write: one write(2) syscall per frame to
	// /dev/null. This matters because a real consumer spends most of its
	// time in this syscall *outside* the queue lock — it cannot monopolize
	// the lock the way a free (no-op) transport would. With a free
	// transport the benchmark becomes an unrealistic consumer-bound spin;
	// the syscall restores the production (producer-bound) regime.
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = devnull.Close() }()
	frame := make([]byte, 16*256) // up to one frame's worth of bytes
	mark := func(n int) error {
		bytesN := n * 256
		if bytesN > len(frame) {
			bytesN = len(frame)
		}
		_, _ = devnull.Write(frame[:bytesN])
		if atomic.AddInt64(&delivered, int64(n)) >= target {
			closeOnce.Do(func() { close(done) })
		}
		return nil
	}

	w := newWriter(writerConfig{
		WriteFn:      func(queue.Item) error { return mark(1) },
		WriteManyFn:  func(items ...queue.Item) error { return mark(len(items)) },
		MaxQueueSize: 1 << 30, // never trip DisconnectSlow in the benchmark
	}, 16) // realistic small initial capacity: the ring grows/shrinks under
	// load just like production — resize is real writer work and is measured.

	// Start the consumer in the requested mode. Timer mode is
	// non-blocking (driven by AfterFunc); the others use a goroutine.
	if c.useTimer {
		w.run(c.writeDelay, c.frame, c.shrinkDelay, true)
	} else {
		go w.run(c.writeDelay, c.frame, c.shrinkDelay, false)
	}

	item := queue.Item{Data: make([]byte, 256), Channel: "bench"}
	batchBuf := make([]queue.Item, c.itemsPerCall)
	for i := range batchBuf {
		batchBuf[i] = item
	}

	var failed atomic.Pointer[Disconnect]
	per := b.N / producers

	b.ResetTimer()
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		count := per
		if p == producers-1 { // last producer absorbs the remainder
			count = b.N - per*(producers-1)
		}
		wg.Add(1)
		go func(count int) {
			defer wg.Done()
			var sink uint64
			done2 := 0
			for done2 < count {
				var d *Disconnect
				var n int
				if c.itemsPerCall <= 1 {
					sink += encodeCost(item.Data) // model encoding 1 message
					d = w.enqueue(item)
					n = 1
				} else {
					n = c.itemsPerCall
					if count-done2 < n {
						n = count - done2
					}
					for k := 0; k < n; k++ {
						sink += encodeCost(item.Data) // model encoding each message
					}
					d = w.enqueueMany(batchBuf[:n]...)
				}
				done2 += n
				if d != nil { // never expected here; surface it instead of hanging
					failed.Store(d)
					closeOnce.Do(func() { close(done) })
					atomic.AddUint64(&benchSink, sink)
					return
				}
				// Safety net only: with realistic encode pacing the consumer
				// keeps up and the queue stays shallow, so this never spins.
				// Checked rarely so its Len() doesn't perturb the comparison.
				if done2&8191 == 0 {
					for w.messages.Len() > depthCap {
						runtime.Gosched()
					}
				}
			}
			atomic.AddUint64(&benchSink, sink)
		}(count)
	}
	wg.Wait()
	if d := failed.Load(); d != nil {
		b.Fatalf("enqueue returned disconnect: %v", d)
	}
	if b.N > 0 {
		<-done // ensure every produced message was transformed to a write
	}
	b.StopTimer()

	_ = w.close(false)
}

// BenchmarkWriterQueue is the full matrix: {writer config} x {producers}.
func BenchmarkWriterQueue(b *testing.B) {
	for _, c := range writerBenchCases {
		for _, producers := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/producers=%d", c.name, producers), func(b *testing.B) {
				runWriterQueueBench(b, producers, c)
			})
		}
	}
}
