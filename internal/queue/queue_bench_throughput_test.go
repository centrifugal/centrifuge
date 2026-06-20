package queue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmarks for shrink mechanism - realistic producer/consumer scenario

// Baseline benchmarks without BeginCollect/FinishCollect overhead
// These use RemoveMany instead of RemoveManyInto to show pure queue performance

func runProducerConsumerBaseline(b *testing.B, batchSize int) {
	q := New(2, ItemSize)
	item := Item{Data: []byte("test message"), Channel: "test"}

	// Stop signal for consumer
	done := make(chan struct{})

	// Consumer goroutine - pure RemoveMany without collect/shrink
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			if !q.Wait() {
				return
			}

			// Just remove without any shrink logic
			_, ok := q.RemoveMany(batchSize)
			if !ok {
				return
			}

			time.Sleep(100 * time.Nanosecond) // Emulate write syscall.
		}
	}()

	b.ResetTimer()

	// Producer - add N items
	for b.Loop() {
		// Backpressure if consumer can't keep up
		for q.Len() > 10000 {
			time.Sleep(100 * time.Microsecond)
		}
		q.Add(item)
	}

	b.StopTimer()

	close(done)
	time.Sleep(100 * time.Millisecond)
	q.Close()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// runProducerConsumer runs a producer and consumer goroutine to measure throughput.
// Producer adds items continuously, consumer removes items in batches with collect/shrink logic.
// This measures the actual overhead of the shrink mechanism in a realistic scenario.
func runProducerConsumer(b *testing.B, batchSize int, shrinkDelay time.Duration, useShrink bool) {
	q := New(2, ItemSize)
	buf := make([]Item, batchSize)
	item := Item{Data: []byte("test message"), Channel: "test"}

	// Stop signal for consumer
	done := make(chan struct{})

	// Consumer goroutine
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}

			if !q.Wait() {
				return
			}

			q.RemoveManyInto(buf, batchSize)

			time.Sleep(100 * time.Nanosecond) // Emulate write syscall.

			if useShrink {
				q.FinishCollect(shrinkDelay)
			}
		}
	}()

	b.ResetTimer()

	// Producer - add N items
	for b.Loop() {
		// Backpressure if consumer can't keep up
		for q.Len() > 10000 {
			time.Sleep(100 * time.Microsecond)
		}
		q.Add(item)
	}

	b.StopTimer()

	close(done)
	// Drain remaining items
	time.Sleep(100 * time.Millisecond)
	q.Close()

	// Report throughput
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkThroughput_Batch1_Baseline(b *testing.B) {
	runProducerConsumerBaseline(b, 1)
}

func BenchmarkThroughput_Batch1_NoShrink(b *testing.B) {
	runProducerConsumer(b, 1, 0, false)
}

func BenchmarkThroughput_Batch1_ImmediateShrink(b *testing.B) {
	runProducerConsumer(b, 1, 0, true)
}

func BenchmarkThroughput_Batch1_DelayedShrink10ms(b *testing.B) {
	runProducerConsumer(b, 1, 10*time.Millisecond, true)
}

func BenchmarkThroughput_Batch1_DelayedShrink100ms(b *testing.B) {
	runProducerConsumer(b, 1, 100*time.Millisecond, true)
}

func BenchmarkThroughput_Batch16_Baseline(b *testing.B) {
	runProducerConsumerBaseline(b, 16)
}

func BenchmarkThroughput_Batch16_NoShrink(b *testing.B) {
	runProducerConsumer(b, 16, 0, false)
}

func BenchmarkThroughput_Batch16_ImmediateShrink(b *testing.B) {
	runProducerConsumer(b, 16, 0, true)
}

func BenchmarkThroughput_Batch16_DelayedShrink10ms(b *testing.B) {
	runProducerConsumer(b, 16, 10*time.Millisecond, true)
}

func BenchmarkThroughput_Batch16_DelayedShrink100ms(b *testing.B) {
	runProducerConsumer(b, 16, 100*time.Millisecond, true)
}

func BenchmarkThroughput_Batch64_Baseline(b *testing.B) {
	runProducerConsumerBaseline(b, 64)
}

func BenchmarkThroughput_Batch64_NoShrink(b *testing.B) {
	runProducerConsumer(b, 64, 0, false)
}

func BenchmarkThroughput_Batch64_ImmediateShrink(b *testing.B) {
	runProducerConsumer(b, 64, 0, true)
}

func BenchmarkThroughput_Batch64_DelayedShrink10ms(b *testing.B) {
	runProducerConsumer(b, 64, 10*time.Millisecond, true)
}

func BenchmarkThroughput_Batch64_DelayedShrink100ms(b *testing.B) {
	runProducerConsumer(b, 64, 100*time.Millisecond, true)
}

// runMPSC measures the realistic Centrifuge fan-out shape: many producer
// goroutines (broadcast workers) Add into one consumer (a client writer)
// that drains in batches. The producer count is the contention dimension
// — that's where the writer-queue mutex actually hurts in production.
func runMPSC(b *testing.B, producers, batchSize int, useShrink bool) {
	q := New(2, ItemSize)
	buf := make([]Item, batchSize)
	item := Item{Data: []byte("test message"), Channel: "test"}

	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if !q.Wait() {
				return
			}
			q.RemoveManyInto(buf, batchSize)
			time.Sleep(100 * time.Nanosecond) // emulate write syscall
			if useShrink {
				q.FinishCollect(0)
			}
		}
	}()

	var produced atomic.Int64
	target := int64(b.N)
	var wg sync.WaitGroup

	b.ResetTimer()

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if produced.Add(1) > target {
					return
				}
				for q.Len() > 10000 {
					time.Sleep(100 * time.Microsecond)
				}
				q.Add(item)
			}
		}()
	}

	wg.Wait()

	b.StopTimer()

	close(done)
	time.Sleep(100 * time.Millisecond)
	q.Close()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkMPSC_P2_Batch16(b *testing.B)   { runMPSC(b, 2, 16, true) }
func BenchmarkMPSC_P4_Batch16(b *testing.B)   { runMPSC(b, 4, 16, true) }
func BenchmarkMPSC_P8_Batch16(b *testing.B)   { runMPSC(b, 8, 16, true) }
func BenchmarkMPSC_P16_Batch16(b *testing.B)  { runMPSC(b, 16, 16, true) }
func BenchmarkMPSC_P32_Batch16(b *testing.B)  { runMPSC(b, 32, 16, true) }
func BenchmarkMPSC_P64_Batch16(b *testing.B)  { runMPSC(b, 64, 16, true) }
func BenchmarkMPSC_P128_Batch16(b *testing.B) { runMPSC(b, 128, 16, true) }

func BenchmarkMPSC_P8_Batch64(b *testing.B)  { runMPSC(b, 8, 64, true) }
func BenchmarkMPSC_P32_Batch64(b *testing.B) { runMPSC(b, 32, 64, true) }

// BenchmarkLen models the common backpressure check `q.Len() > X` from
// many goroutines. Was RLock+load+RUnlock; now atomic load.
func BenchmarkLen_Parallel(b *testing.B) {
	q := New(2, ItemSize)
	for i := 0; i < 100; i++ {
		q.Add(Item{Data: []byte("x")})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = q.Len()
		}
	})
}

// BenchmarkSize same but for Size — used for MaxQueueSize backpressure.
func BenchmarkSize_Parallel(b *testing.B) {
	q := New(2, ItemSize)
	for i := 0; i < 100; i++ {
		q.Add(Item{Data: []byte("x")})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = q.Size()
		}
	})
}
