package queue

import (
	"testing"
	"time"
)

// Benchmarks for shrink mechanism - realistic producer/consumer scenario

// Baseline benchmarks without BeginCollect/FinishCollect overhead
// These use RemoveMany instead of RemoveManyInto to show pure queue performance

func runProducerConsumerBaseline(b *testing.B, batchSize int) {
	q := New(2)
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
	q := New(2)
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

			// Emulate batching with BeginCollect/FinishCollect
			if useShrink {
				q.BeginCollect()
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
