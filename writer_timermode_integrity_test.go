package centrifuge

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
	"github.com/stretchr/testify/require"
)

// recordingTransport records the Data of every item written, in order, so a test
// can assert no message is lost, duplicated, or reordered.
type recordingTransport struct {
	mu   sync.Mutex
	seen []uint64
}

func (r *recordingTransport) write(item queue.Item) error {
	r.mu.Lock()
	r.seen = append(r.seen, binary.BigEndian.Uint64(item.Data))
	r.mu.Unlock()
	return nil
}

func (r *recordingTransport) writeMany(items ...queue.Item) error {
	r.mu.Lock()
	for _, it := range items {
		r.seen = append(r.seen, binary.BigEndian.Uint64(it.Data))
	}
	r.mu.Unlock()
	return nil
}

func seqItem(n uint64) queue.Item {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return queue.Item{Data: b}
}

// TestWriterTimerMode_NoLossSingleProducer drives the write_with_timer path with
// a single producer (the real per-connection invariant: the message writer is
// MPSC, and for one connection the producer is the fan-out for that connection).
// Every enqueued message must be written exactly once and in order.
func TestWriterTimerMode_NoLossSingleProducer(t *testing.T) {
	t.Parallel()
	rec := &recordingTransport{}
	w := newWriter(writerConfig{
		MaxQueueSize: 1 << 20,
		WriteFn:      rec.write,
		WriteManyFn:  rec.writeMany,
	}, 0)
	// write_delay + write_with_timer.
	w.run(2*time.Millisecond, 4, 0, true)

	const n = 5000
	for i := uint64(0); i < n; i++ {
		require.Nil(t, w.enqueue(seqItem(i)))
	}
	require.NoError(t, w.close(true)) // flush remaining

	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.seen) == n
	}, 5*time.Second, 5*time.Millisecond, "wrote %d of %d", len(rec.seen), n)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for i := uint64(0); i < n; i++ {
		require.Equal(t, i, rec.seen[i], "message %d out of order or lost/duplicated", i)
	}
}

// TestWriterTimerMode_TimerDrainsWithoutClose targets the lost-wakeup path: it
// verifies the flush TIMER drains the queue on its own, without relying on
// close() to mop up remaining messages. A missed scheduleFlush (message added
// but no timer armed) would leave the queue stuck and time out here.
//
// Runs many rounds so a rare enqueue-vs-flush race has chances to surface.
func TestWriterTimerMode_TimerDrainsWithoutClose(t *testing.T) {
	t.Parallel()
	rec := &recordingTransport{}
	w := newWriter(writerConfig{
		MaxQueueSize: 1 << 20,
		WriteFn:      rec.write,
		WriteManyFn:  rec.writeMany,
	}, 0)
	w.run(time.Millisecond, 4, 0, true)
	defer func() { _ = w.close(false) }()

	var next uint64
	for round := 0; round < 200; round++ {
		batch := 1 + round%9 // vary batch size around the maxMessagesInFrame of 4
		var producers sync.WaitGroup
		for i := 0; i < batch; i++ {
			producers.Add(1)
			go func(v uint64) {
				defer producers.Done()
				w.enqueue(seqItem(v))
			}(next)
			next++
		}
		producers.Wait()
		want := int(next)
		// The timer alone must drain everything enqueued so far.
		require.Eventually(t, func() bool {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			return len(rec.seen) == want
		}, 3*time.Second, time.Millisecond,
			// Do not read len(rec.seen) here: require.Eventually evaluates the
			// message args eagerly on the caller goroutine while the flush timer is
			// still appending under rec.mu — that would be a data race (and the
			// pre-poll count is stale anyway).
			"round %d: timer left the queue stuck (want %d written) — lost flush wakeup",
			round, want)
	}
}

// TestWriterTimerMode_NoLossManyProducers stresses the timerScheduled handoff
// between enqueue and the flush timer under concurrent producers. Ordering is
// not asserted (concurrent producers have no total order), but every message
// must be written exactly once — no loss, no duplication.
func TestWriterTimerMode_NoLossManyProducers(t *testing.T) {
	t.Parallel()
	rec := &recordingTransport{}
	w := newWriter(writerConfig{
		MaxQueueSize: 1 << 20,
		WriteFn:      rec.write,
		WriteManyFn:  rec.writeMany,
	}, 0)
	w.run(time.Millisecond, 8, 0, true)

	const producers = 16
	const perProducer = 2000
	const total = producers * perProducer
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < perProducer; i++ {
				w.enqueue(seqItem(base + i))
			}
		}(uint64(p) * perProducer)
	}
	wg.Wait()
	require.NoError(t, w.close(true))

	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.seen) == total
	}, 10*time.Second, 5*time.Millisecond, "wrote %d of %d", len(rec.seen), total)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	got := make(map[uint64]int, total)
	for _, v := range rec.seen {
		got[v]++
	}
	for i := uint64(0); i < total; i++ {
		require.Equal(t, 1, got[i], "message %d written %d times (expected exactly once)", i, got[i])
	}
}
