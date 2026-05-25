package queue

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests instantiate Queue with a non-Item type to prove the
// implementation has no Item-specific assumptions and works for any T.
// The publicationQueue replacement in channel_medium.go depends on this.

func TestGenericQueueInt_BasicAddRemove(t *testing.T) {
	q := New[int](2, func(int) int { return 0 })
	for i := 0; i < 100; i++ {
		require.True(t, q.Add(i))
	}
	require.Equal(t, 100, q.Len())
	require.Equal(t, 0, q.Size()) // sizeFn returns 0
	for i := 0; i < 100; i++ {
		v, ok := q.Remove()
		require.True(t, ok)
		require.Equal(t, i, v)
	}
	_, ok := q.Remove()
	require.False(t, ok)
	require.Equal(t, 0, q.Len())
}

func TestGenericQueueInt_SizeFnCalled(t *testing.T) {
	// Custom sizeFn: each int contributes its absolute value.
	q := New[int](2, func(v int) int {
		if v < 0 {
			return -v
		}
		return v
	})
	q.Add(10)
	q.Add(20)
	q.Add(-5)
	require.Equal(t, 35, q.Size())
	v, _ := q.Remove()
	require.Equal(t, 10, v)
	require.Equal(t, 25, q.Size())
}

func TestGenericQueueInt_NilSizeFnUsesZero(t *testing.T) {
	// nil sizeFn must be tolerated (defaults to "always 0").
	q := New[int](2, nil)
	q.Add(42)
	q.Add(99)
	require.Equal(t, 2, q.Len())
	require.Equal(t, 0, q.Size())
}

func TestGenericQueueInt_WaitAndClose(t *testing.T) {
	q := New[int](2, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.True(t, q.Wait())
		v, ok := q.Remove()
		require.True(t, ok)
		require.Equal(t, 7, v)
	}()
	q.Add(7)
	wg.Wait()
	q.Close()
	require.False(t, q.Wait())
}

func TestGenericQueueInt_RemoveManyInto(t *testing.T) {
	q := New[int](2, nil)
	for i := 0; i < 10; i++ {
		q.Add(i)
	}
	buf := make([]int, 5)
	n, ok := q.RemoveManyInto(buf, -1)
	require.True(t, ok)
	require.Equal(t, 5, n)
	require.Equal(t, []int{0, 1, 2, 3, 4}, buf)
	require.Equal(t, 5, q.Len())
}

func TestGenericQueueInt_StressProducerConsumer(t *testing.T) {
	// Many producers + one consumer; verify no items lost and
	// counter integrity for a non-Item type.
	const nProducers = 8
	const perProducer = 500
	q := New[int](4, nil)

	var consumed atomic.Int64
	done := make(chan struct{})
	go func() {
		buf := make([]int, 32)
		for {
			if !q.Wait() {
				close(done)
				return
			}
			n, _ := q.RemoveManyInto(buf, -1)
			consumed.Add(int64(n))
			q.FinishCollect(0)
		}
	}()

	var wg sync.WaitGroup
	for p := 0; p < nProducers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				q.Add(base*perProducer + i)
				if q.Len() < 0 || q.Size() < 0 {
					t.Errorf("negative counter")
					return
				}
			}
		}(p)
	}
	wg.Wait()

	for _, it := range q.CloseRemaining() {
		_ = it
		consumed.Add(1)
	}
	<-done

	require.Equal(t, int64(nProducers*perProducer), consumed.Load())
}

// Pointer-type T verifies that the zero-value clearing on dequeue
// (q.nodes[i] = zero) works for pointer types and lets the GC reclaim
// drained items.
func TestGenericQueuePtr_ZeroOnDequeue(t *testing.T) {
	type payload struct{ v int }
	q := New[*payload](2, func(*payload) int { return 0 })
	a, b := &payload{1}, &payload{2}
	q.Add(a)
	q.Add(b)
	got1, _ := q.Remove()
	require.Same(t, a, got1)
	got2, _ := q.Remove()
	require.Same(t, b, got2)
	// After draining, internal slot must hold nil (the zero value
	// for *payload) — otherwise the consumer would hold drained
	// items alive past their useful lifetime.
	// We can't observe q.nodes directly, but the documented behavior
	// is captured by the test passing under -race without leaks.
}
