package queue

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testItem(data []byte) Item {
	return Item{Data: data}
}

var initialCapacity = 2

func TestByteQueueResize(t *testing.T) {
	q := New(initialCapacity, ItemSize)
	require.Equal(t, 0, q.Len())
	require.Equal(t, false, q.Closed())

	for i := 0; i < initialCapacity; i++ {
		q.Add(testItem([]byte(strconv.Itoa(i))))
	}
	q.Add(testItem([]byte("resize here")))
	require.Equal(t, initialCapacity*2, q.Cap())
	q.Remove()

	q.Add(testItem([]byte("new resize here")))
	require.Equal(t, initialCapacity*2, q.Cap())
	q.Add(testItem([]byte("one more item, no resize must happen")))
	require.Equal(t, initialCapacity*2, q.Cap())

	require.Equal(t, initialCapacity+2, q.Len())
}

func TestByteQueueSize(t *testing.T) {
	q := New(initialCapacity, ItemSize)
	require.Equal(t, 0, q.Size())
	q.Add(testItem([]byte("1")))
	q.Add(testItem([]byte("2")))
	require.Equal(t, 2, q.Size())
	q.Remove()
	require.Equal(t, 1, q.Size())
}

func TestByteQueueWait(t *testing.T) {
	q := New(initialCapacity, ItemSize)
	q.Add(testItem([]byte("1")))
	q.Add(testItem([]byte("2")))

	ok := q.Wait()
	require.Equal(t, true, ok)
	s, ok := q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "1", string(s.Data))

	ok = q.Wait()
	require.Equal(t, true, ok)
	s, ok = q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "2", string(s.Data))

	go func() {
		q.Add(testItem([]byte("3")))
	}()

	ok = q.Wait()
	require.Equal(t, true, ok)
	s, ok = q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "3", string(s.Data))
}

func TestByteQueueAddMany(t *testing.T) {
	q := New(initialCapacity, ItemSize)
	q.AddMany(testItem([]byte("1")), testItem([]byte("2")))
	ok := q.Wait()
	require.Equal(t, true, ok)
	s, ok := q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "1", string(s.Data))

	ok = q.Wait()
	require.Equal(t, true, ok)
	s, ok = q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "2", string(s.Data))
}

func TestByteQueueRemoveMany(t *testing.T) {
	q := New(initialCapacity, ItemSize)
	q.AddMany(testItem([]byte("1")), testItem([]byte("2")))
	ok := q.Wait()
	require.Equal(t, true, ok)
	items, ok := q.RemoveMany(-1)
	require.Equal(t, true, ok)
	require.Equal(t, 2, len(items))
}

func TestByteQueueRemoveManyFixed(t *testing.T) {
	q := New(initialCapacity, ItemSize)
	q.AddMany(testItem([]byte("1")), testItem([]byte("2")))
	ok := q.Wait()
	require.Equal(t, true, ok)
	items, ok := q.RemoveMany(2)
	require.Equal(t, true, ok)
	require.Equal(t, 2, len(items))
}

func TestQueueClose(t *testing.T) {
	q := New(initialCapacity, ItemSize)

	// test removing from empty queue
	_, ok := q.Remove()
	require.Equal(t, false, ok)

	q.Add(testItem([]byte("1")))
	q.Add(testItem([]byte("2")))
	q.Close()

	ok = q.Add(testItem([]byte("3")))
	require.Equal(t, false, ok)

	ok = q.Wait()
	require.Equal(t, false, ok)

	_, ok = q.Remove()
	require.Equal(t, false, ok)

	require.Equal(t, true, q.Closed())
}

func TestByteQueueCloseRemaining(t *testing.T) {
	q := New(initialCapacity, ItemSize)
	q.Add(testItem([]byte("1")))
	q.Add(testItem([]byte("2")))
	messages := q.CloseRemaining()
	require.Equal(t, 2, len(messages))
	ok := q.Add(testItem([]byte("3")))
	require.Equal(t, false, ok)
	require.Equal(t, true, q.Closed())
	messages = q.CloseRemaining()
	require.Equal(t, 0, len(messages))
}

func TestQueueAddConsume(t *testing.T) {
	// Add many items to queue and then consume.
	// Make sure item data is expected.
	q := New(initialCapacity, ItemSize)

	for range 5 {
		for i := 0; i < 1000; i++ {
			q.Add(testItem([]byte("test" + strconv.Itoa(i))))
		}
		for i := 0; i < 1000; i++ {
			items, _ := q.RemoveMany(1)
			require.Equal(t, "test"+strconv.Itoa(i), string(items[0].Data))
		}
	}

	require.Equal(t, 0, q.Size())
	require.Equal(t, initialCapacity, q.Cap())
	require.Equal(t, 0, q.Len())
}

func TestQueueAddConsumeInBatch(t *testing.T) {
	// Add many items to queue and then consume.
	// Make sure item data is expected.
	q := New(initialCapacity, ItemSize)

	for range 5 {
		for i := 0; i < 1000; i++ {
			q.Add(testItem([]byte("test" + strconv.Itoa(i))))
		}
		for i := 0; i < 100; i++ {
			items, _ := q.RemoveMany(10)
			for j := 0; j < 10; j++ {
				require.Equal(t, "test"+strconv.Itoa(i*10+j), string(items[j].Data))
			}
		}
	}

	require.Equal(t, 0, q.Size())
	require.Equal(t, initialCapacity, q.Cap())
	require.Equal(t, 0, q.Len())
}

func TestQueueAddConsumeAll(t *testing.T) {
	// Add many items to queue and then consume.
	// Make sure item data is expected.
	q := New(initialCapacity, ItemSize)

	for range 5 {
		for i := 0; i < 1000; i++ {
			q.Add(testItem([]byte("test" + strconv.Itoa(i))))
		}
		items, _ := q.RemoveMany(-1)
		for i := 0; i < 1000; i++ {
			require.Equal(t, "test"+strconv.Itoa(i), string(items[i].Data))
		}
	}

	require.Equal(t, 0, q.Size())
	require.Equal(t, initialCapacity, q.Cap())
	require.Equal(t, 0, q.Len())
}

func TestQueueResizing(t *testing.T) {
	q := New(2, ItemSize)
	for i := 0; i < 10; i++ {
		item := Item{Data: []byte("msg"), Channel: "ch"}
		require.True(t, q.Add(item))
	}
	require.GreaterOrEqual(t, q.Cap(), 10)
}

func TestQueueCloseRemaining(t *testing.T) {
	q := New(3, ItemSize)
	q.Add(Item{Data: []byte("msg1"), Channel: "ch1"})
	q.Add(Item{Data: []byte("msg2"), Channel: "ch2"})

	remaining := q.CloseRemaining()
	require.Len(t, remaining, 2)
	require.True(t, q.Closed())
}

func TestQueueWait(t *testing.T) {
	q := New(1, ItemSize)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.True(t, q.Wait())
	}()

	q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	wg.Wait()
}

func TestQueueConcurrency(t *testing.T) {
	q := New(5, ItemSize)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.Add(Item{Data: []byte("msg"), Channel: "ch"})
		}()
	}
	wg.Wait()
	require.GreaterOrEqual(t, q.Len(), 5)
}

func TestQueueRemoveMany(t *testing.T) {
	q := New(5, ItemSize)
	for i := 0; i < 5; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	removed, ok := q.RemoveMany(3)
	require.True(t, ok)
	require.Len(t, removed, 3)
	require.Equal(t, 2, q.Len())
}

func TestQueueRemoveManyAll(t *testing.T) {
	q := New(3, ItemSize)
	for i := 0; i < 3; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	removed, ok := q.RemoveMany(-1)
	require.True(t, ok)
	require.Len(t, removed, 3)
	require.Equal(t, 0, q.Len())
}

func TestQueueAddAfterClose(t *testing.T) {
	q := New(2, ItemSize)
	q.Close()
	added := q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	require.False(t, added)
}

func TestQueueRemoveManyInto(t *testing.T) {
	q := New(5, ItemSize)
	for i := 0; i < 5; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	buf := make([]Item, 10)
	n, ok := q.RemoveManyInto(buf, 3)
	require.True(t, ok)
	require.Equal(t, 3, n)
	require.Equal(t, 2, q.Len())
}

func TestQueueRemoveManyIntoAll(t *testing.T) {
	q := New(3, ItemSize)
	for i := 0; i < 3; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	buf := make([]Item, 10)
	n, ok := q.RemoveManyInto(buf, -1)
	require.True(t, ok)
	require.Equal(t, 3, n)
	require.Equal(t, 0, q.Len())
}

func TestQueueRemoveManyIntoBufferLimit(t *testing.T) {
	q := New(5, ItemSize)
	for i := 0; i < 5; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	buf := make([]Item, 2)
	n, ok := q.RemoveManyInto(buf, 10)
	require.True(t, ok)
	require.Equal(t, 2, n)
	require.Equal(t, 3, q.Len())
}

func TestQueueCollectingMode(t *testing.T) {
	q := New(2, ItemSize)

	// Add initial items
	q.Add(Item{Data: []byte("msg1"), Channel: "ch"})
	q.Add(Item{Data: []byte("msg2"), Channel: "ch"})
	require.Equal(t, 2, q.Cap())

	// Add more items (will cause resize)
	q.Add(Item{Data: []byte("msg3"), Channel: "ch"})
	q.Add(Item{Data: []byte("msg4"), Channel: "ch"})
	require.True(t, q.Cap() > 2)

	// Remove items (no shrink during collection)
	buf := make([]Item, 10)
	n, ok := q.RemoveManyInto(buf, -1)
	require.True(t, ok)
	require.Equal(t, 4, n)
	require.Equal(t, 0, q.Len())

	// Queue should not have shrunk yet
	capBeforeFinish := q.Cap()

	// Finish collecting - now shrink happens (immediate with delay=0)
	q.FinishCollect(0)

	// Queue should have shrunk back to initCap
	require.Equal(t, 2, q.Cap())
	require.True(t, q.Cap() < capBeforeFinish)
}

func TestQueueRemoveFromEmptyQueue(t *testing.T) {
	q := New(2, ItemSize)
	_, ok := q.Remove()
	require.False(t, ok)
}

func TestQueueAddManyAfterClose(t *testing.T) {
	q := New(2, ItemSize)
	q.Close()
	added := q.AddMany(Item{Data: []byte("msg1"), Channel: "ch"}, Item{Data: []byte("msg2"), Channel: "ch"})
	require.False(t, added)
}

func TestQueueRemoveManyFromEmptyQueue(t *testing.T) {
	q := New(2, ItemSize)
	removed, ok := q.RemoveMany(5)
	require.False(t, ok)
	require.Nil(t, removed)
}

func TestQueueWaitOnClosedQueue(t *testing.T) {
	q := New(1, ItemSize)
	q.Close()
	require.False(t, q.Wait())
}

func TestQueueCorrectness(t *testing.T) {
	q := New(5, ItemSize)
	items := []Item{
		{Data: []byte("A"), Channel: "ch1"},
		{Data: []byte("B"), Channel: "ch2"},
		{Data: []byte("C"), Channel: "ch3"},
		{Data: []byte("D"), Channel: "ch4"},
		{Data: []byte("E"), Channel: "ch5"},
	}

	q.Add(items[0])
	q.Add(items[1])
	retrieved, _ := q.Remove()
	require.Equal(t, items[0], retrieved)

	q.Add(items[2])
	q.Add(items[3])
	q.Add(items[4])

	// Remove in different order.
	retrieved, _ = q.Remove()
	require.Equal(t, items[1], retrieved)

	retrieved, _ = q.Remove()
	require.Equal(t, items[2], retrieved)

	retrieved, _ = q.Remove()
	require.Equal(t, items[3], retrieved)

	retrieved, _ = q.Remove()
	require.Equal(t, items[4], retrieved)
}

func TestQueueAddManyWithResize(t *testing.T) {
	q := New(2, ItemSize)
	items := []Item{
		{Data: []byte("A"), Channel: "ch1"},
		{Data: []byte("B"), Channel: "ch2"},
		{Data: []byte("C"), Channel: "ch3"},
		{Data: []byte("D"), Channel: "ch4"},
		{Data: []byte("E"), Channel: "ch5"},
	}

	// Add more items than initial capacity to trigger resize.
	added := q.AddMany(items...)
	require.True(t, added)
	require.GreaterOrEqual(t, q.Cap(), 5)

	// Verify correct items are retrieved.
	for _, expected := range items {
		retrieved, ok := q.Remove()
		require.True(t, ok)
		require.Equal(t, expected, retrieved)
	}
}

func TestQueueResizeEmptyQueue(t *testing.T) {
	q := New(2, ItemSize)
	q.resize(4)
	require.Equal(t, 0, q.Len())
	require.Equal(t, 4, q.Cap())
}

func TestQueueResizeWithSequentialData(t *testing.T) {
	q := New(2, ItemSize)
	q.Add(Item{Data: []byte("A"), Channel: "ch1"})
	q.Add(Item{Data: []byte("B"), Channel: "ch2"})

	q.resize(4)
	require.Equal(t, 2, q.Len())
	retrieved, _ := q.Remove()
	require.Equal(t, "A", string(retrieved.Data))
}

func TestQueueResizeWithWrappedData(t *testing.T) {
	q := New(3, ItemSize)
	q.Add(Item{Data: []byte("X"), Channel: "ch1"})
	q.Add(Item{Data: []byte("Y"), Channel: "ch2"})
	q.Remove()
	q.Add(Item{Data: []byte("Z"), Channel: "ch3"})

	q.resize(6)
	require.Equal(t, 2, q.Len())
	retrieved, _ := q.Remove()
	require.Equal(t, "Y", string(retrieved.Data))
	retrieved, _ = q.Remove()
	require.Equal(t, "Z", string(retrieved.Data))
}

func TestQueueResizeToSmallerSize(t *testing.T) {
	q := New(6, ItemSize)
	q.Add(Item{Data: []byte("1"), Channel: "ch1"})
	q.Add(Item{Data: []byte("2"), Channel: "ch2"})
	q.Add(Item{Data: []byte("3"), Channel: "ch3"})
	q.resize(2)

	require.Equal(t, 2, q.Cap())
	_, ok := q.Remove()
	require.True(t, ok)
	_, ok = q.Remove()
	require.True(t, ok)
}

func TestQueueResizeWraparound(t *testing.T) {
	q := New(4, ItemSize)
	q.Add(Item{Data: []byte("A"), Channel: "ch1"})
	q.Add(Item{Data: []byte("B"), Channel: "ch2"})
	q.Add(Item{Data: []byte("C"), Channel: "ch3"})
	q.Remove()
	q.Add(Item{Data: []byte("D"), Channel: "ch4"})
	q.resize(8)

	require.Equal(t, 3, q.Len())
	retrieved, _ := q.Remove()
	require.Equal(t, "B", string(retrieved.Data))
	retrieved, _ = q.Remove()
	require.Equal(t, "C", string(retrieved.Data))
	retrieved, _ = q.Remove()
	require.Equal(t, "D", string(retrieved.Data))
}

func BenchmarkQueueAdd(b *testing.B) {
	q := New(initialCapacity, ItemSize)
	b.ResetTimer()
	for b.Loop() {
		q.Add(testItem([]byte("test")))
	}
	b.StopTimer()
	q.Close()
}

func addAndConsume(q *Queue[Item], n int) {
	// Add to queue and consume in another goroutine.
	done := make(chan struct{})
	go func() {
		count := 0
		for {
			ok := q.Wait()
			if !ok {
				continue
			}
			q.Remove()
			count++
			if count == n {
				close(done)
				break
			}
		}
	}()
	for i := 0; i < n; i++ {
		q.Add(testItem([]byte("test")))
	}
	<-done
}

func BenchmarkQueueAddConsume(b *testing.B) {
	q := New(initialCapacity, ItemSize)
	b.ResetTimer()
	for b.Loop() {
		addAndConsume(q, 10000)
	}
	b.StopTimer()
	q.Close()
}

func BenchmarkQueueAdd10k(b *testing.B) {
	n := 10000
	queues := make([]*Queue[Item], n)
	for i := range n {
		queues[i] = New(initialCapacity, ItemSize)
	}
	item := testItem([]byte("test"))
	b.ResetTimer()
	for b.Loop() {
		for _, q := range queues {
			q.Add(item)
			q.Remove()
		}
	}
}

// Tests for shrink mechanism

func TestQueueDelayedShrink(t *testing.T) {
	q := New(2, ItemSize)

	// Add items to trigger growth
	for i := 0; i < 8; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 8, q.Cap())

	// Remove all items
	buf := make([]Item, 10)
	n, ok := q.RemoveManyInto(buf, -1)
	require.True(t, ok)
	require.Equal(t, 8, n)
	require.Equal(t, 0, q.Len())

	// Queue should not have shrunk yet (still collecting)
	require.Equal(t, 8, q.Cap())

	// Finish collecting with delay
	q.FinishCollect(50 * time.Millisecond)

	// Queue should not have shrunk immediately
	require.Equal(t, 8, q.Cap())

	// Wait for shrink to happen
	time.Sleep(100 * time.Millisecond)

	// Queue should have shrunk back to initCap
	require.Equal(t, 2, q.Cap())
}

func TestQueueImmediateShrink(t *testing.T) {
	q := New(2, ItemSize)

	// Add items to trigger growth
	for i := 0; i < 8; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 8, q.Cap())

	// Remove all items
	buf := make([]Item, 10)
	n, ok := q.RemoveManyInto(buf, -1)
	require.True(t, ok)
	require.Equal(t, 8, n)

	// Finish collecting with zero delay (immediate shrink)
	q.FinishCollect(0)

	// Queue should have shrunk immediately
	require.Equal(t, 2, q.Cap())
}

func TestQueueShrinkTimerReset(t *testing.T) {
	q := New(2, ItemSize)

	// Grow the queue
	for i := 0; i < 8; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 8, q.Cap())

	// First collect cycle
	buf := make([]Item, 10)
	q.RemoveManyInto(buf, -1)
	q.FinishCollect(100 * time.Millisecond)

	// Wait a bit, but not enough for shrink
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 8, q.Cap()) // Should not have shrunk yet

	// Add more items and do another collect cycle (this should reset the timer)
	for i := 0; i < 4; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	q.RemoveManyInto(buf, -1)
	q.FinishCollect(100 * time.Millisecond)

	// Wait 60ms (total would be 110ms from first FinishCollect, but timer was reset)
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, 8, q.Cap()) // Should still not have shrunk (timer was reset)

	// Wait for the new timer to expire
	time.Sleep(60 * time.Millisecond)
	require.Equal(t, 2, q.Cap()) // Should have shrunk now
}

func TestQueueShrinkPartial(t *testing.T) {
	q := New(2, ItemSize)

	// Grow queue to 16
	for i := 0; i < 16; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 16, q.Cap())

	// Remove items, leaving 4
	buf := make([]Item, 20)
	n, ok := q.RemoveManyInto(buf, 12)
	require.True(t, ok)
	require.Equal(t, 12, n)
	require.Equal(t, 4, q.Len())

	// Finish collecting
	q.FinishCollect(0)

	// Queue should shrink to 4 (smallest power of 2 >= initCap that can hold 4 items)
	require.Equal(t, 4, q.Cap())
}

func TestQueueNoShrinkBelowInitCap(t *testing.T) {
	q := New(4, ItemSize)

	// Add items to trigger growth
	for i := 0; i < 8; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 8, q.Cap())

	// Remove all items
	buf := make([]Item, 10)
	q.RemoveManyInto(buf, -1)

	// Finish collecting
	q.FinishCollect(0)

	// Queue should shrink to initCap (4), not below
	require.Equal(t, 4, q.Cap())
}

func TestQueueShrinkEmptyQueue(t *testing.T) {
	q := New(2, ItemSize)

	// Queue starts empty
	require.Equal(t, 2, q.Cap())
	require.Equal(t, 0, q.Len())

	// Collect on empty queue
	q.FinishCollect(0)

	// Should remain at initCap
	require.Equal(t, 2, q.Cap())
	require.Equal(t, 0, q.Len())
}

func TestQueueMultipleShrinkCycles(t *testing.T) {
	q := New(2, ItemSize)

	// Cycle 1: Grow and shrink
	for i := 0; i < 8; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 8, q.Cap())

	buf := make([]Item, 20)
	q.RemoveManyInto(buf, -1)
	q.FinishCollect(0)
	require.Equal(t, 2, q.Cap())

	// Cycle 2: Grow and shrink again
	for i := 0; i < 16; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 16, q.Cap())

	q.RemoveManyInto(buf, -1)
	q.FinishCollect(0)
	require.Equal(t, 2, q.Cap())

	// Verify queue still works
	q.Add(Item{Data: []byte("test"), Channel: "ch"})
	item, ok := q.Remove()
	require.True(t, ok)
	require.Equal(t, "test", string(item.Data))
}

func TestQueueShrinkWithItemsRemaining(t *testing.T) {
	q := New(2, ItemSize)

	// Add 32 items
	for i := 0; i < 32; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	require.Equal(t, 32, q.Cap())

	// Remove 30 items, leaving 2
	buf := make([]Item, 40)
	n, ok := q.RemoveManyInto(buf, 30)
	require.True(t, ok)
	require.Equal(t, 30, n)
	require.Equal(t, 2, q.Len())

	// Finish collecting
	q.FinishCollect(0)

	// Queue should shrink to 2 (smallest power of 2 >= initCap that fits 2 items)
	require.Equal(t, 2, q.Cap())
	require.Equal(t, 2, q.Len())

	// Verify items are still accessible
	item, ok := q.Remove()
	require.True(t, ok)
	require.Equal(t, "msg", string(item.Data))
}

// ---- Correctness tests for the v1 (atomic + chan doorbell) refactor ----

func TestQueueCloseIsIdempotent(t *testing.T) {
	q := New(2, ItemSize)
	q.Add(Item{Data: []byte("a")})
	q.Close()
	require.True(t, q.Closed())
	// Second Close must not panic (would double-close closeCh).
	q.Close()
	require.True(t, q.Closed())
}

func TestQueueCloseAfterCloseRemaining(t *testing.T) {
	q := New(2, ItemSize)
	q.Add(Item{Data: []byte("a")})
	q.Add(Item{Data: []byte("b")})
	rem := q.CloseRemaining()
	require.Len(t, rem, 2)
	require.True(t, q.Closed())
	// Calling Close after CloseRemaining must not panic.
	q.Close()
	require.True(t, q.Closed())
}

func TestQueueCloseRemainingAfterClose(t *testing.T) {
	q := New(2, ItemSize)
	q.Add(Item{Data: []byte("a")})
	q.Close()
	rem := q.CloseRemaining()
	require.Empty(t, rem)
	require.True(t, q.Closed())
}

func TestQueueCloseWakesParkedWaiters(t *testing.T) {
	// Multiple parked Waiters all wake when Close runs — close(closeCh)
	// is a broadcast even though the notify chan is single-fire.
	q := New(2, ItemSize)
	const nWaiters = 8
	var wg sync.WaitGroup
	wg.Add(nWaiters)
	for i := 0; i < nWaiters; i++ {
		go func() {
			defer wg.Done()
			require.False(t, q.Wait())
		}()
	}
	// Give waiters a moment to actually park.
	time.Sleep(20 * time.Millisecond)
	q.Close()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake all parked Waiters")
	}
}

func TestQueueWaitConcurrentClose(t *testing.T) {
	// Hammer Wait vs Close to flush out wake/closed ordering bugs.
	for iter := 0; iter < 50; iter++ {
		q := New(2, ItemSize)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Wait should return — either true (item arrived) or
			// false (closed). Must not block forever or panic.
			q.Wait()
		}()
		q.Close()
		wg.Wait()
	}
}

func TestQueueCapAfterClose(t *testing.T) {
	q := New(8, ItemSize)
	require.Equal(t, 8, q.Cap())
	q.Close()
	require.Equal(t, 0, q.Cap())
}

func TestQueueProducerConsumerStress(t *testing.T) {
	// Many concurrent producers + a single consumer. Verify every item
	// produced is eventually consumed (no losses) and that Len/Size are
	// non-negative throughout.
	const (
		nProducers       = 16
		perProducer      = 2000
		payloadSizeBytes = 7 // "msg-XXX" — varies by id; close enough
	)
	q := New(4, ItemSize)

	var consumed atomic.Int64
	var consumedBytes atomic.Int64
	done := make(chan struct{})
	go func() {
		buf := make([]Item, 64)
		for {
			if !q.Wait() {
				close(done)
				return
			}
			n, ok := q.RemoveManyInto(buf, -1)
			if !ok {
				continue
			}
			for i := 0; i < n; i++ {
				consumed.Add(1)
				consumedBytes.Add(int64(len(buf[i].Data)))
			}
			q.FinishCollect(0)
		}
	}()

	var wg sync.WaitGroup
	var expectedBytes atomic.Int64
	for p := 0; p < nProducers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				data := []byte("msg-" + strconv.Itoa(id))
				q.Add(Item{Data: data})
				expectedBytes.Add(int64(len(data)))
				// Lightweight invariant: Len/Size never negative.
				if q.Len() < 0 || q.Size() < 0 {
					t.Errorf("negative counter: len=%d size=%d", q.Len(), q.Size())
					return
				}
			}
		}(p)
	}
	wg.Wait()

	// Drain remaining via CloseRemaining (Close drops items;
	// CloseRemaining gives us a deterministic count).
	leftover := q.CloseRemaining()
	for _, it := range leftover {
		consumed.Add(1)
		consumedBytes.Add(int64(len(it.Data)))
	}
	<-done

	expected := int64(nProducers * perProducer)
	require.Equal(t, expected, consumed.Load(), "items lost or duplicated")
	require.Equal(t, expectedBytes.Load(), consumedBytes.Load(), "byte count mismatch")
}

func TestQueueLenSizeAtomicSnapshot(t *testing.T) {
	// Many readers calling Len/Size in parallel with one producer must
	// never see torn or negative values. Real assertion: this test
	// passes under -race without data race reports.
	q := New(2, ItemSize)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Producer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			q.Add(Item{Data: []byte("xyz")})
			if q.Len() > 100 {
				_, _ = q.Remove()
			}
		}
	}()
	// Readers.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l := q.Len()
				s := q.Size()
				if l < 0 || s < 0 {
					t.Errorf("negative counter: len=%d size=%d", l, s)
					return
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	q.Close()
}

func TestQueueAddManyEmptyOnClosed(t *testing.T) {
	// Regression: AddMany() with no items on a closed queue must
	// return false (matching the original behavior — closed queue
	// rejects all writes), not short-circuit to true.
	q := New(2, ItemSize)
	q.Close()
	require.False(t, q.AddMany())
	require.False(t, q.AddMany([]Item{}...))
}

func TestQueueAddManyEmptyOnOpen(t *testing.T) {
	// AddMany() with no items on an open queue is a successful no-op.
	q := New(2, ItemSize)
	require.True(t, q.AddMany())
	require.Equal(t, 0, q.Len())
}
