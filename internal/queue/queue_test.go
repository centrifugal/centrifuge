package queue

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func testItem(data []byte) Item {
	return Item{Data: data}
}

var initialCapacity = 2

func TestByteQueueResize(t *testing.T) {
	q := New(initialCapacity)
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
	q := New(initialCapacity)
	require.Equal(t, 0, q.Size())
	q.Add(testItem([]byte("1")))
	q.Add(testItem([]byte("2")))
	require.Equal(t, 2, q.Size())
	q.Remove()
	require.Equal(t, 1, q.Size())
}

func TestByteQueueWait(t *testing.T) {
	q := New(initialCapacity)
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
	q := New(initialCapacity)
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
	q := New(initialCapacity)
	q.AddMany(testItem([]byte("1")), testItem([]byte("2")))
	ok := q.Wait()
	require.Equal(t, true, ok)
	items, ok := q.RemoveMany(-1)
	require.Equal(t, true, ok)
	require.Equal(t, 2, len(items))
}

func TestByteQueueRemoveManyFixed(t *testing.T) {
	q := New(initialCapacity)
	q.AddMany(testItem([]byte("1")), testItem([]byte("2")))
	ok := q.Wait()
	require.Equal(t, true, ok)
	items, ok := q.RemoveMany(2)
	require.Equal(t, true, ok)
	require.Equal(t, 2, len(items))
}

func TestQueueClose(t *testing.T) {
	q := New(initialCapacity)

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
	q := New(initialCapacity)
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
	q := New(initialCapacity)

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
	q := New(initialCapacity)

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
	q := New(initialCapacity)

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
	q := New(2)
	for i := 0; i < 10; i++ {
		item := Item{Data: []byte("msg"), Channel: "ch"}
		require.True(t, q.Add(item))
	}
	require.GreaterOrEqual(t, q.Cap(), 10)
}

func TestQueueCloseRemaining(t *testing.T) {
	q := New(3)
	q.Add(Item{Data: []byte("msg1"), Channel: "ch1"})
	q.Add(Item{Data: []byte("msg2"), Channel: "ch2"})

	remaining := q.CloseRemaining()
	require.Len(t, remaining, 2)
	require.True(t, q.Closed())
}

func TestQueueWait(t *testing.T) {
	q := New(1)
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
	q := New(5)
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
	q := New(5)
	for i := 0; i < 5; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	removed, ok := q.RemoveMany(3)
	require.True(t, ok)
	require.Len(t, removed, 3)
	require.Equal(t, 2, q.Len())
}

func TestQueueRemoveManyAll(t *testing.T) {
	q := New(3)
	for i := 0; i < 3; i++ {
		q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	}
	removed, ok := q.RemoveMany(-1)
	require.True(t, ok)
	require.Len(t, removed, 3)
	require.Equal(t, 0, q.Len())
}

func TestQueueAddAfterClose(t *testing.T) {
	q := New(2)
	q.Close()
	added := q.Add(Item{Data: []byte("msg"), Channel: "ch"})
	require.False(t, added)
}

func TestQueueRemoveFromEmptyQueue(t *testing.T) {
	q := New(2)
	_, ok := q.Remove()
	require.False(t, ok)
}

func TestQueueAddManyAfterClose(t *testing.T) {
	q := New(2)
	q.Close()
	added := q.AddMany(Item{Data: []byte("msg1"), Channel: "ch"}, Item{Data: []byte("msg2"), Channel: "ch"})
	require.False(t, added)
}

func TestQueueRemoveManyFromEmptyQueue(t *testing.T) {
	q := New(2)
	removed, ok := q.RemoveMany(5)
	require.False(t, ok)
	require.Nil(t, removed)
}

func TestQueueWaitOnClosedQueue(t *testing.T) {
	q := New(1)
	q.Close()
	require.False(t, q.Wait())
}

func TestQueueCorrectness(t *testing.T) {
	q := New(5)
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
	q := New(2)
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

func BenchmarkQueueAdd(b *testing.B) {
	q := New(initialCapacity)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Add(testItem([]byte("test")))
	}
	b.StopTimer()
	q.Close()
}

func addAndConsume(q *Queue, n int) {
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
	q := New(initialCapacity)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addAndConsume(q, 10000)
	}
	b.StopTimer()
	q.Close()
}
