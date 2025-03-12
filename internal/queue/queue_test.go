package queue

import (
	"strconv"
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
