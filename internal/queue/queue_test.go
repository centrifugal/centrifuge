package queue

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteQueueResize(t *testing.T) {
	q := New()
	require.Equal(t, 0, q.Len())
	require.Equal(t, false, q.Closed())

	for i := 0; i < initialCapacity; i++ {
		q.Add([]byte(strconv.Itoa(i)))
	}
	q.Add([]byte("resize here"))
	require.Equal(t, initialCapacity*2, q.Cap())
	q.Remove()

	q.Add([]byte("new resize here"))
	require.Equal(t, initialCapacity*2, q.Cap())
	q.Add([]byte("one more item, no resize must happen"))
	require.Equal(t, initialCapacity*2, q.Cap())

	require.Equal(t, initialCapacity+2, q.Len())
}

func TestByteQueueSize(t *testing.T) {
	q := New()
	require.Equal(t, 0, q.Size())
	q.Add([]byte("1"))
	q.Add([]byte("2"))
	require.Equal(t, 2, q.Size())
	q.Remove()
	require.Equal(t, 1, q.Size())
}

func TestByteQueueWait(t *testing.T) {
	q := New()
	q.Add([]byte("1"))
	q.Add([]byte("2"))

	ok := q.Wait()
	require.Equal(t, true, ok)
	s, ok := q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "1", string(s))

	ok = q.Wait()
	require.Equal(t, true, ok)
	s, ok = q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "2", string(s))

	go func() {
		q.Add([]byte("3"))
	}()

	ok = q.Wait()
	require.Equal(t, true, ok)
	s, ok = q.Remove()
	require.Equal(t, true, ok)
	require.Equal(t, "3", string(s))
}

func TestByteQueueClose(t *testing.T) {
	q := New()

	// test removing from empty queue
	_, ok := q.Remove()
	require.Equal(t, false, ok)

	q.Add([]byte("1"))
	q.Add([]byte("2"))
	q.Close()

	ok = q.Add([]byte("3"))
	require.Equal(t, false, ok)

	ok = q.Wait()
	require.Equal(t, false, ok)

	_, ok = q.Remove()
	require.Equal(t, false, ok)

	require.Equal(t, true, q.Closed())
}

func TestByteQueueCloseRemaining(t *testing.T) {
	q := New()
	q.Add([]byte("1"))
	q.Add([]byte("2"))
	msgs := q.CloseRemaining()
	require.Equal(t, 2, len(msgs))
	ok := q.Add([]byte("3"))
	require.Equal(t, false, ok)
	require.Equal(t, true, q.Closed())
	msgs = q.CloseRemaining()
	require.Equal(t, 0, len(msgs))
}

func BenchmarkQueueAdd(b *testing.B) {
	q := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Add([]byte("test"))
	}
	b.StopTimer()
	q.Close()
}

func addAndConsume(q Queue, n int) {
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
		q.Add([]byte("test"))
	}
	<-done
}

func BenchmarkQueueAddConsume(b *testing.B) {
	q := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		addAndConsume(q, 10000)
	}
	b.StopTimer()
	q.Close()
}
