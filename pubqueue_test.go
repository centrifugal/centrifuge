package centrifuge

import (
	"testing"

	"github.com/centrifugal/centrifuge/internal/prepared"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func newTestPreparedPub(data []byte) preparedPub {
	reply := prepared.NewReply(&protocol.Reply{
		Result: data,
	}, protocol.TypeProtobuf)
	return preparedPub{reply: reply}
}

func TestPubQueueResize(t *testing.T) {
	q := newPubQueue()
	require.Equal(t, 1, len(q.nodes))
	require.Equal(t, false, q.Closed())
	for i := 0; i < initialCapacity; i++ {
		q.Add(newTestPreparedPub([]byte("test")))
	}
	q.Add(newTestPreparedPub([]byte("test")))
	require.Equal(t, initialCapacity*2, cap(q.nodes))
	q.Remove()
	require.Equal(t, initialCapacity, cap(q.nodes))
}

func TestPubQueueSize(t *testing.T) {
	q := newPubQueue()
	require.Equal(t, 0, q.Size())

	preparedPub := newTestPreparedPub([]byte("1"))
	size := len(preparedPub.reply.Data())

	q.Add(preparedPub)
	q.Add(preparedPub)
	require.Equal(t, 2*size, q.Size())
	q.Remove()
	require.Equal(t, size, q.Size())
}

func TestPubQueueWait(t *testing.T) {
	q := newPubQueue()
	pub1 := newTestPreparedPub([]byte("1"))
	q.Add(pub1)
	pub2 := newTestPreparedPub([]byte("2"))
	q.Add(pub2)

	s, ok := q.Wait()
	require.Equal(t, true, ok)
	require.Equal(t, pub1, s)

	s, ok = q.Wait()
	require.Equal(t, true, ok)
	require.Equal(t, pub2, s)

	pub3 := newTestPreparedPub([]byte("3"))
	go func() {
		q.Add(pub3)
	}()

	s, ok = q.Wait()
	require.Equal(t, true, ok)
	require.Equal(t, pub3, s)
}

func TestPubQueueClose(t *testing.T) {
	q := newPubQueue()

	// test removing from empty queue
	_, ok := q.Remove()
	require.Equal(t, false, ok)

	q.Add(newTestPreparedPub([]byte("1")))
	q.Add(newTestPreparedPub([]byte("2")))
	q.Close()

	ok = q.Add(newTestPreparedPub([]byte("3")))
	require.Equal(t, false, ok)

	_, ok = q.Wait()
	require.Equal(t, false, ok)

	_, ok = q.Remove()
	require.Equal(t, false, ok)

	require.Equal(t, true, q.Closed())
}
