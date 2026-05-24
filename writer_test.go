package centrifuge

import (
	"bytes"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"

	"github.com/stretchr/testify/require"
)

const numQueueMessages = 4

type benchmarkTransport struct {
	f     *os.File
	ch    chan struct{}
	count int64
	buf   []byte
}

func newBenchmarkTransport() *benchmarkTransport {
	f, err := os.Create("/dev/null")
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 512)
	for i := 0; i < 512; i++ {
		buf[i] = 'a'
	}

	return &benchmarkTransport{
		f:   f,
		ch:  make(chan struct{}),
		buf: buf,
	}
}

func (t *benchmarkTransport) inc(num int) {
	atomic.AddInt64(&t.count, int64(num))
	if atomic.LoadInt64(&t.count) == numQueueMessages {
		atomic.StoreInt64(&t.count, 0)
		close(t.ch)
	}
}

func (t *benchmarkTransport) writeCombined(items ...queue.Item) error {
	buffers := make([][]byte, len(items))
	for i := 0; i < len(items); i++ {
		buffers[i] = items[i].Data
	}
	_, err := t.f.Write(bytes.Join(buffers, []byte("\n")))
	if err != nil {
		panic(err)
	}
	t.inc(len(buffers))
	return nil
}

func (t *benchmarkTransport) writeSingle(item queue.Item) error {
	_, err := t.f.Write(item.Data)
	if err != nil {
		panic(err)
	}
	t.inc(1)
	return nil
}

func (t *benchmarkTransport) close() error {
	return t.f.Close()
}

func runWrite(w *writer, t *benchmarkTransport) {
	go func() {
		for j := 0; j < numQueueMessages; j++ {
			w.messages.Add(queue.Item{Data: t.buf})
		}
	}()
	<-t.ch
	t.ch = make(chan struct{})
}

// BenchmarkWriteMerge allows to be sure that merging messages into one frame
// works and makes sense from syscall economy perspective. Compare result to
// BenchmarkWriteMergeDisabled.
func BenchmarkWriteMerge(b *testing.B) {
	transport := newBenchmarkTransport()
	defer func() { _ = transport.close() }()
	writer := newWriter(writerConfig{
		WriteFn:     transport.writeSingle,
		WriteManyFn: transport.writeCombined,
	}, 0)
	go writer.run(0, 4, 0, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(writer, transport)
	}
	b.StopTimer()
}

func BenchmarkWriteMergeDisabled(b *testing.B) {
	transport := newBenchmarkTransport()
	defer func() { _ = transport.close() }()
	writer := newWriter(writerConfig{
		WriteFn:     transport.writeSingle,
		WriteManyFn: transport.writeCombined,
	}, 0)
	go writer.run(0, 1, 0, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(writer, transport)
	}
	b.StopTimer()
}

type fakeTransport struct {
	count          int
	writeError     error
	ch             chan struct{}
	writeCalls     int
	writeManyCalls int
}

func newFakeTransport(writeError error) *fakeTransport {
	return &fakeTransport{
		ch:         make(chan struct{}, 1),
		writeError: writeError,
	}
}

func (t *fakeTransport) writeMany(items ...queue.Item) error {
	t.writeManyCalls++
	for range items {
		t.count++
		t.ch <- struct{}{}
	}
	if t.writeError != nil {
		return t.writeError
	}
	return nil
}

func (t *fakeTransport) write(_ queue.Item) error {
	t.writeCalls++
	t.count++
	t.ch <- struct{}{}
	if t.writeError != nil {
		return t.writeError
	}
	return nil
}

func TestWriter(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		WriteFn:     transport.write,
		WriteManyFn: transport.writeMany,
	}, 0)
	go w.run(0, 4, 0, false)

	disconnect := w.enqueue(queue.Item{Data: []byte("test")})
	require.Nil(t, disconnect)
	<-transport.ch
	require.Equal(t, transport.count, 1)
	err := w.close(true)
	require.NoError(t, err)
	require.True(t, w.closed)
	// Close already deactivated Writer.
	err = w.close(true)
	require.NoError(t, err)
}

func TestWriterWriteMany(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)

	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)

	maxMessagesInFrame := 4
	numMessages := 4 * maxMessagesInFrame

	// Timer-driven mode: run() is non-blocking when writeDelay > 0 and useWriteTimer is true.
	w.run(10*time.Millisecond, maxMessagesInFrame, 0, true)

	// Enqueue messages - this will trigger the timer
	for i := 0; i < numMessages; i++ {
		disconnect := w.enqueue(queue.Item{Data: []byte("test")})
		require.Nil(t, disconnect)
	}

	for i := 0; i < numMessages; i++ {
		<-transport.ch
	}

	require.Equal(t, transport.count, numMessages)
	require.Equal(t, numMessages/maxMessagesInFrame, transport.writeManyCalls)
	err := w.close(true)
	require.NoError(t, err)
}

func TestWriterWriteRemaining(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)

	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)

	maxMessagesInFrame := 4
	numMessages := 4 * maxMessagesInFrame
	for i := 0; i < numMessages; i++ {
		disconnect := w.enqueue(queue.Item{Data: []byte("test")})
		require.Nil(t, disconnect)
	}

	go func() {
		err := w.close(true)
		require.NoError(t, err)
	}()

	for i := 0; i < numMessages; i++ {
		<-transport.ch
	}

	require.Equal(t, transport.count, numMessages)
	require.Equal(t, 1, transport.writeManyCalls)
}

func TestWriterDisconnectSlow(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)

	w := newWriter(writerConfig{
		MaxQueueSize: 1,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	defer func() { _ = w.close(true) }()

	disconnect := w.enqueue(queue.Item{Data: []byte("test")})
	require.Equal(t, DisconnectSlow.Code, disconnect.Code)
}

func TestWriterDisconnectNormalOnClosedQueue(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)

	w := newWriter(writerConfig{
		MaxQueueSize: 1,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	go w.run(0, 0, 0, false)
	_ = w.close(true)

	disconnect := w.enqueue(queue.Item{Data: []byte("test")})
	require.Equal(t, DisconnectConnectionClosed.Code, disconnect.Code)
}

func TestWriterWriteError(t *testing.T) {
	t.Parallel()
	errWrite := errors.New("write error")
	transport := newFakeTransport(errWrite)

	w := newWriter(writerConfig{
		MaxQueueSize: 1,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)

	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		w.run(0, 0, 0, false)
	}()

	defer func() { _ = w.close(true) }()

	disconnect := w.enqueue(queue.Item{Data: []byte("test")})
	require.NotNil(t, disconnect)

	go func() {
		for {
			select {
			case <-doneCh:
				return
			case <-transport.ch:
			}
		}
	}()

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for write routine close")
	}
}

// TestWriterEnqueueMany covers the multi-item enqueue path.
func TestWriterEnqueueMany(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	go w.run(0, 8, 0, false)
	defer func() { _ = w.close(true) }()

	dis := w.enqueueMany(
		queue.Item{Data: []byte("a")},
		queue.Item{Data: []byte("b")},
		queue.Item{Data: []byte("c")},
	)
	require.Nil(t, dis)
	for i := 0; i < 3; i++ {
		select {
		case <-transport.ch:
		case <-time.After(time.Second):
			t.Fatal("did not receive expected message")
		}
	}
}

// TestWriterEnqueueManyClosed verifies that enqueueing into a closed queue returns
// DisconnectConnectionClosed.
func TestWriterEnqueueManyClosed(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 1,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	go w.run(0, 0, 0, false)
	_ = w.close(true)

	dis := w.enqueueMany(queue.Item{Data: []byte("x")})
	require.NotNil(t, dis)
	require.Equal(t, DisconnectConnectionClosed.Code, dis.Code)
}

// TestWriterEnqueueManyDisconnectSlow exercises the size-exceeded branch
// in enqueueMany.
func TestWriterEnqueueManyDisconnectSlow(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 1, // 1 byte cap so even a tiny payload trips DisconnectSlow.
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	defer func() { _ = w.close(true) }()

	dis := w.enqueueMany(queue.Item{Data: []byte("biggerthanonebyte")})
	require.NotNil(t, dis)
	require.Equal(t, DisconnectSlow.Code, dis.Code)
}

// TestWriterEnqueueManyTimerMode covers the timerMode branch of enqueueMany,
// which schedules a flush via the writer's flush timer.
func TestWriterEnqueueManyTimerMode(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	w.run(20*time.Millisecond, 16, 0, true) // useWriteTimer=true → timer mode
	defer func() { _ = w.close(true) }()

	dis := w.enqueueMany(
		queue.Item{Data: []byte("a")},
		queue.Item{Data: []byte("b")},
	)
	require.Nil(t, dis)
	for i := 0; i < 2; i++ {
		select {
		case <-transport.ch:
		case <-time.After(time.Second):
			t.Fatal("timer-mode flush did not deliver message")
		}
	}
}

// TestWriter_NextPrevLogBase2_Zero covers the early-return branches for
// nextLogBase2(0) and prevLogBase2(0).
func TestWriter_NextPrevLogBase2_Zero(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint32(0), nextLogBase2(0))
	require.Equal(t, uint32(0), prevLogBase2(0))
}

// TestWriter_PrevLogBase2_NonPowerOfTwo covers the final return next-1
// branch in prevLogBase2 (input is not a power of two).
func TestWriter_PrevLogBase2_NonPowerOfTwo(t *testing.T) {
	t.Parallel()
	// 5 is between 4 (2^2) and 8 (2^3); rounded down → log2(4) = 2.
	require.Equal(t, uint32(2), prevLogBase2(5))
	// 100 is between 64 (2^6) and 128 (2^7); rounded down → 6.
	require.Equal(t, uint32(6), prevLogBase2(100))
}

// TestWriter_GetItemBuf_ZeroLength covers the length<=0 branch in getItemBuf
// which falls back to defaultMaxMessagesInFrame.
func TestWriter_GetItemBuf_ZeroLength(t *testing.T) {
	t.Parallel()
	buf := getItemBuf(0)
	require.NotNil(t, buf)
	require.Equal(t, defaultMaxMessagesInFrame, len(buf.B))
	putItemBuf(buf)
}

// TestWriter_GetItemBuf_OversizedLength covers the length>maxItemBufLength
// branch which bypasses the pool and allocates fresh.
func TestWriter_GetItemBuf_OversizedLength(t *testing.T) {
	t.Parallel()
	buf := getItemBuf(maxItemBufLength + 1)
	require.NotNil(t, buf)
	require.Equal(t, maxItemBufLength+1, len(buf.B))
	// putItemBuf must drop oversized buffers (cap > maxItemBufLength branch).
	putItemBuf(buf)
}

// TestWriter_PutItemBuf_EmptyCapacity covers the cap==0 drop branch in
// putItemBuf. We construct an itemBuf with empty backing slice manually.
func TestWriter_PutItemBuf_EmptyCapacity(t *testing.T) {
	t.Parallel()
	// Empty buf — cap is 0 → drop.
	putItemBuf(&itemBuf{B: nil})
	// Sanity: also pass an oversized cap which goes through the same drop path.
	putItemBuf(&itemBuf{B: make([]queue.Item, 0, maxItemBufLength+8)})
}

// TestWriter_WriteDelay_SingleMessage covers the writeDelay path where exactly
// one message is in the queue, hitting the n==1 → WriteFn branch (instead of
// WriteManyFn).
func TestWriter_WriteDelay_SingleMessage(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	// Goroutine mode (useWriteTimer=false) with writeDelay > 0 — exercises the
	// writeDelay branch in waitSendMessage.
	go w.run(20*time.Millisecond, 16, 0, false)
	defer func() { _ = w.close(true) }()

	dis := w.enqueue(queue.Item{Data: []byte("only-one")})
	require.Nil(t, dis)

	select {
	case <-transport.ch:
	case <-time.After(time.Second):
		t.Fatal("single message in delay mode not delivered")
	}
	require.GreaterOrEqual(t, transport.writeCalls, 1)
}

// TestWriter_WriteDelay_Unlimited covers the maxMessagesInFrame=-1 branch in
// the writeDelay path of waitSendMessage (bufSize := w.messages.Len()).
func TestWriter_WriteDelay_Unlimited(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	go w.run(20*time.Millisecond, -1, 0, false)
	defer func() { _ = w.close(true) }()

	for i := 0; i < 4; i++ {
		require.Nil(t, w.enqueue(queue.Item{Data: []byte("x")}))
	}
	for i := 0; i < 4; i++ {
		select {
		case <-transport.ch:
		case <-time.After(time.Second):
			t.Fatal("delivery timed out")
		}
	}
}

// TestWriter_WriteDelay_WriteError covers the writeErr != nil branch in
// the writeDelay path of waitSendMessage.
func TestWriter_WriteDelay_WriteError(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(errors.New("boom"))
	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	done := make(chan struct{})
	go func() {
		w.run(20*time.Millisecond, 16, 0, false)
		close(done)
	}()
	defer func() { _ = w.close(false) }()

	require.Nil(t, w.enqueue(queue.Item{Data: []byte("err")}))
	// WriteFn returns error → waitSendMessage returns false → run loop exits.
	select {
	case <-transport.ch:
	case <-time.After(time.Second):
		t.Fatal("write was not attempted")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writer goroutine did not exit on write error")
	}
}

// TestWriter_TimerFlush_Unlimited covers the bufSize<0 branch in the timer
// flush path (when maxMessagesInFrame is -1).
func TestWriter_TimerFlush_Unlimited(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	w.run(20*time.Millisecond, -1, 0, true) // timer mode + unlimited frame
	defer func() { _ = w.close(true) }()

	require.Nil(t, w.enqueueMany(
		queue.Item{Data: []byte("a")},
		queue.Item{Data: []byte("b")},
		queue.Item{Data: []byte("c")},
	))
	for i := 0; i < 3; i++ {
		select {
		case <-transport.ch:
		case <-time.After(time.Second):
			t.Fatal("delivery timed out")
		}
	}
}

// TestWriter_ScheduleFlushLocked_AlreadyScheduled covers the early-return
// branches in scheduleFlushLocked and scheduleFlushImmediateLocked when a
// timer is already scheduled.
func TestWriter_ScheduleFlushLocked_AlreadyScheduled(t *testing.T) {
	t.Parallel()
	transport := newFakeTransport(nil)
	w := newWriter(writerConfig{
		MaxQueueSize: 10 * 1024,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	}, 0)
	w.run(time.Hour, 1, 0, true) // timer mode with very long delay
	defer func() { _ = w.close(false) }()

	w.mu.Lock()
	w.scheduleFlushLocked()
	require.True(t, w.timerScheduled)
	// Second call must early-return (no panic, no double-Reset).
	w.scheduleFlushLocked()
	w.scheduleFlushImmediateLocked()
	w.mu.Unlock()
}
