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
