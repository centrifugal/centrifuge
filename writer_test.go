package centrifuge

import (
	"bytes"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

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

func (t *benchmarkTransport) writeCombined(bufs ...[]byte) error {
	_, err := t.f.Write(bytes.Join(bufs, []byte("\n")))
	if err != nil {
		panic(err)
	}
	t.inc(len(bufs))
	return nil
}

func (t *benchmarkTransport) writeSingle(data []byte) error {
	_, err := t.f.Write(data)
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
			w.messages.Add(t.buf)
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
		MaxMessagesInFrame: 4,
		WriteFn:            transport.writeSingle,
		WriteManyFn:        transport.writeCombined,
	})
	go writer.run()

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
		MaxMessagesInFrame: 1,
		WriteFn:            transport.writeSingle,
		WriteManyFn:        transport.writeCombined,
	})
	go writer.run()

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

func (t *fakeTransport) writeMany(bufs ...[]byte) error {
	t.writeManyCalls++
	for range bufs {
		t.count++
		t.ch <- struct{}{}
	}
	if t.writeError != nil {
		return t.writeError
	}
	return nil
}

func (t *fakeTransport) write(_ []byte) error {
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
		MaxMessagesInFrame: 4,
		WriteFn:            transport.write,
		WriteManyFn:        transport.writeMany,
	})
	go w.run()

	disconnect := w.enqueue([]byte("test"))
	require.Nil(t, disconnect)
	<-transport.ch
	require.Equal(t, transport.count, 1)
	err := w.close()
	require.NoError(t, err)
	require.True(t, w.closed)
	// Close already deactivated Writer.
	err = w.close()
	require.NoError(t, err)
}

func TestWriterWriteMany(t *testing.T) {
	transport := newFakeTransport(nil)

	w := newWriter(writerConfig{
		MaxQueueSize:       10 * 1024,
		MaxMessagesInFrame: 4,
		WriteFn:            transport.write,
		WriteManyFn:        transport.writeMany,
	})

	numMessages := 4 * w.config.MaxMessagesInFrame
	for i := 0; i < numMessages; i++ {
		disconnect := w.enqueue([]byte("test"))
		require.Nil(t, disconnect)
	}

	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		w.run()
	}()

	for i := 0; i < numMessages; i++ {
		<-transport.ch
	}

	require.Equal(t, transport.count, numMessages)
	require.Equal(t, numMessages/w.config.MaxMessagesInFrame, transport.writeManyCalls)
	err := w.close()
	require.NoError(t, err)

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for write routine close")
	}
}

func TestWriterWriteRemaining(t *testing.T) {
	transport := newFakeTransport(nil)

	w := newWriter(writerConfig{
		MaxQueueSize:       10 * 1024,
		MaxMessagesInFrame: 4,
		WriteFn:            transport.write,
		WriteManyFn:        transport.writeMany,
	})

	numMessages := 4 * w.config.MaxMessagesInFrame
	for i := 0; i < numMessages; i++ {
		disconnect := w.enqueue([]byte("test"))
		require.Nil(t, disconnect)
	}

	go func() {
		err := w.close()
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
	})
	defer func() { _ = w.close() }()

	disconnect := w.enqueue([]byte("test"))
	require.Equal(t, DisconnectSlow, disconnect)
}

func TestWriterDisconnectNormalOnClosedQueue(t *testing.T) {
	transport := newFakeTransport(nil)

	w := newWriter(writerConfig{
		MaxQueueSize: 1,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	})
	go w.run()
	_ = w.close()

	disconnect := w.enqueue([]byte("test"))
	require.Equal(t, DisconnectNormal, disconnect)
}

func TestWriterWriteError(t *testing.T) {
	errWrite := errors.New("write error")
	transport := newFakeTransport(errWrite)

	w := newWriter(writerConfig{
		MaxQueueSize: 1,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	})

	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		w.run()
	}()

	defer func() { _ = w.close() }()

	disconnect := w.enqueue([]byte("test"))
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
