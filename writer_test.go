package centrifuge

import (
	"bytes"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
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
// works and makes sense from syscal economy perspective. Compare result to
// BenchmarkWriteMergeDisabled.
func BenchmarkWriteMerge(b *testing.B) {
	transport := newBenchmarkTransport()
	defer transport.close()
	writer := newWriter(writerConfig{
		MaxMessagesInFrame: 4,
		WriteFn:            transport.writeSingle,
		WriteManyFn:        transport.writeCombined,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(writer, transport)
	}
	b.StopTimer()
}

func BenchmarkWriteMergeDisabled(b *testing.B) {
	transport := newBenchmarkTransport()
	defer transport.close()
	writer := newWriter(writerConfig{
		MaxMessagesInFrame: 1,
		WriteFn:            transport.writeSingle,
		WriteManyFn:        transport.writeCombined,
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(writer, transport)
	}
	b.StopTimer()
}

type fakeTransport struct {
	count int
	ch    chan struct{}
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		ch: make(chan struct{}, 1),
	}
}

func (t *fakeTransport) writeMany(bufs ...[]byte) error {
	for range bufs {
		t.count++
		t.ch <- struct{}{}
	}
	return nil
}

func (t *fakeTransport) write(buf []byte) error {
	t.count++
	t.ch <- struct{}{}
	return nil
}

func TestWriter(t *testing.T) {
	transport := newFakeTransport()
	w := newWriter(writerConfig{
		MaxMessagesInFrame: 4,
		WriteFn:            transport.write,
		WriteManyFn:        transport.writeMany,
	})
	disconnect := w.enqueue([]byte("test"))
	assert.Nil(t, disconnect)
	<-transport.ch
	assert.Equal(t, transport.count, 1)
	w.close()
	assert.True(t, w.closed)
}

func TestWriterDisconnect(t *testing.T) {
	transport := newFakeTransport()
	w := newWriter(writerConfig{
		MaxQueueSize: 1,
		WriteFn:      transport.write,
		WriteManyFn:  transport.writeMany,
	})
	disconnect := w.enqueue([]byte("test"))
	assert.NotNil(t, disconnect)
}
