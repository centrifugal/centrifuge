package centrifuge

import (
	"bytes"
	"os"
	"sync/atomic"
	"testing"
)

const numQueueMessages = 100

type testTransport struct {
	f            *os.File
	ch           chan struct{}
	count        int64
	smallPayload []byte
	largePayload []byte
}

func newTestTransport() *testTransport {
	f, err := os.Create("/dev/null")
	if err != nil {
		panic(err)
	}

	smallBuf := make([]byte, 512)
	for i := 0; i < 512; i++ {
		smallBuf[i] = 'a'
	}

	largeBuf := make([]byte, 10000)
	for i := 0; i < 10000; i++ {
		largeBuf[i] = 'a'
	}

	return &testTransport{
		f:            f,
		ch:           make(chan struct{}),
		smallPayload: smallBuf,
		largePayload: largeBuf,
	}
}

func (t *testTransport) inc(num int) {
	atomic.AddInt64(&t.count, int64(num))
	if atomic.LoadInt64(&t.count) == numQueueMessages {
		atomic.StoreInt64(&t.count, 0)
		close(t.ch)
	}
}

func (t *testTransport) writeCombined(bufs ...[]byte) error {
	_, err := t.f.Write(bytes.Join(bufs, []byte("\n")))
	if err != nil {
		panic(err)
	}
	t.inc(len(bufs))
	return nil
}

func (t *testTransport) close() error {
	return t.f.Close()
}

func runWrite(w *writer, t *testTransport) {
	go func() {
		for j := 0; j < numQueueMessages; j++ {
			w.messages.Add(t.smallPayload)
		}
	}()
	<-t.ch
	t.ch = make(chan struct{})
}

func BenchmarkWriteMerge(b *testing.B) {
	transport := newTestTransport()
	defer transport.close()
	writer := newWriter(writerConfig{MaxMessagesInFrame: 4})
	writer.onWrite(transport.writeCombined)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(writer, transport)
	}
	b.StopTimer()
}

func BenchmarkWriteMergeDisabled(b *testing.B) {
	transport := newTestTransport()
	defer transport.close()
	writer := newWriter(writerConfig{MaxMessagesInFrame: 1})
	writer.onWrite(transport.writeCombined)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runWrite(writer, transport)
	}
	b.StopTimer()
}
