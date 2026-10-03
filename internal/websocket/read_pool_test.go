package websocket

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testReadBufferPool is used by newTestConn when set.
var testReadBufferPool *ReadBufferPool

// TestReadBufferPool runs read tests again with connections taking read
// buffers from a pool.
func TestReadBufferPool(t *testing.T) {
	testReadBufferPool = NewReadBufferPool(0)
	cstUpgrader.ReadBufferPool = NewReadBufferPool(0)
	defer func() {
		testReadBufferPool = nil
		cstUpgrader.ReadBufferPool = nil
	}()
	tests := []struct {
		name string
		f    func(*testing.T)
	}{
		{"Framing", TestFraming},
		{"Control", TestControl},
		{"CloseFrameBeforeFinalMessageFrame", TestCloseFrameBeforeFinalMessageFrame},
		{"EOFWithinFrame", TestEOFWithinFrame},
		{"EOFBeforeFinalFrame", TestEOFBeforeFinalFrame},
		{"ReadLimit", TestReadLimit},
		{"DecompressedReadLimit_Bomb", TestDecompressedReadLimit_Bomb},
		{"DecompressedReadLimit_Boundary", TestDecompressedReadLimit_Boundary},
		{"DecompressedReadLimit_LegitCompressible", TestDecompressedReadLimit_LegitCompressible},
		{"DecompressedReadLimit_ErrorIsPermanent", TestDecompressedReadLimit_ErrorIsPermanent},
		{"DecompressedReadLimit_StreamingReads", TestDecompressedReadLimit_StreamingReads},
		{"Dial", TestDial},
		{"DialTLS", TestDialTLS},
		{"DialCompression", TestDialCompression},
		{"Handshake", TestHandshake},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.f)
	}
}

type countingPool struct {
	p          sync.Pool
	gets, puts atomic.Int64
}

func (p *countingPool) Get() any {
	p.gets.Add(1)
	return p.p.Get()
}

func (p *countingPool) Put(x any) {
	p.puts.Add(1)
	p.p.Put(x)
}

func TestReadBufferPoolIdle(t *testing.T) {
	pool := &countingPool{}
	readPool := NewReadBufferPool(0)
	readPool.pool = pool
	upgrader := Upgrader{ReadBufferPool: readPool}
	serverConn := make(chan *Conn, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		serverConn <- c
	}))
	defer s.Close()

	wc, resp, _, err := (&Dialer{}).Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	defer func() { _ = wc.Close() }()
	rc := <-serverConn
	defer func() { _ = rc.Close() }()

	if rc.br == nil {
		t.Fatal("hijacked reader not used for the first frames")
	}

	messages := make(chan string)
	go func() {
		defer close(messages)
		for {
			_, p, err := rc.ReadMessage()
			if err != nil {
				return
			}
			messages <- string(p)
		}
	}()

	// waitPooled waits until the pool holds all readers taken from it plus the
	// hijacked one, so the connection holds none.
	waitPooled := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for pool.puts.Load()-pool.gets.Load() != 1 {
			if time.Now().After(deadline) {
				t.Fatalf("read buffer not returned to pool: %d gets, %d puts", pool.gets.Load(), pool.puts.Load())
			}
			time.Sleep(time.Millisecond)
		}
	}

	expectGets := func(msgs []string, want int64) {
		t.Helper()
		gets := pool.gets.Load()
		var batch []byte
		for _, msg := range msgs {
			w := &bufferWriter{}
			bc := newConn(fakeNetConn{Writer: w}, false, 1024, 1024, nil, nil, nil, nil)
			if err := bc.WriteMessage(TextMessage, []byte(msg)); err != nil {
				t.Fatal(err)
			}
			batch = append(batch, w.buf...)
		}
		// One write, so frames arrive together.
		if _, err := wc.NetConn().Write(batch); err != nil {
			t.Fatal(err)
		}
		for _, msg := range msgs {
			if got := <-messages; got != msg {
				t.Fatalf("got message of len %d, want %d", len(got), len(msg))
			}
		}
		waitPooled()
		if n := pool.gets.Load() - gets; n != want {
			t.Fatalf("got %d pool gets, want %d", n, want)
		}
	}

	// The first frame is read with the hijacked reader, which then goes to the pool.
	expectGets([]string{`{"id":1,"connect":{"token":"` + strings.Repeat("t", 300) + `"}}`}, 0)
	// Later frames take a reader from the pool, one for frames arriving together.
	expectGets([]string{"{}"}, 1)
	expectGets([]string{strings.Repeat("x", 10000)}, 1)
	expectGets([]string{strings.Repeat("a", 100), strings.Repeat("b", 100), strings.Repeat("c", 100)}, 1)

	// Ping arriving at an idle connection is answered.
	pong := make(chan struct{}, 1)
	wc.SetPongHandler(func([]byte) error { pong <- struct{}{}; return nil })
	go func() {
		for {
			if _, _, err := wc.NextReader(); err != nil {
				return
			}
		}
	}()
	if err := wc.WriteControl(PingMessage, []byte("p"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pong:
	case <-time.After(5 * time.Second):
		t.Fatal("no pong")
	}
	waitPooled()

	_ = wc.NetConn().Close()
	for range messages {
	}
}

// TestControlFrameLargerThanReadBuffer checks control frames with payloads
// that do not fit the small read buffer used for HTTP/2 and while idle with a
// read buffer pool.
func TestControlFrameLargerThanReadBuffer(t *testing.T) {
	newConns := map[string]func(r io.Reader) *Conn{
		"HTTP2": func(r io.Reader) *Conn {
			stream := &http2Stream{
				ReadCloser: io.NopCloser(r),
				Writer:     io.Discard,
				rc:         http.NewResponseController(httptest.NewRecorder()),
			}
			return (&Upgrader{}).newHTTP2Conn(stream)
		},
		"ReadBufferPool": func(r io.Reader) *Conn {
			return newConn(fakeNetConn{Reader: r, Writer: io.Discard}, true, 0, 0, NewReadBufferPool(0), nil, nil, nil)
		},
	}
	for name, newRC := range newConns {
		t.Run(name, func(t *testing.T) {
			for _, n := range []int{0, 1, 9, 10, 11, 16, 17, 100, maxControlFramePayloadSize} {
				payload := strings.Repeat("p", n)
				var buf bytes.Buffer
				wc := newTestConn(nil, &buf, false)
				if err := wc.WriteControl(PingMessage, []byte(payload), time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				if err := wc.WriteMessage(TextMessage, []byte("after")); err != nil {
					t.Fatal(err)
				}
				// Close payload is a 2-byte code plus the reason.
				reason := ""
				if n > 2 {
					reason = strings.Repeat("r", n-2)
				}
				if err := wc.WriteControl(CloseMessage, FormatCloseMessage(CloseNormalClosure, reason), time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}

				rc := newRC(&buf)
				var ping string
				rc.SetPingHandler(func(p []byte) error { ping = string(p); return nil })
				_, p, err := rc.ReadMessage()
				if err != nil {
					t.Fatalf("%d: %v", n, err)
				}
				if ping != payload {
					t.Fatalf("%d: got ping of len %d", n, len(ping))
				}
				if string(p) != "after" {
					t.Fatalf("%d: got message %q", n, p)
				}
				_, _, err = rc.ReadMessage()
				var closeErr *CloseError
				if !errors.As(err, &closeErr) || closeErr.Code != CloseNormalClosure || closeErr.Text != reason {
					t.Fatalf("%d: got %v, want close with reason of len %d", n, err, len(reason))
				}
			}
		})
	}
}

type bufferWriter struct{ buf []byte }

func (w *bufferWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

var _ io.Writer = (*bufferWriter)(nil)
var _ net.Conn = fakeNetConn{}

// newBenchConnPair returns server and client connections over loopback TCP.
func newBenchConnPair(b *testing.B, pool *ReadBufferPool) (server, client *Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			b.Error(err)
		}
		accepted <- c
	}()
	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	sc := <-accepted
	b.Cleanup(func() { _ = cc.Close(); _ = sc.Close() })
	return newConn(sc, true, 0, 0, pool, nil, nil, nil), newConn(cc, false, 0, 0, nil, nil, nil, nil)
}

func BenchmarkReadMessage(b *testing.B) {
	b.Run("Command", func(b *testing.B) {
		benchReadMessage(b, []byte(`{"id":1,"publish":{"channel":"chat","data":{"input":"hello"}}}`))
	})
	b.Run("Pong", func(b *testing.B) { benchReadMessage(b, []byte(`{}`)) })
}

func benchReadMessage(b *testing.B, msg []byte) {
	for _, pooled := range []bool{false, true} {
		name := "Buffer"
		var pool *ReadBufferPool
		if pooled {
			name = "Pool"
			pool = NewReadBufferPool(0)
		}
		// Every frame arrives at an idle connection.
		b.Run(name+"/Idle", func(b *testing.B) {
			sc, cc := newBenchConnPair(b, pool)
			ack := make([]byte, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cc.WriteMessage(TextMessage, msg); err != nil {
					b.Fatal(err)
				}
				if _, _, err := sc.ReadMessage(); err != nil {
					b.Fatal(err)
				}
				if _, err := sc.conn.Write(ack); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(cc.conn, ack); err != nil {
					b.Fatal(err)
				}
			}
		})
		// Frames arrive back to back.
		b.Run(name+"/Stream", func(b *testing.B) {
			sc, cc := newBenchConnPair(b, pool)
			go func() {
				for i := 0; i < b.N; i++ {
					if err := cc.WriteMessage(TextMessage, msg); err != nil {
						return
					}
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := sc.ReadMessage(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkConnectFirstFrame establishes connections that send one connect-like
// frame and stay idle, like clients do during mass reconnects. Besides
// allocations it reports the heap retained per idle connection (client side
// included, it is the same in both cases).
func BenchmarkConnectFirstFrame(b *testing.B) {
	frame := []byte(`{"id":1,"connect":{"token":"` + strings.Repeat("t", 350) + `"}}`)
	for _, pooled := range []bool{false, true} {
		name := "Buffer"
		upgrader := Upgrader{}
		if pooled {
			name = "Pool"
			upgrader.ReadBufferPool = NewReadBufferPool(0)
		}
		b.Run(name, func(b *testing.B) {
			read := make(chan struct{}, 1)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, _, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					b.Error(err)
					return
				}
				go func() {
					defer func() { _ = c.Close() }()
					for {
						if _, _, err := c.ReadMessage(); err != nil {
							return
						}
						read <- struct{}{}
					}
				}()
			}))
			defer s.Close()
			url := "ws" + strings.TrimPrefix(s.URL, "http")
			conns := make([]*Conn, 0, b.N)
			defer func() {
				for _, c := range conns {
					_ = c.Close()
				}
			}()

			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, resp, _, err := (&Dialer{}).Dial(url, nil)
				if err != nil {
					b.Fatal(err)
				}
				_ = resp.Body.Close()
				conns = append(conns, c)
				if err := c.WriteMessage(TextMessage, frame); err != nil {
					b.Fatal(err)
				}
				<-read
			}
			b.StopTimer()
			// Let read loops get back to waiting for the next frame.
			time.Sleep(100 * time.Millisecond)
			for i := 0; i < 4; i++ {
				runtime.GC()
			}
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "live-B/conn")
		})
	}
}
