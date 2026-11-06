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
	"sync/atomic"
	"testing"
	"time"
)

// testReadBufferPool is used by newTestConn when set.
var testReadBufferPool *readBufferPool

// TestReadBufferPool runs read tests again with connections taking read
// buffers from a pool. The tests must not be parallel: the pool is set in
// package-level variables only while TestReadBufferPool runs.
func TestReadBufferPool(t *testing.T) {
	testReadBufferPool = sharedReadBufferPool(0)
	cstUpgrader.UseReadBufferPool = true
	defer func() {
		testReadBufferPool = nil
		cstUpgrader.UseReadBufferPool = false
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
		done := false
		t.Run(tt.name, func(t *testing.T) {
			tt.f(t)
			done = true
		})
		// A parallel test would run after the pool is unset.
		if !done {
			t.Fatalf("%s must not be parallel", tt.name)
		}
	}
}

// countingListener counts 1-byte reads of accepted connections: a connection
// with a read buffer pool waits for a frame with such a read after returning
// its read buffer, and takes a buffer from the pool once it returns. Each
// change of the counters is signalled on changed.
type countingListener struct {
	net.Listener
	started, returned *atomic.Int64
	changed           chan struct{}
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return countingConn{Conn: c, l: l}, nil
}

type countingConn struct {
	net.Conn
	l countingListener
}

func (c countingConn) Read(p []byte) (int, error) {
	if len(p) != 1 {
		return c.Conn.Read(p)
	}
	c.l.started.Add(1)
	notify(c.l.changed)
	defer notify(c.l.changed)
	defer c.l.returned.Add(1)
	return c.Conn.Read(p)
}

// readingListener counts reads in progress on accepted connections: a
// connection waiting for the next frame has one. Each change is signalled on
// changed.
type readingListener struct {
	net.Listener
	reading *atomic.Int64
	changed chan struct{}
}

func (l readingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return readingConn{Conn: c, l: l}, nil
}

type readingConn struct {
	net.Conn
	l readingListener
}

func (c readingConn) Read(p []byte) (int, error) {
	c.l.reading.Add(1)
	notify(c.l.changed)
	defer notify(c.l.changed)
	defer c.l.reading.Add(-1)
	return c.Conn.Read(p)
}

// notify signals a change without blocking: changed has a buffer of one, a
// waiter rechecks its condition after each signal.
func notify(changed chan struct{}) {
	select {
	case changed <- struct{}{}:
	default:
	}
}

// waitFor waits until cond holds, checking it again on each signal on changed.
func waitFor(tb testing.TB, changed <-chan struct{}, cond func() bool, what string) {
	tb.Helper()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for !cond() {
		select {
		case <-changed:
		case <-timeout.C:
			tb.Fatal(what)
		}
	}
}

func TestReadBufferPoolIdle(t *testing.T) {
	var started, returned atomic.Int64
	upgrader := Upgrader{UseReadBufferPool: true}
	serverConn := make(chan *Conn, 1)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		serverConn <- c
	}))
	changed := make(chan struct{}, 1)
	s.Listener = countingListener{Listener: s.Listener, started: &started, returned: &returned, changed: changed}
	s.Start()
	defer s.Close()

	wc, resp, _, err := (&Dialer{}).Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	defer func() { _ = wc.Close() }()
	rc := <-serverConn
	defer func() { _ = rc.Close() }()

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

	// waitIdle waits until the connection waits for a frame without a read
	// buffer.
	waitIdle := func() {
		t.Helper()
		waitFor(t, changed, func() bool { return started.Load()-returned.Load() == 1 }, "read buffer not returned to pool")
	}

	// expectGets sends messages in one write and checks the number of readers
	// taken from the pool to read them. Loopback TCP may still deliver one
	// write in several reads, then frames after a split take another reader.
	expectGets := func(msgs []string, wantMin, wantMax int64) {
		t.Helper()
		gets := returned.Load()
		var batch []byte
		for _, msg := range msgs {
			w := &bufferWriter{}
			bc := newConn(fakeNetConn{Writer: w}, false, 1024, 1024, nil, nil, nil, nil)
			if err := bc.WriteMessage(TextMessage, []byte(msg)); err != nil {
				t.Fatal(err)
			}
			batch = append(batch, w.buf...)
		}
		if _, err := wc.NetConn().Write(batch); err != nil {
			t.Fatal(err)
		}
		for _, msg := range msgs {
			if got := <-messages; got != msg {
				t.Fatalf("got message of len %d, want %d", len(got), len(msg))
			}
		}
		waitIdle()
		if n := returned.Load() - gets; n < wantMin || n > wantMax {
			t.Fatalf("got %d pool gets, want %d to %d", n, wantMin, wantMax)
		}
	}

	// The first frame takes a reader from the pool, where the hijacked reader went.
	expectGets([]string{`{"id":1,"connect":{"token":"` + strings.Repeat("t", 300) + `"}}`}, 1, 1)
	// Later frames take a reader from the pool, one for frames arriving together.
	expectGets([]string{"{}"}, 1, 1)
	expectGets([]string{strings.Repeat("x", 10000)}, 1, 1)
	expectGets([]string{strings.Repeat("a", 100), strings.Repeat("b", 100), strings.Repeat("c", 100)}, 1, 3)

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
	waitIdle()

	_ = wc.NetConn().Close()
	for range messages {
	}
}

// TestReadBufferPoolHijackedReaderSize checks that the hijacked reader goes to
// the pool only if it has the pool's size: connections read with readers of
// the pool's size whatever it is.
func TestReadBufferPoolHijackedReaderSize(t *testing.T) {
	for _, size := range []int{0, 128, 8192} {
		upgrader := Upgrader{UseReadBufferPool: true, ReadBufferSize: size}
		serverConn := make(chan *Conn, 1)
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, _, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			serverConn <- c
		}))
		wc, resp, _, err := (&Dialer{}).Dial("ws"+strings.TrimPrefix(s.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		rc := <-serverConn
		if rc.br != nil {
			t.Fatalf("%d: connection starts with a read buffer", size)
		}
		if err := wc.WriteMessage(TextMessage, []byte("hello")); err != nil {
			t.Fatal(err)
		}
		if _, p, err := rc.ReadMessage(); err != nil || string(p) != "hello" {
			t.Fatalf("%d: got %q, %v", size, p, err)
		}
		// The reader of the message is kept until the next frame starts.
		if rc.br.Size() != effectiveReadBufferSize(size) {
			t.Fatalf("%d: read with a reader of size %d", size, rc.br.Size())
		}
		_ = wc.Close()
		_ = rc.Close()
		s.Close()
	}
}

// TestHijackedReaderHasDefaultReadBufferSize checks that the buffered reader of
// a hijacked HTTP connection has the default read buffer size: only then does
// it go to the pool, so connecting allocates no extra read buffer.
func TestHijackedReaderHasDefaultReadBufferSize(t *testing.T) {
	size := make(chan int, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, brw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		size <- brw.Reader.Size()
		_ = c.Close()
	}))
	defer s.Close()
	if resp, err := http.Get(s.URL); err == nil {
		_ = resp.Body.Close()
	}
	if n := <-size; n != defaultReadBufferSize {
		t.Fatalf("hijacked reader size %d, want %d", n, defaultReadBufferSize)
	}
}

// TestReadBufferPoolReleasedOnReadError checks that a connection gives its
// read buffer back to the pool when reading fails: read errors are permanent.
func TestReadBufferPoolReleasedOnReadError(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte // what the client sends before it goes away.
	}{
		// Fails reading the frame header (NextReader).
		{"header", []byte{0x81, 0x80 | 10, 1, 2}},
		// Fails reading the payload (the message reader).
		{"payload", []byte{0x81, 0x80 | 10, 1, 2, 3, 4, 'a', 'b', 'c'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upgrader := Upgrader{UseReadBufferPool: true}
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
			rc := <-serverConn
			defer func() { _ = rc.Close() }()
			if _, err := wc.NetConn().Write(tc.raw); err != nil {
				t.Fatal(err)
			}
			_ = wc.NetConn().Close()
			if _, _, err := rc.ReadMessage(); err == nil {
				t.Fatal("expected a read error")
			}
			if rc.br != nil {
				t.Fatal("read buffer kept after a read error")
			}
		})
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
			return newConn(fakeNetConn{Reader: r, Writer: io.Discard}, true, 0, 0, sharedReadBufferPool(0), nil, nil, nil)
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
func newBenchConnPair(b *testing.B, pool *readBufferPool) (server, client *Conn) {
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
		var pool *readBufferPool
		if pooled {
			name = "Pool"
			pool = sharedReadBufferPool(0)
		}
		// Request/response: every frame arrives when nothing is buffered.
		b.Run(name+"/RoundTrip", func(b *testing.B) {
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
			upgrader.UseReadBufferPool = true
		}
		b.Run(name, func(b *testing.B) {
			read := make(chan struct{}, 1)
			var reading atomic.Int64
			changed := make(chan struct{}, 1)
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			s.Listener = readingListener{Listener: s.Listener, reading: &reading, changed: changed}
			s.Start()
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
			// Wait until every read loop waits for the next frame.
			waitFor(b, changed, func() bool { return reading.Load() == int64(b.N) }, "read loops not waiting")
			for i := 0; i < 4; i++ {
				runtime.GC()
			}
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "live-B/conn")
		})
	}
}
