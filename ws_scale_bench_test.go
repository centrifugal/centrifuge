//go:build wsscale

package centrifuge

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"
)

// TestWSScale opens WS_CONNS real WebSocket connections against a real
// WebsocketHandler and measures process CPU over a window. Client and server run
// in the same process, so the reported CPU is combined — but the client read
// loops are identical across the runtime-vs-wheel arms, so the DELTA between
// arms is attributable to the server timer machinery.
//
// This is the real-socket counterpart to BenchmarkIdle_*: unlike the in-process
// countingTransport, every connection here has a real socket and a server-side
// read goroutine parked on epoll.
//
// Env:
//
//	WS_CONNS   total connections            (default 200000)
//	WS_PORTS   server listen ports          (default 8; spreads conns to dodge
//	                                          loopback ephemeral-port exhaustion)
//	WS_WHEEL   1 = timer wheel, 0 = runtime  (default 0)
//	WS_SHARDS  wheel shard count             (default 64)
//	WS_PING    server ping interval seconds  (default 25)
//	WS_WINDOW  measurement window seconds    (default 30)
func TestWSScale(t *testing.T) {
	conns := envInt("WS_CONNS", 200000)
	ports := envInt("WS_PORTS", 8)
	useWheel := envInt("WS_WHEEL", 0) == 1
	shards := envInt("WS_SHARDS", 64)
	pingSec := envInt("WS_PING", 25)
	windowSec := envInt("WS_WINDOW", 30)

	cfg := Config{
		LogLevel:   LogLevelError,
		LogHandler: func(LogEntry) {},
	}
	var wheel *ShardedTimerWheel
	if useWheel {
		wheel = NewShardedTimerWheel(shards, 256, 100*time.Millisecond, 4)
		cfg.ClientTimerScheduler = wheel
	}
	node, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: ""}}, nil
	})
	if err := node.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	handler := NewWebsocketHandler(node, WebsocketConfig{
		// Server never expects a pong, so the dumb client below need not send one
		// (mirrors PongTimeout<0 in the in-process benches). Server still runs its
		// ping timer and writes pings to the real socket — the work under test.
		PingPongConfig:     PingPongConfig{PingInterval: time.Duration(pingSec) * time.Second, PongTimeout: -1},
		ReadBufferSize:     256, // small buffers: 2*conns sockets must fit in RAM
		WriteBufferSize:    256,
		UseWriteBufferPool: true,
	})
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", handler)

	addrs := make([]string, ports)
	for i := 0; i < ports; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = ln.Addr().String()
		srv := &http.Server{Handler: mux}
		go func() { _ = srv.Serve(ln) }()
		defer srv.Close()
	}

	// Dial all connections, spread across ports.
	dialer := &websocket.Dialer{ReadBufferSize: 256, WriteBufferSize: 256, HandshakeTimeout: 30 * time.Second}
	var connected atomic.Int64
	var dialFail atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 512) // bound concurrent handshakes
	t.Logf("dialing %d connections across %d ports...", conns, ports)
	dialStart := time.Now()
	for i := 0; i < conns; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			url := "ws://" + addrs[i%ports] + "/connection/websocket"
			conn, resp, _, err := dialer.Dial(url, nil)
			if err != nil {
				dialFail.Add(1)
				return
			}
			_ = resp.Body.Close()
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"id":1,"connect":{}}`)); err != nil {
				dialFail.Add(1)
				_ = conn.Close()
				return
			}
			connected.Add(1)
			// Drain forever so the socket read buffer never fills.
			go func() {
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}()
		}(i)
	}
	wg.Wait()
	t.Logf("dialed in %v: connected=%d dialFail=%d hub=%d",
		time.Since(dialStart), connected.Load(), dialFail.Load(), node.hub.NumClients())

	if node.hub.NumClients() < conns*9/10 {
		t.Fatalf("only %d/%d connections established — cannot measure", node.hub.NumClients(), conns)
	}

	time.Sleep(2 * time.Second) // settle
	before := node.hub.NumClients()

	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	cpu0 := procCPU()
	start := time.Now()

	time.Sleep(time.Duration(windowSec) * time.Second)

	cpuUsed := procCPU() - cpu0
	wall := time.Since(start)
	runtime.ReadMemStats(&ms1)
	after := node.hub.NumClients()

	if after < before*9/10 {
		t.Fatalf("connection churn during window: %d -> %d", before, after)
	}

	mode := "runtime"
	if useWheel {
		mode = fmt.Sprintf("wheel-%d", shards)
	}
	t.Logf("RESULT mode=%s conns=%d ping=%ds window=%ds  cpu-cores=%.4f  heap-GB=%.2f  goroutines=%d",
		mode, after, pingSec, windowSec,
		float64(cpuUsed)/float64(wall),
		float64(ms1.HeapAlloc)/(1<<30),
		runtime.NumGoroutine())
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func procCPU() time.Duration {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	u := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	s := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return u + s
}
