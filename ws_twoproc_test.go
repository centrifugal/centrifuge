//go:build wsscale2

package centrifuge

import (
	"bufio"
	"context"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"
)

// Two-process WebSocket scale harness. Run the server and the client as separate
// OS processes so the server's own VmRSS and pprof profiles are clean (no client
// read-loops contaminating them).
//
//	server: go test -tags wsscale2 -run TestWSServer  (blocks WS_RUN seconds)
//	client: go test -tags wsscale2 -run TestWSClient  (dials, holds, blocks)
//
// Env: WS_CONNS, WS_PORTS, WS_WHEEL, WS_SHARDS, WS_PING, WS_RUN, WS_ADDR_FILE,
// WS_PPROF (server pprof addr, default 127.0.0.1:6060).

func envI(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func TestWSServer(t *testing.T) {
	ports := envI("WS_PORTS", 8)
	useWheel := envI("WS_WHEEL", 0) == 1
	shards := envI("WS_SHARDS", 64)
	pingSec := envI("WS_PING", 25)
	runSec := envI("WS_RUN", 120)
	addrFile := os.Getenv("WS_ADDR_FILE")
	pprofAddr := os.Getenv("WS_PPROF")
	if pprofAddr == "" {
		pprofAddr = "127.0.0.1:6060"
	}

	cfg := Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}}
	if useWheel {
		cfg.ClientTimerScheduler = NewShardedTimerWheel(shards, 256, 100*time.Millisecond, 4)
	}
	node, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	writeDelayMs := envI("WS_WRITE_DELAY_MS", 0)
	writeWithTimer := envI("WS_WRITE_TIMER", 0) == 1
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials:    &Credentials{UserID: ""},
			WriteDelay:     time.Duration(writeDelayMs) * time.Millisecond,
			WriteWithTimer: writeWithTimer,
		}, nil
	})
	if err := node.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	// pprof on its own mux/port (DefaultServeMux has the pprof handlers).
	go func() { _ = http.ListenAndServe(pprofAddr, nil) }()

	handler := NewWebsocketHandler(node, WebsocketConfig{
		PingPongConfig:     PingPongConfig{PingInterval: time.Duration(pingSec) * time.Second, PongTimeout: -1},
		ReadBufferSize:     256,
		WriteBufferSize:    256,
		UseWriteBufferPool: true,
	})
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", handler)

	var addrs []string
	for i := 0; i < ports; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, ln.Addr().String())
		srv := &http.Server{Handler: mux}
		go func() { _ = srv.Serve(ln) }()
		defer srv.Close()
	}
	if addrFile != "" {
		if err := os.WriteFile(addrFile, []byte(strings.Join(addrs, "\n")), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("server up: pid=%d ports=%v pprof=%s wheel=%v shards=%d", os.Getpid(), addrs, pprofAddr, useWheel, shards)

	deadline := time.After(time.Duration(runSec) * time.Second)
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Logf("server exiting: hub=%d goroutines=%d", node.hub.NumClients(), runtime.NumGoroutine())
			return
		case <-tick.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			t.Logf("server: hub=%d goroutines=%d heapAlloc=%.2fGB heapSys=%.2fGB stackInuse=%.2fGB",
				node.hub.NumClients(), runtime.NumGoroutine(),
				float64(ms.HeapAlloc)/(1<<30), float64(ms.HeapSys)/(1<<30), float64(ms.StackInuse)/(1<<30))
		}
	}
}

func TestWSClient(t *testing.T) {
	conns := envI("WS_CONNS", 200000)
	runSec := envI("WS_RUN", 120)
	addrFile := os.Getenv("WS_ADDR_FILE")

	data, err := os.ReadFile(addrFile)
	if err != nil {
		t.Fatal(err)
	}
	var addrs []string
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			addrs = append(addrs, line)
		}
	}
	if len(addrs) == 0 {
		t.Fatal("no server addrs")
	}

	dialer := &websocket.Dialer{ReadBufferSize: 256, WriteBufferSize: 256, HandshakeTimeout: 30 * time.Second}
	var connected, dialFail atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 512)
	holders := make([]*websocket.Conn, conns)
	start := time.Now()
	for i := 0; i < conns; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			url := "ws://" + addrs[i%len(addrs)] + "/connection/websocket"
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
			holders[i] = conn
			connected.Add(1)
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
	t.Logf("client: dialed %d in %v (fail=%d)", connected.Load(), time.Since(start), dialFail.Load())
	runtime.KeepAlive(holders)
	// Hold connections open for the server measurement window.
	time.Sleep(time.Duration(runSec) * time.Second)
}
