//go:build integration

package centrifuge

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// testStallProxy forwards TCP connections to a real Redis and can switch into
// blackhole mode: connections stay open on both sides, but no bytes are
// forwarded anymore. From the client's perspective this is a silently stalled
// Redis — the peer stops replying while TCP stays perfectly healthy, the
// failure shape of a frozen server or a network partition without resets.
type testStallProxy struct {
	ln        net.Listener
	upstream  string
	blackhole atomic.Bool
	mu        sync.Mutex
	conns     []net.Conn
}

func startTestStallProxy(tb testing.TB, upstream string) *testStallProxy {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	p := &testStallProxy{ln: ln, upstream: upstream}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go p.pump(up, c)
			go p.pump(c, up)
		}
	}()
	return p
}

// pump copies src to dst until either side closes. In blackhole mode bytes
// are read and discarded, keeping both connections open and silent.
func (p *testStallProxy) pump(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.blackhole.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
}

func (p *testStallProxy) addr() string { return p.ln.Addr().String() }

func (p *testStallProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// TestRedisShardCommandFailsOnSilentStall pins that a command issued without
// a context deadline fails in bounded time when the connection silently
// stalls — no error, no reset, the peer just stops replying.
//
// No write deadline fires in this state (writes keep succeeding into the
// void), so the only rescue is the rueidis keepalive: a PING sent after
// Dialer.KeepAlive of reply silence, bounded by IOTimeout, whose failure
// tears the connection down and errors every pending command. Worst case is
// 2*KeepAlive + IOTimeout (see the comment in NewRedisShard). If the
// keepalive were ever disabled or broken, the command below would hang
// forever.
//
// The test deliberately does not assert a threshold tight enough to
// discriminate the exact KeepAlive value — that would be timing-flaky in CI.
// It pins the mechanism and a generous bound; the constant itself is
// documented by the formula at its definition.
func TestRedisShardCommandFailsOnSilentStall(t *testing.T) {
	proxy := startTestStallProxy(t, "127.0.0.1:6379")
	t.Cleanup(proxy.close)

	node := testNode(t)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	s, err := NewRedisShard(node, RedisShardConfig{
		Address:        proxy.addr(),
		IOTimeout:      2 * time.Second, // worst-case bound: 2*0.4s + 2s = 2.8s
		ConnectTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(s.Close)

	// Warm up every muxwire. rueidis multiplexes a single-client connection
	// over several wires (4 with Centrifuge's config on multi-core machines)
	// and picks one per command at random; a command that lands on a wire
	// created AFTER the blackhole measures the fresh-connection handshake —
	// which is separately bounded by ConnectTimeout — instead of the
	// stalled-pending-command path this test is about. Enough sequential
	// commands make it overwhelmingly likely every wire exists and is
	// healthy before the stall. SET is used everywhere in this test: it is
	// not flagged retryable by rueidis, so the result does not depend on
	// client-level retry settings.
	for i := 0; i < 64; i++ {
		err = s.client.Do(context.Background(), s.client.B().Set().Key("centrifuge-stall-test").Value("v").Build()).Error()
		require.NoError(t, err)
	}

	// Redis goes silent: connections stay open, nothing is delivered.
	proxy.blackhole.Store(true)

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- s.client.Do(context.Background(), s.client.B().Set().Key("centrifuge-stall-test").Value("v2").Build()).Error()
	}()
	select {
	case err := <-done:
		elapsed := time.Since(start)
		t.Logf("failed after %v with: %v", elapsed, err)
		require.Error(t, err, "command against silently stalled Redis must fail, not succeed")
		require.False(t, rueidis.IsRedisNil(err))
		require.Less(t, elapsed, 4500*time.Millisecond,
			"command took %v to fail — expected within 2*KeepAlive+IOTimeout (2.8s) plus slack", elapsed)
	case <-time.After(10 * time.Second):
		t.Fatal("command did not return within 10s on a silently stalled connection — keepalive rescue is broken")
	}
}
