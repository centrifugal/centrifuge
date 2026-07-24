//go:build integration

package centrifuge

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// testTCPProxy forwards TCP connections to a real Redis, so a test can build
// a working shard and then take Redis "down" from the client's perspective by
// stopping the proxy: established connections break and new dials to the
// proxy port are refused — the same errors a crashed Redis produces.
type testTCPProxy struct {
	ln       net.Listener
	upstream string
	mu       sync.Mutex
	conns    []net.Conn
}

func startTestTCPProxy(tb testing.TB, upstream string) *testTCPProxy {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	p := &testTCPProxy{ln: ln, upstream: upstream}
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
			go func() { _, _ = io.Copy(up, c); _ = up.Close(); _ = c.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close(); _ = up.Close() }()
		}
	}()
	return p
}

func (p *testTCPProxy) addr() string { return p.ln.Addr().String() }

// stop kills the proxy: no new connections, all existing ones broken.
func (p *testTCPProxy) stop() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// TestRedisShardReadFailsFastDuringOutage pins that Redis commands issued
// without a context deadline fail within the IOTimeout envelope when Redis is
// unreachable — for read-only commands too.
//
// Centrifuge never passes per-request deadlines; every call is expected to
// fail within roughly IOTimeout when Redis is down. Writes and Lua scripts
// always behaved that way. Read-only commands did not: rueidis retries them
// on network errors until the context is done, and with context.Background()
// that means never — a presence-style read issued during an outage hung
// (retrying roughly once per second) until Redis came back. DisableRetry
// makes reads fail like writes. This test fails without it: the GET below
// keeps retrying against the dead address and does not return.
func TestRedisShardReadFailsFastDuringOutage(t *testing.T) {
	proxy := startTestTCPProxy(t, "127.0.0.1:6379")
	t.Cleanup(proxy.stop)

	node := testNode(t)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	s, err := NewRedisShard(node, RedisShardConfig{
		Address:        proxy.addr(),
		IOTimeout:      time.Second,
		ConnectTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(s.Close)

	// Sanity: reads work through the proxy (rueidis.Nil for a missing key
	// is a server reply, i.e. success at the transport level).
	resp := s.client.Do(context.Background(), s.client.B().Get().Key("centrifuge-retry-test").Build())
	if err := resp.Error(); err != nil {
		require.True(t, rueidis.IsRedisNil(err), "unexpected error before outage: %v", err)
	}

	// Redis goes away.
	proxy.stop()

	// A read-only command with no context deadline must return an error
	// within the IOTimeout envelope instead of retrying forever.
	done := make(chan error, 1)
	go func() {
		done <- s.client.Do(context.Background(), s.client.B().Get().Key("centrifuge-retry-test").Build()).Error()
	}()
	select {
	case err := <-done:
		require.Error(t, err, "read against dead Redis must fail, not succeed")
		require.False(t, rueidis.IsRedisNil(err), "expected a network error, got a server reply")
	case <-time.After(5 * time.Second):
		t.Fatal("read-only command did not return within 5s during Redis outage — client-level retries are hiding the failure")
	}
}
