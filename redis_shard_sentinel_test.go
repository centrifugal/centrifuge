package centrifuge

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Sentinel connections are dialled with the data connections' connect timeout
// and keep-alive, whether the shard is configured with SentinelAddresses or a
// redis+sentinel:// address (centrifugo#1325).
func TestRedisClientOptions_SentinelDialer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		conf RedisShardConfig
	}{
		{
			name: "sentinel addresses",
			conf: RedisShardConfig{
				SentinelAddresses:  []string{"127.0.0.1:26379"},
				SentinelMasterName: "mymaster",
			},
		},
		{
			name: "sentinel URL",
			conf: RedisShardConfig{
				Address: "redis+sentinel://127.0.0.1:26379?sentinel_master_name=mymaster&connect_timeout=3s",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := tc.conf
			conf.ConnectTimeout = defaultRedisConnectTimeout
			conf.IOTimeout = defaultRedisIOTimeout
			options, _, isSentinel, _, err := redisClientOptions(conf)
			require.NoError(t, err)
			require.True(t, isSentinel)
			require.NotZero(t, options.Dialer.Timeout)
			require.NotZero(t, options.Dialer.KeepAlive)
			require.Equal(t, options.Dialer.Timeout, options.Sentinel.Dialer.Timeout)
			require.Equal(t, options.Dialer.KeepAlive, options.Sentinel.Dialer.KeepAlive)
		})
	}
	// The address's connect_timeout reaches the Sentinel connections too.
	options, _, _, _, err := redisClientOptions(RedisShardConfig{
		Address:        "redis+sentinel://127.0.0.1:26379?sentinel_master_name=mymaster&connect_timeout=3s",
		ConnectTimeout: defaultRedisConnectTimeout,
		IOTimeout:      defaultRedisIOTimeout,
	})
	require.NoError(t, err)
	require.Equal(t, 3*time.Second, options.Sentinel.Dialer.Timeout)
}

// blackholeProxy forwards TCP connections to a target until it is told to go
// silent: then it keeps every connection open and reads what arrives, but
// forwards nothing in either direction, like a network path that drops
// packets without resetting anything.
type blackholeProxy struct {
	ln       net.Listener
	target   string
	accepted atomic.Int64
	silent   atomic.Bool
	mu       sync.Mutex
	conns    []net.Conn
}

func newBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &blackholeProxy{ln: ln, target: target}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *blackholeProxy) addr() string { return p.ln.Addr().String() }

func (p *blackholeProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.accepted.Add(1)
		u, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, u)
		p.mu.Unlock()
		go p.pipe(u, c)
		go p.pipe(c, u)
	}
}

// pipe copies from src to dst while the proxy isn't silent; while it is, what
// src sends is read and dropped.
func (p *blackholeProxy) pipe(dst io.Writer, src io.Reader) {
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.silent.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *blackholeProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

// A Sentinel connection that stops answering without being closed is detected
// and replaced: the client dials a Sentinel again instead of waiting on the
// silent connection forever, so it can learn about a failover
// (centrifugo#1325). Without a keep-alive on the Sentinel connection nothing
// is ever sent on it while it waits for Sentinel events, and the topology
// refresh waits on it too.
func TestRedisShardSentinel_SilentSentinelConnectionReplaced(t *testing.T) {
	t.Parallel()
	proxy := newBlackholeProxy(t, "127.0.0.1:26379")
	n, _ := New(Config{})
	s, err := NewRedisShard(n, RedisShardConfig{
		SentinelAddresses:  []string{proxy.addr()},
		SentinelMasterName: "mymaster",
		IOTimeout:          time.Second,
	})
	require.NoError(t, err)
	defer s.Close()
	// Closing the proxy first ends a request stuck on a silent connection, so
	// the shard can close even when the test fails.
	defer proxy.close()
	require.Equal(t, int64(1), proxy.accepted.Load())

	proxy.silent.Store(true)
	require.Eventually(t, func() bool {
		return proxy.accepted.Load() > 1
	}, 10*time.Second, 50*time.Millisecond, "the client never dialled a Sentinel again")
}
