//go:build integration

package centrifuge

import (
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

// Failover tests run against pairs of standalone servers the tests make
// primary and replica of each other. They need servers of their own (they
// FLUSHALL them), so they run only when CENTRIFUGE_REDIS_FAILOVER_PAIRS is set,
// for example:
//
//	CENTRIFUGE_REDIS_FAILOVER_PAIRS="redis=127.0.0.1:6390,127.0.0.1:6391;valkey=127.0.0.1:6392,127.0.0.1:6393"
//
// A TCP proxy in front of the pair plays the single address (a DNS name or a
// virtual IP) that moves from one server to the other.
type failoverPair struct {
	name string
	a, b string
}

func failoverPairs(t *testing.T) []failoverPair {
	v := os.Getenv("CENTRIFUGE_REDIS_FAILOVER_PAIRS")
	if v == "" {
		t.Skip("CENTRIFUGE_REDIS_FAILOVER_PAIRS not set")
	}
	var out []failoverPair
	for _, p := range strings.Split(v, ";") {
		name, addrs, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		a, b, ok := strings.Cut(addrs, ",")
		if !ok {
			continue
		}
		out = append(out, failoverPair{name: name, a: a, b: b})
	}
	return out
}

// tcpProxy forwards connections to a target it can move.
type tcpProxy struct {
	ln     net.Listener
	mu     sync.Mutex
	target string
	pairs  map[net.Conn]net.Conn
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &tcpProxy{ln: ln, target: target, pairs: map[net.Conn]net.Conn{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.closeAll() })
	return p
}

func (p *tcpProxy) addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve(c net.Conn) {
	p.mu.Lock()
	target := p.target
	p.mu.Unlock()
	up, err := net.Dial("tcp", target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	p.pairs[c] = up
	p.mu.Unlock()
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	_, _ = io.Copy(c, up)
	_ = c.Close()
	p.mu.Lock()
	delete(p.pairs, c)
	p.mu.Unlock()
}

// move points new connections at target; with drop, existing ones are closed
// (a virtual IP moving), without it they stay on the old server (a DNS change).
func (p *tcpProxy) move(target string, drop bool) {
	p.mu.Lock()
	p.target = target
	p.mu.Unlock()
	if drop {
		p.closeAll()
	}
}

func (p *tcpProxy) closeAll() {
	p.mu.Lock()
	pairs := p.pairs
	p.pairs = map[net.Conn]net.Conn{}
	p.mu.Unlock()
	for c, up := range pairs {
		_ = c.Close()
		_ = up.Close()
	}
}

type adminRedis struct {
	t    *testing.T
	addr string
	c    rueidis.Client
}

func newAdmin(t *testing.T, addr string) *adminRedis {
	c, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true, ForceSingleClient: true})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return &adminRedis{t: t, addr: addr, c: c}
}

func (a *adminRedis) do(args ...string) rueidis.RedisResult {
	return a.c.Do(context.Background(), a.c.B().Arbitrary(args...).Build())
}

func (a *adminRedis) replicaOf(primary string) {
	host, port, _ := net.SplitHostPort(primary)
	require.NoError(a.t, a.do("REPLICAOF", host, port).Error())
}

func (a *adminRedis) promote() {
	require.NoError(a.t, a.do("REPLICAOF", "NO", "ONE").Error())
}

func (a *adminRedis) replOffset() int64 {
	info, err := a.do("INFO", "replication").ToString()
	require.NoError(a.t, err)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "master_repl_offset:"); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
		if v, ok := strings.CutPrefix(line, "slave_repl_offset:"); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	return -1
}

func (a *adminRedis) linkUp() bool {
	info, err := a.do("INFO", "replication").ToString()
	return err == nil && strings.Contains(info, "master_link_status:up")
}

// waitSynced waits until replica has everything primary has.
func waitSynced(t *testing.T, primary, replica *adminRedis) {
	t.Helper()
	require.Eventually(t, func() bool {
		return replica.linkUp() && replica.replOffset() >= primary.replOffset()
	}, 10*time.Second, 20*time.Millisecond)
}

// setupPair makes a primary, b its replica, both empty, and returns a broker
// behind a proxy pointing at a.
func setupPair(t *testing.T, p failoverPair) (*RedisBroker, *tcpProxy, *adminRedis, *adminRedis) {
	t.Helper()
	return setupPairOpts(t, p, false)
}

func setupPairOpts(t *testing.T, p failoverPair, noLineage bool) (*RedisBroker, *tcpProxy, *adminRedis, *adminRedis) {
	t.Helper()
	return setupPairHook(t, p, noLineage, nil)
}

// setupPairHook is setupPairConf with a hook on the shard before the broker
// starts.
func setupPairHook(t *testing.T, p failoverPair, noLineage bool, hook func(*RedisShard)) (*RedisBroker, *tcpProxy, *adminRedis, *adminRedis) {
	t.Helper()
	a, b := newAdmin(t, p.a), newAdmin(t, p.b)
	a.promote()
	b.promote()
	for _, s := range []*adminRedis{a, b} {
		require.NoError(t, s.do("FLUSHALL").Error())
		require.NoError(t, s.do("SCRIPT", "FLUSH").Error())
	}
	b.replicaOf(p.a)
	require.Eventually(t, b.linkUp, 10*time.Second, 20*time.Millisecond)

	proxy := newTCPProxy(t, p.a)
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConfAddr(proxy.addr()))
	require.NoError(t, err)
	prefix := getUniquePrefix()
	s.lineageFor(prefix).disabledForTest = noLineage
	if hook != nil {
		hook(s)
	}
	br, err := NewRedisBroker(node, RedisBrokerConfig{Prefix: prefix, Shards: []*RedisShard{s}, DetectLostWrites: !noLineage})
	require.NoError(t, err)
	node.SetBroker(br)
	require.NoError(t, node.Run())
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(br)
		a.promote()
		b.promote()
	})
	return br, proxy, a, b
}

func testSingleRedisConfAddr(addr string) RedisShardConfig {
	conf := testSingleRedisConf(6379)
	conf.Address = addr
	conf.IOTimeout = 2 * time.Second
	return conf
}

func publishEventually(t *testing.T, b *RedisBroker, ch string) StreamPosition {
	t.Helper()
	var sp StreamPosition
	require.Eventually(t, func() bool {
		res, err := b.Publish(ch, []byte(`{}`), PublishOptions{HistorySize: 10, HistoryTTL: time.Minute})
		if err != nil {
			return false
		}
		sp = res.StreamPosition
		return true
	}, 15*time.Second, 50*time.Millisecond)
	return sp
}

// centrifugo#1319: a failover to a replica that missed writes starts a new
// epoch instead of reusing offsets under the old one.
func TestRedisFailover_LossyFailoverStartsNewEpoch(t *testing.T) {
	for _, p := range failoverPairs(t) {
		t.Run(p.name, func(t *testing.T) {
			br, proxy, a, b := setupPair(t, p)
			ch := "failover_" + randString(6)
			for i := 0; i < 3; i++ {
				publishEventually(t, br, ch)
			}
			waitSynced(t, a, b)

			// b stops replicating: the next writes are acknowledged by a only.
			b.promote()
			var lost StreamPosition
			for i := 0; i < 2; i++ {
				lost = publishEventually(t, br, ch)
			}
			require.Equal(t, uint64(5), lost.Offset)

			// a dies, the address moves to b.
			proxy.move(p.b, true)
			a.replicaOf(p.b)

			sp := publishEventually(t, br, ch)
			require.NotEqual(t, lost.Epoch, sp.Epoch, "offsets 4 and 5 must not be reused under the old epoch")
			require.Equal(t, uint64(1), sp.Offset)
		})
	}
}

// centrifugo#1320: the old primary is demoted and keeps the connections; the
// broker reconnects on READONLY and lands on the new primary.
func TestRedisFailover_ReadOnlyReconnects(t *testing.T) {
	for _, p := range failoverPairs(t) {
		t.Run(p.name, func(t *testing.T) {
			br, proxy, a, b := setupPair(t, p)
			ch := "readonly_" + randString(6)
			before := publishEventually(t, br, ch)
			waitSynced(t, a, b)

			// A planned failover: b promoted, a demoted; the address moves, the
			// existing connections stay on a.
			b.promote()
			a.replicaOf(p.b)
			proxy.move(p.b, false)

			sp := publishEventually(t, br, ch)
			require.NotEqual(t, before.Epoch, sp.Epoch, "the new primary started a new lineage")
		})
	}
}

// A lossy failover, then a failback to the demoted server, which kept its
// script cache through the resync. The probe sees the new server behind the
// address and rotates there, so the old server's cached scripts don't match
// any more when it serves again.
func TestRedisFailover_FailbackToDemotedServer(t *testing.T) {
	setLineageProbeInterval(t, 100*time.Millisecond)
	for _, p := range failoverPairs(t) {
		t.Run(p.name, func(t *testing.T) {
			br, proxy, a, b := setupPair(t, p)
			ch := "failback_" + randString(6)
			for i := 0; i < 3; i++ {
				publishEventually(t, br, ch)
			}
			waitSynced(t, a, b)

			b.promote()
			lostA := publishEventually(t, br, ch) // acknowledged by a only
			require.Equal(t, uint64(4), lostA.Offset)

			// Failover to b without any traffic reaching it, a demoted and
			// resynced from b (it keeps its script cache).
			proxy.move(p.b, true)
			a.replicaOf(p.b)
			waitSynced(t, b, a)
			// Let the probe see b behind the address.
			require.Eventually(t, func() bool {
				br.lin(br.shards[0].shard).stats.mu.Lock()
				defer br.lin(br.shards[0].shard).stats.mu.Unlock()
				return br.lin(br.shards[0].shard).stats.rotations >= 2
			}, 10*time.Second, 20*time.Millisecond)
			waitSynced(t, b, a)

			// Failback: a promoted again, b stops.
			a.promote()
			b.replicaOf(p.a)
			proxy.move(p.a, true)

			sp := publishEventually(t, br, ch)
			require.NotEqual(t, lostA.Epoch, sp.Epoch, "offset 4 must not be reused under the old epoch")
			if sp.Epoch == lostA.Epoch {
				t.Fatalf("stale scripts ran on the demoted server")
			}
		})
	}
}

// The control: without lineage detection the same failover keeps the epoch and
// reuses offsets (centrifugo#1319 as reported).
func TestRedisFailover_WithoutLineageReusesOffsets(t *testing.T) {
	for _, p := range failoverPairs(t) {
		t.Run(p.name, func(t *testing.T) {
			br, proxy, a, b := setupPairOpts(t, p, true)
			ch := "nolineage_" + randString(6)
			for i := 0; i < 3; i++ {
				publishEventually(t, br, ch)
			}
			waitSynced(t, a, b)
			b.promote()
			var lost StreamPosition
			for i := 0; i < 2; i++ {
				lost = publishEventually(t, br, ch)
			}
			proxy.move(p.b, true)
			a.replicaOf(p.b)
			sp := publishEventually(t, br, ch)
			require.Equal(t, lost.Epoch, sp.Epoch, "without detection the epoch survives the lossy failover")
			require.Equal(t, uint64(4), sp.Offset, "and offset 4 is handed out again")
		})
	}
}

// Redis Cluster with replicas (CENTRIFUGE_REDIS_CLUSTER_REPLICAS_ADDRS, the
// addresses of a cluster where every primary has a replica): a replica takes
// over a slot's primary with CLUSTER FAILOVER. The probe sees a new owner of the
// slot and rotates there, and flushes the demoted primary, which keeps our
// scripts.
func TestRedisFailover_ClusterFailover(t *testing.T) {
	v := os.Getenv("CENTRIFUGE_REDIS_CLUSTER_REPLICAS_ADDRS")
	if v == "" {
		t.Skip("CENTRIFUGE_REDIS_CLUSTER_REPLICAS_ADDRS not set")
	}
	setLineageProbeInterval(t, 100*time.Millisecond)

	node := testNode(t)
	s, err := NewRedisShard(node, RedisShardConfig{ClusterAddresses: strings.Split(v, ","), IOTimeout: 5 * time.Second})
	require.NoError(t, err)
	br, err := NewRedisBroker(node, RedisBrokerConfig{Prefix: getUniquePrefix(), Shards: []*RedisShard{s}, DetectLostWrites: true})
	require.NoError(t, err)
	node.SetBroker(br)
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()); stopRedisBroker(br) })

	// A previous run's failback may still be in progress: wait for a cluster
	// where every primary serving slots has a replica.
	require.Eventually(t, func() bool {
		nodes, err := s.client.Do(context.Background(), s.client.B().ClusterNodes().Build()).ToString()
		if err != nil {
			return false
		}
		replicaOf := map[string]bool{}
		for _, line := range strings.Split(nodes, "\n") {
			f := strings.Fields(line)
			if len(f) > 3 && strings.Contains(f[2], "slave") && !strings.Contains(f[2], "fail") {
				replicaOf[f[3]] = true
			}
		}
		for _, n := range lineageParseClusterNodes(nodes) {
			if n.primary && len(n.slots) > 0 && !replicaOf[n.id] {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)

	ch := "cluster_failover_" + randString(6)
	publishEventually(t, br, ch)
	before := publishEventually(t, br, ch)
	require.Equal(t, uint64(2), before.Offset)

	// The primary and a synced replica of the channel's slot (a previous run
	// may have moved the slot).
	slot := int(redisKeySlot(string(br.historyMetaKey(s, ch))))
	ctx := context.Background()
	var primaryAddr string // for failure messages
	var replica *adminRedis
	require.Eventually(t, func() bool {
		nodes, err := s.client.Do(ctx, s.client.B().ClusterNodes().Build()).ToString()
		if err != nil {
			return false
		}
		var primaryID string
		for _, n := range lineageParseClusterNodes(nodes) {
			for _, sl := range n.slots {
				if sl == slot {
					primaryID, primaryAddr = n.id, n.addr
				}
			}
		}
		for _, line := range strings.Split(nodes, "\n") {
			f := strings.Fields(line)
			if len(f) > 3 && strings.Contains(f[2], "slave") && !strings.Contains(f[2], "fail") && f[3] == primaryID {
				r := newAdmin(t, strings.SplitN(f[1], "@", 2)[0])
				if r.linkUp() {
					replica = r
					return true
				}
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "the slot's primary needs a synced replica")
	require.NoError(t, replica.do("CLUSTER", "FAILOVER").Error())

	// CLUSTER FAILOVER returns before the failover: wait for the replica to
	// become the slot's primary.
	require.Eventually(t, func() bool {
		hello, err := replica.c.Do(ctx, replica.c.B().Hello().Build()).AsMap()
		return err == nil && parseHelloReply(helloFields(hello)).primary
	}, 10*time.Second, 20*time.Millisecond)
	// A coordinated failover loses nothing: the promoted replica starts a new
	// lineage (it holds none of our scripts), or - if it was the primary under
	// the current token before and kept its scripts - continues it. Either way
	// no offset is given out twice.
	sp := publishEventually(t, br, ch)
	if sp.Epoch == before.Epoch {
		require.Equal(t, uint64(3), sp.Offset, "continued lineage: offsets continue (old primary %s)", primaryAddr)
	} else {
		require.Equal(t, uint64(1), sp.Offset, "new lineage: a new epoch (old primary %s)", primaryAddr)
	}
}

func lineageRotations(b *RedisBroker) int {
	st := &b.lin(b.shards[0].shard).stats
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.rotations
}

// A fast failback the script cache can't see: the old primary takes a full
// copy of the new one (losing its last write), keeps our scripts and gets the
// primary role back, behind the same address, before any call or probe
// reached the new one. A subscriber that saw the lost offset catches it at the
// next publication: the server's lineage rotates. Without a subscriber of that
// channel it goes unseen - the documented gap (DetectLostWritesWithInfo closes
// it).
func TestRedisFailover_FastFailbackCaughtByReusedOffset(t *testing.T) {
	testFastFailback(t, true)
}

func TestRedisFailover_FastFailbackUnwatchedGap(t *testing.T) {
	testFastFailback(t, false)
}

func testFastFailback(t *testing.T, watched bool) {
	setLineageProbeInterval(t, time.Hour) // nothing reaches the new primary
	for _, p := range failoverPairs(t) {
		t.Run(p.name, func(t *testing.T) {
			br, _, a, b := setupPairHook(t, p, false, nil)
			ch := "fast_failback_" + randString(6)
			if watched {
				client := newTestConnectedClientV2(t, br.node, "42")
				_, _, err := br.node.hub.addSub(ch, subInfo{client: client})
				require.NoError(t, err)
				require.NoError(t, br.Subscribe(ch))
			}
			for i := 0; i < 3; i++ {
				publishEventually(t, br, ch)
			}
			waitSynced(t, a, b)

			b.promote()
			lostA := publishEventually(t, br, ch)
			require.Equal(t, uint64(4), lostA.Offset)
			if watched {
				require.Eventually(t, func() bool {
					ws := br.watermarks.shard(ch)
					ws.mu.Lock()
					defer ws.mu.Unlock()
					return ws.m[ch].offset == 4
				}, 5*time.Second, 10*time.Millisecond, "the subscriber saw offset 4")
			}

			a.replicaOf(p.b)
			waitSynced(t, b, a)
			a.promote()
			b.replicaOf(p.a)

			sp := publishEventually(t, br, ch) // offset 4 again
			if !watched {
				require.Equal(t, lostA.Epoch, sp.Epoch, "the gap: nothing saw the loss")
				require.Equal(t, uint64(4), sp.Offset)
				return
			}
			require.Eventually(t, func() bool {
				top, err := br.node.streamTop(ch, time.Hour)
				return err == nil && top.Epoch != lostA.Epoch
			}, 10*time.Second, 20*time.Millisecond, "the reused offset rotates the server's lineage")
		})
	}
}
