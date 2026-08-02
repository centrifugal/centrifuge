//go:build integration

package centrifuge

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func counterValue(tb testing.TB, c prometheus.Counter) float64 {
	tb.Helper()
	m := &dto.Metric{}
	require.NoError(tb, c.Write(m))
	return m.GetCounter().GetValue()
}

// Helpers for talking to specific Redis instances directly (out-of-band from
// the broker under test).

func probeTestRedisDo(tb testing.TB, addr string, args ...string) rueidis.RedisResult {
	tb.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{addr},
		DisableCache:      true,
		ForceSingleClient: true,
	})
	require.NoError(tb, err)
	defer client.Close()
	return client.Do(context.Background(), client.B().Arbitrary(args...).Build())
}

const (
	probeTestSentinelAddr = "127.0.0.1:26379"
	probeTestMasterHost   = "127.0.0.1"
	probeTestMasterPort   = "6380"
)

var probeTestReplicaAddrs = []string{"127.0.0.1:6381", "127.0.0.1:6382"}

// waitReplicasHealthy waits until both replicas replicate from the master and
// the sentinel knows about them — the precondition for building a broker with
// a replica client.
func waitReplicasHealthy(tb testing.TB) {
	tb.Helper()
	require.Eventually(tb, func() bool {
		for _, addr := range probeTestReplicaAddrs {
			info, err := probeTestRedisDo(tb, addr, "INFO", "replication").ToString()
			if err != nil {
				return false
			}
			if !strings.Contains(info, "role:slave") || !strings.Contains(info, "master_link_status:up") {
				return false
			}
		}
		replicas, err := probeTestRedisDo(tb, probeTestSentinelAddr, "SENTINEL", "REPLICAS", "mymaster").ToArray()
		return err == nil && len(replicas) >= 2
	}, 60*time.Second, 500*time.Millisecond, "replicas did not become healthy — is docker-compose sentinel service running?")
}

// replicaHoldingSubscription returns the address of the replica the broker's
// PUB/SUB connection landed on (detected via PUBSUB CHANNELS on each replica)
// and the address of the other replica.
func replicaHoldingSubscription(tb testing.TB, shardChannel string) (subscribed string, other string) {
	tb.Helper()
	require.Eventually(tb, func() bool {
		for i, addr := range probeTestReplicaAddrs {
			channels, err := probeTestRedisDo(tb, addr, "PUBSUB", "CHANNELS", shardChannel).ToArray()
			if err == nil && len(channels) > 0 {
				subscribed = addr
				other = probeTestReplicaAddrs[1-i]
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "PUB/SUB subscription not found on any replica")
	return subscribed, other
}

func newProbeTestSentinelBroker(tb testing.TB, probeInterval time.Duration, name string) (*RedisBroker, chan string, chan []byte) {
	tb.Helper()
	var logMu sync.Mutex
	var logClosed bool
	node, err := New(Config{
		LogLevel: LogLevelDebug,
		LogHandler: func(entry LogEntry) {
			logMu.Lock()
			defer logMu.Unlock()
			if !logClosed {
				tb.Logf("[node] %s %v", entry.Message, entry.Fields)
			}
		},
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		logMu.Lock()
		logClosed = true
		logMu.Unlock()
	})
	redisConf := RedisShardConfig{
		SentinelAddresses:    []string{probeTestSentinelAddr},
		SentinelMasterName:   "mymaster",
		ReplicaClientEnabled: true,
		IOTimeout:            10 * time.Second,
		ConnectTimeout:       10 * time.Second,
	}
	s, err := NewRedisShard(node, redisConf)
	require.NoError(tb, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:              getUniquePrefix(),
		Name:                name,
		Shards:              []*RedisShard{s},
		SubscribeOnReplica:  true,
		pubSubProbeInterval: probeInterval,
	})
	require.NoError(tb, err)
	node.SetBroker(b)
	tb.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})

	received := make(chan string, 128)
	controlReceived := make(chan []byte, 128)
	handler := &testBrokerEventHandler{
		HandleControlFunc: func(data []byte) error {
			controlReceived <- data
			return nil
		},
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			received <- ch
			return nil
		},
	}
	require.NoError(tb, b.RegisterControlEventHandler(handler))
	require.NoError(tb, b.RegisterBrokerEventHandler(handler))
	return b, received, controlReceived
}

func drainReceived(received chan string) {
	for {
		select {
		case <-received:
		default:
			return
		}
	}
}

// waitDelivery publishes to the channel until a publication is delivered or
// the deadline passes. Returns true when delivery works.
//
// It re-issues the channel subscription on every attempt. In production,
// channels are resubscribed automatically after a PUB/SUB loop restart from
// Hub().Channels(); broker-level tests bypass the Hub, so the restarted loop
// would not resubscribe test channels on its own. Re-subscribing does not
// weaken the test: subscribing goes to whatever node the current PUB/SUB
// connection is attached to, so while the loop sits on a detached node the
// published messages still do not arrive.
func waitDelivery(tb testing.TB, b *RedisBroker, received chan string, channel string, deadline time.Duration) bool {
	tb.Helper()
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		if err := b.Subscribe(channel); err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		_, err := b.Publish(channel, []byte("payload"), PublishOptions{})
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		select {
		case <-received:
			return true
		case <-time.After(500 * time.Millisecond):
		}
	}
	return false
}

// detachReplica turns the given replica into a standalone Redis and makes the
// sentinel forget it, so nothing ever reconfigures it back during the test.
// This reproduces, through real Redis commands only, the state behind
// centrifugal/centrifugo#1189: the broker's PUB/SUB connection stays perfectly
// healthy on the TCP level — the node answers keepalive pings and accepts
// commands — but the node is outside the master's replication chain, so no
// published message ever reaches it. No connection error fires, ever.
func detachReplica(tb testing.TB, addr string) {
	tb.Helper()
	require.NoError(tb, probeTestRedisDo(tb, addr, "REPLICAOF", "NO", "ONE").Error())
	tb.Cleanup(func() {
		// Restore topology for other tests.
		_ = probeTestRedisDo(tb, addr, "REPLICAOF", probeTestMasterHost, probeTestMasterPort).Error()
	})
	// Give the master a moment to drop the replication link, so the sentinel
	// re-discovers only the still-attached replica after the reset below.
	time.Sleep(500 * time.Millisecond)
	require.NoError(tb, probeTestRedisDo(tb, probeTestSentinelAddr, "SENTINEL", "RESET", "mymaster").Error())
}

// TestRedisBrokerPubSubProbeRecoversDetachedReplica reproduces the silent
// starvation from centrifugal/centrifugo#1189 end to end and checks that the
// liveness probe recovers from it.
//
// Geometry: the broker subscribes on a replica (SubscribeOnReplica) while
// publishes go to the master. The replica holding the PUB/SUB connection is
// then detached from the master and forgotten by the sentinel. The connection
// stays healthy — the detached node answers pings — but published messages no
// longer reach it. Without the probe the subscriber starves until process
// restart (see TestRedisBrokerPubSubDisabledProbeStarves). With the probe the
// idle connection fails its liveness check, the loop restarts, the client
// re-resolves replicas via the sentinel, lands on the surviving replica, and
// delivery resumes — with no connection error involved at any point.
func TestRedisBrokerPubSubProbeRecoversDetachedReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode: requires sentinel with replicas")
	}
	waitReplicasHealthy(t)

	name := "probe-recover-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	b, received, _ := newProbeTestSentinelBroker(t, 300*time.Millisecond, name)

	channel := "probe-test-ch"
	require.NoError(t, b.Subscribe(channel))

	// Sanity: delivery works through the replica before the detach.
	require.True(t, waitDelivery(t, b, received, channel, 10*time.Second),
		"initial publish was not delivered via replica subscription")

	shardChannel := string(b.pubSubShardChannelID(0, 0, false))
	subscribedReplica, _ := replicaHoldingSubscription(t, shardChannel)
	detachReplica(t, subscribedReplica)
	drainReceived(received)

	// Starvation: publishes succeed, nothing arrives, no error anywhere.
	_, err := b.Publish(channel, []byte("starved"), PublishOptions{})
	require.NoError(t, err)
	select {
	case ch := <-received:
		t.Fatalf("unexpected delivery on %s right after replica detach", ch)
	case <-time.After(1 * time.Second):
	}

	// Recovery: probe detects the stale connection, the loop restarts and
	// re-resolves to the surviving replica. The sentinel needs up to ~10s
	// (one INFO period) to re-discover the surviving replica after RESET,
	// so the deadline is generous.
	require.True(t, waitDelivery(t, b, received, channel, 45*time.Second),
		"delivery did not recover after replica detach — probe restart did not happen or did not help")

	// Prove recovery came from the probe: the probe timeout must have fired
	// at least once for this broker.
	probeTimeouts := counterValue(t, b.node.metrics.brokerPubSub.errors.WithLabelValues(name, "probe_timeout"))
	require.GreaterOrEqual(t, probeTimeouts, 1.0, "expected at least one probe timeout to trigger the restart")
}

// TestRedisBrokerPubSubDisabledProbeStarves documents the behavior this probe
// exists to fix: with probing disabled, the same detached-replica state
// starves the subscriber indefinitely — publishes succeed, the PUB/SUB
// connection stays healthy, and nothing ever restarts it.
func TestRedisBrokerPubSubDisabledProbeStarves(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode: requires sentinel with replicas")
	}
	waitReplicasHealthy(t)

	name := "probe-disabled-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	b, received, _ := newProbeTestSentinelBroker(t, -1, name)

	channel := "probe-test-ch-disabled"
	require.NoError(t, b.Subscribe(channel))
	require.True(t, waitDelivery(t, b, received, channel, 10*time.Second),
		"initial publish was not delivered via replica subscription")

	shardChannel := string(b.pubSubShardChannelID(0, 0, false))
	subscribedReplica, _ := replicaHoldingSubscription(t, shardChannel)
	detachReplica(t, subscribedReplica)
	drainReceived(received)

	until := time.Now().Add(4 * time.Second)
	for time.Now().Before(until) {
		_, err := b.Publish(channel, []byte("starved"), PublishOptions{})
		require.NoError(t, err, "publishes must keep succeeding — that is what makes this failure silent")
		select {
		case ch := <-received:
			t.Fatalf("unexpected delivery on %s: without the probe nothing should recover the subscription", ch)
		case <-time.After(400 * time.Millisecond):
		}
	}
}

// TestRedisBrokerPubSubProbeIdleConnection checks the probe steady state on a
// healthy idle connection: probes are published to the shard service channel,
// loop back through Redis, are consumed by the loop itself without reaching
// message handlers, and no restart happens.
func TestRedisBrokerPubSubProbeIdleConnection(t *testing.T) {
	name := "probe-idle-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:              getUniquePrefix(),
		Name:                name,
		Shards:              []*RedisShard{s},
		pubSubProbeInterval: 150 * time.Millisecond,
	})
	require.NoError(t, err)
	node.SetBroker(b)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})

	received := make(chan string, 128)
	handler := &testBrokerEventHandler{
		HandleControlFunc: func([]byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			received <- ch
			return nil
		},
	}
	require.NoError(t, b.RegisterBrokerEventHandler(handler))
	channel := "probe-idle-ch"
	require.NoError(t, b.Subscribe(channel))

	// Observe the shard service channel out-of-band: the probes the broker
	// publishes there are visible to any subscriber of that channel.
	shardChannel := string(b.pubSubShardChannelID(0, 0, false))
	probeSeen := make(chan struct{}, 16)
	observer, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{"127.0.0.1:6379"},
		DisableCache:      true,
		ForceSingleClient: true,
	})
	require.NoError(t, err)
	defer observer.Close()
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		_ = observer.Receive(context.Background(), observer.B().Subscribe().Channel(shardChannel).Build(), func(msg rueidis.PubSubMessage) {
			if msg.Message == pubSubProbeMessage {
				select {
				case probeSeen <- struct{}{}:
				default:
				}
			}
		})
	}()

	// Idle through several probe intervals.
	select {
	case <-probeSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("no liveness probe observed on the shard channel while the connection was idle")
	}
	time.Sleep(500 * time.Millisecond) // a few more cycles

	// Probes must not surface as publications and must not be treated as
	// probe timeouts (the loopback keeps the connection alive).
	select {
	case ch := <-received:
		t.Fatalf("unexpected publication on %s: probes must be consumed by the PUB/SUB loop", ch)
	default:
	}
	probeTimeouts := counterValue(t, b.node.metrics.brokerPubSub.errors.WithLabelValues(name, "probe_timeout"))
	require.Equal(t, 0.0, probeTimeouts, "healthy idle connection must not fail liveness checks")

	// And the connection still delivers real messages.
	_, err = b.Publish(channel, []byte("after-idle"), PublishOptions{})
	require.NoError(t, err)
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("publication not delivered after idle probing period")
	}

	observer.Close()
	<-observerDone
}

// drainControl empties the control receive channel.
func drainControl(controlReceived chan []byte) {
	for {
		select {
		case <-controlReceived:
		default:
			return
		}
	}
}

// waitControlDelivery publishes a control message until one is delivered back
// through the control PUB/SUB connection or the deadline passes.
func waitControlDelivery(tb testing.TB, b *RedisBroker, controlReceived chan []byte, deadline time.Duration) bool {
	tb.Helper()
	until := time.Now().Add(deadline)
	for time.Now().Before(until) {
		if err := b.PublishControl([]byte("control-payload"), "", ""); err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		select {
		case <-controlReceived:
			return true
		case <-time.After(500 * time.Millisecond):
		}
	}
	return false
}

// TestRedisBrokerControlPubSubProbeIdle verifies control-loop probing on a
// healthy but silent control connection. In production the control channel
// carries node info pings every few seconds, which keeps the connection
// non-idle and probing dormant. Here the node is not running, so the control
// channel is genuinely silent — the state in which probing must activate,
// loop back through the per-node probe channel, and cause no restarts.
func TestRedisBrokerControlPubSubProbeIdle(t *testing.T) {
	name := "ctl-probe-idle-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:              getUniquePrefix(),
		Name:                name,
		Shards:              []*RedisShard{s},
		pubSubProbeInterval: 150 * time.Millisecond,
	})
	require.NoError(t, err)
	node.SetBroker(b)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})

	controlReceived := make(chan []byte, 128)
	handler := &testBrokerEventHandler{
		HandleControlFunc: func(data []byte) error {
			controlReceived <- data
			return nil
		},
	}
	require.NoError(t, b.RegisterControlEventHandler(handler))

	// Observe the per-node probe channel out-of-band.
	probeChannel := b.nodeChannel + ".probe"
	probeSeen := make(chan struct{}, 16)
	observer, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{"127.0.0.1:6379"},
		DisableCache:      true,
		ForceSingleClient: true,
	})
	require.NoError(t, err)
	defer observer.Close()
	observerDone := make(chan struct{})
	observerCtx, observerCancel := context.WithCancel(context.Background())
	defer observerCancel()
	go func() {
		defer close(observerDone)
		_ = observer.Receive(observerCtx, observer.B().Subscribe().Channel(probeChannel).Build(), func(msg rueidis.PubSubMessage) {
			if msg.Message == pubSubProbeMessage {
				select {
				case probeSeen <- struct{}{}:
				default:
				}
			}
		})
	}()

	select {
	case <-probeSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("no control liveness probe observed while the control connection was idle")
	}
	time.Sleep(500 * time.Millisecond) // a few more probe cycles

	// Probes must not reach the control handler and must not count as
	// timeouts (the loopback keeps the connection alive).
	select {
	case data := <-controlReceived:
		t.Fatalf("unexpected control message %q: probes must be consumed by the control PUB/SUB loop", data)
	default:
	}
	timeouts := counterValue(t, b.node.metrics.redisBrokerPubSubErrors.WithLabelValues(name, "control_probe_timeout"))
	require.Equal(t, 0.0, timeouts, "healthy idle control connection must not fail liveness checks")

	// Real control messages still flow.
	require.True(t, waitControlDelivery(t, b, controlReceived, 5*time.Second),
		"control message not delivered after idle probing period")
}

// TestRedisBrokerControlPubSubProbeRecoversDetachedReplica: the control-loop
// variant of the detached-replica scenario. With SubscribeOnReplica the
// control connection also lives on a replica; detaching that replica silently
// starves the node of all control traffic — other nodes' pings and commands —
// while the connection stays healthy. The probe must detect the silence and
// restart the control connection, which lands on the surviving replica.
// Unlike client channels, the control channel set is fixed, so restart-based
// recovery is fully self-contained.
func TestRedisBrokerControlPubSubProbeRecoversDetachedReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode: requires sentinel with replicas")
	}
	waitReplicasHealthy(t)

	name := "ctl-probe-recover-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	b, _, controlReceived := newProbeTestSentinelBroker(t, 300*time.Millisecond, name)

	// Sanity: control delivery works through the replica.
	require.True(t, waitControlDelivery(t, b, controlReceived, 10*time.Second),
		"initial control message was not delivered via replica subscription")

	// Find the replica holding the control subscription and detach it.
	subscribedReplica, _ := replicaHoldingSubscription(t, b.controlChannel)
	detachReplica(t, subscribedReplica)
	drainControl(controlReceived)

	// Starvation: control publishes succeed, nothing arrives.
	require.NoError(t, b.PublishControl([]byte("starved"), "", ""))
	select {
	case <-controlReceived:
		t.Fatal("unexpected control delivery right after replica detach")
	case <-time.After(1 * time.Second):
	}

	// Recovery via probe restart.
	require.True(t, waitControlDelivery(t, b, controlReceived, 45*time.Second),
		"control delivery did not recover after replica detach")
	timeouts := counterValue(t, b.node.metrics.redisBrokerPubSubErrors.WithLabelValues(name, "control_probe_timeout"))
	require.GreaterOrEqual(t, timeouts, 1.0, "expected at least one control probe timeout to trigger the restart")
}

// TestRedisBrokerPubSubProbeShardedIdle verifies the probe steady state in
// (non-node-grouped) sharded PUB/SUB mode against a real Redis Cluster: each
// partition loop probes its own hash-tagged shard channel via SPUBLISH, the
// probe routes to the right cluster node, loops back, and causes neither
// publications nor restarts.
func TestRedisBrokerPubSubProbeShardedIdle(t *testing.T) {
	name := "probe-sharded-idle-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	node := testNode(t)
	prefix := getUniquePrefix()
	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
		IOTimeout:        10 * time.Second,
		ConnectTimeout:   10 * time.Second,
	}
	s, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)

	// Sharded PUB/SUB is a Redis 7+ feature.
	result := s.client.Do(context.Background(), s.client.B().Spublish().Channel(prefix+"._").Message("").Build())
	if result.Error() != nil && strings.Contains(result.Error().Error(), "unknown command") {
		t.Skip("sharded PUB/SUB not supported by this Redis version, skipping test")
	}

	numPartitions := 4
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix:                     prefix,
		Name:                       name,
		Shards:                     []*RedisShard{s},
		NumShardedPubSubPartitions: numPartitions,
		pubSubProbeInterval:        150 * time.Millisecond,
	})
	require.NoError(t, err)
	node.SetBroker(b)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})

	received := make(chan string, 128)
	handler := &testBrokerEventHandler{
		HandleControlFunc: func([]byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			received <- ch
			return nil
		},
	}
	require.NoError(t, b.RegisterBrokerEventHandler(handler))

	// Observe every partition's shard channel: each partition loop must
	// probe its own channel.
	observer, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
		DisableCache: true,
	})
	require.NoError(t, err)
	defer observer.Close()
	var probesSeen sync.Map
	observerCtx, observerCancel := context.WithCancel(context.Background())
	defer observerCancel()
	for partIdx := 0; partIdx < numPartitions; partIdx++ {
		shardCh := string(b.pubSubShardChannelID(partIdx, 0, true))
		go func(shardCh string) {
			_ = observer.Receive(observerCtx, observer.B().Ssubscribe().Channel(shardCh).Build(), func(msg rueidis.PubSubMessage) {
				if msg.Message == pubSubProbeMessage {
					probesSeen.Store(shardCh, struct{}{})
				}
			})
		}(shardCh)
	}

	require.Eventually(t, func() bool {
		count := 0
		probesSeen.Range(func(_, _ any) bool { count++; return true })
		return count == numPartitions
	}, 10*time.Second, 100*time.Millisecond, "every partition loop must probe its own shard channel")

	select {
	case ch := <-received:
		t.Fatalf("unexpected publication on %s: probes must be consumed by the PUB/SUB loop", ch)
	default:
	}
	timeouts := counterValue(t, b.node.metrics.brokerPubSub.errors.WithLabelValues(name, "probe_timeout"))
	require.Equal(t, 0.0, timeouts, "healthy idle sharded connections must not fail liveness checks")

	// Real sharded delivery still works.
	channel := "probe-sharded-ch"
	require.NoError(t, b.Subscribe(channel))
	_, err = b.Publish(channel, []byte("after-idle"), PublishOptions{})
	require.NoError(t, err)
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("sharded publication not delivered after idle probing period")
	}
}

// TestRedisMapBrokerPubSubProbeIdle verifies the probe steady state for the
// map broker, which shares runPubSubLoop with RedisBroker but has its own
// config, metrics and shard channel naming.
func TestRedisMapBrokerPubSubProbeIdle(t *testing.T) {
	name := "map-probe-idle-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	node := testNode(t)

	received := make(chan string, 128)
	handler := &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			received <- ch
			return nil
		},
	}
	prefix := getUniquePrefix()
	e := redisMapBrokerFactory{}.makeCustom(t, node, handler, func(c *RedisMapBrokerConfig) {
		c.Prefix = prefix
		c.Name = name
		c.pubSubProbeInterval = 150 * time.Millisecond
	})

	shardChannel := e.pubSubShardChannelID(0, 0, false)
	probeSeen := make(chan struct{}, 16)
	observer, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{"127.0.0.1:6379"},
		DisableCache:      true,
		ForceSingleClient: true,
	})
	require.NoError(t, err)
	defer observer.Close()
	observerCtx, observerCancel := context.WithCancel(context.Background())
	defer observerCancel()
	go func() {
		_ = observer.Receive(observerCtx, observer.B().Subscribe().Channel(shardChannel).Build(), func(msg rueidis.PubSubMessage) {
			if msg.Message == pubSubProbeMessage {
				select {
				case probeSeen <- struct{}{}:
				default:
				}
			}
		})
	}()

	select {
	case <-probeSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("no liveness probe observed on the map broker shard channel while idle")
	}
	time.Sleep(500 * time.Millisecond)

	select {
	case ch := <-received:
		t.Fatalf("unexpected publication on %s: probes must be consumed by the PUB/SUB loop", ch)
	default:
	}
	timeouts := counterValue(t, node.metrics.mapBrokerPubSub.errors.WithLabelValues(name, "probe_timeout"))
	require.Equal(t, 0.0, timeouts, "healthy idle map broker connection must not fail liveness checks")

	// Real map delivery still works.
	channel := "map-probe-ch"
	require.NoError(t, e.Subscribe(channel))
	_, err = e.Publish(context.Background(), channel, "key1", MapPublishOptions{})
	require.NoError(t, err)
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("map publication not delivered after idle probing period")
	}
}

// TestRedisBrokerPubSubRestartRestoresSharedPollKeyChannels is a regression
// test for shared poll key channels being lost on PUB/SUB reconnect.
//
// Shared poll subscribes per-key channels at broker level and tracks them in
// its own registry — they are not in the Hub. The PUB/SUB loop used to
// resubscribe only Hub().Channels() after re-establishing a connection, so
// every reconnect (connection error, failover, probe restart) silently
// dropped all key channels at Redis while shared poll still believed they
// were subscribed. Key events then never arrived again on this node until
// each key was untracked and re-tracked — live-query push latency degraded
// to poll cadence, permanently, with no error anywhere.
//
// The test drives the real subscription path (subscribeToBrokerKeys — the
// exact function shared poll uses when a key starts being tracked), kills
// the PUB/SUB connections through Redis (CLIENT KILL, the same connection
// error a Redis restart produces), and requires the key channel to be
// subscribed and delivering again after the loop reconnects. Fails against
// the Hub-only resubscribe: the shard service channel comes back, the key
// channel never does.
func TestRedisBrokerPubSubRestartRestoresSharedPollKeyChannels(t *testing.T) {
	name := "sp-keys-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	node := testNode(t)
	s, err := NewRedisShard(node, testSingleRedisConf(6379))
	require.NoError(t, err)
	b, err := NewRedisBroker(node, RedisBrokerConfig{
		Prefix: getUniquePrefix(),
		Name:   name,
		Shards: []*RedisShard{s},
	})
	require.NoError(t, err)
	node.SetBroker(b)
	// The manager is normally created by Node.Run when shared poll is
	// configured; construct it directly to keep the test at broker level.
	node.sharedPollManager = newSharedPollManager(node)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
		stopRedisBroker(b)
	})

	received := make(chan string, 128)
	handler := &testBrokerEventHandler{
		HandleControlFunc: func([]byte) error { return nil },
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			received <- ch
			return nil
		},
	}
	require.NoError(t, b.RegisterBrokerEventHandler(handler))

	// Subscribe a key channel through the real shared poll path.
	channel := "sp-channel"
	require.NoError(t, node.sharedPollManager.subscribeToBrokerKeys(channel, []string{"k1"}))
	keyChannel := sharedPollKeyChannel(channel, "k1")
	redisKeyChannel := string(b.messageChannelID(s, keyChannel))
	redisShardChannel := string(b.pubSubShardChannelID(0, 0, false))

	subscribedAtRedis := func(redisChannel string) bool {
		chans, err := probeTestRedisDo(t, "127.0.0.1:6379", "PUBSUB", "CHANNELS", redisChannel).ToArray()
		return err == nil && len(chans) > 0
	}
	require.Eventually(t, func() bool { return subscribedAtRedis(redisKeyChannel) },
		5*time.Second, 50*time.Millisecond, "key channel must be subscribed at Redis after subscribeToBrokerKeys")

	// Sanity: key channel delivers.
	_, err = b.Publish(keyChannel, []byte("v1"), PublishOptions{})
	require.NoError(t, err)
	select {
	case ch := <-received:
		require.Equal(t, keyChannel, ch)
	case <-time.After(5 * time.Second):
		t.Fatal("key channel publication not delivered before reconnect")
	}

	// Kill all PUB/SUB connections — the same connection error a Redis
	// restart or failover produces. The loop restarts and resubscribes.
	require.NoError(t, probeTestRedisDo(t, "127.0.0.1:6379", "CLIENT", "KILL", "TYPE", "pubsub").Error())

	// The loop is back once its shard service channel is subscribed again.
	require.Eventually(t, func() bool { return subscribedAtRedis(redisShardChannel) },
		10*time.Second, 50*time.Millisecond, "PUB/SUB loop did not come back after CLIENT KILL")

	// Regression assertion: the key channel must come back too. Against the
	// Hub-only resubscribe it never does.
	require.Eventually(t, func() bool { return subscribedAtRedis(redisKeyChannel) },
		10*time.Second, 50*time.Millisecond,
		"shared poll key channel was not restored after PUB/SUB reconnect")

	// And it must actually deliver again.
	drainReceived(received)
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err = b.Publish(keyChannel, []byte("v2"), PublishOptions{})
		require.NoError(t, err)
		select {
		case ch := <-received:
			require.Equal(t, keyChannel, ch)
			return
		case <-time.After(500 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("key channel publication not delivered after reconnect")
		}
	}
}
