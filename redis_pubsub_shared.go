package centrifuge

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/redis/rueidis"
)

const (
	pubSubProcessorBufferSize = 4096

	// defaultPubSubProbeInterval is the default idle interval after which a
	// PUB/SUB connection gets a liveness probe.
	defaultPubSubProbeInterval = 30 * time.Second
)

// pubSubProbeMessage is the payload of PUB/SUB liveness probes. Probes are
// recognized by the channel they arrive on (the shard service channel), not
// by payload — the payload only helps when inspecting traffic by hand.
const pubSubProbeMessage = "probe"

// publishPubSubProbe publishes a liveness probe to the shard service channel
// through the regular publish path. The publish always goes through the main
// shard client (the master), even when the PUB/SUB loop subscribes on a
// replica — in that mode a delivered probe additionally verifies the
// replication link.
func publishPubSubProbe(shard *RedisShard, node *Node, psm redisPubSubMetrics, name, shardChannel string, useShardedPubSub bool, timeout time.Duration, logFields map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var cmd rueidis.Completed
	if useShardedPubSub {
		cmd = shard.client.B().Spublish().Channel(shardChannel).Message(pubSubProbeMessage).Build()
	} else {
		cmd = shard.client.B().Publish().Channel(shardChannel).Message(pubSubProbeMessage).Build()
	}
	if err := shard.client.Do(ctx, cmd).Error(); err != nil {
		// A failed publish becomes a failed probe on the next tick. Still
		// worth logging separately: it means the publish path is broken too,
		// so restarting the PUB/SUB connection alone may not help.
		psm.incErrors(name, "probe_publish")
		node.logger.log(newErrorLogEntry(err, "error publishing PUB/SUB probe", logFields))
	}
}

// pubSubCallbacks carries the type-varying behavior as function pointers.
// Both RedisBroker and RedisMapBroker provide their own callbacks.
type pubSubCallbacks struct {
	// handleMessage processes a message received from PUB/SUB.
	handleMessage func(isCluster bool, handler BrokerEventHandler, ch string, data []byte) error
	// shardChannelID returns the shard channel ID for a given cluster shard index and pub/sub shard index.
	shardChannelID func(clusterIdx, psIdx int, useShardedPubSub bool) string
	// messageChannelID returns the pub/sub channel name for a given user channel.
	messageChannelID func(ch string) string
	// shardForChannel returns the RedisShard for a given channel (for filtering during resubscribe).
	shardForChannel func(ch string) *RedisShard
}

func getPubSubStartLogFields(s *RedisShard, logFields map[string]any) map[string]any {
	startLogFields := make(map[string]any, len(logFields))
	for k, v := range logFields {
		startLogFields[k] = v
	}
	if s.isCluster {
		startLogFields["cluster"] = true
	}
	return startLogFields
}

func logResubscribed(node *Node, numChannels int, elapsed time.Duration, logFields map[string]any) {
	combinedLogFields := make(map[string]any, len(logFields)+2)
	for k, v := range logFields {
		combinedLogFields[k] = v
	}
	combinedLogFields["elapsed"] = elapsed.String()
	combinedLogFields["num_channels"] = numChannels
	node.logger.log(newLogEntry(LogLevelDebug, "resubscribed to channels", combinedLogFields))
}

// runPubSubLoop is the unified PUB/SUB loop used by both RedisBroker and RedisMapBroker.
// It handles connection setup, message processing, resubscription, and error handling.
func runPubSubLoop(
	shard *RedisShard,
	subClientsMu *sync.Mutex,
	subClients [][]rueidis.DedicatedClient,
	cb pubSubCallbacks,
	node *Node,
	name string,
	psm redisPubSubMetrics,
	subscribeOnReplica bool,
	probeInterval time.Duration,
	numProcessors, numResubscribeShards, numSubscribeShards, numPartitions int,
	logFields map[string]any,
	eventHandler BrokerEventHandler,
	clusterShardIndex, psShardIndex int,
	useShardedPubSub bool,
	startOnce func(error),
) {
	shardChannel := cb.shardChannelID(clusterShardIndex, psShardIndex, useShardedPubSub)

	if node.logEnabled(LogLevelDebug) {
		debugLogValues := map[string]any{
			"num_processors": numProcessors,
		}
		if useShardedPubSub {
			debugLogValues["cluster_shard_index"] = clusterShardIndex
		}
		pubSubStartLogFields := getPubSubStartLogFields(shard, logFields)
		combinedLogFields := make(map[string]any, len(pubSubStartLogFields)+len(debugLogValues))
		for k, v := range pubSubStartLogFields {
			combinedLogFields[k] = v
		}
		for k, v := range debugLogValues {
			combinedLogFields[k] = v
		}
		node.logger.log(newLogEntry(LogLevelDebug, "running Redis PUB/SUB", combinedLogFields))
		defer func() {
			node.logger.log(newLogEntry(LogLevelDebug, "stopping Redis PUB/SUB", combinedLogFields))
		}()
	}

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDoneOnce := func() {
		doneOnce.Do(func() {
			close(done)
		})
	}
	defer closeDoneOnce()

	// Run PUB/SUB message processors to spread received message processing work over worker goroutines.
	processors := make(map[int]chan rueidis.PubSubMessage)
	for i := 0; i < numProcessors; i++ {
		processingCh := make(chan rueidis.PubSubMessage, pubSubProcessorBufferSize)
		processors[i] = processingCh
		go func(ch chan rueidis.PubSubMessage) {
			for {
				select {
				case <-done:
					return
				case msg := <-ch:
					err := cb.handleMessage(shard.isCluster, eventHandler, msg.Channel, convert.StringToBytes(msg.Message))
					if err != nil {
						psm.incErrors(name, "handle_client_message")
						node.logger.log(newErrorLogEntry(err, "error handling client message", logFields))
						continue
					}
				}
			}
		}(processingCh)
	}

	// Buffer monitoring goroutine.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for i := 0; i < numProcessors; i++ {
					psm.buffered.WithLabelValues(name, "client", strconv.Itoa(i)).Set(float64(len(processors[i])))
				}
			}
		}
	}()

	client := shard.client
	if subscribeOnReplica {
		client = shard.replicaClient
	}

	conn, cancel := client.Dedicate()
	defer cancel()
	defer conn.Close()

	// receivedCount counts messages delivered by this connection. It feeds
	// the liveness probing below: a connection that received anything since
	// the previous check is alive and is not probed. A counter increment is
	// the only per-message cost of probing.
	var receivedCount atomic.Uint64

	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			receivedCount.Add(1)
			if msg.Channel == shardChannel {
				// The shard channel is a service channel: nothing publishes
				// real traffic to it, only liveness probes arrive here. The
				// probe did its job by updating receivedCount — don't pass it
				// to message processors.
				return
			}
			select {
			case processors[index(msg.Channel, numProcessors)] <- msg:
			case <-done:
			default:
				// Buffer is full, drop the message. It's expected that PUB/SUB layer
				// only provides at most once delivery guarantee.
				// Blocking here will block Redis connection read loop which is not a
				// good thing and can lead to slower command processing and potentially
				// to deadlocks (see https://github.com/redis/rueidis/issues/596).
				psm.dropped.WithLabelValues(name, "client").Inc()
			}
		},
		OnSubscription: func(ps rueidis.PubSubSubscription) {
			if !useShardedPubSub {
				return
			}
			if ps.Kind == "sunsubscribe" && ps.Channel == shardChannel {
				// Helps to handle slot migration.
				node.logger.log(newLogEntry(LogLevelInfo, "pub/sub restart due to slot migration", logFields))
				closeDoneOnce()
			}
		},
	})

	var err error
	if useShardedPubSub {
		err = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(shardChannel).Build()).Error()
	} else {
		err = conn.Do(context.Background(), conn.B().Subscribe().Channel(shardChannel).Build()).Error()
	}
	if err != nil {
		startOnce(err)
		psm.incErrors(name, "subscribe_shard_channel")
		node.logger.log(newErrorLogEntry(err, "pub/sub subscribe error", logFields))
		return
	}

	channels := node.Hub().Channels()

	var wg sync.WaitGroup
	started := time.Now()

	for i := 0; i < numResubscribeShards; i++ {
		wg.Add(1)
		go func(subscriberIndex int) {
			defer wg.Done()
			estimatedCap := len(channels) / numResubscribeShards / numSubscribeShards
			if useShardedPubSub {
				estimatedCap /= numPartitions
			}
			chIDs := make([]string, 0, estimatedCap)

			for _, ch := range channels {
				if cb.shardForChannel(ch) != shard {
					continue
				}
				if useShardedPubSub && consistentIndex(ch, numPartitions) != clusterShardIndex {
					continue
				}
				if index(ch, numSubscribeShards) != psShardIndex {
					continue
				}
				if index(ch, numResubscribeShards) != subscriberIndex {
					continue
				}
				chIDs = append(chIDs, cb.messageChannelID(ch))
			}

			subscribeBatch := func(batch []string) error {
				if useShardedPubSub {
					return conn.Do(context.Background(), conn.B().Ssubscribe().Channel(batch...).Build()).Error()
				}
				return conn.Do(context.Background(), conn.B().Subscribe().Channel(batch...).Build()).Error()
			}

			batch := make([]string, 0, redisSubscribeBatchLimit)

			for i, ch := range chIDs {
				if len(batch) > 0 && i%redisSubscribeBatchLimit == 0 {
					err := subscribeBatch(batch)
					if err != nil {
						psm.incErrors(name, "subscribe_channel")
						node.logger.log(newErrorLogEntry(err, "error subscribing", logFields))
						closeDoneOnce()
						return
					}
					batch = batch[:0]
				}
				batch = append(batch, ch)
			}
			if len(batch) > 0 {
				err := subscribeBatch(batch)
				if err != nil {
					psm.incErrors(name, "subscribe_channel")
					node.logger.log(newErrorLogEntry(err, "error subscribing", logFields))
					closeDoneOnce()
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		if len(channels) > 0 && node.logEnabled(LogLevelDebug) {
			logResubscribed(node, len(channels), time.Since(started), logFields)
		}
		select {
		case <-done:
			startOnce(errors.New("error resubscribing"))
		default:
			subClientsMu.Lock()
			subClients[clusterShardIndex][psShardIndex] = conn
			subClientsMu.Unlock()
			defer func() {
				// Compare-and-swap: only nil the slot if it still holds OUR
				// conn. A subsequent run of this same loop (after topology
				// rebuild closed our `done`) may have already written its own
				// fresh conn into this slot before our defer fires. Without
				// the equality check, our nil write would clobber a live
				// connection.
				subClientsMu.Lock()
				if subClients[clusterShardIndex][psShardIndex] == conn {
					subClients[clusterShardIndex][psShardIndex] = nil
				}
				subClientsMu.Unlock()
			}()
			startOnce(nil)
		}
		<-done
	}()

	// The loop below parks until the connection errors, the loop is asked to
	// stop, or the shard closes. A healthy-looking connection is not enough
	// to park on forever: after a Redis failover the connection may end up
	// attached to a node that answers keepalive pings and accepts commands
	// but never receives the published traffic — a demoted master, a node
	// outside the replication chain, or an unrelated Redis on a reused IP
	// (see centrifugal/centrifugo#1189). No connection error ever fires in
	// that state, so subscribers starve silently until process restart.
	//
	// The probe ticker breaks that: when nothing has been received for a
	// full interval, publish a small probe to the shard service channel
	// through the regular publish path and expect it back on this
	// connection. If a probe was sent and a whole further interval passed
	// without receiving ANYTHING (not even the probe), the connection is
	// considered stale and the loop restarts, re-resolving the topology.
	// Under regular traffic the probe never fires, so the steady-state cost
	// is one atomic load per interval.
	var probeTickerCh <-chan time.Time
	if probeInterval > 0 {
		probeTicker := time.NewTicker(probeInterval)
		defer probeTicker.Stop()
		probeTickerCh = probeTicker.C
	}
	var seenCount uint64
	var probeOutstanding bool
	for {
		select {
		case err = <-wait:
			startOnce(err)
			if err != nil {
				psm.incErrors(name, "connection")
				node.logger.log(newErrorLogEntry(err, "pub/sub connection error", logFields))
			}
			return
		case <-done:
			return
		case <-shard.closeCh:
			return
		case <-probeTickerCh:
			cur := receivedCount.Load()
			if cur != seenCount {
				// The connection delivered something during the last
				// interval (possibly a previous probe) — alive.
				seenCount = cur
				probeOutstanding = false
				continue
			}
			if probeOutstanding {
				// A probe was published a full interval ago and nothing at
				// all has been received since — the connection is attached
				// to a node that does not deliver published traffic.
				psm.incErrors(name, "probe_timeout")
				node.logger.log(newLogEntry(LogLevelWarn, "no PUB/SUB message received since liveness probe was sent, restarting PUB/SUB connection", logFields))
				return
			}
			probeOutstanding = true
			go publishPubSubProbe(shard, node, psm, name, shardChannel, useShardedPubSub, probeInterval, logFields)
		}
	}
}

