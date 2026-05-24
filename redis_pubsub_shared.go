package centrifuge

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/redis/rueidis"
)

const (
	pubSubProcessorBufferSize = 4096
)

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
	subscribeOnReplica bool,
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
						node.metrics.incRedisBrokerPubSubErrors(name, "handle_client_message")
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
					node.metrics.redisBrokerPubSubBufferedMessages.WithLabelValues(name, "client", strconv.Itoa(i)).Set(float64(len(processors[i])))
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

	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			select {
			case processors[index(msg.Channel, numProcessors)] <- msg:
			case <-done:
			default:
				// Buffer is full, drop the message. It's expected that PUB/SUB layer
				// only provides at most once delivery guarantee.
				// Blocking here will block Redis connection read loop which is not a
				// good thing and can lead to slower command processing and potentially
				// to deadlocks (see https://github.com/redis/rueidis/issues/596).
				node.metrics.redisBrokerPubSubDroppedMessages.WithLabelValues(name, "client").Inc()
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
		node.metrics.incRedisBrokerPubSubErrors(name, "subscribe_shard_channel")
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
						node.metrics.incRedisBrokerPubSubErrors(name, "subscribe_channel")
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
					node.metrics.incRedisBrokerPubSubErrors(name, "subscribe_channel")
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

	select {
	case err = <-wait:
		startOnce(err)
		if err != nil {
			node.metrics.incRedisBrokerPubSubErrors(name, "connection")
			node.logger.log(newErrorLogEntry(err, "pub/sub connection error", logFields))
		}
	case <-done:
	case <-shard.closeCh:
	}
}

