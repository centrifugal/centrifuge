package centrifuge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/timers"

	"github.com/centrifugal/protocol"
	"github.com/rueian/rueidis"
)

var errRedisOpTimeout = errors.New("redis: operation timed out")
var errRedisClosed = errors.New("redis: closed")

const (
	// redisSubscribeBatchLimit is a maximum number of channels to include in a single
	// batch subscribe call.
	redisSubscribeBatchLimit = 512
	// redisControlChannelSuffix is a suffix for control channel.
	redisControlChannelSuffix = ".control"
	// redisNodeChannelPrefix is a suffix for node channel.
	redisNodeChannelPrefix = ".node."
	// redisPingChannelSuffix is a suffix for ping channel.
	redisPingChannelSuffix = ".ping"
	// redisClientChannelPrefix is a prefix before channel name for client messages.
	redisClientChannelPrefix = ".client."
	// redisShardChannelPrefix is a prefix before channel name which we use to establish shared PUB/SUB connection.
	redisPubSubShardChannelSuffix = ".shard"
)

var _ Broker = (*RedisBroker)(nil)

// RedisBroker uses Redis to implement Broker functionality. This broker allows
// scaling Centrifuge-based server to many instances and load balance client
// connections between them.
// Broker additionally supports Redis Sentinel, client-side consistent sharding
// and can work with Redis Cluster (including client-side sharding between different
// Redis Clusters to scale PUB/SUB).
// By default, Redis >= 5 required (due to the fact Broker uses STREAM data structure).
type RedisBroker struct {
	controlRound           uint64 // Keep atomic on struct top for 32-bit architectures.
	node                   *Node
	sharding               bool
	config                 RedisBrokerConfig
	shards                 []*RedisShard
	historyListScript      *rueidis.Lua
	historyStreamScript    *rueidis.Lua
	addHistoryListScript   *rueidis.Lua
	addHistoryStreamScript *rueidis.Lua
	shardChannel           string
	messagePrefix          string
	pingChannel            string
	controlChannel         string
	nodeChannel            string
	closeOnce              sync.Once
	closeCh                chan struct{}
}

// DefaultRedisBrokerPrefix is a default value for RedisBrokerConfig.Prefix.
const DefaultRedisBrokerPrefix = "centrifugo"

// RedisBrokerConfig is a config for Broker.
type RedisBrokerConfig struct {
	// Prefix to use before every channel name and key in Redis. By default,
	// DefaultRedisBrokerPrefix will be used.
	Prefix string

	// HistoryMetaTTL sets a time of stream meta key expiration in Redis. Stream
	// meta key is a Redis HASH that contains top offset in channel and epoch value.
	// By default stream meta keys do not expire.
	//
	// Though in some cases – when channels created for а short time and then
	// not used anymore – created stream meta keys can stay in memory while
	// not actually useful. For example you can have a personal user channel but
	// after using your app for a while user left it forever. In long-term
	// perspective this can be an unwanted memory leak. Setting a reasonable
	// value to this option (usually much bigger than history retention period)
	// can help. In this case unused channel stream meta data will eventually expire.
	//
	// TODO v1: since we have epoch, things should also properly work without meta
	// information at all (but we loose possibility of long-term recover in stream
	// without new messages). We can make this optional and disabled by default at
	// least.
	HistoryMetaTTL time.Duration

	// UseLists allows enabling usage of Redis LIST instead of STREAM data
	// structure to keep history. LIST support exist mostly for backward
	// compatibility since STREAM seems superior. If you have a use case
	// where you need to turn on this option in new setup - please share,
	// otherwise LIST support can be removed at some point in the future.
	// Iteration over history in reversed order not supported with lists.
	UseLists bool

	// PubSubNumWorkers sets how many PUB/SUB message processing workers will
	// be started. By default, runtime.NumCPU() workers used.
	PubSubNumWorkers int

	// Shards is a list of Redis shards to use. At least one shard must be provided.
	Shards []*RedisShard

	// NumClusterShards when greater than zero allows turning on a mode when broker
	// will use Redis Cluster sharded PUB/SUB feature. To achieve subscribe efficiency broker
	// reduces possible slots to NumClusterShards value and starts separate PUB/SUB
	// routines for each shard. By default, sharded PUB/SUB is not used - i.e. we are using
	// globally distributed PUBLISH commands.
	// Requires Redis >= 7.
	NumClusterShards int
}

// NewRedisBroker initializes Redis Broker.
func NewRedisBroker(n *Node, config RedisBrokerConfig) (*RedisBroker, error) {
	if len(config.Shards) == 0 {
		return nil, errors.New("broker: no Redis shards provided in configuration")
	}

	if len(config.Shards) > 1 {
		n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("broker: Redis sharding enabled: %d shards", len(config.Shards))))
	}

	if config.Prefix == "" {
		config.Prefix = DefaultRedisBrokerPrefix
	}

	b := &RedisBroker{
		node:                   n,
		shards:                 config.Shards,
		config:                 config,
		sharding:               len(config.Shards) > 1,
		historyStreamScript:    rueidis.NewLuaScript(historyStreamSource),
		historyListScript:      rueidis.NewLuaScript(historyListSource),
		addHistoryStreamScript: rueidis.NewLuaScript(addHistoryStreamSource),
		addHistoryListScript:   rueidis.NewLuaScript(addHistoryListSource),
		closeCh:                make(chan struct{}),
	}

	b.shardChannel = config.Prefix + redisPubSubShardChannelSuffix
	b.messagePrefix = config.Prefix + redisClientChannelPrefix
	b.pingChannel = config.Prefix + redisPingChannelSuffix
	b.nodeChannel = string(b.nodeChannelID(n.ID()))
	b.controlChannel = config.Prefix + redisControlChannelSuffix
	return b, nil
}

const (
	// Add to history and optionally publish.
	// KEYS[1] - history list key
	// KEYS[2] - sequence meta hash key
	// ARGV[1] - message payload
	// ARGV[2] - history size ltrim right bound
	// ARGV[3] - history lifetime
	// ARGV[4] - channel to publish message to if needed
	// ARGV[5] - history meta key expiration time
	// ARGV[6] - new epoch value if no epoch set yet
	// ARGV[7] - command to publish (publish or spublish)
	addHistoryListSource = `
local epoch
if redis.call('exists', KEYS[2]) ~= 0 then
  epoch = redis.call("hget", KEYS[2], "e")
end
if epoch == false or epoch == nil then
  epoch = ARGV[6]
  redis.call("hset", KEYS[2], "e", epoch)
end
local offset = redis.call("hincrby", KEYS[2], "s", 1)
if ARGV[5] ~= '0' then
	redis.call("expire", KEYS[2], ARGV[5])
end
local payload = "__" .. "p1:" .. offset .. ":" .. epoch .. "__" .. ARGV[1]
redis.call("lpush", KEYS[1], payload)
redis.call("ltrim", KEYS[1], 0, ARGV[2])
redis.call("expire", KEYS[1], ARGV[3])
if ARGV[4] ~= '' then
	redis.call(ARGV[7], ARGV[4], payload)
end
return {offset, epoch}
		`

	// addHistoryStreamSource contains a Lua script to save data to Redis stream and
	// publish it into channel.
	// KEYS[1] - history stream key
	// KEYS[2] - stream meta hash key
	// ARGV[1] - message payload
	// ARGV[2] - stream size
	// ARGV[3] - stream lifetime
	// ARGV[4] - channel to publish message to if needed
	// ARGV[5] - history meta key expiration time
	// ARGV[6] - new epoch value if no epoch set yet
	// ARGV[7] - command to publish (publish or spublish)
	addHistoryStreamSource = `
local epoch
if redis.call('exists', KEYS[2]) ~= 0 then
  epoch = redis.call("hget", KEYS[2], "e")
end
if epoch == false or epoch == nil then
  epoch = ARGV[6]
  redis.call("hset", KEYS[2], "e", epoch)
end
local offset = redis.call("hincrby", KEYS[2], "s", 1)
if ARGV[5] ~= '0' then
	redis.call("expire", KEYS[2], ARGV[5])
end
redis.call("xadd", KEYS[1], "MAXLEN", ARGV[2], offset, "d", ARGV[1])
redis.call("expire", KEYS[1], ARGV[3])
if ARGV[4] ~= '' then
	local payload = "__" .. "p1:" .. offset .. ":" .. epoch .. "__" .. ARGV[1]
	redis.call(ARGV[7], ARGV[4], payload)
end
return {offset, epoch}
	`

	// Retrieve channel history information.
	// KEYS[1] - history list key
	// KEYS[2] - list meta hash key
	// ARGV[1] - include publications into response
	// ARGV[2] - publications list right bound
	// ARGV[3] - list meta hash key expiration time
	// ARGV[4] - new epoch value if no epoch set yet
	historyListSource = `
local offset = redis.call("hget", KEYS[2], "s")
local epoch
if redis.call('exists', KEYS[2]) ~= 0 then
  epoch = redis.call("hget", KEYS[2], "e")
end
if epoch == false or epoch == nil then
  epoch = ARGV[4]
  redis.call("hset", KEYS[2], "e", epoch)
end
if ARGV[3] ~= '0' then
	redis.call("expire", KEYS[2], ARGV[3])
end
local pubs = nil
if ARGV[1] ~= "0" then
	pubs = redis.call("lrange", KEYS[1], 0, ARGV[2])
end
return {offset, epoch, pubs}
	`

	// historyStreamSource ...
	// KEYS[1] - history stream key
	// KEYS[2] - stream meta hash key
	// ARGV[1] - include publications into response
	// ARGV[2] - offset
	// ARGV[3] - limit
	// ARGV[4] - reverse
	// ARGV[5] - stream meta hash key expiration time
	// ARGV[6] - new epoch value if no epoch set yet
	historyStreamSource = `
local offset = redis.call("hget", KEYS[2], "s")
local epoch
if redis.call('exists', KEYS[2]) ~= 0 then
  epoch = redis.call("hget", KEYS[2], "e")
end
if epoch == false or epoch == nil then
  epoch = ARGV[6]
  redis.call("hset", KEYS[2], "e", epoch)
end
if ARGV[5] ~= '0' then
	redis.call("expire", KEYS[2], ARGV[5])
end
local pubs = nil
if ARGV[1] ~= "0" then
  if ARGV[3] ~= "0" then
	if ARGV[4] == '0' then
    	pubs = redis.call("xrange", KEYS[1], ARGV[2], "+", "COUNT", ARGV[3])
	else
		local getOffset = offset
		local incomingOffset = tonumber(ARGV[2])
		if incomingOffset ~= 0 then
			getOffset = incomingOffset
		end
		pubs = redis.call("xrevrange", KEYS[1], getOffset, "-", "COUNT", ARGV[3])
	end
  else
	if ARGV[4] == '0' then
		pubs = redis.call("xrange", KEYS[1], ARGV[2], "+")
	else
		local getOffset = offset
		local incomingOffset = tonumber(ARGV[2])
		if incomingOffset ~= 0 then
			getOffset = incomingOffset
		end
		pubs = redis.call("xrevrange", KEYS[1], getOffset, "-")
	end
  end
end
return {offset, epoch, pubs}
	`
)

func (b *RedisBroker) getShard(channel string) *RedisShard {
	if !b.sharding {
		return b.shards[0]
	}
	return b.shards[consistentIndex(channel, len(b.shards))]
}

// Run – see Broker.Run.
func (b *RedisBroker) Run(h BrokerEventHandler) error {
	startChannels := make([]chan error, 0, len(b.shards))
	for shardNum, shard := range b.shards {
		if shard.useCluster && b.config.NumClusterShards > 0 {
			shard.shardedSubCh = make([]chan subRequest, b.config.NumClusterShards)
			for i := 0; i < b.config.NumClusterShards; i++ {
				shard.shardedSubCh[i] = make(chan subRequest)
			}
		}
		startCh := make(chan error, 1)
		startChannels = append(startChannels, startCh)
		err := b.runShard(shard, h, startCh)
		if err != nil {
			return err
		}
		if err := b.checkCapabilities(shard); err != nil {
			return fmt.Errorf("capability error on shard [%d]: %v", shardNum, err)
		}
	}
	for i := 0; i < len(b.shards); i++ {
		<-startChannels[i]
	}
	return nil
}

func (b *RedisBroker) checkCapabilities(shard *RedisShard) error {
	if b.config.UseLists {
		return nil
	}
	// Check whether Redis Streams supported.
	resp := shard.client.Do(context.Background(), shard.client.B().Xrange().Key("_").Start("0-0").End("0-0").Build())
	if resp.Error() != nil {
		if strings.Contains(resp.Error().Error(), "ERR unknown command") {
			return errors.New("STREAM only available since Redis >= 5, consider upgrading Redis or using LIST structure for history")
		}
		return resp.Error()
	}
	return nil
}

// runForever keeps another function running indefinitely.
// The reason this loop is not inside the function itself is
// so that defer can be used to clean-up nicely.
func (b *RedisBroker) runForever(fn func()) {
	for {
		select {
		case <-b.closeCh:
			return
		default:
		}
		fn()
		select {
		case <-b.closeCh:
			return
		case <-time.After(250 * time.Millisecond):
			// Wait for a while to prevent busy loop when reconnecting to Redis.
		}
	}
}

func (b *RedisBroker) runShard(s *RedisShard, h BrokerEventHandler, startCh chan error) error {
	var startOnce sync.Once
	go b.runForever(func() {
		select {
		case <-b.closeCh:
			return
		default:
		}
		b.runControlPubSub(s, h, func(err error) {
			startOnce.Do(func() {
				startCh <- err
			})
		})
	})
	if s.useCluster && b.config.NumClusterShards > 0 {
		for i := 0; i < b.config.NumClusterShards; i++ {
			i := i
			go b.runForever(func() {
				select {
				case <-b.closeCh:
					return
				default:
				}
				b.runShardedPubSub(s, h, i)
			})
		}
	} else {
		go b.runForever(func() {
			select {
			case <-b.closeCh:
				return
			default:
			}
			b.runPubSub(s, h)
		})
	}
	return nil
}

func (b *RedisBroker) Close(_ context.Context) error {
	b.closeOnce.Do(func() {
		close(b.closeCh)
	})
	return nil
}

func (b *RedisBroker) runControlPubSub(s *RedisShard, eventHandler BrokerEventHandler, startOnce func(error)) {
	numWorkers := b.config.PubSubNumWorkers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}

	b.node.Log(NewLogEntry(LogLevelDebug, fmt.Sprintf("running Redis PUB/SUB, num workers: %d", numWorkers), map[string]interface{}{"shard": s.string()}))
	defer func() {
		b.node.Log(NewLogEntry(LogLevelDebug, "stopping Redis PUB/SUB", map[string]interface{}{"shard": s.string()}))
	}()

	controlChannel := b.controlChannel
	nodeChannel := b.nodeChannel

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDoneOnce := func() {
		doneOnce.Do(func() {
			close(done)
		})
	}
	defer closeDoneOnce()

	conn, cancel := s.client.Dedicate()
	defer cancel()
	defer conn.Close()

	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			err := eventHandler.HandleControl([]byte(msg.Message))
			if err != nil {
				b.node.Log(NewLogEntry(LogLevelError, "error handling control message", map[string]interface{}{"error": err.Error()}))
			}
		},
	})

	err := conn.Do(context.Background(), conn.B().Subscribe().Channel(controlChannel, nodeChannel).Build()).Error()
	if err != nil {
		b.node.Log(NewLogEntry(LogLevelError, "control pub/sub error", map[string]interface{}{"error": err.Error()}))
		return
	}

	startOnce(nil)

	select {
	case err := <-wait:
		if err != nil {
			b.node.Log(NewLogEntry(LogLevelError, "control pub/sub error", map[string]interface{}{"error": err.Error()}))
		}
	case <-s.closeCh:
	}
}

func (b *RedisBroker) runShardedPubSub(s *RedisShard, eventHandler BrokerEventHandler, shardIndex int) {
	numWorkers := 1

	b.node.Log(NewLogEntry(LogLevelDebug, fmt.Sprintf("running sharded Redis PUB/SUB, num workers: %d", numWorkers), map[string]interface{}{"shard": s.string()}))
	defer func() {
		b.node.Log(NewLogEntry(LogLevelDebug, "stopping Redis PUB/SUB", map[string]interface{}{"shard": s.string()}))
	}()

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDoneOnce := func() {
		doneOnce.Do(func() {
			close(done)
		})
	}
	defer closeDoneOnce()

	// Run workers to spread received message processing work over worker goroutines.
	workers := make(map[int]chan rueidis.PubSubMessage)
	for i := 0; i < numWorkers; i++ {
		workerCh := make(chan rueidis.PubSubMessage)
		workers[i] = workerCh
		go func(ch chan rueidis.PubSubMessage) {
			for {
				select {
				case <-done:
					return
				case msg := <-ch:
					err := b.handleRedisClientMessage(eventHandler, channelID(msg.Channel), []byte(msg.Message))
					if err != nil {
						b.node.Log(NewLogEntry(LogLevelError, "error handling client message", map[string]interface{}{"error": err.Error()}))
						continue
					}
				}
			}
		}(workerCh)
	}

	conn, cancel := s.client.Dedicate()
	defer cancel()
	defer conn.Close()

	shardChannel := b.pubSubShardChannelID(s, shardIndex)

	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			workers[index(msg.Channel, numWorkers)] <- msg
		},
		OnSubscription: func(s rueidis.PubSubSubscription) {
			if s.Kind == "sunsubscribe" && s.Channel == string(shardChannel) {
				closeDoneOnce()
			}
		},
	})

	// Run subscriber goroutine.
	go func() {
		b.node.Log(NewLogEntry(LogLevelDebug, "starting RedisBroker Subscriber", map[string]interface{}{"shard": s.string()}))
		defer func() {
			b.node.Log(NewLogEntry(LogLevelDebug, "stopping RedisBroker Subscriber", map[string]interface{}{"shard": s.string()}))
		}()
		for {
			select {
			case <-done:
				conn.Close()
				return
			case r := <-s.shardedSubCh[shardIndex]:
				isSubscribe := r.subscribe
				channelBatch := []subRequest{r}

				chIDs := make([]string, 0, len(r.channels))
				for _, ch := range r.channels {
					chIDs = append(chIDs, string(ch))
				}

				var otherR *subRequest

			loop:
				for len(chIDs) < redisSubscribeBatchLimit {
					select {
					case r := <-s.shardedSubCh[shardIndex]:
						if r.subscribe != isSubscribe {
							// We can not mix subscribe and unsubscribe request into one batch
							// so must stop here. As we consumed a subRequest value from channel
							// we should take care of it later.
							otherR = &r
							break loop
						}
						channelBatch = append(channelBatch, r)
						for _, ch := range r.channels {
							chIDs = append(chIDs, string(ch))
						}
					default:
						break loop
					}
				}

				var opErr error
				if isSubscribe {
					opErr = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(chIDs...).Build()).Error()
				} else {
					opErr = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(chIDs...).Build()).Error()
				}

				if opErr != nil {
					for _, r := range channelBatch {
						r.done(opErr)
					}
					if otherR != nil {
						otherR.done(opErr)
					}
					// Close conn, this should cause Receive to return with err below
					// and whole runPubSub method to restart.
					conn.Close()
					return
				}
				for _, r := range channelBatch {
					r.done(nil)
				}
				if otherR != nil {
					chIDs := make([]string, 0, len(otherR.channels))
					for _, ch := range otherR.channels {
						chIDs = append(chIDs, string(ch))
					}
					var opErr error
					if otherR.subscribe {
						opErr = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(chIDs...).Build()).Error()
					} else {
						opErr = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(chIDs...).Build()).Error()
					}
					if opErr != nil {
						otherR.done(opErr)
						// Close conn, this should cause Receive to return with err below
						// and whole runPubSub method to restart.
						conn.Close()
						return
					}
					otherR.done(nil)
				}
			}
		}
	}()

	go func() {
		chIDs := make([]channelID, 1)
		chIDs[0] = shardChannel

		for _, ch := range b.node.Hub().Channels() {
			if b.getShard(ch) == s && consistentIndex(ch, b.config.NumClusterShards) == shardIndex {
				chIDs = append(chIDs, b.messageChannelID(s, ch))
			}
		}

		batch := make([]channelID, 0)

		for i, ch := range chIDs {
			if len(batch) > 0 && i%redisSubscribeBatchLimit == 0 {
				r := newSubRequest(batch, true)
				err := b.sendSubscribeSharded(s, r, shardIndex)
				if err != nil {
					b.node.Log(NewLogEntry(LogLevelError, "error subscribing", map[string]interface{}{"error": err.Error()}))
					closeDoneOnce()
					return
				}
				batch = nil
			}
			batch = append(batch, ch)
		}
		if len(batch) > 0 {
			r := newSubRequest(batch, true)
			err := b.sendSubscribeSharded(s, r, shardIndex)
			if err != nil {
				b.node.Log(NewLogEntry(LogLevelError, "error subscribing", map[string]interface{}{"error": err.Error()}))
				closeDoneOnce()
				return
			}
		}
	}()

	select {
	case err := <-wait:
		if err != nil {
			b.node.Log(NewLogEntry(LogLevelError, "sharded pub/sub error", map[string]interface{}{"error": err.Error()}))
		}
	case <-s.closeCh:
	}
}

func (b *RedisBroker) runPubSub(s *RedisShard, eventHandler BrokerEventHandler) {
	numWorkers := b.config.PubSubNumWorkers
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}

	b.node.Log(NewLogEntry(LogLevelDebug, fmt.Sprintf("running Redis PUB/SUB, num workers: %d", numWorkers), map[string]interface{}{"shard": s.string()}))
	defer func() {
		b.node.Log(NewLogEntry(LogLevelDebug, "stopping Redis PUB/SUB", map[string]interface{}{"shard": s.string()}))
	}()

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDoneOnce := func() {
		doneOnce.Do(func() {
			close(done)
		})
	}
	defer closeDoneOnce()

	// Run workers to spread received message processing work over worker goroutines.
	workers := make(map[int]chan rueidis.PubSubMessage)
	for i := 0; i < numWorkers; i++ {
		workerCh := make(chan rueidis.PubSubMessage)
		workers[i] = workerCh
		go func(ch chan rueidis.PubSubMessage) {
			for {
				select {
				case <-done:
					return
				case msg := <-ch:
					err := b.handleRedisClientMessage(eventHandler, channelID(msg.Channel), []byte(msg.Message))
					if err != nil {
						b.node.Log(NewLogEntry(LogLevelError, "error handling client message", map[string]interface{}{"error": err.Error()}))
						continue
					}
				}
			}
		}(workerCh)
	}

	conn, cancel := s.client.Dedicate()
	defer cancel()
	defer conn.Close()

	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			select {
			case workers[index(msg.Channel, numWorkers)] <- msg:
			case <-done:
				return
			}
		},
	})

	// Run subscriber goroutine.
	go func() {
		b.node.Log(NewLogEntry(LogLevelDebug, "starting RedisBroker Subscriber", map[string]interface{}{"shard": s.string()}))
		defer func() {
			b.node.Log(NewLogEntry(LogLevelDebug, "stopping RedisBroker Subscriber", map[string]interface{}{"shard": s.string()}))
		}()
		for {
			select {
			case <-done:
				conn.Close()
				return
			case r := <-s.subCh:
				isSubscribe := r.subscribe
				channelBatch := []subRequest{r}

				chIDs := make([]string, 0, len(r.channels))
				for _, ch := range r.channels {
					chIDs = append(chIDs, string(ch))
				}

				var otherR *subRequest

			loop:
				for len(chIDs) < redisSubscribeBatchLimit {
					select {
					case r := <-s.subCh:
						if r.subscribe != isSubscribe {
							// We can not mix subscribe and unsubscribe request into one batch
							// so must stop here. As we consumed a subRequest value from channel
							// we should take care of it later.
							otherR = &r
							break loop
						}
						channelBatch = append(channelBatch, r)
						for _, ch := range r.channels {
							chIDs = append(chIDs, string(ch))
						}
					default:
						break loop
					}
				}

				var opErr error
				if isSubscribe {
					opErr = conn.Do(context.Background(), conn.B().Subscribe().Channel(chIDs...).Build()).Error()
				} else {
					opErr = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(chIDs...).Build()).Error()
				}

				if opErr != nil {
					for _, r := range channelBatch {
						r.done(opErr)
					}
					if otherR != nil {
						otherR.done(opErr)
					}
					// Close conn, this should cause Receive to return with err below
					// and whole runPubSub method to restart.
					conn.Close()
					return
				}
				for _, r := range channelBatch {
					r.done(nil)
				}
				if otherR != nil {
					chIDs := make([]string, 0, len(otherR.channels))
					for _, ch := range otherR.channels {
						chIDs = append(chIDs, string(ch))
					}
					var opErr error
					if otherR.subscribe {
						opErr = conn.Do(context.Background(), conn.B().Subscribe().Channel(chIDs...).Build()).Error()
					} else {
						opErr = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(chIDs...).Build()).Error()
					}
					if opErr != nil {
						otherR.done(opErr)
						// Close conn, this should cause Receive to return with err below
						// and whole runPubSub method to restart.
						conn.Close()
						return
					}
					otherR.done(nil)
				}
			}
		}
	}()

	go func() {
		chIDs := make([]channelID, 1)
		chIDs[0] = b.pubSubShardChannelID(s, 0)

		for _, ch := range b.node.Hub().Channels() {
			if b.getShard(ch) == s {
				chIDs = append(chIDs, b.messageChannelID(s, ch))
			}
		}

		batch := make([]channelID, 0)

		for i, ch := range chIDs {
			if len(batch) > 0 && i%redisSubscribeBatchLimit == 0 {
				r := newSubRequest(batch, true)
				err := b.sendSubscribe(s, r)
				if err != nil {
					b.node.Log(NewLogEntry(LogLevelError, "error subscribing", map[string]interface{}{"error": err.Error()}))
					closeDoneOnce()
					return
				}
				batch = nil
			}
			batch = append(batch, ch)
		}
		if len(batch) > 0 {
			r := newSubRequest(batch, true)
			err := b.sendSubscribe(s, r)
			if err != nil {
				b.node.Log(NewLogEntry(LogLevelError, "error subscribing", map[string]interface{}{"error": err.Error()}))
				closeDoneOnce()
				return
			}
		}
	}()

	select {
	case err := <-wait:
		if err != nil {
			b.node.Log(NewLogEntry(LogLevelError, "pub/sub error", map[string]interface{}{"error": err.Error()}))
		}
	case <-s.closeCh:
	}
}

func (b *RedisBroker) sendSubscribe(s *RedisShard, r subRequest) error {
	select {
	case s.subCh <- r:
	default:
		timer := timers.AcquireTimer(s.readTimeout())
		defer timers.ReleaseTimer(timer)
		select {
		case s.subCh <- r:
		case <-b.closeCh:
			return errRedisClosed
		case <-timer.C:
			return errRedisOpTimeout
		}
	}
	return r.result()
}

func (b *RedisBroker) sendSubscribeSharded(s *RedisShard, r subRequest, shardIndex int) error {
	select {
	case s.shardedSubCh[shardIndex] <- r:
	default:
		timer := timers.AcquireTimer(s.readTimeout())
		defer timers.ReleaseTimer(timer)
		select {
		case s.shardedSubCh[shardIndex] <- r:
		case <-timer.C:
			return errRedisOpTimeout
		}
	}
	return r.result()
}

// Publish - see Broker.Publish.
func (b *RedisBroker) Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, error) {
	return b.publish(b.getShard(ch), ch, data, opts)
}

func (b *RedisBroker) publish(s *RedisShard, ch string, data []byte, opts PublishOptions) (StreamPosition, error) {
	protoPub := &protocol.Publication{
		Data: data,
		Info: infoToProto(opts.ClientInfo),
		Tags: opts.Tags,
	}
	byteMessage, err := protoPub.MarshalVT()
	if err != nil {
		return StreamPosition{}, err
	}

	publishChannel := b.messageChannelID(s, ch)
	useShardedPublish := b.useShardedPubSub(s)
	var publishCommand = "publish"
	if useShardedPublish {
		publishCommand = "spublish"
	}

	if opts.HistorySize <= 0 || opts.HistoryTTL <= 0 {
		var resp rueidis.RedisResult
		if useShardedPublish {
			cmd := s.client.B().Spublish().Channel(string(publishChannel)).Message(string(byteMessage)).Build()
			resp = s.client.Do(context.Background(), cmd)
		} else {
			cmd := s.client.B().Publish().Channel(string(publishChannel)).Message(string(byteMessage)).Build()
			resp = s.client.Do(context.Background(), cmd)
		}
		return StreamPosition{}, resp.Error()
	}

	historyMetaKey := b.historyMetaKey(s, ch)
	historyMetaTTLSeconds := int(b.config.HistoryMetaTTL.Seconds())

	var streamKey channelID
	var size int
	var script *rueidis.Lua
	if !b.config.UseLists {
		streamKey = b.historyStreamKey(s, ch)
		size = opts.HistorySize
		script = b.addHistoryStreamScript
	} else {
		streamKey = b.historyListKey(s, ch)
		size = opts.HistorySize - 1
		script = b.addHistoryListScript
	}

	replies, err := script.Exec(context.Background(), s.client, []string{string(streamKey), string(historyMetaKey)}, []string{string(byteMessage), strconv.Itoa(size), strconv.Itoa(int(opts.HistoryTTL.Seconds())), string(publishChannel), strconv.Itoa(historyMetaTTLSeconds), strconv.FormatInt(time.Now().Unix(), 10), publishCommand}).ToArray()
	if err != nil {
		return StreamPosition{}, err
	}
	if len(replies) != 2 {
		return StreamPosition{}, errors.New("wrong Redis reply")
	}
	offset, err := replies[0].AsInt64()
	if err != nil {
		return StreamPosition{}, errors.New("wrong Redis reply offset")
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return StreamPosition{}, errors.New("wrong Redis reply epoch")
	}
	return StreamPosition{Offset: uint64(offset), Epoch: epoch}, nil
}

// PublishJoin - see Broker.PublishJoin.
func (b *RedisBroker) PublishJoin(ch string, info *ClientInfo) error {
	return b.publishJoin(b.getShard(ch), ch, info)
}

func (b *RedisBroker) useShardedPubSub(s *RedisShard) bool {
	return s.useCluster && b.config.NumClusterShards > 0
}

func (b *RedisBroker) publishJoin(s *RedisShard, ch string, info *ClientInfo) error {
	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	chID := b.messageChannelID(s, ch)

	var resp rueidis.RedisResult
	if b.useShardedPubSub(s) {
		cmd := s.client.B().Spublish().Channel(string(chID)).Message(string(append(joinTypePrefix, byteMessage...))).Build()
		resp = s.client.Do(context.Background(), cmd)
	} else {
		cmd := s.client.B().Publish().Channel(string(chID)).Message(string(append(joinTypePrefix, byteMessage...))).Build()
		resp = s.client.Do(context.Background(), cmd)
	}
	return resp.Error()
}

// PublishLeave - see Broker.PublishLeave.
func (b *RedisBroker) PublishLeave(ch string, info *ClientInfo) error {
	return b.publishLeave(b.getShard(ch), ch, info)
}

func (b *RedisBroker) publishLeave(s *RedisShard, ch string, info *ClientInfo) error {
	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	chID := b.messageChannelID(s, ch)

	var resp rueidis.RedisResult
	if b.useShardedPubSub(s) {
		cmd := s.client.B().Spublish().Channel(string(chID)).Message(string(append(leaveTypePrefix, byteMessage...))).Build()
		resp = s.client.Do(context.Background(), cmd)
	} else {
		cmd := s.client.B().Publish().Channel(string(chID)).Message(string(append(leaveTypePrefix, byteMessage...))).Build()
		resp = s.client.Do(context.Background(), cmd)
	}
	return resp.Error()
}

// PublishControl - see Broker.PublishControl.
func (b *RedisBroker) PublishControl(data []byte, nodeID string, _ string) error {
	currentRound := atomic.AddUint64(&b.controlRound, 1)
	index := currentRound % uint64(len(b.shards))
	s := b.shards[index]
	return b.publishControl(s, data, nodeID)
}

func (b *RedisBroker) publishControl(s *RedisShard, data []byte, nodeID string) error {
	var chID channelID
	if nodeID == "" {
		chID = channelID(b.controlChannel)
	} else {
		chID = b.nodeChannelID(nodeID)
	}

	cmd := s.client.B().Publish().Channel(string(chID)).Message(string(data)).Build()
	resp := s.client.Do(context.Background(), cmd)
	return resp.Error()
}

// Subscribe - see Broker.Subscribe.
func (b *RedisBroker) Subscribe(ch string) error {
	return b.subscribe(b.getShard(ch), ch)
}

func (b *RedisBroker) subscribe(s *RedisShard, ch string) error {
	if b.node.LogEnabled(LogLevelDebug) {
		b.node.Log(NewLogEntry(LogLevelDebug, "subscribe node on channel", map[string]interface{}{"channel": ch}))
	}
	r := newSubRequest([]channelID{b.messageChannelID(s, ch)}, true)
	if b.useShardedPubSub(s) {
		return b.sendSubscribeSharded(s, r, consistentIndex(ch, b.config.NumClusterShards))
	}
	return b.sendSubscribe(s, r)
}

// Unsubscribe - see Broker.Unsubscribe.
func (b *RedisBroker) Unsubscribe(ch string) error {
	return b.unsubscribe(b.getShard(ch), ch)
}

func (b *RedisBroker) unsubscribe(s *RedisShard, ch string) error {
	if b.node.LogEnabled(LogLevelDebug) {
		b.node.Log(NewLogEntry(LogLevelDebug, "unsubscribe node from channel", map[string]interface{}{"channel": ch}))
	}
	r := newSubRequest([]channelID{b.messageChannelID(s, ch)}, false)
	if b.useShardedPubSub(s) {
		return b.sendSubscribeSharded(s, r, consistentIndex(ch, b.config.NumClusterShards))
	}
	return b.sendSubscribe(s, r)
}

// History - see Broker.History.
func (b *RedisBroker) History(ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	return b.history(b.getShard(ch), ch, filter)
}

func (b *RedisBroker) history(s *RedisShard, ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	if !b.config.UseLists {
		return b.historyStream(s, ch, filter)
	}
	return b.historyList(s, ch, filter)
}

// RemoveHistory - see Broker.RemoveHistory.
func (b *RedisBroker) RemoveHistory(ch string) error {
	return b.removeHistory(b.getShard(ch), ch)
}

func (b *RedisBroker) removeHistory(s *RedisShard, ch string) error {
	var key channelID
	if !b.config.UseLists {
		key = b.historyStreamKey(s, ch)
	} else {
		key = b.historyListKey(s, ch)
	}
	cmd := s.client.B().Del().Key(string(key)).Build()
	resp := s.client.Do(context.Background(), cmd)
	return resp.Error()
}

func (b *RedisBroker) messageChannelID(s *RedisShard, ch string) channelID {
	if b.useShardedPubSub(s) {
		ch = "{" + strconv.Itoa(consistentIndex(ch, b.config.NumClusterShards)) + "}." + ch
	}
	return channelID(b.messagePrefix + ch)
}

func (b *RedisBroker) pubSubShardChannelID(s *RedisShard, shardIndex int) channelID {
	if b.useShardedPubSub(s) {
		return channelID(b.shardChannel + ".{" + strconv.Itoa(shardIndex) + "}")
	}
	return channelID(b.shardChannel)
}

func (b *RedisBroker) nodeChannelID(nodeID string) channelID {
	return channelID(b.config.Prefix + redisNodeChannelPrefix + nodeID)
}

func (b *RedisBroker) historyListKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		if b.config.NumClusterShards > 0 {
			ch = "{" + strconv.Itoa(consistentIndex(ch, b.config.NumClusterShards)) + "}." + ch
		} else {
			ch = "{" + ch + "}"
		}
	}
	return channelID(b.config.Prefix + ".list." + ch)
}

func (b *RedisBroker) historyStreamKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		if b.config.NumClusterShards > 0 {
			ch = "{" + strconv.Itoa(consistentIndex(ch, b.config.NumClusterShards)) + "}." + ch
		} else {
			ch = "{" + ch + "}"
		}
	}
	return channelID(b.config.Prefix + ".stream." + ch)
}

func (b *RedisBroker) historyMetaKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		if b.config.NumClusterShards > 0 {
			ch = "{" + strconv.Itoa(consistentIndex(ch, b.config.NumClusterShards)) + "}." + ch
		} else {
			ch = "{" + ch + "}"
		}
	}
	if !b.config.UseLists {
		return channelID(b.config.Prefix + ".stream.meta." + ch)
	}
	return channelID(b.config.Prefix + ".list.meta." + ch)
}

func (b *RedisBroker) extractChannel(chID channelID) string {
	ch := strings.TrimPrefix(string(chID), b.messagePrefix)
	if b.config.NumClusterShards == 0 {
		return ch
	}
	if strings.HasPrefix(ch, "{") {
		i := strings.Index(ch, ".")
		return ch[i+1:]
	}
	return ch
}

// Define prefixes to distinguish Join and Leave messages coming from PUB/SUB.
var (
	joinTypePrefix  = []byte("__j__")
	leaveTypePrefix = []byte("__l__")
)

func (b *RedisBroker) handleRedisClientMessage(eventHandler BrokerEventHandler, chID channelID, data []byte) error {
	pushData, pushType, sp, ok := extractPushData(data)
	if !ok {
		return fmt.Errorf("malformed PUB/SUB data: %s", data)
	}
	channel := b.extractChannel(chID)
	if pushType == pubPushType {
		var pub protocol.Publication
		err := pub.UnmarshalVT(pushData)
		if err != nil {
			return err
		}
		if pub.Offset == 0 {
			// When adding to history and publishing happens atomically in Broker
			// position info is prepended to Publication payload. In this case we should attach
			// it to unmarshalled Publication.
			pub.Offset = sp.Offset
		}
		_ = eventHandler.HandlePublication(channel, pubFromProto(&pub), sp)
	} else if pushType == joinPushType {
		var info protocol.ClientInfo
		err := info.UnmarshalVT(pushData)
		if err != nil {
			return err
		}
		_ = eventHandler.HandleJoin(channel, infoFromProto(&info))
	} else if pushType == leavePushType {
		var info protocol.ClientInfo
		err := info.UnmarshalVT(pushData)
		if err != nil {
			return err
		}
		_ = eventHandler.HandleLeave(channel, infoFromProto(&info))
	}
	return nil
}

// subRequest is an internal request to subscribe or unsubscribe from one or more channels.
type subRequest struct {
	channels  []channelID
	subscribe bool
	err       chan error
}

// newSubRequest creates a new request to subscribe or unsubscribe form a channel.
func newSubRequest(chIDs []channelID, subscribe bool) subRequest {
	return subRequest{
		channels:  chIDs,
		subscribe: subscribe,
		err:       make(chan error, 1),
	}
}

// done should only be called once for subRequest.
func (sr *subRequest) done(err error) {
	sr.err <- err
}

func (sr *subRequest) result() error {
	return <-sr.err
}

func (b *RedisBroker) historyStream(s *RedisShard, ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	historyKey := b.historyStreamKey(s, ch)
	historyMetaKey := b.historyMetaKey(s, ch)

	var includePubs = "1"
	var offset uint64
	if filter.Since != nil {
		if filter.Reverse {
			offset = filter.Since.Offset - 1
			if offset == 0 {
				includePubs = "0"
			}
		} else {
			offset = filter.Since.Offset + 1
		}
	}
	var limit int
	if filter.Limit == 0 {
		includePubs = "0"
	}
	if filter.Limit > 0 {
		limit = filter.Limit
	}

	reverse := "0"
	if filter.Reverse {
		reverse = "1"
	}

	historyMetaTTLSeconds := int(b.config.HistoryMetaTTL.Seconds())

	replies, err := b.historyStreamScript.Exec(context.Background(), s.client, []string{string(historyKey), string(historyMetaKey)}, []string{includePubs, strconv.FormatUint(offset, 10), strconv.Itoa(limit), reverse, strconv.Itoa(historyMetaTTLSeconds), strconv.FormatInt(time.Now().Unix(), 10)}).ToArray()
	if err != nil {
		return nil, StreamPosition{}, err
	}
	if len(replies) < 2 {
		return nil, StreamPosition{}, fmt.Errorf("wrong Redis reply number: %d", len(replies))
	}
	var offs int64
	offs, err = replies[0].AsInt64()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			offs = 0
		} else {
			return nil, StreamPosition{}, fmt.Errorf("wrong Redis reply offset: %w", err)
		}
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return nil, StreamPosition{}, errors.New("wrong Redis reply epoch")
	}

	if includePubs == "1" && len(replies) == 3 {
		values, err := replies[2].ToArray()
		if err != nil {
			return nil, StreamPosition{}, err
		}
		publications := make([]*Publication, 0, len(values))
		for _, v := range values {
			values, err := v.ToArray()
			if err != nil {
				return nil, StreamPosition{}, err
			}
			if len(values) != 2 {
				return nil, StreamPosition{}, fmt.Errorf("got %d, wanted 2", len(values))
			}
			id, err := values[0].ToString()
			if err != nil {
				return nil, StreamPosition{}, err
			}
			fieldValues, err := values[1].AsStrMap()
			if err != nil {
				return nil, StreamPosition{}, err
			}
			hyphenPos := strings.Index(id, "-") // ex. "4-0", 4 is our offset.
			if hyphenPos <= 0 {
				return nil, StreamPosition{}, fmt.Errorf("unexpected offset format: %s", id)
			}
			offset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
			if err != nil {
				return nil, StreamPosition{}, err
			}
			pushData, ok := fieldValues["d"]
			if !ok {
				return nil, StreamPosition{}, errors.New("no element data")
			}
			var pub protocol.Publication
			err = pub.UnmarshalVT([]byte(pushData))
			if err != nil {
				return nil, StreamPosition{}, fmt.Errorf("can not unmarshal value to Publication: %v", err)
			}
			pub.Offset = offset
			publications = append(publications, pubFromProto(&pub))
		}
		return publications, StreamPosition{Offset: uint64(offs), Epoch: epoch}, nil
	}
	return nil, StreamPosition{Offset: uint64(offs), Epoch: epoch}, nil
}

func (b *RedisBroker) historyList(s *RedisShard, ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	historyKey := b.historyListKey(s, ch)
	historyMetaKey := b.historyMetaKey(s, ch)

	var includePubs = "1"
	var rightBound = -1
	if filter.Limit == 0 {
		rightBound = 0
		includePubs = "0"
	}

	historyMetaTTLSeconds := int(b.config.HistoryMetaTTL.Seconds())

	replies, err := b.historyListScript.Exec(context.Background(), s.client, []string{string(historyKey), string(historyMetaKey)}, []string{includePubs, strconv.FormatInt(int64(rightBound), 10), strconv.Itoa(historyMetaTTLSeconds), strconv.FormatInt(time.Now().Unix(), 10)}).ToArray()
	if err != nil {
		return nil, StreamPosition{}, err
	}
	if len(replies) < 2 {
		return nil, StreamPosition{}, fmt.Errorf("wrong Redis reply number: %d", len(replies))
	}
	var offs int64
	offs, err = replies[0].AsInt64()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			offs = 0
		} else {
			return nil, StreamPosition{}, fmt.Errorf("wrong Redis reply offset: %w", err)
		}
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return nil, StreamPosition{}, errors.New("wrong Redis reply epoch")
	}

	latestPosition := StreamPosition{Offset: uint64(offs), Epoch: epoch}

	if includePubs == "0" || len(replies) == 2 {
		return nil, latestPosition, nil
	}

	values, err := replies[2].ToArray()
	if err != nil {
		return nil, StreamPosition{}, err
	}
	publications := make([]*Publication, 0, len(values)/2)

	for i := len(values) - 1; i >= 0; i-- {
		value, err := values[i].ToString()
		if err != nil {
			return nil, StreamPosition{}, errors.New("error getting value")
		}

		pushData, _, sp, ok := extractPushData([]byte(value))
		if !ok {
			return nil, StreamPosition{}, fmt.Errorf("malformed publication value: %s", value)
		}

		var pub protocol.Publication
		err = pub.UnmarshalVT(pushData)
		if err != nil {
			return nil, StreamPosition{}, fmt.Errorf("can not unmarshal value to Pub: %v", err)
		}
		pub.Offset = sp.Offset
		publications = append(publications, pubFromProto(&pub))
	}

	since := filter.Since
	if since == nil {
		if filter.Limit >= 0 && len(publications) >= filter.Limit {
			return publications[:filter.Limit], latestPosition, nil
		}
		return publications, latestPosition, nil
	}

	if latestPosition.Offset == since.Offset && since.Epoch == latestPosition.Epoch {
		return nil, latestPosition, nil
	}

	if latestPosition.Offset < since.Offset {
		return nil, latestPosition, nil
	}

	nextOffset := since.Offset + 1

	position := -1

	for i := 0; i < len(publications); i++ {
		pub := publications[i]
		if pub.Offset == since.Offset {
			position = i + 1
			break
		}
		if pub.Offset == nextOffset {
			position = i
			break
		}
	}

	if position > -1 {
		pubs := publications[position:]
		if filter.Limit >= 0 {
			limit := filter.Limit
			if limit > len(pubs) {
				limit = len(pubs)
			}
			return pubs[:limit], latestPosition, nil
		}
		return pubs, latestPosition, nil
	}

	if filter.Limit >= 0 {
		limit := filter.Limit
		if limit > len(publications) {
			limit = len(publications)
		}
		return publications[:limit], latestPosition, nil
	}
	return publications, latestPosition, nil
}

type pushType int

const (
	pubPushType   pushType = 0
	joinPushType  pushType = 1
	leavePushType pushType = 2
)

var (
	metaSep    = []byte("__")
	contentSep = ":"
)

// See tests for supported format examples.
func extractPushData(data []byte) ([]byte, pushType, StreamPosition, bool) {
	var offset uint64
	var epoch string
	if !bytes.HasPrefix(data, metaSep) {
		return data, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, true
	}
	nextMetaSepPos := bytes.Index(data[len(metaSep):], metaSep)
	if nextMetaSepPos <= 0 {
		return data, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false
	}
	content := data[len(metaSep) : len(metaSep)+nextMetaSepPos]
	contentType := content[0]

	rest := data[len(metaSep)+nextMetaSepPos+len(metaSep):]

	switch contentType {
	case 'j':
		return rest, joinPushType, StreamPosition{}, true
	case 'l':
		return rest, leavePushType, StreamPosition{}, true
	}

	stringContent := string(content)

	if contentType == 'p' {
		// new format p1:offset:epoch
		stringContent = stringContent[3:] // offset:epoch
		epochDelimiterPos := strings.Index(stringContent, contentSep)
		if epochDelimiterPos <= 0 {
			return rest, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false
		}
		var err error
		offset, err = strconv.ParseUint(stringContent[:epochDelimiterPos], 10, 64)
		epoch = stringContent[epochDelimiterPos+1:]
		return rest, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, err == nil
	}

	// old format with offset only: __offset__
	var err error
	offset, err = strconv.ParseUint(stringContent, 10, 64)
	return rest, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, err == nil
}
