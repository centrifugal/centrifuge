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
	"github.com/centrifugal/centrifuge/internal/util"

	"github.com/centrifugal/protocol"
	"github.com/go-redis/redis/v9"
)

const (
	// redisPubSubWorkerChannelSize sets buffer size of channel to which we send all
	// messages received from Redis PUB/SUB connection to process in separate goroutine.
	redisPubSubWorkerChannelSize = 512
	// redisSubscribeBatchLimit is a maximum number of channels to include in a single
	// batch subscribe call.
	redisSubscribeBatchLimit = 512
	// redisPublishBatchLimit is a maximum limit of publish requests one batched publish
	// operation can contain.
	redisPublishBatchLimit = 512
	// redisControlChannelSuffix is a suffix for control channel.
	redisControlChannelSuffix = ".control"
	// redisNodeChannelPrefix is a suffix for node channel.
	redisNodeChannelPrefix = ".node."
	// redisPingChannelSuffix is a suffix for ping channel.
	redisPingChannelSuffix = ".ping"
	// redisClientChannelPrefix is a prefix before channel name for client messages.
	redisClientChannelPrefix = ".client."
)

var _ Broker = (*RedisBroker)(nil)

// RedisBroker uses Redis to implement Broker functionality. This broker allows
// scaling Centrifuge-based server to many instances and load balance client
// connections between them.
// RedisBroker additionally supports Redis Sentinel, client-side consistent sharding
// and can work with Redis Cluster (including client-side sharding between different
// Redis Clusters to scale PUB/SUB).
// By default, Redis >= 5 required (due to the fact RedisBroker uses STREAM data structure).
type RedisBroker struct {
	controlRound           uint64 // Keep atomic on struct top for 32-bit architectures.
	node                   *Node
	sharding               bool
	config                 RedisBrokerConfig
	shards                 []*RedisShard
	historyListScript      *redis.Script
	historyStreamScript    *redis.Script
	addHistoryListScript   *redis.Script
	addHistoryStreamScript *redis.Script
	messagePrefix          string
	pingChannel            string
	controlChannel         string
	nodeChannel            string
	closeOnce              sync.Once
	closeCh                chan struct{}
}

// DefaultRedisBrokerPrefix is a default value for RedisBrokerConfig.Prefix.
const DefaultRedisBrokerPrefix = "centrifuge"

// RedisBrokerConfig is a config for RedisBroker.
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
		historyListScript:      redis.NewScript(historyListSource),
		historyStreamScript:    redis.NewScript(historyStreamSource),
		addHistoryListScript:   redis.NewScript(addHistorySource),
		addHistoryStreamScript: redis.NewScript(addHistoryStreamSource),
		closeCh:                make(chan struct{}),
	}

	for i := range config.Shards {
		config.Shards[i].registerScripts(
			b.historyListScript,
			b.historyStreamScript,
			b.addHistoryListScript,
			b.addHistoryStreamScript,
		)
	}

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
	addHistorySource = `
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
	redis.call("publish", ARGV[4], payload)
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
	redis.call("publish", ARGV[4], payload)
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
	for _, shard := range b.shards {
		err := b.runShard(shard, h)
		if err != nil {
			return err
		}
		if err := b.checkCapabilities(shard); err != nil {
			return fmt.Errorf("capability error on %s: %v", shard.string(), err)
		}
	}
	return nil
}

func (b *RedisBroker) Close(_ context.Context) error {
	b.closeOnce.Do(func() {
		close(b.closeCh)
	})
	return nil
}

// runForever keeps another function running indefinitely.
// The reason this loop is not inside the function itself is
// so that defer can be used to cleanup nicely.
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

func (b *RedisBroker) checkCapabilities(shard *RedisShard) error {
	if b.config.UseLists {
		return nil
	}
	// Check whether Redis Streams supported.
	dr := shard.newDataRequest(nil, nil, []interface{}{"XRANGE", "_", "0-0", "0-0"})
	resp := shard.getDataResponse(dr, b.closeCh)
	if resp.err != nil {
		if strings.Contains(resp.err.Error(), "ERR unknown command") {
			return errors.New("STREAM only available since Redis >= 5, consider upgrading Redis or using LIST structure for history")
		}
		return resp.err
	}
	return nil
}

func (b *RedisBroker) runShard(shard *RedisShard, h BrokerEventHandler) error {
	go b.runForever(func() {
		b.runPublishPipeline(shard)
	})
	go b.runForever(func() {
		b.runPubSubPing(shard)
	})
	go b.runForever(func() {
		b.runPubSub(shard, h)
	})
	go b.runForever(func() {
		b.runControlPubSub(shard, h)
	})
	return nil
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

	publishChannel := b.messageChannelID(ch)

	if opts.HistorySize <= 0 || opts.HistoryTTL <= 0 {
		// Fast path – publish without history.
		eChan := make(chan error, 1)

		pr := pubRequest{
			channel: publishChannel,
			message: byteMessage,
			err:     eChan,
		}
		select {
		case s.pubCh <- pr:
		default:
			timer := timers.AcquireTimer(s.readTimeout())
			defer timers.ReleaseTimer(timer)
			select {
			case s.pubCh <- pr:
			case <-b.closeCh:
				return StreamPosition{}, errRedisClosed
			case <-timer.C:
				return StreamPosition{}, errRedisOpTimeout
			}
		}
		select {
		case err := <-eChan:
			return StreamPosition{}, err
		case <-b.closeCh:
			return StreamPosition{}, errRedisClosed
		}
	}

	historyMetaKey := b.historyMetaKey(s, ch)
	historyMetaTTLSeconds := int(b.config.HistoryMetaTTL.Seconds())

	var streamKey channelID
	var size int
	var script *redis.Script
	if !b.config.UseLists {
		streamKey = b.historyStreamKey(s, ch)
		size = opts.HistorySize
		script = b.addHistoryStreamScript
	} else {
		streamKey = b.historyListKey(s, ch)
		size = opts.HistorySize - 1
		script = b.addHistoryListScript
	}
	dr := s.newDataRequest(script, []string{string(streamKey), string(historyMetaKey)}, []interface{}{byteMessage, size, int(opts.HistoryTTL.Seconds()), string(publishChannel), historyMetaTTLSeconds, time.Now().Unix()})
	resp := s.getDataResponse(dr, b.closeCh)
	if resp.err != nil {
		return StreamPosition{}, resp.err
	}
	replies, ok := resp.reply.([]interface{})
	if !ok || len(replies) != 2 {
		return StreamPosition{}, errors.New("wrong Redis reply")
	}
	r := redis.NewCmdResult(replies[0], nil)
	offset, err := r.Uint64()
	if err != nil {
		return StreamPosition{}, errors.New("wrong Redis reply offset")
	}
	r.SetVal(replies[1])
	epoch, err := r.Text()
	if err != nil {
		return StreamPosition{}, errors.New("wrong Redis reply epoch")
	}
	return StreamPosition{Offset: offset, Epoch: epoch}, nil
}

// PublishJoin - see Broker.PublishJoin.
func (b *RedisBroker) PublishJoin(ch string, info *ClientInfo) error {
	return b.publishJoin(b.getShard(ch), ch, info)
}

func (b *RedisBroker) publishJoin(s *RedisShard, ch string, info *ClientInfo) error {
	eChan := make(chan error, 1)

	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	chID := b.messageChannelID(ch)

	pr := pubRequest{
		channel: chID,
		message: append(joinTypePrefix, byteMessage...),
		err:     eChan,
	}
	select {
	case s.pubCh <- pr:
	default:
		timer := timers.AcquireTimer(s.readTimeout())
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubCh <- pr:
		case <-b.closeCh:
			return errRedisClosed
		case <-timer.C:
			return errRedisOpTimeout
		}
	}
	select {
	case err := <-eChan:
		return err
	case <-b.closeCh:
		return errRedisClosed
	}
}

// PublishLeave - see Broker.PublishLeave.
func (b *RedisBroker) PublishLeave(ch string, info *ClientInfo) error {
	return b.publishLeave(b.getShard(ch), ch, info)
}

func (b *RedisBroker) publishLeave(s *RedisShard, ch string, info *ClientInfo) error {
	eChan := make(chan error, 1)

	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	chID := b.messageChannelID(ch)

	pr := pubRequest{
		channel: chID,
		message: append(leaveTypePrefix, byteMessage...),
		err:     eChan,
	}
	select {
	case s.pubCh <- pr:
	default:
		timer := timers.AcquireTimer(s.readTimeout())
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubCh <- pr:
		case <-b.closeCh:
			return errRedisClosed
		case <-timer.C:
			return errRedisOpTimeout
		}
	}
	select {
	case err := <-eChan:
		return err
	case <-b.closeCh:
		return errRedisClosed
	}
}

// PublishControl - see Broker.PublishControl.
func (b *RedisBroker) PublishControl(data []byte, nodeID, _ string) error {
	currentRound := atomic.AddUint64(&b.controlRound, 1)
	index := currentRound % uint64(len(b.shards))
	s := b.shards[index]
	return b.publishControl(s, data, nodeID)
}

func (b *RedisBroker) publishControl(s *RedisShard, data []byte, nodeID string) error {
	eChan := make(chan error, 1)

	var chID channelID
	if nodeID == "" {
		chID = channelID(b.controlChannel)
	} else {
		chID = b.nodeChannelID(nodeID)
	}

	pr := pubRequest{
		channel: chID,
		message: data,
		err:     eChan,
	}
	select {
	case s.pubCh <- pr:
	default:
		timer := timers.AcquireTimer(s.readTimeout())
		defer timers.ReleaseTimer(timer)
		select {
		case s.pubCh <- pr:
		case <-timer.C:
			return errRedisOpTimeout
		}
	}
	select {
	case err := <-eChan:
		return err
	case <-b.closeCh:
		return errRedisClosed
	}
}

// Subscribe - see Broker.Subscribe.
func (b *RedisBroker) Subscribe(ch string) error {
	return b.subscribe(b.getShard(ch), ch)
}

func (b *RedisBroker) subscribe(s *RedisShard, ch string) error {
	if b.node.LogEnabled(LogLevelDebug) {
		b.node.Log(NewLogEntry(LogLevelDebug, "subscribe node on channel", map[string]interface{}{"channel": ch}))
	}
	r := newSubRequest([]channelID{b.messageChannelID(ch)}, true)
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
	r := newSubRequest([]channelID{b.messageChannelID(ch)}, false)
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
	dr := s.newDataRequest(nil, nil, []interface{}{"DEL", string(key)})
	resp := s.getDataResponse(dr, b.closeCh)

	return resp.err
}

func (b *RedisBroker) messageChannelID(ch string) channelID {
	return channelID(b.messagePrefix + ch)
}

func (b *RedisBroker) nodeChannelID(nodeID string) channelID {
	return channelID(b.config.Prefix + redisNodeChannelPrefix + nodeID)
}

func (b *RedisBroker) historyListKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		ch = "{" + ch + "}"
	}
	return channelID(b.config.Prefix + ".list." + ch)
}

func (b *RedisBroker) historyStreamKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		ch = "{" + ch + "}"
	}
	return channelID(b.config.Prefix + ".stream." + ch)
}

func (b *RedisBroker) historyMetaKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		ch = "{" + ch + "}"
	}
	if !b.config.UseLists {
		return channelID(b.config.Prefix + ".stream.meta." + ch)
	}
	return channelID(b.config.Prefix + ".list.meta." + ch)
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

	pubsub := s.client.Subscribe(context.Background())

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDoneOnce := func() {
		doneOnce.Do(func() {
			close(done)
			_ = pubsub.Close()
		})
	}
	defer closeDoneOnce()

	err := pubsub.Subscribe(context.Background(), b.pingChannel)
	if err != nil {
		b.node.Log(NewLogEntry(LogLevelError, "ping channel subscribe error", map[string]interface{}{"error": err.Error()}))
		closeDoneOnce()
		return
	}

	// Run subscriber goroutine.
	go func() {
		b.node.Log(NewLogEntry(LogLevelDebug, "starting RedisBroker Subscriber", map[string]interface{}{"shard": s.string()}))
		defer func() {
			b.node.Log(NewLogEntry(LogLevelDebug, "stopping RedisBroker Subscriber", map[string]interface{}{"shard": s.string()}))
		}()
		for {
			select {
			case <-b.closeCh:
				closeDoneOnce()
				return
			case <-done:
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
					opErr = pubsub.Subscribe(context.Background(), chIDs...)
				} else {
					opErr = pubsub.Unsubscribe(context.Background(), chIDs...)
				}

				if opErr != nil {
					for _, r := range channelBatch {
						r.done(opErr)
					}
					if otherR != nil {
						otherR.done(opErr)
					}
					closeDoneOnce()
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
						opErr = pubsub.Subscribe(context.Background(), chIDs...)
					} else {
						opErr = pubsub.Unsubscribe(context.Background(), chIDs...)
					}
					if opErr != nil {
						otherR.done(opErr)
						closeDoneOnce()
						return
					}
					otherR.done(nil)
				}
			}
		}
	}()

	// Run workers to spread received message processing work over worker goroutines.
	workers := make(map[int]chan *redis.Message)
	for i := 0; i < numWorkers; i++ {
		workerCh := make(chan *redis.Message, redisPubSubWorkerChannelSize)
		workers[i] = workerCh
		go func(ch chan *redis.Message) {
			for {
				select {
				case <-done:
					return
				case n := <-ch:
					switch n.Channel {
					case b.pingChannel:
						// Do nothing - this message just maintains connection open.
					default:
						err := b.handleRedisClientMessage(eventHandler, channelID(n.Channel), util.StringToBytes(n.Payload))
						if err != nil {
							b.node.Log(NewLogEntry(LogLevelError, "error handling client message", map[string]interface{}{"error": err.Error()}))
							continue
						}
					}
				}
			}
		}(workerCh)
	}

	go func() {
		channels := b.node.Hub().Channels()
		chIDs := make([]channelID, 0, len(channels)/len(b.shards))
		for _, ch := range channels {
			if b.getShard(ch) == s {
				chIDs = append(chIDs, b.messageChannelID(ch))
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

	for {
		m, err := pubsub.ReceiveTimeout(context.Background(), 10*time.Second)
		if err != nil {
			b.node.Log(NewLogEntry(LogLevelError, "Redis PUB/SUB error", map[string]interface{}{"error": err.Error()}))
			s.reloadPipeline()
			return
		}
		switch m := m.(type) {
		case *redis.Message:
			// Add message to worker channel preserving message order - i.b. messages
			// from the same channel will be processed in the same worker.
			select {
			case workers[index(m.Channel, numWorkers)] <- m:
			case <-done:
				return
			}
		default:
		}
	}
}

func (b *RedisBroker) runControlPubSub(s *RedisShard, eventHandler BrokerEventHandler) {
	numWorkers := runtime.NumCPU()

	b.node.Log(NewLogEntry(LogLevelDebug, fmt.Sprintf("running Redis control PUB/SUB, num workers: %d", numWorkers), map[string]interface{}{"shard": s.string()}))
	defer func() {
		b.node.Log(NewLogEntry(LogLevelDebug, "stopping Redis control PUB/SUB", map[string]interface{}{"shard": s.string()}))
	}()

	pubsub := s.client.Subscribe(context.Background())

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDoneOnce := func() {
		doneOnce.Do(func() {
			close(done)
			_ = pubsub.Close()
		})
	}
	defer closeDoneOnce()

	controlChannel := b.controlChannel
	nodeChannel := b.nodeChannel
	pingChannel := b.pingChannel

	// Run workers to spread message processing work over worker goroutines.
	workCh := make(chan *redis.Message)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				case n := <-workCh:
					switch n.Channel {
					case pingChannel:
						// Do nothing - this message just maintains connection open.
					default:
						err := eventHandler.HandleControl(util.StringToBytes(n.Payload))
						if err != nil {
							b.node.Log(NewLogEntry(LogLevelError, "error handling control message", map[string]interface{}{"error": err.Error()}))
							continue
						}
					}
				}
			}
		}()
	}

	err := pubsub.Subscribe(context.Background(), controlChannel, nodeChannel, pingChannel)
	if err != nil {
		b.node.Log(NewLogEntry(LogLevelError, "control channel subscribe error", map[string]interface{}{"error": err.Error()}))
		closeDoneOnce()
		return
	}
	go func() {
		select {
		case <-b.closeCh:
			closeDoneOnce()
		case <-done:
			return
		}
	}()

	for {
		m, err := pubsub.ReceiveTimeout(context.Background(), 10*time.Second)
		if err != nil {
			b.node.Log(NewLogEntry(LogLevelError, "Redis control PUB/SUB error", map[string]interface{}{"error": err}))
			return
		}
		switch m := m.(type) {
		case *redis.Message:
			// Add message to worker channel preserving message order - i.b. messages
			// from the same channel will be processed in the same worker.
			select {
			case workCh <- m:
			case <-done:
				return
			}
		default:
		}
	}
}

func (b *RedisBroker) extractChannel(chID channelID) string {
	return strings.TrimPrefix(string(chID), b.messagePrefix)
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
			// When adding to history and publishing happens atomically in RedisBroker
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

func (b *RedisBroker) runPubSubPing(s *RedisShard) {
	pingTicker := time.NewTicker(time.Second)
	defer pingTicker.Stop()
	client := s.client
	for {
		select {
		case <-b.closeCh:
			return
		case <-pingTicker.C:
			// Publish periodically to maintain PUB/SUB connection alive and allow
			// PUB/SUB connection to close early if no data received for a period of time.
			cmd := client.Publish(context.Background(), b.pingChannel, nil)
			if cmd.Err() != nil {
				b.node.Log(NewLogEntry(LogLevelError, "error publish ping to Redis channel", map[string]interface{}{"error": cmd.Err().Error()}))
				return
			}
		}
	}
}

func (b *RedisBroker) runPublishPipeline(s *RedisShard) {
	var prs []pubRequest
	pipe := s.client.Pipeline()

	for {
		select {
		case <-b.closeCh:
			return
		case pr := <-s.pubCh:
			prs = append(prs, pr)
		loop:
			for len(prs) < redisPublishBatchLimit {
				select {
				case pr := <-s.pubCh:
					prs = append(prs, pr)
				default:
					break loop
				}
			}
		}
		for _, pr := range prs {
			_ = pipe.Publish(context.Background(), string(pr.channel), pr.message)
		}
		cmds, _ := pipe.Exec(context.Background())
		for i := range prs {
			prs[i].done(cmds[i].Err())
		}
		prs = nil
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

func extractHistoryResponse(reply interface{}, useStreams bool, includePubs bool) (StreamPosition, []*Publication, error) {
	results := reply.([]interface{})

	r := redis.NewCmdResult(results[0], nil)
	offset, err := r.Uint64()
	if err != nil {
		if r.Val() != nil {
			return StreamPosition{}, nil, err
		}
		offset = 0
	}

	r.SetVal(results[1])
	epoch, err := r.Text()
	if err != nil {
		return StreamPosition{}, nil, err
	}

	streamPosition := StreamPosition{Offset: offset, Epoch: epoch}

	if includePubs {
		var publications []*Publication
		if useStreams {
			publications, err = sliceOfPubsStream(results[2], nil)
		} else {
			publications, err = sliceOfPubsList(results[2], nil)
		}
		if err != nil {
			return StreamPosition{}, nil, err
		}
		return streamPosition, publications, nil
	}

	return streamPosition, nil, nil
}

func (b *RedisBroker) historyStream(s *RedisShard, ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	historyKey := b.historyStreamKey(s, ch)
	historyMetaKey := b.historyMetaKey(s, ch)

	var includePubs = true
	var offset uint64
	if filter.Since != nil {
		if filter.Reverse {
			offset = filter.Since.Offset - 1
			if offset == 0 {
				includePubs = false
			}
		} else {
			offset = filter.Since.Offset + 1
		}
	}
	var limit int
	if filter.Limit == 0 {
		includePubs = false
	}
	if filter.Limit > 0 {
		limit = filter.Limit
	}

	historyMetaTTLSeconds := int(b.config.HistoryMetaTTL.Seconds())

	dr := s.newDataRequest(b.historyStreamScript, []string{string(historyKey), string(historyMetaKey)}, []interface{}{includePubs, offset, limit, filter.Reverse, historyMetaTTLSeconds, time.Now().Unix()})
	resp := s.getDataResponse(dr, b.closeCh)
	if resp.err != nil {
		return nil, StreamPosition{}, resp.err
	}

	latestPosition, publications, err := extractHistoryResponse(resp.reply, !b.config.UseLists, includePubs)
	if err != nil {
		return nil, StreamPosition{}, err
	}

	return publications, latestPosition, nil
}

func (b *RedisBroker) historyList(s *RedisShard, ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	historyKey := b.historyListKey(s, ch)
	historyMetaKey := b.historyMetaKey(s, ch)

	var includePubs = true
	var rightBound = -1
	if filter.Limit == 0 {
		rightBound = 0
		includePubs = false
	}

	historyMetaTTLSeconds := int(b.config.HistoryMetaTTL.Seconds())

	dr := s.newDataRequest(b.historyListScript, []string{string(historyKey), string(historyMetaKey)}, []interface{}{includePubs, rightBound, historyMetaTTLSeconds, time.Now().Unix()})
	resp := s.getDataResponse(dr, b.closeCh)
	if resp.err != nil {
		return nil, StreamPosition{}, resp.err
	}

	latestPosition, publications, err := extractHistoryResponse(resp.reply, !b.config.UseLists, includePubs)
	if err != nil {
		return nil, StreamPosition{}, err
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

	stringContent := util.BytesToString(content)

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

func sliceOfPubsStream(result interface{}, err error) ([]*Publication, error) {
	values, err := redis.NewCmdResult(result, err).Slice()
	if err != nil {
		return nil, err
	}
	pubs := make([]*Publication, 0, len(values))

	for i := 0; i < len(values); i++ {
		r := redis.NewCmdResult(values[i], nil)
		streamElementValues, err := r.Slice()
		if err != nil {
			return nil, err
		}

		if len(streamElementValues) != 2 {
			return nil, errors.New("malformed reply: number of streamElementValues is not 2")
		}

		r.SetVal(streamElementValues[0])
		offsetStr, err := r.Text()
		if err != nil {
			return nil, err
		}
		hyphenPos := strings.Index(offsetStr, "-") // ex. "4-0", 4 is our offset.
		if hyphenPos <= 0 {
			return nil, fmt.Errorf("unexpected offset format: %s", offsetStr)
		}
		offset, err := strconv.ParseUint(offsetStr[:hyphenPos], 10, 64)
		if err != nil {
			return nil, err
		}

		r.SetVal(streamElementValues[1])
		payloadElementValues, err := r.Slice()
		if err != nil {
			return nil, err
		}

		if len(payloadElementValues) < 2 {
			return nil, errors.New("malformed reply: number of payloadElementValues less than 2")
		}

		pushData, ok := payloadElementValues[1].(string)
		if !ok {
			return nil, errors.New("error getting string push data")
		}

		var pub protocol.Publication
		err = pub.UnmarshalVT(util.StringToBytes(pushData))
		if err != nil {
			return nil, fmt.Errorf("can not unmarshal value to Publication: %v", err)
		}
		pub.Offset = offset
		pubs = append(pubs, pubFromProto(&pub))
	}
	return pubs, nil
}

func sliceOfPubsList(result interface{}, err error) ([]*Publication, error) {
	values, err := redis.NewCmdResult(result, err).Slice()
	if err != nil {
		return nil, err
	}
	pubs := make([]*Publication, 0, len(values))

	for i := len(values) - 1; i >= 0; i-- {
		value, okValue := values[i].(string)
		if !okValue {
			return nil, errors.New("error getting string value")
		}

		pushData, _, sp, ok := extractPushData(util.StringToBytes(value))
		if !ok {
			return nil, fmt.Errorf("malformed publication value: %s", value)
		}

		var pub protocol.Publication
		err = pub.UnmarshalVT(pushData)
		if err != nil {
			return nil, fmt.Errorf("can not unmarshal value to Pub: %v", err)
		}
		pub.Offset = sp.Offset
		pubs = append(pubs, pubFromProto(&pub))
	}
	return pubs, nil
}
