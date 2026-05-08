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

	_ "embed"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/centrifuge/internal/epoch"

	"github.com/centrifugal/protocol"
	"github.com/redis/rueidis"
)

var (
	errPubSubConnUnavailable = errors.New("redis: pub/sub connection temporary unavailable")
)

const (
	// redisSubscribeBatchLimit is a maximum number of channels to include in a single
	// batch subscribe call.
	redisSubscribeBatchLimit = 512
	// redisControlChannelSuffix is a suffix for control channel.
	redisControlChannelSuffix = ".control"
	// redisNodeChannelPrefix is a suffix for node channel.
	redisNodeChannelPrefix = ".node."
	// redisClientChannelPrefix is a prefix before channel name for client messages.
	redisClientChannelPrefix = ".client."
	// redisPubSubShardChannelSuffix is a suffix in channel name which we use to establish a sharded PUB/SUB connection.
	redisPubSubShardChannelSuffix = ".shard"
)

var _ Broker = (*RedisBroker)(nil)
var _ Controller = (*RedisBroker)(nil)

type pubSubStart struct {
	once  sync.Once
	errCh chan error
}

type controlPubSubStart struct {
	once  sync.Once
	errCh chan error
}

type shardWrapper struct {
	shard               *RedisShard
	subClientsMu        sync.Mutex
	subClients          [][]rueidis.DedicatedClient
	pubSubStartChannels [][]*pubSubStart
	controlPubSubStart  *controlPubSubStart
	logFields           map[string]any
	pubSubRunner        brokerPubSubRunner
}

// brokerPubSubRunner abstracts the subscriber-side pub/sub strategy for
// RedisBroker. One instance per shard, selected at construction time. init is
// called from NewRedisBroker (no goroutines), run from RegisterBrokerEventHandler
// (launches subscribers and returns; readiness is signaled via
// shardWrapper.pubSubStartChannels). subClientsIndex maps a partition-derived
// cluster shard index to the first-dimension index used in
// shardWrapper.subClients (default is identity; non-default strategies may
// remap, e.g. partition → node).
type brokerPubSubRunner interface {
	init(s *shardWrapper, shard *RedisShard) error
	run(s *shardWrapper, h BrokerEventHandler) error
	subClientsIndex(clusterShardIdx int) int
}

// RedisBroker uses Redis to implement Broker functionality. This broker allows
// scaling Centrifuge-based server to many instances and load balance client
// connections between them. Centrifuge nodes will be connected over Redis PUB/SUB.
// RedisBroker supports standalone Redis, Redis in master-replica setup with Sentinel,
// Redis Cluster. Also, it supports client-side consistent sharding between isolated
// Redis setups.
// By default, Redis >= 5 required (due to the fact RedisBroker uses STREAM data
// structure to keep publication history for a channel).
type RedisBroker struct {
	controlRound            uint64
	node                    *Node
	sharding                bool
	config                  RedisBrokerConfig
	shards                  []*shardWrapper
	publishIdempotentScript *rueidis.Lua
	historyListScript       *rueidis.Lua
	historyStreamScript     *rueidis.Lua
	addHistoryListScript    *rueidis.Lua
	addHistoryStreamScript  *rueidis.Lua
	shardChannel            string
	messagePrefix           string
	controlChannel          string
	nodeChannel             string
	closeOnce               sync.Once
	closeCh                 chan struct{}
}

// RedisBrokerConfig is a config for Broker.
type RedisBrokerConfig struct {
	// Prefix to use before every channel name and key in Redis. By default,
	// RedisBroker will use prefix "centrifuge".
	Prefix string

	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	// Data will be consistently sharded by channel over provided Redis shards.
	Shards []*RedisShard

	// UseLists allows enabling usage of Redis LIST instead of STREAM data
	// structure to keep history. LIST support exist mostly for backward
	// compatibility since STREAM seems superior. If you have a use case
	// where you need to turn on this option in new setup - please share,
	// otherwise LIST support can be removed at some point in the future.
	// Iteration over history in reversed order not supported with lists.
	UseLists bool

	// Subscribe on replica Redis nodes. This only works for Redis Cluster
	// and Sentinel setups and requires replica client to be initialized in
	// each RedisShard using RedisShardConfig.ReplicaClientEnabled.
	SubscribeOnReplica bool

	// SkipPubSub enables mode when Redis broker only saves history, without
	// publishing to channels and using PUB/SUB.
	SkipPubSub bool

	// Name of broker, for observability purposes – i.e. becomes part of metrics/logs.
	// By default, empty string is used.
	Name string

	// NumShardedPubSubPartitions when greater than zero allows turning on a mode in which
	// broker will use Redis Cluster with sharded PUB/SUB feature available in
	// Redis >= 7: https://redis.io/docs/manual/pubsub/#sharded-pubsub
	//
	// To achieve sharded PUB/SUB efficiency RedisBroker reduces 16384 Redis Cluster
	// slots to the NumShardedPubSubPartitions value and starts a separate PUB/SUB for each
	// partition. This is necessary because in Centrifuge case one node can work with
	// thousands of different channels – and we can't afford running a separate
	// PUB/SUB connection for each of 16384 possible slots. We re-use partition
	// connection for many channels and make sure that all channels in the partition
	// point to the same Redis Cluster slot.
	//
	// By default, sharded PUB/SUB is not used in Redis Cluster case - Centrifuge uses
	// globally distributed PUBLISH commands in Redis Cluster where each publish is
	// distributed to all nodes in Redis Cluster.
	//
	// Note (!), that turning on NumShardedPubSubPartitions will cause Centrifuge to generate
	// different key names for history and different Redis channel names than in the base
	// Redis Cluster mode due to reasons outlined above.
	NumShardedPubSubPartitions int

	// numSubscribeShards defines how many subscribe shards will be used by Centrifuge.
	// Each subscribe shard uses a dedicated connection to Redis for making subscriptions.
	// Zero value means 1.
	numSubscribeShards int

	// numResubscribeShards defines how many subscriber goroutines will be used by
	// Centrifuge for resubscribing process for each subscribe shard. Zero value tells
	// Centrifuge to use 16 subscriber goroutines per subscribe shard.
	numResubscribeShards int

	// numPubSubProcessors allows configuring number of workers which will process
	// messages coming from Redis PUB/SUB. Zero value tells Centrifuge to use the
	// number calculated as:
	// runtime.NumCPU / numSubscribeShards / NumShardedPubSubPartitions (if used) (minimum 1).
	numPubSubProcessors int

	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
	// it on the client side. This is useful for FIPS compliance.
	LoadSHA1 bool
}

// NewRedisBroker initializes Redis Broker.
func NewRedisBroker(n *Node, config RedisBrokerConfig) (*RedisBroker, error) {
	if len(config.Shards) == 0 {
		return nil, errors.New("broker: no Redis shards provided in configuration")
	}

	if config.SubscribeOnReplica {
		for i, s := range config.Shards {
			if s.replicaClient == nil {
				return nil, fmt.Errorf("broker: SubscribeOnReplica enabled but no replica client initialized in shard[%d] (ReplicaClientEnabled option)", i)
			}
		}
	}

	if len(config.Shards) > 1 {
		n.logger.log(newLogEntry(LogLevelInfo, fmt.Sprintf("broker: Redis sharding enabled: %d shards", len(config.Shards)), map[string]any{"broker_name": config.Name}))
	}

	if config.Prefix == "" {
		config.Prefix = "centrifuge"
	}

	if config.numSubscribeShards == 0 {
		config.numSubscribeShards = 1
	}

	if config.numResubscribeShards == 0 {
		config.numResubscribeShards = 16
	}

	if config.numPubSubProcessors == 0 {
		config.numPubSubProcessors = runtime.NumCPU() / config.numSubscribeShards
		if config.NumShardedPubSubPartitions > 0 {
			config.numPubSubProcessors /= config.NumShardedPubSubPartitions
		}
		if config.numPubSubProcessors < 1 {
			config.numPubSubProcessors = 1
		}
	}

	shardWrappers := make([]*shardWrapper, 0, len(config.Shards))
	for _, s := range config.Shards {
		logFields := map[string]any{
			"shard": s.string(),
		}
		if config.Name != "" {
			logFields["broker_name"] = config.Name
		}
		shardWrappers = append(shardWrappers, &shardWrapper{shard: s, logFields: logFields})
	}

	b := &RedisBroker{
		node:                    n,
		config:                  config,
		shards:                  shardWrappers,
		sharding:                len(config.Shards) > 1,
		publishIdempotentScript: rueidis.NewLuaScript(publishIdempotentSource, rueidis.WithLoadSHA1(config.LoadSHA1)),
		historyStreamScript:     rueidis.NewLuaScript(historyStreamSource, rueidis.WithLoadSHA1(config.LoadSHA1)),
		historyListScript:       rueidis.NewLuaScript(historyListSource, rueidis.WithLoadSHA1(config.LoadSHA1)),
		addHistoryStreamScript:  rueidis.NewLuaScript(addHistoryStreamSource, rueidis.WithLoadSHA1(config.LoadSHA1)),
		addHistoryListScript:    rueidis.NewLuaScript(addHistoryListSource, rueidis.WithLoadSHA1(config.LoadSHA1)),
		closeCh:                 make(chan struct{}),
	}
	b.shardChannel = config.Prefix + redisPubSubShardChannelSuffix
	b.messagePrefix = config.Prefix + redisClientChannelPrefix
	b.nodeChannel = string(b.nodeChannelID(n.ID()))
	b.controlChannel = config.Prefix + redisControlChannelSuffix

	for _, sw := range b.shards {
		shard := sw.shard
		if !shard.isCluster && b.config.NumShardedPubSubPartitions > 0 {
			return nil, errors.New("can use sharded PUB/SUB feature (non-zero number of pub/sub partitions) only with Redis Cluster")
		}
		if newBrokerPubSubRunnerHook != nil {
			sw.pubSubRunner = newBrokerPubSubRunnerHook(b, shard)
		}
		if sw.pubSubRunner == nil {
			sw.pubSubRunner = &defaultBrokerPubSubRunner{broker: b}
		}
		if err := sw.pubSubRunner.init(sw, shard); err != nil {
			return nil, err
		}
	}

	return b, nil
}

// newBrokerPubSubRunnerHook is an optional package-private hook used by
// auxiliary modules to install an alternative pub/sub runner. The default
// implementation returns nil so the broker falls back to defaultBrokerPubSubRunner.
var newBrokerPubSubRunnerHook func(b *RedisBroker, shard *RedisShard) brokerPubSubRunner

// defaultBrokerPubSubRunner is the standard partition-sharded pub/sub runner.
// All per-shard state lives on the shardWrapper, the runner is stateless beyond
// a back-pointer to the broker.
type defaultBrokerPubSubRunner struct {
	broker *RedisBroker
}

func (r *defaultBrokerPubSubRunner) init(s *shardWrapper, shard *RedisShard) error {
	b := r.broker
	subChannels := make([][]rueidis.DedicatedClient, 0)
	pubSubStartChannels := make([][]*pubSubStart, 0)

	if b.useShardedPubSub(shard) {
		for i := 0; i < b.config.NumShardedPubSubPartitions; i++ {
			subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
			pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
		}
	} else {
		subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
		pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
	}

	for i := 0; i < len(subChannels); i++ {
		for j := 0; j < b.config.numSubscribeShards; j++ {
			subChannels[i] = append(subChannels[i], nil)
			pubSubStartChannels[i] = append(pubSubStartChannels[i], &pubSubStart{errCh: make(chan error, 1)})
		}
	}

	s.subClients = subChannels
	s.pubSubStartChannels = pubSubStartChannels
	return nil
}

func (r *defaultBrokerPubSubRunner) subClientsIndex(clusterShardIdx int) int {
	return clusterShardIdx
}

func (r *defaultBrokerPubSubRunner) run(s *shardWrapper, h BrokerEventHandler) error {
	b := r.broker
	if b.config.SkipPubSub {
		return nil
	}
	for i := 0; i < len(s.subClients); i++ { // Cluster shards.
		clusterShardIndex := i
		for j := 0; j < len(s.subClients[i]); j++ { // PUB/SUB shards.
			pubSubShardIndex := j
			logFields := getBaseLogFields(s)
			logFields["pub_sub_shard"] = pubSubShardIndex
			go b.runForever(func() {
				select {
				case <-b.closeCh:
					return
				default:
				}
				b.runPubSub(s, logFields, h, clusterShardIndex, pubSubShardIndex, b.useShardedPubSub(s.shard), func(err error) {
					s.pubSubStartChannels[clusterShardIndex][pubSubShardIndex].once.Do(func() {
						s.pubSubStartChannels[clusterShardIndex][pubSubShardIndex].errCh <- err
					})
				})
			})
		}
	}
	return nil
}

var (
	//go:embed internal/redis_lua/broker_publish_idempotent.lua
	publishIdempotentSource string

	//go:embed internal/redis_lua/broker_history_add_list.lua
	addHistoryListSource string

	//go:embed internal/redis_lua/broker_history_add_stream.lua
	addHistoryStreamSource string

	//go:embed internal/redis_lua/broker_history_list.lua
	historyListSource string

	//go:embed internal/redis_lua/broker_history_stream.lua
	historyStreamSource string
)

func (b *RedisBroker) getShard(channel string) *shardWrapper {
	if !b.sharding {
		return b.shards[0]
	}
	return b.shards[consistentIndex(channel, len(b.shards))]
}

func (b *RedisBroker) RegisterControlEventHandler(h ControlEventHandler) error {
	for _, wrapper := range b.shards {
		wrapper.controlPubSubStart = &controlPubSubStart{errCh: make(chan error, 1)}
		err := b.runControlShard(wrapper, h)
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishControl - see Broker.PublishControl.
func (b *RedisBroker) PublishControl(data []byte, nodeID string, _ string) error {
	currentRound := atomic.AddUint64(&b.controlRound, 1)
	index := currentRound % uint64(len(b.shards))
	s := b.shards[index]
	return b.publishControl(s, data, nodeID)
}

func (b *RedisBroker) publishControl(s *shardWrapper, data []byte, nodeID string) error {
	var chID channelID
	if nodeID == "" {
		chID = channelID(b.controlChannel)
	} else {
		chID = b.nodeChannelID(nodeID)
	}
	cmd := s.shard.client.B().Publish().Channel(string(chID)).Message(convert.BytesToString(data)).Build()
	resp := s.shard.client.Do(context.Background(), cmd)
	return resp.Error()
}

// RegisterBrokerEventHandler – see Broker.RegisterBrokerEventHandler.
func (b *RedisBroker) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	// Run all shards.
	for _, wrapper := range b.shards {
		if err := wrapper.pubSubRunner.run(wrapper, h); err != nil {
			return err
		}
		if err := b.checkCapabilities(wrapper.shard); err != nil {
			return fmt.Errorf("capability error on shard [%s]: %v", wrapper.shard.string(), err)
		}
	}
	for i := 0; i < len(b.shards); i++ {
		if b.shards[i].controlPubSubStart != nil {
			<-b.shards[i].controlPubSubStart.errCh
		}
		for j := 0; j < len(b.shards[i].pubSubStartChannels); j++ {
			for k := 0; k < len(b.shards[i].pubSubStartChannels[j]); k++ {
				if !b.config.SkipPubSub {
					<-b.shards[i].pubSubStartChannels[j][k].errCh
				}
			}
		}
	}
	return nil
}

func (b *RedisBroker) checkCapabilities(shard *RedisShard) error {
	if !b.config.UseLists {
		// Check whether Redis Streams supported.
		if result := shard.client.Do(context.Background(), shard.client.B().Xrange().Key(b.config.Prefix+".__.check.stream").Start("0-0").End("0-0").Build()); result.Error() != nil {
			if strings.Contains(result.Error().Error(), "unknown command") {
				return errors.New("STREAM only available since Redis >= 5, consider upgrading Redis or using LIST structure for history")
			}
			return result.Error()
		}
	}
	if b.useShardedPubSub(shard) {
		// Check whether Redis Cluster sharded PUB/SUB supported.
		if result := shard.client.Do(context.Background(), shard.client.B().Spublish().Channel(b.config.Prefix+".__check.spublish").Message("").Build()); result.Error() != nil {
			if strings.Contains(result.Error().Error(), "unknown command") {
				return errors.New("this Redis version does not support cluster sharded PUB/SUB feature")
			}
			return result.Error()
		}
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

func getBaseLogFields(s *shardWrapper) map[string]any {
	baseLogFields := make(map[string]any, len(s.logFields))
	for k, v := range s.logFields {
		baseLogFields[k] = v
	}
	return baseLogFields
}

func (b *RedisBroker) runControlShard(s *shardWrapper, h ControlEventHandler) error {
	baseLogFields := getBaseLogFields(s)
	go b.runForever(func() {
		select {
		case <-b.closeCh:
			return
		default:
		}
		b.runControlPubSub(s.shard, baseLogFields, h, func(err error) {
			s.controlPubSubStart.once.Do(func() {
				s.controlPubSubStart.errCh <- err
			})
		})
	})
	return nil
}

func (b *RedisBroker) Close(_ context.Context) error {
	b.closeOnce.Do(func() {
		close(b.closeCh)
	})
	return nil
}

func (b *RedisBroker) runControlPubSub(s *RedisShard, logFields map[string]any, eventHandler ControlEventHandler, startOnce func(error)) {
	b.node.logger.log(newLogEntry(LogLevelDebug, "running Redis control PUB/SUB", getPubSubStartLogFields(s, logFields)))
	defer func() {
		b.node.logger.log(newLogEntry(LogLevelDebug, "stopping Redis control PUB/SUB", logFields))
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

	client := s.client
	if b.config.SubscribeOnReplica {
		client = s.replicaClient
	}

	conn, cancel := client.Dedicate()
	defer cancel()
	defer conn.Close()

	numProcessors := runtime.NumCPU()

	// Run workers to spread message processing work over worker goroutines.
	workCh := make(chan rueidis.PubSubMessage, controlPubSubProcessorBufferSize)
	for i := 0; i < numProcessors; i++ {
		go func() {
			for {
				select {
				case <-done:
					return
				case msg := <-workCh:
					err := eventHandler.HandleControl(convert.StringToBytes(msg.Message))
					if err != nil {
						b.node.metrics.incRedisBrokerPubSubErrors(b.config.Name, "handle_control_message")
						b.node.logger.log(newErrorLogEntry(err, "error handling control message", logFields))
					}
				}
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				b.node.metrics.redisBrokerPubSubBufferedMessages.WithLabelValues(b.config.Name, "control", "0").Set(float64(len(workCh)))
			}
		}
	}()

	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			select {
			case workCh <- msg:
			case <-done:
			default:
				// Buffer is full, drop the message. It's expected that PUB/SUB layer
				// only provides at most once delivery guarantee.
				// Blocking here will block Redis connection read loop which is not a
				// good thing and can lead to slower command processing and potentially
				// to deadlocks (see https://github.com/redis/rueidis/issues/596).
				b.node.metrics.redisBrokerPubSubDroppedMessages.WithLabelValues(b.config.Name, "control").Inc()
			}
		},
	})

	err := conn.Do(context.Background(), conn.B().Subscribe().Channel(controlChannel, nodeChannel).Build()).Error()
	if err != nil {
		startOnce(err)
		b.node.metrics.incRedisBrokerPubSubErrors(b.config.Name, "subscribe_control_channel")
		b.node.logger.log(newErrorLogEntry(err, "control pub/sub subscribe error", logFields))
		return
	}

	startOnce(nil)

	select {
	case err := <-wait:
		if err != nil {
			b.node.metrics.incRedisBrokerPubSubErrors(b.config.Name, "control_connection")
			b.node.logger.log(newErrorLogEntry(err, "control pub/sub connection error", logFields))
		}
	case <-done:
	case <-s.closeCh:
	}
}

const (
	controlPubSubProcessorBufferSize = 4096
)

// makePubSubCallbacks builds the pubSubCallbacks used by the pub/sub runner.
func (b *RedisBroker) makePubSubCallbacks(s *shardWrapper) pubSubCallbacks {
	return pubSubCallbacks{
		handleMessage: func(isCluster bool, handler BrokerEventHandler, ch string, data []byte) error {
			return b.handleRedisClientMessage(isCluster, handler, channelID(ch), data)
		},
		shardChannelID: func(clusterIdx, psIdx int, sharded bool) string {
			return string(b.pubSubShardChannelID(clusterIdx, psIdx, sharded))
		},
		messageChannelID: func(ch string) string {
			return string(b.messageChannelID(s.shard, ch))
		},
		shardForChannel: func(ch string) *RedisShard {
			return b.getShard(ch).shard
		},
	}
}

func (b *RedisBroker) runPubSub(s *shardWrapper, logFields map[string]any, eventHandler BrokerEventHandler, clusterShardIndex, psShardIndex int, useShardedPubSub bool, startOnce func(error)) {
	cb := b.makePubSubCallbacks(s)
	numPartitions := b.config.NumShardedPubSubPartitions
	if numPartitions == 0 {
		numPartitions = 1
	}
	runPubSubLoop(
		s.shard,
		&s.subClientsMu,
		s.subClients,
		cb,
		b.node,
		b.config.Name,
		b.config.SubscribeOnReplica,
		b.config.numPubSubProcessors,
		b.config.numResubscribeShards,
		b.config.numSubscribeShards,
		numPartitions,
		logFields,
		eventHandler,
		clusterShardIndex, psShardIndex,
		useShardedPubSub,
		startOnce,
	)
}

func (b *RedisBroker) useShardedPubSub(s *RedisShard) bool {
	return s.isCluster && b.config.NumShardedPubSubPartitions > 0
}

// Publish - see Broker.Publish.
func (b *RedisBroker) Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	return b.publish(b.getShard(ch), ch, data, opts)
}

func (b *RedisBroker) publish(s *shardWrapper, ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	protoPub := &protocol.Publication{
		Data:     data,
		Info:     infoToProto(opts.ClientInfo),
		Tags:     opts.Tags,
		Time:     time.Now().UnixMilli(),
		Key:      opts.Key,
		Removed:  opts.Removed,
		Score:    opts.score,
		Offset:   opts.Offset,
		Epoch:    opts.Epoch,
		PrevData: opts.PrevData,
		Version:  opts.Version,
	}
	if opts.HistorySize <= 0 || opts.HistoryTTL <= 0 {
		// In no history case we communicate delta flag over Publication field. This field is then
		// cleaned up before passing to the Node layer when handling Redis message.
		protoPub.Delta = opts.UseDelta
	}

	byteMessage, err := protoPub.MarshalVT()
	if err != nil {
		return PublishResult{}, err
	}

	publishChannel := b.messageChannelID(s.shard, ch)
	useShardedPublish := b.useShardedPubSub(s.shard)
	var publishCommand = "publish"
	if useShardedPublish {
		publishCommand = "spublish"
	}

	idempotencyKey := opts.IdempotencyKey
	resultKey := b.resultCacheKey(s.shard, ch, idempotencyKey)
	var resultExpire string
	if idempotencyKey != "" {
		if opts.IdempotentResultTTL != 0 {
			resultExpire = strconv.Itoa(int(opts.IdempotentResultTTL.Seconds()))
		} else {
			resultExpire = strconv.Itoa(defaultIdempotentResultExpireSeconds)
		}
	}

	publishChannelStr := string(publishChannel)
	if b.config.SkipPubSub {
		publishChannelStr = ""
	}

	if opts.HistorySize <= 0 || opts.HistoryTTL <= 0 {
		var resp rueidis.RedisResult
		if useShardedPublish {
			if resultExpire == "" {
				if publishChannelStr == "" {
					return PublishResult{}, nil
				}
				cmd := s.shard.client.B().Spublish().Channel(string(publishChannel)).Message(convert.BytesToString(byteMessage)).Build()
				resp = s.shard.client.Do(context.Background(), cmd)
			} else {
				resp = b.publishIdempotentScript.Exec(
					context.Background(),
					s.shard.client,
					[]string{string(resultKey)},
					[]string{
						convert.BytesToString(byteMessage),
						publishChannelStr,
						publishCommand,
						resultExpire,
					},
				)
			}
		} else {
			if resultExpire == "" {
				if publishChannelStr == "" {
					return PublishResult{}, nil
				}
				cmd := s.shard.client.B().Publish().Channel(string(publishChannel)).Message(convert.BytesToString(byteMessage)).Build()
				resp = s.shard.client.Do(context.Background(), cmd)
			} else {
				resp = b.publishIdempotentScript.Exec(
					context.Background(),
					s.shard.client,
					[]string{string(resultKey)},
					[]string{
						convert.BytesToString(byteMessage),
						publishChannelStr,
						publishCommand,
						resultExpire,
					},
				)
			}
		}
		return PublishResult{}, resp.Error()
	}

	historyMetaKey := b.historyMetaKey(s.shard, ch)

	historyMetaTTL := opts.HistoryMetaTTL
	if historyMetaTTL == 0 {
		historyMetaTTL = b.node.config.HistoryMetaTTL
	}

	historyMetaTTLSeconds := int(historyMetaTTL.Seconds())

	var streamKey channelID
	var size int
	var script *rueidis.Lua
	if b.config.UseLists {
		streamKey = b.historyListKey(s.shard, ch)
		size = opts.HistorySize - 1
		script = b.addHistoryListScript
	} else {
		streamKey = b.historyStreamKey(s.shard, ch)
		size = opts.HistorySize
		script = b.addHistoryStreamScript
	}

	var useDelta string
	if opts.UseDelta {
		useDelta = "1"
	}

	version := "0"
	if opts.Version > 0 {
		version = strconv.Itoa(int(opts.Version))
	}
	versionEpoch := opts.VersionEpoch

	replies, err := script.Exec(
		context.Background(),
		s.shard.client,
		[]string{string(streamKey), string(historyMetaKey), string(resultKey)},
		[]string{
			convert.BytesToString(byteMessage),
			strconv.Itoa(size),
			strconv.Itoa(int(opts.HistoryTTL.Seconds())),
			publishChannelStr,
			strconv.Itoa(historyMetaTTLSeconds),
			epoch.Generate(),
			publishCommand,
			resultExpire,
			useDelta,
			version,
			versionEpoch,
		},
	).ToArray()
	if err != nil {
		return PublishResult{}, err
	}
	if len(replies) != 2 && len(replies) != 3 && len(replies) != 4 {
		return PublishResult{}, errors.New("wrong Redis reply")
	}
	offset, err := replies[0].AsInt64()
	if err != nil {
		return PublishResult{}, errors.New("wrong Redis reply offset")
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return PublishResult{}, errors.New("wrong Redis reply epoch")
	}
	result := PublishResult{StreamPosition: StreamPosition{Offset: uint64(offset), Epoch: epoch}}
	if len(replies) >= 3 {
		fromCacheStr, err := replies[2].ToString()
		if err != nil {
			return PublishResult{}, errors.New("wrong Redis reply from cache flag")
		}
		if fromCacheStr == "1" {
			result.Suppressed = true
			result.SuppressReason = SuppressReasonIdempotency
		}
	}
	if len(replies) >= 4 {
		skippedStr, err := replies[3].ToString()
		if err != nil {
			return PublishResult{}, errors.New("wrong Redis reply skipped flag")
		}
		if skippedStr == "1" {
			result.Suppressed = true
			result.SuppressReason = SuppressReasonVersion
		}
	}

	return result, nil
}

// PublishJoin - see Broker.PublishJoin.
func (b *RedisBroker) PublishJoin(ch string, info *ClientInfo) error {
	return b.publishJoin(b.getShard(ch), ch, info)
}

func (b *RedisBroker) publishJoin(s *shardWrapper, ch string, info *ClientInfo) error {
	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	chID := b.messageChannelID(s.shard, ch)

	var resp rueidis.RedisResult
	if b.useShardedPubSub(s.shard) {
		cmd := s.shard.client.B().Spublish().Channel(string(chID)).Message(convert.BytesToString(append(joinTypePrefix, byteMessage...))).Build()
		resp = s.shard.client.Do(context.Background(), cmd)
	} else {
		cmd := s.shard.client.B().Publish().Channel(string(chID)).Message(convert.BytesToString(append(joinTypePrefix, byteMessage...))).Build()
		resp = s.shard.client.Do(context.Background(), cmd)
	}
	return resp.Error()
}

// PublishLeave - see Broker.PublishLeave.
func (b *RedisBroker) PublishLeave(ch string, info *ClientInfo) error {
	return b.publishLeave(b.getShard(ch), ch, info)
}

func (b *RedisBroker) publishLeave(s *shardWrapper, ch string, info *ClientInfo) error {
	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	chID := b.messageChannelID(s.shard, ch)

	var resp rueidis.RedisResult
	if b.useShardedPubSub(s.shard) {
		cmd := s.shard.client.B().Spublish().Channel(string(chID)).Message(convert.BytesToString(append(leaveTypePrefix, byteMessage...))).Build()
		resp = s.shard.client.Do(context.Background(), cmd)
	} else {
		cmd := s.shard.client.B().Publish().Channel(string(chID)).Message(convert.BytesToString(append(leaveTypePrefix, byteMessage...))).Build()
		resp = s.shard.client.Do(context.Background(), cmd)
	}
	return resp.Error()
}

// Subscribe - see Broker.Subscribe.
func (b *RedisBroker) Subscribe(channels ...string) error {
	if len(channels) == 0 {
		return nil
	}
	if len(channels) == 1 {
		return b.subscribe(b.getShard(channels[0]), channels[0])
	}
	return b.subscribeBatch(channels, false)
}

func (b *RedisBroker) subscribe(s *shardWrapper, ch string) error {
	if b.node.logEnabled(LogLevelDebug) {
		b.node.logger.log(newLogEntry(LogLevelDebug, "subscribe node on channel", map[string]any{"broker_name": b.config.Name, "channel": ch}))
	}
	psShardIndex := index(ch, b.config.numSubscribeShards)
	var clusterShardIndex int
	if b.useShardedPubSub(s.shard) {
		clusterShardIndex = consistentIndex(ch, b.config.NumShardedPubSubPartitions)
	}
	clusterShardIndex = s.pubSubRunner.subClientsIndex(clusterShardIndex)

	s.subClientsMu.Lock()
	conn := s.subClients[clusterShardIndex][psShardIndex]
	if conn == nil {
		s.subClientsMu.Unlock()
		return errPubSubConnUnavailable
	}
	s.subClientsMu.Unlock()

	var err error
	if b.useShardedPubSub(s.shard) {
		err = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(string(b.messageChannelID(s.shard, ch))).Build()).Error()
	} else {
		err = conn.Do(context.Background(), conn.B().Subscribe().Channel(string(b.messageChannelID(s.shard, ch))).Build()).Error()
	}
	return err
}

type brokerConnKey struct {
	shardIdx        int
	clusterShardIdx int
	psShardIdx      int
}

type brokerConnGroup struct {
	shard    *shardWrapper
	channels []string
}

func (b *RedisBroker) subscribeBatch(channels []string, unsub bool) error {
	groups := make(map[brokerConnKey]*brokerConnGroup)
	for _, ch := range channels {
		var shardIdx int
		if b.sharding {
			shardIdx = consistentIndex(ch, len(b.shards))
		}
		s := b.shards[shardIdx]
		psShardIdx := index(ch, b.config.numSubscribeShards)
		var clusterShardIdx int
		if b.useShardedPubSub(s.shard) {
			clusterShardIdx = consistentIndex(ch, b.config.NumShardedPubSubPartitions)
		}
		clusterShardIdx = s.pubSubRunner.subClientsIndex(clusterShardIdx)
		key := brokerConnKey{shardIdx: shardIdx, clusterShardIdx: clusterShardIdx, psShardIdx: psShardIdx}
		g, ok := groups[key]
		if !ok {
			g = &brokerConnGroup{shard: s}
			groups[key] = g
		}
		g.channels = append(g.channels, ch)
	}
	// Track completed groups so we can roll back on partial failure.
	var completed []brokerConnKey
	for key, g := range groups {
		s := g.shard
		s.subClientsMu.Lock()
		conn := s.subClients[key.clusterShardIdx][key.psShardIdx]
		if conn == nil {
			s.subClientsMu.Unlock()
			if !unsub {
				b.rollbackSubscribeBatch(groups, completed)
			}
			return errPubSubConnUnavailable
		}
		s.subClientsMu.Unlock()

		chIDs := make([]string, len(g.channels))
		for i, ch := range g.channels {
			chIDs[i] = string(b.messageChannelID(s.shard, ch))
		}

		var err error
		if b.useShardedPubSub(s.shard) {
			if unsub {
				err = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(chIDs...).Build()).Error()
			} else {
				err = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(chIDs...).Build()).Error()
			}
		} else {
			if unsub {
				err = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(chIDs...).Build()).Error()
			} else {
				err = conn.Do(context.Background(), conn.B().Subscribe().Channel(chIDs...).Build()).Error()
			}
		}
		if err != nil {
			if !unsub {
				b.rollbackSubscribeBatch(groups, completed)
			}
			return err
		}
		completed = append(completed, key)
	}
	return nil
}

// rollbackSubscribeBatch unsubscribes channels from groups that were already
// successfully subscribed. Best-effort: errors are ignored.
func (b *RedisBroker) rollbackSubscribeBatch(groups map[brokerConnKey]*brokerConnGroup, completed []brokerConnKey) {
	for _, key := range completed {
		g := groups[key]
		s := g.shard
		s.subClientsMu.Lock()
		conn := s.subClients[key.clusterShardIdx][key.psShardIdx]
		s.subClientsMu.Unlock()
		if conn == nil {
			continue
		}
		chIDs := make([]string, len(g.channels))
		for i, ch := range g.channels {
			chIDs[i] = string(b.messageChannelID(s.shard, ch))
		}
		if b.useShardedPubSub(s.shard) {
			_ = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(chIDs...).Build()).Error()
		} else {
			_ = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(chIDs...).Build()).Error()
		}
	}
}

// Unsubscribe - see Broker.Unsubscribe.
func (b *RedisBroker) Unsubscribe(channels ...string) error {
	if len(channels) == 0 {
		return nil
	}
	if len(channels) == 1 {
		return b.unsubscribe(b.getShard(channels[0]), channels[0])
	}
	return b.subscribeBatch(channels, true)
}

func (b *RedisBroker) unsubscribe(s *shardWrapper, ch string) error {
	if b.node.logEnabled(LogLevelDebug) {
		b.node.logger.log(newLogEntry(LogLevelDebug, "unsubscribe node from channel", map[string]any{"broker_name": b.config.Name, "channel": ch}))
	}
	psShardIndex := index(ch, b.config.numSubscribeShards)
	var clusterShardIndex int
	if b.useShardedPubSub(s.shard) {
		clusterShardIndex = consistentIndex(ch, b.config.NumShardedPubSubPartitions)
	}
	clusterShardIndex = s.pubSubRunner.subClientsIndex(clusterShardIndex)

	s.subClientsMu.Lock()
	conn := s.subClients[clusterShardIndex][psShardIndex]
	if conn == nil {
		s.subClientsMu.Unlock()
		return errPubSubConnUnavailable
	}
	s.subClientsMu.Unlock()

	var err error
	if b.useShardedPubSub(s.shard) {
		err = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(string(b.messageChannelID(s.shard, ch))).Build()).Error()
	} else {
		err = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(string(b.messageChannelID(s.shard, ch))).Build()).Error()
	}
	return err
}

// History - see Broker.History.
func (b *RedisBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	return b.history(b.getShard(ch), ch, opts)
}

func (b *RedisBroker) history(s *shardWrapper, ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	if b.config.UseLists {
		return b.historyList(s.shard, ch, opts.Filter)
	}
	return b.historyStream(s.shard, ch, opts)
}

// RemoveHistory - see Broker.RemoveHistory.
func (b *RedisBroker) RemoveHistory(ch string) error {
	return b.removeHistory(b.getShard(ch), ch)
}

func (b *RedisBroker) removeHistory(s *shardWrapper, ch string) error {
	var key channelID
	if b.config.UseLists {
		key = b.historyListKey(s.shard, ch)
	} else {
		key = b.historyStreamKey(s.shard, ch)
	}
	cmd := s.shard.client.B().Del().Key(string(key)).Build()
	resp := s.shard.client.Do(context.Background(), cmd)
	return resp.Error()
}

func (b *RedisBroker) messageChannelID(s *RedisShard, ch string) channelID {
	if !b.useShardedPubSub(s) {
		// In cluster mode, always use hash tags to ensure channels and keys (history,
		// result cache) are in the same hash slot. This is required for Lua scripts in
		// serverless Redis (e.g., AWS Elasticache Serverless) where PUBLISH commands
		// inside Lua scripts are slot-affecting. Standard Redis Cluster works fine with
		// this since PUBLISH is broadcast to all nodes regardless of hash slot.
		// See https://github.com/centrifugal/centrifugo/issues/1087#issuecomment-3667377731
		if s.isCluster {
			// Build: messagePrefix + "{" + ch + "}"
			var builder strings.Builder
			builder.Grow(len(b.messagePrefix) + 1 + len(ch) + 1)
			builder.WriteString(b.messagePrefix)
			builder.WriteByte('{')
			builder.WriteString(ch)
			builder.WriteByte('}')
			return channelID(builder.String())
		}
		// Non-cluster path: no hash tags needed
		var builder strings.Builder
		builder.Grow(len(b.messagePrefix) + len(ch))
		builder.WriteString(b.messagePrefix)
		builder.WriteString(ch)
		return channelID(builder.String())
	}

	// Sharded pub/sub path: uses partition-based hash tags
	idx := consistentIndex(ch, b.config.NumShardedPubSubPartitions)
	idxStr := strconv.Itoa(idx)

	// Pre-calculate capacity: messagePrefix + "{" + idxStr + "}." + ch
	capacity := len(b.messagePrefix) + 1 + len(idxStr) + 2 + len(ch)

	var builder strings.Builder
	builder.Grow(capacity)
	builder.WriteString(b.messagePrefix)
	builder.WriteByte('{')
	builder.WriteString(idxStr)
	builder.WriteString("}.")
	builder.WriteString(ch)

	return channelID(builder.String())
}

// pubSubPartitionHashTag returns the Redis Cluster hash tag for the given partition
// index. Used both when constructing PUB/SUB channel names (so messages route
// to a specific slot) and by alternative pub/sub strategies that need to
// determine slot ownership for a partition. Publisher and subscriber must
// agree on the scheme — keep this as the single source of truth.
func (b *RedisBroker) pubSubPartitionHashTag(partitionIdx int) string {
	return strconv.Itoa(partitionIdx)
}

func (b *RedisBroker) pubSubShardChannelID(clusterShardIndex int, psShardIndex int, useShardedPubSub bool) channelID {
	psShardStr := strconv.Itoa(psShardIndex)

	if !useShardedPubSub {
		// Fast path: shardChannel + "." + psShardIndex
		var builder strings.Builder
		builder.Grow(len(b.shardChannel) + 1 + len(psShardStr))
		builder.WriteString(b.shardChannel)
		builder.WriteByte('.')
		builder.WriteString(psShardStr)
		return channelID(builder.String())
	}

	// Sharded path: shardChannel + "." + psShardIndex + ".{" + pubSubPartitionHashTag + "}"
	clusterShardStr := b.pubSubPartitionHashTag(clusterShardIndex)
	capacity := len(b.shardChannel) + 1 + len(psShardStr) + 2 + len(clusterShardStr) + 1

	var builder strings.Builder
	builder.Grow(capacity)
	builder.WriteString(b.shardChannel)
	builder.WriteByte('.')
	builder.WriteString(psShardStr)
	builder.WriteString(".{")
	builder.WriteString(clusterShardStr)
	builder.WriteByte('}')
	return channelID(builder.String())
}

func (b *RedisBroker) nodeChannelID(nodeID string) channelID {
	return channelID(b.config.Prefix + redisNodeChannelPrefix + nodeID)
}

func (b *RedisBroker) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) channelID {
	if !s.isCluster {
		// Fast path: prefix + ".result." + ch + "." + idempotencyKey
		var builder strings.Builder
		builder.Grow(len(b.config.Prefix) + 9 + len(ch) + 1 + len(idempotencyKey))
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".result.")
		builder.WriteString(ch)
		builder.WriteByte('.')
		builder.WriteString(idempotencyKey)
		return channelID(builder.String())
	}

	var builder strings.Builder

	if b.config.NumShardedPubSubPartitions > 0 {
		// Sharded cluster: prefix + ".result." + "{" + idx + "}." + ch + "." + idempotencyKey
		idx := consistentIndex(ch, b.config.NumShardedPubSubPartitions)
		idxStr := strconv.Itoa(idx)
		capacity := len(b.config.Prefix) + 9 + 1 + len(idxStr) + 2 + len(ch) + 1 + len(idempotencyKey)
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".result.{")
		builder.WriteString(idxStr)
		builder.WriteString("}.")
		builder.WriteString(ch)
		builder.WriteByte('.')
		builder.WriteString(idempotencyKey)
	} else {
		// Non-sharded cluster: prefix + ".result." + "{" + ch + "}" + "." + idempotencyKey
		capacity := len(b.config.Prefix) + 9 + 1 + len(ch) + 2 + len(idempotencyKey)
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".result.{")
		builder.WriteString(ch)
		builder.WriteString("}.")
		builder.WriteString(idempotencyKey)
	}

	return channelID(builder.String())
}

func (b *RedisBroker) historyListKey(s *RedisShard, ch string) channelID {
	if !s.isCluster {
		// Fast path: prefix + ".list." + ch
		var builder strings.Builder
		builder.Grow(len(b.config.Prefix) + 6 + len(ch))
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".list.")
		builder.WriteString(ch)
		return channelID(builder.String())
	}

	var builder strings.Builder

	if b.config.NumShardedPubSubPartitions > 0 {
		// Sharded cluster: prefix + ".list." + "{" + idx + "}." + ch
		idx := consistentIndex(ch, b.config.NumShardedPubSubPartitions)
		idxStr := strconv.Itoa(idx)
		capacity := len(b.config.Prefix) + 6 + 1 + len(idxStr) + 2 + len(ch)
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".list.{")
		builder.WriteString(idxStr)
		builder.WriteString("}.")
		builder.WriteString(ch)
	} else {
		// Non-sharded cluster: prefix + ".list." + "{" + ch + "}"
		capacity := len(b.config.Prefix) + 6 + 1 + len(ch) + 1
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".list.{")
		builder.WriteString(ch)
		builder.WriteByte('}')
	}

	return channelID(builder.String())
}

func (b *RedisBroker) historyStreamKey(s *RedisShard, ch string) channelID {
	if !s.isCluster {
		// Fast path: prefix + ".stream." + ch
		var builder strings.Builder
		builder.Grow(len(b.config.Prefix) + 8 + len(ch))
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".stream.")
		builder.WriteString(ch)
		return channelID(builder.String())
	}

	var builder strings.Builder

	if b.config.NumShardedPubSubPartitions > 0 {
		// Sharded cluster: prefix + ".stream." + "{" + idx + "}." + ch
		idx := consistentIndex(ch, b.config.NumShardedPubSubPartitions)
		idxStr := strconv.Itoa(idx)
		capacity := len(b.config.Prefix) + 8 + 1 + len(idxStr) + 2 + len(ch)
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".stream.{")
		builder.WriteString(idxStr)
		builder.WriteString("}.")
		builder.WriteString(ch)
	} else {
		// Non-sharded cluster: prefix + ".stream." + "{" + ch + "}"
		capacity := len(b.config.Prefix) + 8 + 1 + len(ch) + 1
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(".stream.{")
		builder.WriteString(ch)
		builder.WriteByte('}')
	}

	return channelID(builder.String())
}

func (b *RedisBroker) historyMetaKey(s *RedisShard, ch string) channelID {
	infix := ".stream.meta."
	if b.config.UseLists {
		infix = ".list.meta."
	}

	if !s.isCluster {
		// Fast path: prefix + infix + ch
		var builder strings.Builder
		builder.Grow(len(b.config.Prefix) + len(infix) + len(ch))
		builder.WriteString(b.config.Prefix)
		builder.WriteString(infix)
		builder.WriteString(ch)
		return channelID(builder.String())
	}

	var builder strings.Builder

	if b.config.NumShardedPubSubPartitions > 0 {
		// Sharded cluster: prefix + infix + "{" + idx + "}." + ch
		idx := consistentIndex(ch, b.config.NumShardedPubSubPartitions)
		idxStr := strconv.Itoa(idx)
		capacity := len(b.config.Prefix) + len(infix) + 1 + len(idxStr) + 2 + len(ch)
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(infix)
		builder.WriteByte('{')
		builder.WriteString(idxStr)
		builder.WriteString("}.")
		builder.WriteString(ch)
	} else {
		// Non-sharded cluster: prefix + infix + "{" + ch + "}"
		capacity := len(b.config.Prefix) + len(infix) + 1 + len(ch) + 1
		builder.Grow(capacity)
		builder.WriteString(b.config.Prefix)
		builder.WriteString(infix)
		builder.WriteByte('{')
		builder.WriteString(ch)
		builder.WriteByte('}')
	}

	return channelID(builder.String())
}

func (b *RedisBroker) extractChannel(isCluster bool, chID channelID) string {
	ch := strings.TrimPrefix(string(chID), b.messagePrefix)

	// Handle sharded PUB/SUB case: {idx}.channel
	if b.config.NumShardedPubSubPartitions > 0 {
		if !strings.HasPrefix(ch, "{") {
			return "" // Invalid: expected {idx}.channel format
		}
		i := strings.Index(ch, ".")
		if i > 0 {
			return ch[i+1:]
		}
		return "" // Invalid: missing dot separator
	}

	// Handle cluster case: must have {channel} format
	if isCluster {
		channelLen := len(ch)
		if channelLen < 2 || ch[0] != '{' || ch[channelLen-1] != '}' {
			return ""
		}
		return ch[1 : channelLen-1]
	}

	// Non-cluster: plain channel name
	return ch
}

// Define prefixes to distinguish Join and Leave messages coming from PUB/SUB.
var (
	joinTypePrefix  = []byte("__j__")
	leaveTypePrefix = []byte("__l__")
)

func (b *RedisBroker) handleRedisClientMessage(isCluster bool, eventHandler BrokerEventHandler, chID channelID, data []byte) error {
	pushData, typeOfPush, sp, delta, prevPayload, ok := extractPushData(data)
	if !ok {
		return fmt.Errorf("malformed PUB/SUB data: %s", data)
	}
	channel := b.extractChannel(isCluster, chID)
	if channel == "" {
		return fmt.Errorf("unsupported channel: %s", chID)
	}
	if typeOfPush == pubPushType {
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
		// Use Publication's Offset/Epoch for StreamPosition when available (map broker fan-out).
		// This takes precedence over metadata prefix since keyed publications carry position in the message.
		if pub.Offset > 0 {
			sp.Offset = pub.Offset
		}
		if pub.Epoch != "" {
			sp.Epoch = pub.Epoch
			pub.Epoch = "" // Clear — epoch travels in StreamPosition, not in Publication to clients.
		}
		if pub.Delta {
			// In at most once scenario we are passing delta in Publication itself. But need to clean it
			// before passing further.
			delta = true
			pub.Delta = false
		}
		if len(pub.PrevData) > 0 {
			// PrevData from map broker fan-out: carry previous data for delta computation.
			prevPub := &Publication{Data: pub.PrevData}
			pub.PrevData = nil // Clean before passing to Node layer.
			_ = eventHandler.HandlePublication(channel, pubFromProto(&pub), sp, true, prevPub)
		} else if delta && len(prevPayload) > 0 {
			var prevPub protocol.Publication
			err = prevPub.UnmarshalVT(prevPayload)
			if err != nil {
				return err
			}
			_ = eventHandler.HandlePublication(channel, pubFromProto(&pub), sp, true, pubFromProto(&prevPub))
		} else {
			_ = eventHandler.HandlePublication(channel, pubFromProto(&pub), sp, delta, nil)
		}
	} else if typeOfPush == joinPushType {
		var info protocol.ClientInfo
		err := info.UnmarshalVT(pushData)
		if err != nil {
			return err
		}
		_ = eventHandler.HandleJoin(channel, infoFromProto(&info))
	} else if typeOfPush == leavePushType {
		var info protocol.ClientInfo
		err := info.UnmarshalVT(pushData)
		if err != nil {
			return err
		}
		_ = eventHandler.HandleLeave(channel, infoFromProto(&info))
	}
	return nil
}

func (b *RedisBroker) historyStream(s *RedisShard, ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	historyKey := b.historyStreamKey(s, ch)
	historyMetaKey := b.historyMetaKey(s, ch)

	filter := opts.Filter

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

	historyMetaTTL := opts.MetaTTL
	if historyMetaTTL == 0 {
		historyMetaTTL = b.node.config.HistoryMetaTTL
	}

	historyMetaTTLSeconds := int(historyMetaTTL.Seconds())

	replies, err := b.historyStreamScript.Exec(context.Background(), s.client, []string{string(historyKey), string(historyMetaKey)}, []string{includePubs, strconv.FormatUint(offset, 10), strconv.Itoa(limit), reverse, strconv.Itoa(historyMetaTTLSeconds), epoch.Generate()}).ToArray()
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
			fieldValues, err := values[1].ToArray()
			if err != nil {
				return nil, StreamPosition{}, err
			}
			var pushData []byte
			for i := 0; i < len(fieldValues); i += 2 {
				k, _ := fieldValues[i].ToString()
				if k != "d" {
					continue
				}
				v, _ := fieldValues[i+1].ToString()
				pushData = convert.StringToBytes(v)
				break
			}
			if pushData == nil {
				return nil, StreamPosition{}, errors.New("no push data found in entry")
			}
			hyphenPos := strings.Index(id, "-") // ex. "4-0", 4 is our offset.
			if hyphenPos <= 0 {
				return nil, StreamPosition{}, fmt.Errorf("unexpected offset format: %s", id)
			}
			offset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
			if err != nil {
				return nil, StreamPosition{}, err
			}
			var pub protocol.Publication
			err = pub.UnmarshalVT(pushData)
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
	var rightBound = "-1"
	if filter.Limit == 0 {
		rightBound = "0"
		includePubs = "0"
	}

	historyMetaTTLSeconds := int(b.node.config.HistoryMetaTTL.Seconds())

	replies, err := b.historyListScript.Exec(context.Background(), s.client, []string{string(historyKey), string(historyMetaKey)}, []string{includePubs, rightBound, strconv.Itoa(historyMetaTTLSeconds), epoch.Generate()}).ToArray()
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

		pushData, _, sp, _, _, ok := extractPushData(convert.StringToBytes(value))
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
func extractPushData(data []byte) ([]byte, pushType, StreamPosition, bool, []byte, bool) {
	var offset uint64
	var epoch string
	if !bytes.HasPrefix(data, metaSep) {
		return data, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, true
	}

	content := data[len(metaSep):]
	if len(content) == 0 {
		return data, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, false
	}

	contentType := content[0]

	switch contentType {
	case 'j':
		// __j__payload.
		nextMetaSepPos := bytes.Index(data[len(metaSep):], metaSep)
		if nextMetaSepPos <= 0 {
			return data, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, false
		}
		rest := data[len(metaSep)+nextMetaSepPos+len(metaSep):]
		return rest, joinPushType, StreamPosition{}, false, nil, true
	case 'l':
		// __l__payload.
		nextMetaSepPos := bytes.Index(data[len(metaSep):], metaSep)
		if nextMetaSepPos <= 0 {
			return data, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, false
		}
		rest := data[len(metaSep)+nextMetaSepPos+len(metaSep):]
		return rest, leavePushType, StreamPosition{}, false, nil, true
	case 'p':
		// p1:offset:epoch__payload
		nextMetaSepPos := bytes.Index(data[len(metaSep):], metaSep)
		if nextMetaSepPos <= 0 {
			return data, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, false
		}
		header := data[len(metaSep) : len(metaSep)+nextMetaSepPos]
		stringHeader := convert.BytesToString(header)

		rest := data[len(metaSep)+nextMetaSepPos+len(metaSep):]

		stringHeader = stringHeader[3:] // offset:epoch
		epochDelimiterPos := strings.Index(stringHeader, contentSep)
		if epochDelimiterPos <= 0 {
			return rest, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, false
		}
		var err error
		offset, err = strconv.ParseUint(stringHeader[:epochDelimiterPos], 10, 64)
		epoch = stringHeader[epochDelimiterPos+1:]
		return rest, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, err == nil
	case 'd':
		// d1:offset:epoch:prev_payload_length:prev_payload:payload_length:payload
		stringContent := convert.BytesToString(content)
		parsedDelta, err := parseDeltaPush(stringContent)
		if err != nil {
			// Unexpected error.
			return nil, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, false
		}
		return convert.StringToBytes(parsedDelta.Payload), pubPushType, StreamPosition{Epoch: parsedDelta.Epoch, Offset: parsedDelta.Offset}, true, convert.StringToBytes(parsedDelta.PrevPayload), true
	default:
		// Unknown content type.
		return nil, pubPushType, StreamPosition{Epoch: epoch, Offset: offset}, false, nil, false
	}
}

type deltaPublicationPush struct {
	Offset            uint64
	Epoch             string
	PrevPayloadLength int
	PrevPayload       string
	PayloadLength     int
	Payload           string
}

func parseDeltaPush(input string) (deltaPublicationPush, error) {
	// d1:offset:epoch:prev_payload_length:prev_payload:payload_length:payload
	const prefix = "d1:"
	if !strings.HasPrefix(input, prefix) {
		return deltaPublicationPush{}, fmt.Errorf("input does not start with the expected prefix")
	}
	input = input[len(prefix):] // Remove prefix

	// offset:epoch:prev_payload_length:prev_payload:payload_length:payload

	idx := strings.IndexByte(input, ':')
	if idx == -1 {
		return deltaPublicationPush{}, fmt.Errorf("invalid format, missing offset")
	}
	offset, err := strconv.ParseUint(input[:idx], 10, 64)
	if err != nil {
		return deltaPublicationPush{}, fmt.Errorf("error parsing offset: %v", err)
	}
	input = input[idx+1:]

	// epoch:prev_payload_length:prev_payload:payload_length:payload

	idx = strings.IndexByte(input, ':')
	if idx == -1 {
		return deltaPublicationPush{}, fmt.Errorf("invalid format, missing epoch")
	}
	epoch := input[:idx]
	input = input[idx+1:]

	// prev_payload_length:prev_payload:payload_length:payload

	idx = strings.IndexByte(input, ':')
	if idx == -1 {
		return deltaPublicationPush{}, fmt.Errorf("invalid format, missing prev payload length")
	}
	prevPayloadLength, err := strconv.Atoi(input[:idx])
	if err != nil {
		return deltaPublicationPush{}, fmt.Errorf("error parsing prev payload length: %v", err)
	}

	input = input[idx+1:]

	// Extract prev_payload based on prev_payload_length
	if len(input) < prevPayloadLength {
		return deltaPublicationPush{}, fmt.Errorf("input is shorter than expected prev payload length")
	}
	prevPayload := input[:prevPayloadLength]
	input = input[prevPayloadLength+1:]

	// payload_length:payload
	idx = strings.IndexByte(input, ':')
	if idx == -1 {
		return deltaPublicationPush{}, fmt.Errorf("invalid format, missing payload")
	}
	payloadLength, err := strconv.Atoi(input[:idx])
	if err != nil {
		return deltaPublicationPush{}, fmt.Errorf("error parsing payload_length: %v", err)
	}
	input = input[idx+1:]

	// Extract payload based on payload_length
	if len(input) < payloadLength {
		return deltaPublicationPush{}, fmt.Errorf("input is shorter than expected payload length")
	}
	payload := input[:payloadLength]

	return deltaPublicationPush{
		Offset:            offset,
		Epoch:             epoch,
		PrevPayloadLength: prevPayloadLength,
		PrevPayload:       prevPayload,
		PayloadLength:     payloadLength,
		Payload:           payload,
	}, nil
}
