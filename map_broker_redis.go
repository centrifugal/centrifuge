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
	"time"

	_ "embed"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/centrifuge/internal/epoch"
	"github.com/centrifugal/centrifuge/internal/redispartition"
	"github.com/centrifugal/protocol"
	"github.com/redis/rueidis"
)

var (
	//go:embed internal/redis_lua/map_broker_add.lua
	brokerStatePublishScriptSource string
	//go:embed internal/redis_lua/map_broker_read_ordered.lua
	brokerStateReadOrderedScriptSource string
	//go:embed internal/redis_lua/map_broker_read_unordered.lua
	brokerStateReadUnorderedScriptSource string
	//go:embed internal/redis_lua/map_broker_stream_read.lua
	brokerStateReadStreamScriptSource string
	//go:embed internal/redis_lua/map_broker_read_meta.lua
	brokerStateReadMetaScriptSource string
	//go:embed internal/redis_lua/map_broker_stats.lua
	brokerStateStatsScriptSource string
	//go:embed internal/redis_lua/map_broker_find_expired.lua
	brokerStateFindExpiredScriptSource string
	//go:embed internal/redis_lua/map_broker_batch_remove.lua
	brokerStateBatchRemoveScriptSource string
)

type brokerShardWrapper struct {
	shard               *RedisShard
	subClientsMu        sync.Mutex
	subClients          [][]rueidis.DedicatedClient
	pubSubStartChannels [][]*pubSubStart
	pubSubRunner        mapBrokerPubSubRunner
}

// mapBrokerPubSubRunner abstracts the subscriber-side pub/sub strategy for
// RedisMapBroker. Mirrors brokerPubSubRunner; one instance per shard, selected
// at construction time. subClientsIndex maps a partition-derived cluster shard
// index to the first-dimension index used in brokerShardWrapper.subClients
// (default is identity; non-default strategies may remap, e.g. partition → node).
type mapBrokerPubSubRunner interface {
	init(s *brokerShardWrapper, shard *RedisShard) error
	run(s *brokerShardWrapper, h BrokerEventHandler) error
	subClientsIndex(clusterShardIdx int) int
}

// RedisMapBroker is a Redis-based MapBroker.
// Note – it does not work properly with Redis eviction, use with disabled eviction
// to avoid undefined state.
//
// Message Formats
// ===============
//
// This broker uses simplified message formats published by Lua scripts:
//
// 1. No prefix:
//   - Raw protobuf bytes (Publication)
//   - Used by direct PUBLISH calls without Lua scripts when stream is not used.
//
// 2. Non-delta publications:
//   - Format: "offset:epoch:Publication"
//   - Where Publication is in protobuf format.
//
// 3. Delta publications:
//   - Format: "d:offset:epoch:prev_len:prev_publication:curr_len:curr_publication"
//   - Where prev_publication and curr_publication are protocol.Publication in protobuf.
//   - Enables atomic publishing of current + previous publication for delta compression of publication data.
//   - Prev is atomically fetched from stream.
//
// Storage:
// - Streams (XADD): keeps protocol.Publication
// - States (HSET): For keyed state - may keep latest protocol.Publication or custom state.
//
// Pagination:
//   - ordered state use ZRANGEBYSCORE/ZRANGEBYLEX with LIMIT — exact page sizes.
//   - Unordered state use HSCAN with COUNT — COUNT is only a hint, Redis may return
//     more entries than requested (especially for small hashes in listpack encoding).
//     Callers should not rely on exact Limit enforcement for unordered reads.
type RedisMapBroker struct {
	node *Node
	conf RedisMapBrokerConfig

	shards []*brokerShardWrapper

	// partitionTags is non-nil when UsePrecomputedPartitionTags is enabled;
	// indexed by partition index, returns the hash tag string. Read-only
	// after construction (shared with the package-level precomputed table).
	partitionTags []string

	addScript           *rueidis.Lua
	readOrderedScript   *rueidis.Lua
	readUnorderedScript *rueidis.Lua
	readStreamScript    *rueidis.Lua
	readMetaScript      *rueidis.Lua
	presenceStatsScript *rueidis.Lua
	findExpiredScript   *rueidis.Lua
	batchRemoveScript   *rueidis.Lua

	closeCh       chan struct{}
	closeOnce     sync.Once
	shardChannel  string
	messagePrefix string
}

var _ MapBroker = (*RedisMapBroker)(nil)

// RedisMapBrokerConfig is a config for RedisMapBroker.
type RedisMapBrokerConfig struct {
	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	Shards []*RedisShard
	// Prefix to use before every channel name and key in Redis.
	Prefix string
	// Name of broker, for observability purposes – i.e. becomes part of metrics/logs labels.
	// By default, empty string is used.
	Name string
	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
	// it on the client side. This is useful for FIPS compliance.
	LoadSHA1 bool
	// IdempotentResultTTL is a time-to-live for idempotent result.
	IdempotentResultTTL time.Duration
	// SubscribeOnReplica allows subscribing on replica Redis nodes.
	SubscribeOnReplica bool
	// SkipPubSub enables mode when broker only works with data structures, without
	// publishing to channels and using PUB/SUB.
	SkipPubSub bool
	// NumShardedPubSubPartitions when greater than zero allows turning on a mode in which
	// broker will use Redis Cluster with sharded PUB/SUB feature available in
	// Redis >= 7: https://redis.io/docs/manual/pubsub/#sharded-pubsub
	NumShardedPubSubPartitions int

	// UsePrecomputedPartitionTags switches sharded PUB/SUB partition hash tags
	// from the bare partition index ("0", "1", ...) to a precomputed table
	// chosen so CRC16 hash slots distribute evenly across any cluster size.
	// See RedisBrokerConfig.UsePrecomputedPartitionTags for details and
	// migration constraints.
	//
	// When enabled, NumShardedPubSubPartitions must equal one of the sizes
	// returned by redispartition.PrecomputedSizes() (16, 32, 64, 128, 256,
	// 512, 1024, 2048, 4096). Construction returns an error otherwise. The
	// exact tag table is bundled in internal/redispartition/precomputed.go;
	// the tags are stable and will not change.
	//
	// Default: false (backward-compatible bare-index tags).
	UsePrecomputedPartitionTags bool

	// numSubscribeShards defines how many subscribe shards will be used.
	numSubscribeShards int
	// numResubscribeShards defines how many subscriber goroutines will be used for
	// resubscribing process for each subscribe shard.
	numResubscribeShards int
	// numPubSubProcessors allows configuring number of workers which will process
	// messages coming from Redis PUB/SUB.
	numPubSubProcessors int

	// CleanupInterval defines how often to run the cleanup worker that
	// generates remove events for expired keyed state entries (presence and state).
	// Default is 1 second. Set to -1 to disable cleanup (make sure you understand the consequences).
	// Applies to all channels using TTL-based state.
	CleanupInterval time.Duration
	// CleanupBatchSize defines max entries to process per channel per cleanup cycle.
	// Default is 100. Applies to all keyed state (presence and state).
	CleanupBatchSize int
}

// NewRedisMapBroker initializes RedisMapBroker.
func NewRedisMapBroker(n *Node, conf RedisMapBrokerConfig) (*RedisMapBroker, error) {
	if len(conf.Shards) == 0 {
		return nil, errors.New("state broker: no shards provided")
	}

	if conf.SubscribeOnReplica {
		for i, s := range conf.Shards {
			if s.replicaClient == nil {
				return nil, fmt.Errorf("broker: SubscribeOnReplica enabled but no replica client initialized in shard[%d] (ReplicaClientEnabled option)", i)
			}
		}
	}

	if conf.Prefix == "" {
		conf.Prefix = "centrifuge"
	}
	if conf.IdempotentResultTTL == 0 {
		conf.IdempotentResultTTL = 5 * time.Minute
	}

	if conf.numSubscribeShards == 0 {
		conf.numSubscribeShards = 1
	}
	if conf.numResubscribeShards == 0 {
		conf.numResubscribeShards = 16
	}
	if conf.numPubSubProcessors == 0 {
		conf.numPubSubProcessors = runtime.NumCPU() / conf.numSubscribeShards
		if conf.NumShardedPubSubPartitions > 0 {
			conf.numPubSubProcessors /= conf.NumShardedPubSubPartitions
		}
		if conf.numPubSubProcessors < 1 {
			conf.numPubSubProcessors = 1
		}
	}
	if conf.CleanupBatchSize == 0 {
		conf.CleanupBatchSize = 100
	}
	if conf.CleanupInterval == 0 {
		conf.CleanupInterval = time.Second
	}

	var partitionTags []string
	if conf.UsePrecomputedPartitionTags {
		if conf.NumShardedPubSubPartitions <= 0 {
			return nil, errors.New("state broker: UsePrecomputedPartitionTags requires NumShardedPubSubPartitions > 0")
		}
		tags, err := redispartition.FindTags(conf.NumShardedPubSubPartitions)
		if err != nil {
			return nil, fmt.Errorf("state broker: %w", err)
		}
		partitionTags = tags
	}

	shardWrappers := make([]*brokerShardWrapper, 0, len(conf.Shards))
	for _, s := range conf.Shards {
		shardWrappers = append(shardWrappers, &brokerShardWrapper{shard: s})
	}

	e := &RedisMapBroker{
		node:          n,
		conf:          conf,
		shards:        shardWrappers,
		partitionTags: partitionTags,
		addScript: rueidis.NewLuaScript(
			brokerStatePublishScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readOrderedScript: rueidis.NewLuaScript(
			brokerStateReadOrderedScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readUnorderedScript: rueidis.NewLuaScript(
			brokerStateReadUnorderedScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readStreamScript: rueidis.NewLuaScript(
			brokerStateReadStreamScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readMetaScript: rueidis.NewLuaScript(
			brokerStateReadMetaScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		presenceStatsScript: rueidis.NewLuaScript(
			brokerStateStatsScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		findExpiredScript: rueidis.NewLuaScript(
			brokerStateFindExpiredScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		batchRemoveScript: rueidis.NewLuaScript(
			brokerStateBatchRemoveScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		closeCh: make(chan struct{}),
	}
	e.shardChannel = conf.Prefix + redisPubSubShardChannelSuffix
	e.messagePrefix = conf.Prefix + redisClientChannelPrefix

	for _, wrapper := range e.shards {
		shard := wrapper.shard
		if !shard.isCluster && e.conf.NumShardedPubSubPartitions > 0 {
			return nil, errors.New("can use sharded PUB/SUB feature (non-zero number of pub/sub partitions) only with Redis Cluster")
		}
		if shard.isCluster && e.conf.NumShardedPubSubPartitions == 0 {
			return nil, errors.New("redis cluster requires sharded PUB/SUB (set NumShardedPubSubPartitions > 0)")
		}
		if newMapBrokerPubSubRunnerHook != nil {
			wrapper.pubSubRunner = newMapBrokerPubSubRunnerHook(e, shard)
		}
		if wrapper.pubSubRunner == nil {
			wrapper.pubSubRunner = &defaultMapBrokerPubSubRunner{broker: e}
		}
		if err := wrapper.pubSubRunner.init(wrapper, shard); err != nil {
			return nil, err
		}
	}

	if e.conf.CleanupInterval > 0 {
		go e.runCleanupWorker(context.Background())
		go e.runCleanupLagWorker(context.Background())
	}

	return e, nil
}

// parseStateValue parses a state value in format: offset:epoch:payload
// Returns offset, epoch, payload, and error if parsing fails.
func parseStateValue(val []byte) (uint64, string, []byte, error) {
	if len(val) == 0 {
		return 0, "", nil, fmt.Errorf("empty state value")
	}

	// Find first colon (offset separator)
	firstColon := bytes.IndexByte(val, ':')
	if firstColon == -1 {
		return 0, "", nil, fmt.Errorf("missing offset separator in state value")
	}

	// Parse offset
	offset, err := strconv.ParseUint(convert.BytesToString(val[:firstColon]), 10, 64)
	if err != nil {
		return 0, "", nil, fmt.Errorf("invalid offset in state value: %w", err)
	}

	// Find second colon (epoch separator)
	remaining := val[firstColon+1:]
	secondColon := bytes.IndexByte(remaining, ':')
	if secondColon == -1 {
		return 0, "", nil, fmt.Errorf("missing epoch separator in state value")
	}

	// Extract epoch
	epoch := convert.BytesToString(remaining[:secondColon])

	// Everything after second colon is payload
	payload := remaining[secondColon+1:]

	return offset, epoch, payload, nil
}

func (e *RedisMapBroker) useShardedPubSub(s *RedisShard) bool {
	return s.isCluster && e.conf.NumShardedPubSubPartitions > 0
}

func (e *RedisMapBroker) getShard(channel string) *brokerShardWrapper {
	if len(e.shards) == 1 {
		return e.shards[0]
	}
	return e.shards[consistentIndex(channel, len(e.shards))]
}

func (e *RedisMapBroker) streamKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":stream:")
}

func (e *RedisMapBroker) metaKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":meta:")
}

func (e *RedisMapBroker) stateHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":state:")
}

func (e *RedisMapBroker) stateOrderKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":state:order:")
}

func (e *RedisMapBroker) stateExpireKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":state:expire:")
}

func (e *RedisMapBroker) stateMetaKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":state:meta:")
}

func (e *RedisMapBroker) cleanupRegistrationKeyForChannel(s *RedisShard, ch string) string {
	// Get the cleanup registration key with proper hash tag for the channel's partition
	// Single registration ZSET works for ALL keyed state (presence, state, etc.)
	if !s.isCluster {
		return e.conf.Prefix + ":cleanup:channels"
	}
	idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
	return e.conf.Prefix + ":cleanup:channels:{" + e.pubSubPartitionHashTag(idx) + "}"
}

func (e *RedisMapBroker) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) string {
	if !s.isCluster {
		var builder strings.Builder
		builder.Grow(len(e.conf.Prefix) + 9 + len(ch) + 1 + len(idempotencyKey))
		builder.WriteString(e.conf.Prefix)
		builder.WriteString(".result.")
		builder.WriteString(ch)
		builder.WriteByte('.')
		builder.WriteString(idempotencyKey)
		return builder.String()
	}

	idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
	idxStr := e.pubSubPartitionHashTag(idx)

	var builder strings.Builder
	capacity := len(e.conf.Prefix) + 9 + 1 + len(idxStr) + 2 + len(ch) + 1 + len(idempotencyKey)
	builder.Grow(capacity)
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(".result.{")
	builder.WriteString(idxStr)
	builder.WriteString("}.")
	builder.WriteString(ch)
	builder.WriteByte('.')
	builder.WriteString(idempotencyKey)

	return builder.String()
}

// buildKey is a helper function to build Redis keys with proper cluster hash tag support
func (e *RedisMapBroker) buildKey(s *RedisShard, ch string, infix string) string {
	if !s.isCluster {
		var builder strings.Builder
		builder.Grow(len(e.conf.Prefix) + len(infix) + len(ch))
		builder.WriteString(e.conf.Prefix)
		builder.WriteString(infix)
		builder.WriteString(ch)
		return builder.String()
	}

	idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
	idxStr := e.pubSubPartitionHashTag(idx)

	var builder strings.Builder
	capacity := len(e.conf.Prefix) + len(infix) + 1 + len(idxStr) + 2 + len(ch)
	builder.Grow(capacity)
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(infix)
	builder.WriteByte('{')
	builder.WriteString(idxStr)
	builder.WriteString("}.")
	builder.WriteString(ch)

	return builder.String()
}

func (e *RedisMapBroker) messageChannelID(s *RedisShard, ch string) string {
	if !e.useShardedPubSub(s) {
		var builder strings.Builder
		builder.Grow(len(e.messagePrefix) + len(ch))
		builder.WriteString(e.messagePrefix)
		builder.WriteString(ch)
		return builder.String()
	}

	idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
	idxStr := e.pubSubPartitionHashTag(idx)

	capacity := len(e.messagePrefix) + 1 + len(idxStr) + 2 + len(ch)

	var builder strings.Builder
	builder.Grow(capacity)
	builder.WriteString(e.messagePrefix)
	builder.WriteByte('{')
	builder.WriteString(idxStr)
	builder.WriteString("}.")
	builder.WriteString(ch)

	return builder.String()
}

// Close closes the broker.
func (e *RedisMapBroker) Close(_ context.Context) error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
	return nil
}

func (e *RedisMapBroker) Clear(ctx context.Context, ch string, _ MapClearOptions) error {
	s := e.getShard(ch)
	shard := s.shard
	client := shard.client

	dataKeys := []string{
		e.streamKey(shard, ch),
		e.metaKey(shard, ch),
		e.stateHashKey(shard, ch),
		e.stateOrderKey(shard, ch),
		e.stateExpireKey(shard, ch),
		e.stateMetaKey(shard, ch),
	}

	cmds := make(rueidis.Commands, 0, 2)
	cmds = append(cmds, client.B().Del().Key(dataKeys...).Build())
	cleanupKey := e.cleanupRegistrationKeyForChannel(shard, ch)
	cmds = append(cmds, client.B().Zrem().Key(cleanupKey).Member(ch).Build())

	results := client.DoMulti(ctx, cmds...)
	for _, res := range results {
		if err := res.Error(); err != nil {
			return err
		}
	}
	return nil
}

func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// millis converts a duration to a milliseconds string.
// Zero/negative returns "0".
func millis(d time.Duration) string {
	if d <= 0 {
		return "0"
	}
	return strconv.FormatInt(d.Milliseconds(), 10)
}

// Publish publishes data to a stateful channel with optional keyed state.
func (e *RedisMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	// Resolve channel options once for this operation.
	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapUpdateResult{}, err
	}

	// Reject CAS and Version in ephemeral mode.
	if chOpts.Mode.IsEphemeral() {
		if opts.ExpectedPosition != nil {
			return MapUpdateResult{}, errors.New("CAS (ExpectedPosition) requires recoverable or persistent mode")
		}
		if opts.Version > 0 {
			return MapUpdateResult{}, errors.New("version-based dedup requires recoverable or persistent mode")
		}
	}

	// Fast path for non-history, non-idempotent, non-keyed publications.
	if chOpts.Mode.IsEphemeral() && opts.IdempotencyKey == "" && key == "" {
		if e.conf.SkipPubSub {
			return MapUpdateResult{}, nil
		}
		protoPub := &protocol.Publication{
			Data: opts.Data,
			Info: infoToProto(opts.ClientInfo),
			Tags: opts.Tags,
			Time: time.Now().UnixMilli(),
		}
		protoPub.Delta = opts.UseDelta

		pubBytes, err := protoPub.MarshalVT()
		if err != nil {
			return MapUpdateResult{}, err
		}

		payload := "0::" + convert.BytesToString(pubBytes)

		chID := e.messageChannelID(s.shard, ch)
		if e.useShardedPubSub(s.shard) {
			cmd := shardClient.B().Spublish().Channel(chID).Message(payload).Build()
			return MapUpdateResult{}, shardClient.Do(ctx, cmd).Error()
		}
		cmd := shardClient.B().Publish().Channel(chID).Message(payload).Build()
		return MapUpdateResult{}, shardClient.Do(ctx, cmd).Error()
	}

	now := time.Now().UnixMilli()

	// Stream publication (used for stream and pub/sub).
	streamProtoPub := &protocol.Publication{
		Data:  opts.Data,
		Info:  infoToProto(opts.ClientInfo),
		Tags:  opts.Tags,
		Time:  now,
		Key:   key,
		Score: opts.score,
	}
	streamBytes, err := streamProtoPub.MarshalVT()
	if err != nil {
		return MapUpdateResult{}, err
	}

	// stateBytes is nil — Lua will use streamBytes for both state and stream.
	var stateBytes []byte

	var resultKey string
	var resultExpire string
	if opts.IdempotencyKey != "" {
		resultKey = e.resultCacheKey(s.shard, ch, opts.IdempotencyKey)
		if opts.IdempotentResultTTL > 0 {
			resultExpire = millis(opts.IdempotentResultTTL)
		} else {
			resultExpire = millis(e.conf.IdempotentResultTTL)
		}
	}

	var streamKey, metaKey, stateHashKey, stateOrderKey, stateExpireKey, stateMetaKey string
	streamless := chOpts.Mode.IsEphemeral()

	if !streamless {
		streamKey = e.streamKey(s.shard, ch)
		metaKey = e.metaKey(s.shard, ch)
	}

	ordered := chOpts.ordered

	if key != "" {
		stateHashKey = e.stateHashKey(s.shard, ch)
		if !streamless {
			// State meta key tracks epoch for consistency between state and stream.
			// In streamless mode, skip it to prevent multi-node epoch mismatch clearing state.
			stateMetaKey = e.stateMetaKey(s.shard, ch)
		}
		if ordered {
			stateOrderKey = e.stateOrderKey(s.shard, ch)
		}
		stateExpireKey = e.stateExpireKey(s.shard, ch)
	}

	metaExpire := millis(chOpts.MetaTTL)

	useDelta := "0"
	if opts.UseDelta {
		useDelta = "1"
	}

	version := "0"
	if opts.Version > 0 {
		version = strconv.FormatUint(opts.Version, 10)
	}

	publishCommand := "PUBLISH"
	if e.useShardedPubSub(s.shard) {
		publishCommand = "SPUBLISH"
	}
	if e.conf.SkipPubSub {
		publishCommand = ""
	}

	chID := e.messageChannelID(s.shard, ch)
	if e.conf.SkipPubSub {
		chID = ""
	}

	// Setup cleanup registration if KeyTTL is set (keyed state with expiration)
	cleanupRegKey := ""
	if chOpts.KeyTTL > 0 && key != "" && stateExpireKey != "" {
		cleanupRegKey = e.cleanupRegistrationKeyForChannel(s.shard, ch)
	}

	// Prepare ExpectedPosition arguments for CAS
	expectedOffset := ""
	expectedEpoch := ""
	if opts.ExpectedPosition != nil {
		expectedOffset = strconv.FormatUint(opts.ExpectedPosition.Offset, 10)
		expectedEpoch = opts.ExpectedPosition.Epoch
	}

	// In Redis Cluster, all KEYS in a Lua script must hash to the same slot. Empty string
	// keys hash to slot 0, which differs from the hash-tagged real keys. Compute a slot-
	// aligned nil key placeholder and substitute it for any empty KEYS. The Lua script
	// converts these back to '' using ARGV[23].
	nilKey := ""
	if s.shard.isCluster {
		nilKey = e.buildKey(s.shard, ch, ":nil:")
		if streamKey == "" {
			streamKey = nilKey
		}
		if metaKey == "" {
			metaKey = nilKey
		}
		if resultKey == "" {
			resultKey = nilKey
		}
		if stateHashKey == "" {
			stateHashKey = nilKey
		}
		if stateOrderKey == "" {
			stateOrderKey = nilKey
		}
		if stateExpireKey == "" {
			stateExpireKey = nilKey
		}
		if stateMetaKey == "" {
			stateMetaKey = nilKey
		}
		if cleanupRegKey == "" {
			cleanupRegKey = nilKey
		}
	}

	// Pre-compute per-key version field names for Lua.
	versionField, versionEpochField := "", ""
	if opts.Version > 0 && key != "" {
		versionField = "v:" + key
		versionEpochField = "ve:" + key
	}

	replies, err := e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, resultKey, stateHashKey, stateOrderKey, stateExpireKey,
			stateMetaKey, cleanupRegKey,
		},
		[]string{
			key,                                // message_key
			convert.BytesToString(streamBytes), // message_payload (Publication - for stream and publishing)
			strconv.Itoa(chOpts.StreamSize),
			millis(chOpts.StreamTTL),
			chID, // channel (for Lua to publish)
			metaExpire,
			epoch.Generate(), // new_epoch_if_empty
			publishCommand,
			resultExpire,
			useDelta,
			version,
			opts.VersionEpoch,
			"0", // is_remove
			strconv.FormatInt(opts.score, 10),
			millis(chOpts.KeyTTL),
			"0",                                  // use_hpexpire
			ch,                                   // channel_for_cleanup (for cleanup registration)
			string(opts.KeyMode),                 // key_mode
			boolToStr(opts.RefreshTTLOnSuppress), // refresh_ttl_on_suppress
			expectedOffset,                       // expected_offset (for CAS)
			expectedEpoch,                        // expected_epoch (for CAS)
			convert.BytesToString(stateBytes),    // state_payload (for state storage, empty to use message_payload)
			nilKey,                               // nil_key (slot-aligned placeholder for unused KEYS)
			versionField,                         // version_field (pre-computed "v:KEY" or "")
			versionEpochField,                    // version_epoch_field (pre-computed "ve:KEY" or "")
			strconv.FormatInt(now, 10),           // now (current time in milliseconds)
		},
	).ToArray()
	if err != nil {
		return MapUpdateResult{}, err
	}

	return parseAddScriptResult(replies)
}

// Remove removes a key from keyed state state.
func (e *RedisMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	// Resolve channel options once for this operation.
	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapUpdateResult{}, err
	}

	// Reject CAS in ephemeral mode.
	if chOpts.Mode.IsEphemeral() {
		if opts.ExpectedPosition != nil {
			return MapUpdateResult{}, errors.New("CAS (ExpectedPosition) requires recoverable or persistent mode")
		}
	}

	var streamKey, metaKey string
	if chOpts.Mode.HasStream() {
		streamKey = e.streamKey(s.shard, ch)
		metaKey = e.metaKey(s.shard, ch)
	}

	streamless := chOpts.Mode.IsEphemeral()

	// For unpublish, we use state keys to track which keys exist
	stateHashKey := e.stateHashKey(s.shard, ch)
	stateExpireKey := e.stateExpireKey(s.shard, ch)
	var stateMetaKey string
	if !streamless {
		// State meta key tracks epoch for consistency between state and stream.
		// In streamless mode, skip it to prevent multi-node epoch mismatch clearing state.
		stateMetaKey = e.stateMetaKey(s.shard, ch)
	}

	metaExpire := millis(chOpts.MetaTTL)

	publishCommand := "PUBLISH"
	if e.useShardedPubSub(s.shard) {
		publishCommand = "SPUBLISH"
	}
	chID := e.messageChannelID(s.shard, ch)
	if e.conf.SkipPubSub {
		publishCommand = ""
		chID = ""
	}

	now := time.Now().UnixMilli()

	// Create a Publication with key and removed=true to signal removal.
	// Include opts.Tags so server-side tags filtering can route the removal correctly.
	protoPub := &protocol.Publication{
		Key:     key,
		Removed: true,
		Time:    now,
		Tags:    opts.Tags,
	}
	pubBytes, err := protoPub.MarshalVT()
	if err != nil {
		return MapUpdateResult{}, err
	}

	// Handle idempotency key for remove operations.
	var resultKey string
	var resultExpire string
	if opts.IdempotencyKey != "" {
		resultKey = e.resultCacheKey(s.shard, ch, opts.IdempotencyKey)
		if opts.IdempotentResultTTL > 0 {
			resultExpire = millis(opts.IdempotentResultTTL)
		} else {
			resultExpire = millis(e.conf.IdempotentResultTTL)
		}
	}

	// Prepare ExpectedPosition arguments for CAS
	expectedOffset := ""
	expectedEpoch := ""
	if opts.ExpectedPosition != nil {
		expectedOffset = strconv.FormatUint(opts.ExpectedPosition.Offset, 10)
		expectedEpoch = opts.ExpectedPosition.Epoch
	}

	// Compute slot-aligned nil key for unused KEYS in cluster mode (see Publish for details).
	nilKey := ""
	stateOrderKey := ""
	cleanupRegKey := ""
	if s.shard.isCluster {
		nilKey = e.buildKey(s.shard, ch, ":nil:")
		if resultKey == "" {
			resultKey = nilKey
		}
		stateOrderKey = nilKey
		cleanupRegKey = nilKey
		if streamKey == "" {
			streamKey = nilKey
		}
		if metaKey == "" {
			metaKey = nilKey
		}
		if stateMetaKey == "" {
			stateMetaKey = nilKey
		}
	}

	// Pre-compute per-key version field names for cleanup on remove.
	versionField, versionEpochField := "", ""
	if key != "" {
		versionField = "v:" + key
		versionEpochField = "ve:" + key
	}

	replies, err := e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, resultKey,
			stateHashKey,
			stateOrderKey,
			stateExpireKey,
			stateMetaKey,
			cleanupRegKey,
		},
		[]string{
			key,                             // message_key
			convert.BytesToString(pubBytes), // message_payload (Publication with Removed=true for stream)
			strconv.Itoa(chOpts.StreamSize),
			millis(chOpts.StreamTTL),
			chID, // channel (for Lua to publish)
			metaExpire,
			epoch.Generate(), // new_epoch_if_empty
			publishCommand,
			resultExpire, // result_key_expire
			"0", "0", "", // use_delta, version, version_epoch
			"1",                        // is_leave (this triggers removal)
			"0",                        // score
			"0",                        // map_member_ttl
			"0",                        // use_hpexpire
			"",                         // channel_for_cleanup (not used for unpublish)
			"",                         // key_mode (not used for unpublish)
			"0",                        // refresh_ttl_on_suppress (not used for unpublish)
			expectedOffset,             // expected_offset (for CAS)
			expectedEpoch,              // expected_epoch (for CAS)
			"",                         // state_payload (not used for unpublish)
			nilKey,                     // nil_key (slot-aligned placeholder for unused KEYS)
			versionField,               // version_field (pre-computed "v:KEY" or "")
			versionEpochField,          // version_epoch_field (pre-computed "ve:KEY" or "")
			strconv.FormatInt(now, 10), // now (current time in milliseconds)
		},
	).ToArray()
	if err != nil {
		return MapUpdateResult{}, err
	}

	return parseAddScriptResult(replies)
}

// ReadState retrieves state entries with per-entry revisions for a channel.
// Each entry includes its revision so client can filter: entry.Revision <= state_revision.
// If opts.Revision is provided and epoch changed, returns empty entries.
// Returns entries, stream position, next cursor for pagination, and error.
// Cursor "0" or "" means end of iteration.
func (e *RedisMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	// Resolve channel options once for this operation.
	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapStateResult{}, err
	}

	// Handle single key lookup (Key filter) — takes priority over Limit.
	if opts.Key != "" {
		return e.readSingleKeyWithOpts(ctx, ch, opts, chOpts)
	}
	// Limit=0: return only stream position (no entries).
	if opts.Limit == 0 {
		streamResult, err := e.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{Limit: 0},
		})
		if err != nil {
			return MapStateResult{}, err
		}
		return MapStateResult{Position: streamResult.Position}, nil
	}
	if chOpts.ordered {
		return e.readOrderedState(ctx, ch, opts, chOpts)
	}
	return e.readUnorderedState(ctx, ch, opts, chOpts)
}

//func (e *RedisMapBroker) ReadStateZero(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
//	// Resolve channel options once for this operation.
//	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
//	if err != nil {
//		return MapStateResult{}, err
//	}
//
//	// Handle single key lookup (Key filter) — takes priority over Limit.
//	if opts.Key != "" {
//		return e.readSingleKeyWithOpts(ctx, ch, opts, chOpts)
//	}
//	// Limit=0: return only stream position (no entries).
//	if opts.Limit == 0 {
//		streamResult, err := e.ReadStream(ctx, ch, MapReadStreamOptions{
//			Filter: StreamFilter{Limit: 0},
//		})
//		if err != nil {
//			return MapStateResult{}, err
//		}
//		return MapStateResult{Position: streamResult.Position}, nil
//	}
//	if chOpts.ordered {
//		return e.readOrderedState(ctx, ch, opts, chOpts)
//	}
//	return e.readUnorderedStateZero(ctx, ch, opts, chOpts)
//}

// readSingleKeyWithOpts retrieves a single key from the state using HGET instead of HSCAN.
// This is more efficient for single key lookups and supports CAS read-modify-write patterns.
func (e *RedisMapBroker) readSingleKeyWithOpts(ctx context.Context, ch string, opts MapReadStateOptions, chOpts MapChannelOptions) (MapStateResult, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	stateHashKey := e.stateHashKey(s.shard, ch)

	streamless := chOpts.Mode.IsEphemeral()

	if streamless {
		// Streamless mode: just read the key, no meta needed.
		valBytes, err := shardClient.Do(ctx, shardClient.B().Hget().Key(stateHashKey).Field(opts.Key).Build()).AsBytes()
		keyNotFound := rueidis.IsRedisNil(err)
		if err != nil && !keyNotFound {
			return MapStateResult{}, err
		}
		if keyNotFound || len(valBytes) == 0 {
			return MapStateResult{}, nil
		}
		entryOffset, _, payload, err := parseStateValue(valBytes)
		if err != nil {
			return MapStateResult{}, fmt.Errorf("failed to parse state value: %w", err)
		}
		var protoPub protocol.Publication
		if err := protoPub.UnmarshalVT(payload); err != nil {
			return MapStateResult{}, fmt.Errorf("failed to unmarshal publication: %w", err)
		}
		pub := pubFromProto(&protoPub)
		pub.Key = opts.Key
		pub.Offset = entryOffset
		return MapStateResult{Publications: []*Publication{pub}}, nil
	}

	metaKey := e.metaKey(s.shard, ch)

	// Execute HGET and metadata read in pipeline
	cmds := make([]rueidis.Completed, 0, 2)
	cmds = append(cmds, shardClient.B().Hget().Key(stateHashKey).Field(opts.Key).Build())
	cmds = append(cmds, shardClient.B().Hmget().Key(metaKey).Field("s", "e").Build())

	results := shardClient.DoMulti(ctx, cmds...)

	// Parse HGET result
	valBytes, err := results[0].AsBytes()
	keyNotFound := rueidis.IsRedisNil(err)
	if err != nil && !keyNotFound {
		return MapStateResult{}, err
	}

	// Parse metadata
	metaArr, err := results[1].ToArray()
	if err != nil {
		return MapStateResult{}, err
	}

	var streamPos StreamPosition
	if len(metaArr) >= 2 {
		offsetVal, _ := metaArr[0].AsUint64()
		epochVal, _ := metaArr[1].ToString()
		streamPos = StreamPosition{Offset: offsetVal, Epoch: epochVal}
	}

	// Validate epoch if client provided state revision
	if opts.Revision != nil && opts.Revision.Epoch != streamPos.Epoch {
		return MapStateResult{Position: streamPos}, ErrorUnrecoverablePosition
	}

	// Key not found - return empty
	if keyNotFound || len(valBytes) == 0 {
		return MapStateResult{Position: streamPos}, nil
	}

	// Parse value: offset:epoch:payload
	entryOffset, _, payload, err := parseStateValue(valBytes)
	if err != nil {
		return MapStateResult{Position: streamPos}, fmt.Errorf("failed to parse state value: %w", err)
	}

	// Unmarshal Publication from protobuf payload
	var protoPub protocol.Publication
	if err := protoPub.UnmarshalVT(payload); err != nil {
		return MapStateResult{Position: streamPos}, fmt.Errorf("failed to unmarshal publication: %w", err)
	}

	pub := pubFromProto(&protoPub)
	pub.Key = opts.Key
	pub.Offset = entryOffset

	return MapStateResult{Publications: []*Publication{pub}, Position: streamPos}, nil
}

func (e *RedisMapBroker) readUnorderedState(ctx context.Context, ch string, opts MapReadStateOptions, chOpts MapChannelOptions) (MapStateResult, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	stateTTL := "0"
	if chOpts.MetaTTL > 0 {
		stateTTL = millis(chOpts.MetaTTL)
	}

	// Cursor must be "0" to start for Redis HSCAN
	cursor := opts.Cursor
	if cursor == "" {
		cursor = "0"
	}

	streamlessFlag := "0"
	if chOpts.Mode.IsEphemeral() {
		streamlessFlag = "1"
	}

	// In Lua scripts, limit=0 means "return all", limit>0 means paginate.
	// Convert negative limit (e.g. -1 = all) to 0 for Lua.
	luaLimit := opts.Limit
	if luaLimit < 0 {
		luaLimit = 0
	}

	replies, err := e.readUnorderedScript.Exec(ctx, shardClient,
		[]string{
			e.stateHashKey(s.shard, ch),
			e.stateExpireKey(s.shard, ch),
			e.metaKey(s.shard, ch),
			e.stateMetaKey(s.shard, ch),
		},
		[]string{
			cursor,
			strconv.Itoa(luaLimit),
			strconv.FormatInt(time.Now().UnixMilli(), 10),
			millis(chOpts.MetaTTL),
			stateTTL,
			streamlessFlag,
		},
	).ToArray()
	if err != nil {
		return MapStateResult{}, err
	}
	if len(replies) < 4 {
		return MapStateResult{}, errors.New("wrong number of replies")
	}

	streamOffset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return MapStateResult{}, err
	}
	streamEpoch, _ := replies[1].ToString()
	nextCursor, _ := replies[2].ToString()
	if nextCursor == "0" {
		nextCursor = "" // Convert Redis HSCAN "0" (complete) to empty string
	}
	dataArr, _ := replies[3].ToArray()

	streamPos := StreamPosition{Offset: streamOffset, Epoch: streamEpoch}

	// Validate epoch if client provided state revision
	if opts.Revision != nil && opts.Revision.Epoch != streamEpoch {
		// Epoch changed, client needs to restart from beginning
		return MapStateResult{Position: streamPos, Cursor: nextCursor}, ErrorUnrecoverablePosition
	}

	// Parse state values with revisions
	pubs := make([]*Publication, 0, len(dataArr)/2)
	for i := 0; i < len(dataArr); i += 2 {
		key, _ := dataArr[i].ToString()
		val, _ := dataArr[i+1].AsBytes()

		// Parse value: offset:epoch:payload
		entryOffset, _, payload, err := parseStateValue(val)
		if err != nil {
			// Skip malformed entries
			continue
		}

		// Unmarshal Publication from protobuf payload
		var protoPub protocol.Publication
		if err := protoPub.UnmarshalVT(payload); err != nil {
			// Skip malformed entries
			continue
		}

		pub := pubFromProto(&protoPub)
		pub.Key = key
		pub.Offset = entryOffset
		pubs = append(pubs, pub)
	}
	return MapStateResult{Publications: pubs, Position: streamPos, Cursor: nextCursor}, nil
}

//
//func (e *RedisMapBroker) readUnorderedStateZero(
//	ctx context.Context,
//	ch string,
//	opts MapReadStateOptions,
//	chOpts MapChannelOptions,
//) (MapStateResult, error) {
//	s := e.getShard(ch)
//
//	stateTTL := "0"
//	if chOpts.MetaTTL > 0 {
//		stateTTL = millis(chOpts.MetaTTL)
//	}
//
//	cursor := opts.Cursor
//	if cursor == "" {
//		cursor = "0"
//	}
//
//	var (
//		streamPos  StreamPosition
//		nextCursor string
//		pubs       []*Publication
//	)
//
//	streamlessFlag := "0"
//	if chOpts.Mode.IsEphemeral() {
//		streamlessFlag = "1"
//	}
//
//	// In Lua scripts, limit=0 means "return all", limit>0 means paginate.
//	luaLimit := opts.Limit
//	if luaLimit < 0 {
//		luaLimit = 0
//	}
//
//	err := e.readUnorderedScript.ExecWithReader(
//		ctx,
//		s.shard.client,
//		[]string{
//			e.stateHashKey(s.shard, ch),
//			e.stateExpireKey(s.shard, ch),
//			e.metaKey(s.shard, ch),
//			e.stateMetaKey(s.shard, ch),
//		},
//		[]string{
//			cursor,
//			strconv.Itoa(luaLimit),
//			strconv.FormatInt(time.Now().UnixMilli(), 10),
//			millis(chOpts.MetaTTL),
//			stateTTL,
//			streamlessFlag,
//		},
//		func(r *bufio.Reader) error {
//			rr := resp.NewReader(r)
//			if err := rr.ExpectArrayWithLen(4); err != nil {
//				return err
//			}
//
//			// --- reply[0]: offset ---
//			offsetBytes, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			if offsetBytes != nil {
//				streamPos.Offset, err = strconv.ParseUint(
//					unsafe.String(&offsetBytes[0], len(offsetBytes)), 10, 64,
//				)
//				if err != nil {
//					return err
//				}
//			}
//
//			// --- reply[1]: epoch ---
//			epochBytes, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			if epochBytes != nil {
//				streamPos.Epoch = unsafe.String(&epochBytes[0], len(epochBytes))
//			}
//
//			// --- reply[2]: cursor ---
//			cursorBytes, err := rr.ReadStringBytes()
//			if err != nil && !errors.Is(err, rueidis.Nil) {
//				return err
//			}
//			if cursorBytes != nil {
//				nextCursor = unsafe.String(&cursorBytes[0], len(cursorBytes))
//			}
//			if nextCursor == "0" {
//				nextCursor = "" // Convert Redis HSCAN "0" (complete) to empty string
//			}
//
//			// --- reply[3]: key-value array ---
//			kvCount, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//			pubs = make([]*Publication, 0, kvCount/2)
//
//			for i := int64(0); i < kvCount; i += 2 {
//				// Read next key
//				keyBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				// Make a safe copy
//				key := convert.BytesToString(append([]byte(nil), keyBytes...))
//
//				// Read corresponding value
//				valBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				// Parse value: offset:epoch:payload
//				entryOffset, _, payloadBytes, err := parseStateValue(valBytes)
//				if err != nil {
//					// skip malformed entries
//					continue
//				}
//				// Unmarshal Publication from protobuf payload
//				var protoPub protocol.Publication
//				if err := protoPub.UnmarshalVT(payloadBytes); err != nil {
//					// Skip malformed entries
//					continue
//				}
//
//				pub := pubFromProto(&protoPub)
//				pub.Key = key
//				pub.Offset = entryOffset
//				pubs = append(pubs, pub)
//			}
//
//			return nil
//		},
//	)
//
//	if err != nil {
//		return MapStateResult{}, err
//	}
//
//	// Validate state revision (unchanged semantics)
//	if opts.Revision != nil &&
//		opts.Revision.Epoch != streamPos.Epoch {
//		return MapStateResult{Position: streamPos, Cursor: nextCursor}, ErrorUnrecoverablePosition
//	}
//
//	return MapStateResult{Publications: pubs, Position: streamPos, Cursor: nextCursor}, nil
//}

// readOrderedState uses key-based cursor pagination for continuity.
// Cursor format: "score\x00key" to ensure no entries are skipped during concurrent modifications.
func (e *RedisMapBroker) readOrderedState(ctx context.Context, ch string, opts MapReadStateOptions, chOpts MapChannelOptions) (MapStateResult, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	stateTTL := "0"
	if chOpts.MetaTTL > 0 {
		stateTTL = millis(chOpts.MetaTTL)
	}

	// Parse cursor: "score\x00key" format
	cursorScore := ""
	cursorKey := ""
	if opts.Cursor != "" {
		cursorScore, cursorKey = parseOrderedCursor(opts.Cursor)
	}

	streamlessFlag := "0"
	if chOpts.Mode.IsEphemeral() {
		streamlessFlag = "1"
	}

	// In Lua scripts, limit=0 means "return all", limit>0 means paginate.
	luaLimit := opts.Limit
	if luaLimit < 0 {
		luaLimit = 0
	}

	ascFlag := "0"
	if opts.Asc {
		ascFlag = "1"
	}

	replies, err := e.readOrderedScript.Exec(ctx, shardClient,
		[]string{
			e.stateHashKey(s.shard, ch),
			e.stateOrderKey(s.shard, ch),
			e.stateExpireKey(s.shard, ch),
			e.metaKey(s.shard, ch),
			e.stateMetaKey(s.shard, ch),
		},
		[]string{
			strconv.Itoa(luaLimit), // ARGV[1] = limit
			cursorScore,            // ARGV[2] = cursor_score
			cursorKey,              // ARGV[3] = cursor_key
			strconv.FormatInt(time.Now().UnixMilli(), 10), // ARGV[4] = now
			millis(chOpts.MetaTTL),                        // ARGV[5] = meta_ttl
			stateTTL,                                      // ARGV[6] = state_ttl
			streamlessFlag,                                // ARGV[7] = streamless
			ascFlag,                                       // ARGV[8] = asc ("1" = ascending, "0" = descending)
		},
	).ToArray()
	if err != nil {
		return MapStateResult{}, err
	}
	if len(replies) < 6 {
		return MapStateResult{}, errors.New("wrong number of replies")
	}

	streamOffset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return MapStateResult{}, err
	}
	streamEpoch, _ := replies[1].ToString()
	keysArr, _ := replies[2].ToArray()
	valuesArr, _ := replies[3].ToArray()
	nextCursorScore, _ := replies[4].ToString()
	nextCursorKey, _ := replies[5].ToString()

	streamPos := StreamPosition{Offset: streamOffset, Epoch: streamEpoch}

	// Validate epoch if client provided state revision
	if opts.Revision != nil && opts.Revision.Epoch != streamEpoch {
		// Epoch changed, client needs to restart from beginning
		return MapStateResult{Position: streamPos}, ErrorUnrecoverablePosition
	}

	// Parse state values with revisions
	pubs := make([]*Publication, 0, len(keysArr))
	for i := 0; i < len(keysArr); i++ {
		key, _ := keysArr[i].ToString()
		val, _ := valuesArr[i].AsBytes()

		// Parse value: offset:epoch:payload
		entryOffset, _, payload, err := parseStateValue(val)
		if err != nil {
			// Skip malformed entries
			continue
		}

		// Unmarshal Publication from protobuf payload
		var protoPub protocol.Publication
		if err := protoPub.UnmarshalVT(payload); err != nil {
			// Skip malformed entries
			continue
		}

		pub := pubFromProto(&protoPub)
		pub.Key = key
		pub.Offset = entryOffset
		pubs = append(pubs, pub)
	}

	// Build cursor for next page
	cursor := ""
	if nextCursorScore != "" && nextCursorKey != "" {
		cursor = MakeOrderedCursor(nextCursorScore, nextCursorKey)
	}

	return MapStateResult{Publications: pubs, Position: streamPos, Cursor: cursor}, nil
}

//func (e *RedisMapBroker) ReadStreamZero(
//	ctx context.Context,
//	ch string,
//	opts MapReadStreamOptions,
//) (MapStreamResult, error) {
//	s := e.getShard(ch)
//
//	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	var includePubs = true
//	var offset string
//
//	if opts.Filter.Since != nil {
//		if opts.Filter.Reverse {
//			if opts.Filter.Since.Offset == 0 {
//				includePubs = false
//			} else {
//				offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
//			}
//		} else {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset+1, 10)
//		}
//	} else {
//		offset = "-"
//		if opts.Filter.Reverse {
//			offset = "+"
//		}
//	}
//
//	limit := opts.Filter.Limit
//	if limit == 0 {
//		includePubs = false
//	}
//	if limit < 0 {
//		limit = 0
//	}
//
//	reverse := "0"
//	if opts.Filter.Reverse {
//		reverse = "1"
//	}
//
//	metaExpire := "0"
//	if chOpts.MetaTTL > 0 {
//		metaExpire = millis(chOpts.MetaTTL)
//	}
//
//	includePubsStr := "0"
//	if includePubs {
//		includePubsStr = "1"
//	}
//
//	var (
//		streamPos StreamPosition
//		pubs      []*Publication
//	)
//
//	err = e.readStreamScript.ExecWithReader(
//		ctx,
//		s.shard.client,
//		[]string{
//			e.streamKey(s.shard, ch),
//			e.metaKey(s.shard, ch),
//		},
//		[]string{
//			includePubsStr,
//			offset,
//			strconv.Itoa(limit),
//			reverse,
//			metaExpire,
//			e.node.ID(),
//		},
//		func(r *bufio.Reader) error {
//			rr := resp.NewReader(r)
//
//			// ---- top-level reply ----
//			n, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//			if n < 2 {
//				return fmt.Errorf("wrong number of replies: %d", n)
//			}
//
//			// ---- reply[0]: top offset ----
//			switch rr.PeekKind() {
//			case resp.KindInt:
//				v, err := rr.ReadInt64()
//				if err != nil {
//					return err
//				}
//				streamPos.Offset = uint64(v)
//			case resp.KindString:
//				buf, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				v, err := strconv.ParseUint(
//					unsafe.String(&buf[0], len(buf)),
//					10,
//					64,
//				)
//				if err != nil {
//					return err
//				}
//				streamPos.Offset = v
//			default:
//				return fmt.Errorf("unexpected RESP kind for offset")
//			}
//
//			buf, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			// Make a safe copy of the bytes.
//			safeBuf := make([]byte, len(buf))
//			copy(safeBuf, buf)
//			streamPos.Epoch = convert.BytesToString(safeBuf)
//
//			// Validate epoch if provided in Since filter.
//			if opts.Filter.Since != nil && opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != streamPos.Epoch {
//				return ErrorUnrecoverablePosition
//			}
//
//			if !includePubs || n < 3 {
//				return nil
//			}
//
//			// ---- reply[2]: publications ----
//			pubCount, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//
//			pubs = make([]*Publication, 0, pubCount)
//
//			for i := int64(0); i < pubCount; i++ {
//				// entry = [id, fields]
//				if _, err := rr.ExpectArray(); err != nil {
//					return err
//				}
//
//				// ---- id ----
//				idBuf, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//
//				idStr := unsafe.String(&idBuf[0], len(idBuf))
//				hyphen := strings.IndexByte(idStr, '-')
//				if hyphen <= 0 {
//					return fmt.Errorf("invalid stream id")
//				}
//
//				pubOffset, err := strconv.ParseUint(idStr[:hyphen], 10, 64)
//				if err != nil {
//					return err
//				}
//
//				// ---- fields ----
//				fieldCount, err := rr.ExpectArray()
//				if err != nil {
//					return err
//				}
//
//				var payload *bpool.ByteBuffer
//
//				for j := int64(0); j < fieldCount; j += 2 {
//					// key
//					key, err := rr.ReadStringBytes()
//					if err != nil {
//						return err
//					}
//					isData := len(key) == 1 && key[0] == 'd'
//
//					// value
//					if isData {
//						vbuf, err := rr.ReadStringBytes()
//						if err != nil {
//							return err
//						}
//						payload = bpool.GetByteBuffer(len(vbuf))
//						payload.B = payload.B[:len(vbuf)]
//						copy(payload.B, vbuf)
//					} else {
//						if err := rr.SkipValue(); err != nil {
//							return err
//						}
//					}
//				}
//
//				if payload == nil {
//					return errors.New("no payload data found in entry")
//				}
//
//				var protoPub protocol.Publication
//				if err := protoPub.UnmarshalVT(payload.B); err != nil {
//					bpool.PutByteBuffer(payload)
//					return err
//				}
//				bpool.PutByteBuffer(payload)
//
//				protoPub.Offset = pubOffset
//				pubs = append(pubs, &Publication{
//					Offset:  protoPub.Offset,
//					Data:    protoPub.Data,
//					Info:    infoFromProto(protoPub.GetInfo()),
//					Tags:    protoPub.GetTags(),
//					Time:    protoPub.Time,
//					Key:     protoPub.GetKey(),
//					Removed: protoPub.GetRemoved(),
//					Score:   protoPub.GetScore(),
//				})
//			}
//			return nil
//		},
//	)
//
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	return MapStreamResult{Publications: pubs, Position: streamPos}, nil
//}

// ReadStreamZero2 is a 2-call version of ReadStreamZero with zero-alloc optimizations.
// Call 1: Get metadata (epoch, top_offset) using simpler Lua script with ExecWithReader
// Call 2: Read publications using native XRANGE/XREVRANGE with DoWithReader
// Key difference: Must filter out publications with offset > top_offset (non-atomic).
//func (e *RedisMapBroker) ReadStreamZero2(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
//	s := e.getShard(ch)
//
//	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	// 1. Parse options (same as ReadStream/ReadStreamZero)
//	var includePubs = true
//	var offset string
//	if opts.Filter.Since != nil {
//		if opts.Filter.Reverse {
//			if opts.Filter.Since.Offset == 0 {
//				includePubs = false
//			} else {
//				offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
//			}
//		} else {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset+1, 10)
//		}
//	} else {
//		offset = "-"
//		if opts.Filter.Reverse {
//			offset = "+"
//		}
//	}
//
//	limit := opts.Filter.Limit
//	if limit == 0 {
//		includePubs = false
//	}
//	if limit < 0 {
//		limit = 0
//	}
//
//	metaExpire := "0"
//	if chOpts.MetaTTL > 0 {
//		metaExpire = millis(chOpts.MetaTTL)
//	}
//
//	// 2. Call 1: Get metadata with ExecWithReader (zero-alloc)
//	var streamPos StreamPosition
//
//	err = e.readMetaScript.ExecWithReader(
//		ctx,
//		s.shard.client,
//		[]string{e.metaKey(s.shard, ch)},
//		[]string{metaExpire, e.node.ID()},
//		func(r *bufio.Reader) error {
//			rr := resp.NewReader(r)
//
//			// Top-level must be an array of at least 2 elements
//			n, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//			if n < 2 {
//				return fmt.Errorf("wrong number of replies: %d", n)
//			}
//
//			// ---- reply[0]: top offset ----
//			switch rr.PeekKind() {
//			case resp.KindInt:
//				v, err := rr.ReadInt64()
//				if err != nil && !errors.Is(err, rueidis.Nil) {
//					return err
//				}
//				streamPos.Offset = uint64(v)
//			case resp.KindString:
//				b, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				if len(b) > 0 {
//					v, err := strconv.ParseUint(unsafe.String(&b[0], len(b)), 10, 64)
//					if err != nil {
//						return err
//					}
//					streamPos.Offset = v
//				}
//			default:
//				return fmt.Errorf("unexpected RESP type for offset: %q", rr.PeekKind())
//			}
//
//			// ---- reply[1]: epoch ----
//			buf, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			// Make a safe copy of the bytes.
//			safeBuf := make([]byte, len(buf))
//			copy(safeBuf, buf)
//			streamPos.Epoch = convert.BytesToString(safeBuf)
//
//			return nil
//		},
//	)
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	// Validate epoch if provided in Since filter.
//	if opts.Filter.Since != nil && opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != streamPos.Epoch {
//		return MapStreamResult{}, ErrorUnrecoverablePosition
//	}
//
//	// 3. Early return if metadata-only
//	if !includePubs {
//		return MapStreamResult{Position: streamPos}, nil
//	}
//
//	topOffset := streamPos.Offset
//
//	// 4. Call 2: Execute XRANGE with DoWithReader (zero-alloc)
//	streamKey := e.streamKey(s.shard, ch)
//	var cmd rueidis.Completed
//
//	if opts.Filter.Reverse {
//		builder := s.shard.client.B().Xrevrange().Key(streamKey).End(offset).Start("-")
//		if limit > 0 {
//			cmd = builder.Count(int64(limit)).Build()
//		} else {
//			cmd = builder.Build()
//		}
//	} else {
//		builder := s.shard.client.B().Xrange().Key(streamKey).Start(offset).End("+")
//		if limit > 0 {
//			cmd = builder.Count(int64(limit)).Build()
//		} else {
//			cmd = builder.Build()
//		}
//	}
//
//	var pubs []*Publication
//
//	err = s.shard.client.DoWithReader(ctx, cmd, func(r *bufio.Reader) error {
//		rr := resp.NewReader(r)
//
//		// Top-level must be an array
//		entryCount, err := rr.ExpectArray()
//		if err != nil {
//			return err
//		}
//
//		if entryCount > 0 {
//			pubs = make([]*Publication, 0, entryCount)
//		}
//
//		for i := int64(0); i < entryCount; i++ {
//			// Each entry = [id, fields]
//			if _, err := rr.ExpectArray(); err != nil {
//				return err
//			}
//
//			// ---- id ----
//			idBytes, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			idStr := unsafe.String(&idBytes[0], len(idBytes))
//			hyphen := strings.IndexByte(idStr, '-')
//			if hyphen <= 0 {
//				return fmt.Errorf("invalid stream id")
//			}
//
//			pubOffset, err := strconv.ParseUint(idStr[:hyphen], 10, 64)
//			if err != nil {
//				return err
//			}
//
//			// Filter entries written after metadata read
//			if pubOffset > topOffset {
//				if err := rr.SkipValue(); err != nil { // skip fields array
//					return err
//				}
//				continue
//			}
//
//			// ---- fields ----
//			fieldCount, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//
//			var payload *bpool.ByteBuffer
//
//			for j := int64(0); j < fieldCount; j += 2 {
//				// key
//				keyBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				isData := len(keyBytes) == 1 && keyBytes[0] == 'd'
//
//				// value
//				valBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//
//				if isData {
//					payload = bpool.GetByteBuffer(len(valBytes))
//					payload.B = payload.B[:len(valBytes)]
//					copy(payload.B, valBytes)
//				}
//			}
//
//			if payload == nil {
//				return errors.New("no payload data found in entry")
//			}
//
//			var protoPub protocol.Publication
//			if err := protoPub.UnmarshalVT(payload.B); err != nil {
//				bpool.PutByteBuffer(payload)
//				return err
//			}
//			bpool.PutByteBuffer(payload)
//
//			protoPub.Offset = pubOffset
//			pubs = append(pubs, &Publication{
//				Offset:  protoPub.Offset,
//				Data:    protoPub.Data,
//				Info:    infoFromProto(protoPub.GetInfo()),
//				Tags:    protoPub.GetTags(),
//				Time:    protoPub.Time,
//				Key:     protoPub.GetKey(),
//				Removed: protoPub.GetRemoved(),
//				Score:   protoPub.GetScore(),
//			})
//		}
//
//		return nil
//	})
//
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	return MapStreamResult{Publications: pubs, Position: streamPos}, nil
//}

// ReadStream retrieves publication stream for a channel.
func (e *RedisMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	s := e.getShard(ch)

	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapStreamResult{}, err
	}

	var includePubs = true
	var offset string
	if opts.Filter.Since != nil {
		if opts.Filter.Reverse {
			if opts.Filter.Since.Offset == 0 {
				includePubs = false
			} else {
				offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
			}
		} else {
			// Use offset+1 because XRANGE is inclusive, but "since" should be exclusive
			offset = strconv.FormatUint(opts.Filter.Since.Offset+1, 10)
		}
	} else {
		offset = "-"
		if opts.Filter.Reverse {
			offset = "+"
		}
	}

	limit := opts.Filter.Limit
	if limit == 0 {
		includePubs = false
	}
	if limit < 0 {
		limit = 0 // 0 means "get all" in the Lua script
	}

	reverse := "0"
	if opts.Filter.Reverse {
		reverse = "1"
	}

	metaExpire := "0"
	if chOpts.MetaTTL > 0 {
		metaExpire = millis(chOpts.MetaTTL)
	}

	includePubsStr := "0"
	if includePubs {
		includePubsStr = "1"
	}

	replies, err := e.readStreamScript.Exec(ctx, s.shard.client,
		[]string{e.streamKey(s.shard, ch), e.metaKey(s.shard, ch)},
		[]string{
			includePubsStr,
			offset,
			strconv.Itoa(limit),
			reverse,
			metaExpire,
			e.node.ID(),
		},
	).ToArray()
	if err != nil {
		return MapStreamResult{}, err
	}

	if len(replies) < 2 {
		return MapStreamResult{}, fmt.Errorf("wrong number of replies: %d", len(replies))
	}

	topOffset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return MapStreamResult{}, fmt.Errorf("could not parse top offset: %w", err)
	}

	epoch, err := replies[1].ToString()
	if err != nil {
		return MapStreamResult{}, fmt.Errorf("could not parse epoch: %w", err)
	}

	// Validate epoch if provided in Since filter.
	if opts.Filter.Since != nil && opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != epoch {
		return MapStreamResult{}, ErrorUnrecoverablePosition
	}

	streamPos := StreamPosition{Offset: topOffset, Epoch: epoch}
	if !includePubs || len(replies) < 3 {
		return MapStreamResult{Position: streamPos}, nil
	}

	pubValues, err := replies[2].ToArray()
	if err != nil {
		return MapStreamResult{}, err
	}

	pubs := make([]*Publication, len(pubValues))
	for j, v := range pubValues {
		entry, err := v.ToArray()
		if err != nil {
			return MapStreamResult{}, err
		}
		if len(entry) != 2 {
			return MapStreamResult{}, fmt.Errorf("invalid publication format")
		}
		id, err := entry[0].ToString()
		if err != nil {
			return MapStreamResult{}, err
		}

		// entry[1] is an array of [field, value, field, value, ...]
		fieldsArr, err := entry[1].ToArray()
		if err != nil {
			return MapStreamResult{}, err
		}

		var payload []byte
		for i := 0; i < len(fieldsArr)-1; i += 2 {
			k, _ := fieldsArr[i].ToString()
			if k != "d" {
				continue
			}
			v, _ := fieldsArr[i+1].ToString()
			payload = convert.StringToBytes(v)
			break
		}
		if payload == nil {
			return MapStreamResult{}, errors.New("no payload data found in entry")
		}

		hyphenPos := strings.Index(id, "-")
		if hyphenPos <= 0 {
			return MapStreamResult{}, fmt.Errorf("unexpected offset format: %s", id)
		}
		pubOffset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
		if err != nil {
			return MapStreamResult{}, err
		}

		var protoPub protocol.Publication
		err = protoPub.UnmarshalVT(payload)
		if err != nil {
			return MapStreamResult{}, fmt.Errorf("can not unmarshal value to Publication: %v", err)
		}

		protoPub.Offset = pubOffset
		pubs[j] = &Publication{
			Offset:  protoPub.Offset,
			Data:    protoPub.Data,
			Info:    infoFromProto(protoPub.GetInfo()),
			Tags:    protoPub.GetTags(),
			Time:    protoPub.Time,
			Key:     protoPub.GetKey(),
			Removed: protoPub.GetRemoved(),
			Score:   protoPub.GetScore(),
		}
	}
	return MapStreamResult{Publications: pubs, Position: streamPos}, nil
}

// ReadStream2 is a 2-call version of ReadStream that splits metadata and publication reads.
// Call 1: Get metadata (epoch, top_offset) using simpler Lua script
// Call 2: Read publications using native XRANGE/XREVRANGE
// Key difference: Must filter out publications with offset > top_offset (non-atomic).
func (e *RedisMapBroker) ReadStream2(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	s := e.getShard(ch)

	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return MapStreamResult{}, err
	}

	// 1. Parse options (same as ReadStream)
	var includePubs = true
	var offset string
	if opts.Filter.Since != nil {
		if opts.Filter.Reverse {
			if opts.Filter.Since.Offset == 0 {
				includePubs = false
			} else {
				offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
			}
		} else {
			// Use offset+1 because XRANGE is inclusive, but "since" should be exclusive
			offset = strconv.FormatUint(opts.Filter.Since.Offset+1, 10)
		}
	} else {
		offset = "-"
		if opts.Filter.Reverse {
			offset = "+"
		}
	}

	limit := opts.Filter.Limit
	if limit == 0 {
		includePubs = false
	}
	if limit < 0 {
		limit = 0
	}

	metaExpire := "0"
	if chOpts.MetaTTL > 0 {
		metaExpire = millis(chOpts.MetaTTL)
	}

	// 2. Call 1: Get metadata using new simpler Lua script
	metaReplies, err := e.readMetaScript.Exec(ctx, s.shard.client,
		[]string{e.metaKey(s.shard, ch)},
		[]string{metaExpire, e.node.ID()},
	).ToArray()
	if err != nil {
		return MapStreamResult{}, err
	}

	if len(metaReplies) < 2 {
		return MapStreamResult{}, fmt.Errorf("wrong number of replies: %d", len(metaReplies))
	}

	topOffset, err := metaReplies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return MapStreamResult{}, fmt.Errorf("could not parse top offset: %w", err)
	}

	epoch, err := metaReplies[1].ToString()
	if err != nil {
		return MapStreamResult{}, fmt.Errorf("could not parse epoch: %w", err)
	}

	// Validate epoch if provided in Since filter.
	if opts.Filter.Since != nil && opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != epoch {
		return MapStreamResult{}, ErrorUnrecoverablePosition
	}

	streamPos := StreamPosition{Offset: topOffset, Epoch: epoch}

	// 3. Early return if metadata-only
	if !includePubs {
		return MapStreamResult{Position: streamPos}, nil
	}

	// 4. Call 2: Execute native XRANGE/XREVRANGE
	streamKey := e.streamKey(s.shard, ch)
	var cmd rueidis.Completed

	if opts.Filter.Reverse {
		builder := s.shard.client.B().Xrevrange().Key(streamKey).End(offset).Start("-")
		if limit > 0 {
			cmd = builder.Count(int64(limit)).Build()
		} else {
			cmd = builder.Build()
		}
	} else {
		builder := s.shard.client.B().Xrange().Key(streamKey).Start(offset).End("+")
		if limit > 0 {
			cmd = builder.Count(int64(limit)).Build()
		} else {
			cmd = builder.Build()
		}
	}

	result, err := s.shard.client.Do(ctx, cmd).ToArray()
	if err != nil {
		return MapStreamResult{}, err
	}

	// 5. Parse stream entries and filter by topOffset
	pubs := make([]*Publication, 0, len(result))

	for _, v := range result {
		entry, err := v.ToArray()
		if err != nil {
			return MapStreamResult{}, err
		}
		if len(entry) != 2 {
			return MapStreamResult{}, fmt.Errorf("invalid publication format")
		}
		id, err := entry[0].ToString()
		if err != nil {
			return MapStreamResult{}, err
		}

		// Extract offset from stream ID "123-0" -> 123
		hyphenPos := strings.Index(id, "-")
		if hyphenPos <= 0 {
			return MapStreamResult{}, fmt.Errorf("unexpected offset format: %s", id)
		}
		pubOffset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
		if err != nil {
			return MapStreamResult{}, err
		}

		// CRITICAL: Filter entries written after metadata read
		if pubOffset > topOffset {
			continue
		}

		// entry[1] is an array of [field, value, field, value, ...]
		fieldsArr, err := entry[1].ToArray()
		if err != nil {
			return MapStreamResult{}, err
		}

		var payload []byte
		for i := 0; i < len(fieldsArr)-1; i += 2 {
			k, _ := fieldsArr[i].ToString()
			if k != "d" {
				continue
			}
			v, _ := fieldsArr[i+1].ToString()
			payload = convert.StringToBytes(v)
			break
		}
		if payload == nil {
			return MapStreamResult{}, errors.New("no payload data found in entry")
		}

		var protoPub protocol.Publication
		err = protoPub.UnmarshalVT(payload)
		if err != nil {
			return MapStreamResult{}, fmt.Errorf("can not unmarshal value to Publication: %v", err)
		}

		protoPub.Offset = pubOffset
		pubs = append(pubs, &Publication{
			Offset:  protoPub.Offset,
			Data:    protoPub.Data,
			Info:    infoFromProto(protoPub.GetInfo()),
			Tags:    protoPub.GetTags(),
			Time:    protoPub.Time,
			Key:     protoPub.GetKey(),
			Removed: protoPub.GetRemoved(),
			Channel: protoPub.GetChannel(),
			Score:   protoPub.GetScore(),
		})
	}

	return MapStreamResult{Publications: pubs, Position: streamPos}, nil
}

// Stats returns short stats of current presence data.
// This is a read-only operation - cleanup is handled by the cleanup worker.
func (e *RedisMapBroker) Stats(ctx context.Context, ch string) (MapStats, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	replies, err := e.presenceStatsScript.Exec(ctx, shardClient,
		[]string{
			e.stateHashKey(s.shard, ch),
		},
		nil,
	).ToArray()
	if err != nil {
		return MapStats{}, err
	}
	if len(replies) != 1 {
		return MapStats{}, errors.New("wrong number of replies from script")
	}
	numKeys, err := replies[0].AsInt64()
	if err != nil {
		return MapStats{}, err
	}
	return MapStats{NumKeys: int(numKeys)}, nil
}

// ReadPresenceState retrieves presence state with per-entry revisions for converged membership.
// Each entry includes its revision so client can filter: entry.Revision <= state_revision.
// If opts.Revision is provided and epoch changed, returns empty entries.
// Returns Publications with Key=ClientID, Info=ClientInfo, Offset/Epoch for revision.
func (e *RedisMapBroker) ReadPresenceState(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, error) {
	// ReadPresenceState is just ReadState - it already returns Publications with Key and Offset set.
	// This reuses all the state reading logic.
	result, err := e.ReadState(ctx, ch, opts)
	if err != nil {
		return nil, StreamPosition{}, err
	}
	return result.Publications, result.Position, nil
}

// ReadPresenceStream retrieves presence event stream (joins/leaves) for a channel.
// Returns Publications with Info=ClientInfo and Removed flag (true for leave, false for join).
func (e *RedisMapBroker) ReadPresenceStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	// ReadPresenceStream is literally just ReadStream - presence streams store Publications
	// Publication.Removed distinguishes join (false) from leave (true) events.
	return e.ReadStream(ctx, ch, opts)
}

// RegisterEventHandler registers a BrokerEventHandler to handle messages from Pub/Sub.
func (e *RedisMapBroker) RegisterEventHandler(h BrokerEventHandler) error {
	// Run all shards.
	for _, wrapper := range e.shards {
		if err := wrapper.pubSubRunner.run(wrapper, h); err != nil {
			return err
		}
	}
	for i := 0; i < len(e.shards); i++ {
		for j := 0; j < len(e.shards[i].pubSubStartChannels); j++ {
			for k := 0; k < len(e.shards[i].pubSubStartChannels[j]); k++ {
				if !e.conf.SkipPubSub {
					<-e.shards[i].pubSubStartChannels[j][k].errCh
				}
			}
		}
	}
	return nil
}

// newMapBrokerPubSubRunnerHook is an optional package-private hook used by
// auxiliary modules to install an alternative pub/sub runner. The default
// implementation returns nil so the broker falls back to defaultMapBrokerPubSubRunner.
var newMapBrokerPubSubRunnerHook func(e *RedisMapBroker, shard *RedisShard) mapBrokerPubSubRunner

// defaultMapBrokerPubSubRunner is the standard partition-sharded pub/sub runner.
// All per-shard state lives on the brokerShardWrapper, the runner is stateless
// beyond a back-pointer.
type defaultMapBrokerPubSubRunner struct {
	broker *RedisMapBroker
}

func (r *defaultMapBrokerPubSubRunner) init(s *brokerShardWrapper, shard *RedisShard) error {
	e := r.broker
	subChannels := make([][]rueidis.DedicatedClient, 0)
	pubSubStartChannels := make([][]*pubSubStart, 0)

	if e.useShardedPubSub(shard) {
		for i := 0; i < e.conf.NumShardedPubSubPartitions; i++ {
			subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
			pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
		}
	} else {
		subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
		pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
	}

	for i := 0; i < len(subChannels); i++ {
		for j := 0; j < e.conf.numSubscribeShards; j++ {
			subChannels[i] = append(subChannels[i], nil)
			pubSubStartChannels[i] = append(pubSubStartChannels[i], &pubSubStart{errCh: make(chan error, 1)})
		}
	}

	s.subClients = subChannels
	s.pubSubStartChannels = pubSubStartChannels
	return nil
}

func (r *defaultMapBrokerPubSubRunner) subClientsIndex(clusterShardIdx int) int {
	return clusterShardIdx
}

func (r *defaultMapBrokerPubSubRunner) run(s *brokerShardWrapper, h BrokerEventHandler) error {
	e := r.broker
	if e.conf.SkipPubSub {
		return nil
	}
	for i := 0; i < len(s.subClients); i++ { // Cluster shards.
		clusterShardIndex := i
		for j := 0; j < len(s.subClients[i]); j++ { // PUB/SUB shards.
			pubSubShardIndex := j
			logFields := map[string]any{
				"shard":          s.shard.string(),
				"pub_sub_shard":  pubSubShardIndex,
				"cluster_shards": len(s.subClients),
				"pub_sub_shards": len(s.subClients[i]),
			}
			go e.runForever(func() {
				select {
				case <-e.closeCh:
					return
				default:
				}
				e.runPubSub(s, logFields, h, clusterShardIndex, pubSubShardIndex, e.useShardedPubSub(s.shard), func(err error) {
					s.pubSubStartChannels[clusterShardIndex][pubSubShardIndex].once.Do(func() {
						s.pubSubStartChannels[clusterShardIndex][pubSubShardIndex].errCh <- err
					})
				})
			})
		}
	}
	return nil
}

// runForever keeps another function running indefinitely.
func (e *RedisMapBroker) runForever(fn func()) {
	for {
		select {
		case <-e.closeCh:
			return
		default:
		}
		fn()
		select {
		case <-e.closeCh:
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// cleanupChannelBatchSize is the max number of channels to fetch per ZRANGEBYSCORE call
// in the cleanup worker. The worker loops until all expired channels are processed.
const cleanupChannelBatchSize = 10000

// cleanupChannelConcurrency is the max number of concurrent cleanup Lua calls per partition.
// Goroutines automatically pipeline over rueidis's multiplexed connection, reducing
// network round-trip overhead from N sequential calls to ~N/concurrency batches.
const cleanupChannelConcurrency = 64

// runCleanupLagWorker periodically checks the oldest expired entry across all
// cleanup ZSETs and reports it as a lag metric. This runs in a separate goroutine
// to avoid adding extra Redis calls to the hot cleanup path.
func (e *RedisMapBroker) runCleanupLagWorker(ctx context.Context) {
	interval := e.conf.CleanupInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.closeCh:
			return
		case <-ticker.C:
			e.updateCleanupLag(ctx)
		}
	}
}

func (e *RedisMapBroker) updateCleanupLag(ctx context.Context) {
	if e.node.metrics == nil {
		return
	}
	now := time.Now().UnixMilli()
	var maxLagMs int64

	for _, wrapper := range e.shards {
		shard := wrapper.shard
		var cleanupKeys []string
		if e.useShardedPubSub(shard) {
			cleanupKeys = make([]string, 0, e.conf.NumShardedPubSubPartitions)
			for i := 0; i < e.conf.NumShardedPubSubPartitions; i++ {
				cleanupKeys = append(cleanupKeys, e.conf.Prefix+":cleanup:channels:{"+strconv.Itoa(i)+"}")
			}
		} else {
			cleanupKeys = []string{e.conf.Prefix + ":cleanup:channels"}
		}
		for _, cleanupKey := range cleanupKeys {
			cmd := shard.client.B().Arbitrary("ZRANGE").Keys(cleanupKey).Args("0", "0", "WITHSCORES").Build()
			if arr, err := shard.client.Do(ctx, cmd).AsStrSlice(); err == nil && len(arr) >= 2 {
				if oldestScore, parseErr := strconv.ParseFloat(arr[1], 64); parseErr == nil {
					if lagMs := now - int64(oldestScore); lagMs > maxLagMs {
						maxLagMs = lagMs
					}
				}
			}
		}
	}

	if maxLagMs > 0 {
		e.node.metrics.setMapBrokerCleanupLag(e.conf.Name, float64(maxLagMs)/1000.0)
	} else {
		e.node.metrics.setMapBrokerCleanupLag(e.conf.Name, 0)
	}
}

// runCleanupWorker runs the background cleanup worker that generates LEAVE events
// for expired presence entries. This ensures guaranteed delivery of LEAVE events
// even when clients disconnect without explicit leave.
func (e *RedisMapBroker) runCleanupWorker(ctx context.Context) {
	if e.conf.CleanupInterval <= 0 {
		return
	}

	ticker := time.NewTicker(e.conf.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.closeCh:
			return
		case <-ticker.C:
			e.runCleanupCycle(ctx)
		}
	}
}

// runCleanupCycle processes all shards in parallel and cleans up expired entries.
func (e *RedisMapBroker) runCleanupCycle(ctx context.Context) {
	now := time.Now().UnixMilli()

	var wg sync.WaitGroup
	for _, wrapper := range e.shards {
		if ctx.Err() != nil {
			return
		}
		shard := wrapper.shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.cleanupShard(ctx, shard, now)
		}()
	}
	wg.Wait()
}

// cleanupShard processes all partitions within a shard in parallel.
func (e *RedisMapBroker) cleanupShard(ctx context.Context, shard *RedisShard, now int64) {
	var cleanupKeys []string
	if e.useShardedPubSub(shard) {
		cleanupKeys = make([]string, 0, e.conf.NumShardedPubSubPartitions)
		for i := 0; i < e.conf.NumShardedPubSubPartitions; i++ {
			cleanupKeys = append(cleanupKeys, e.conf.Prefix+":cleanup:channels:{"+strconv.Itoa(i)+"}")
		}
	} else {
		cleanupKeys = []string{e.conf.Prefix + ":cleanup:channels"}
	}

	var wg sync.WaitGroup
	for _, cleanupKey := range cleanupKeys {
		if ctx.Err() != nil {
			return
		}
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			e.cleanupPartition(ctx, shard, key, now)
		}(cleanupKey)
	}
	wg.Wait()
}

// cleanupPartition processes all expired channels within a single partition.
// It loops until all expired channels are handled (no artificial cap).
// Within each batch, channels are processed concurrently for pipelining.
func (e *RedisMapBroker) cleanupPartition(ctx context.Context, shard *RedisShard, cleanupKey string, now int64) {
	for {
		if ctx.Err() != nil {
			return
		}

		channels, err := e.getChannelsForCleanup(ctx, shard.client, cleanupKey, now)
		if err != nil {
			if e.node.logEnabled(LogLevelError) {
				e.node.logger.log(newLogEntry(LogLevelError, "cleanup: failed to get channels", map[string]any{
					"error": err.Error(),
				}))
			}
			if e.node.metrics != nil {
				e.node.metrics.incMapBrokerCleanupErrors(e.conf.Name)
			}
			return
		}
		if len(channels) == 0 {
			return
		}

		// Process channels concurrently. Goroutines pipeline over rueidis's
		// multiplexed connection, turning N sequential round-trips into
		// ~N/cleanupChannelConcurrency pipelined batches.
		var wg sync.WaitGroup
		sem := make(chan struct{}, cleanupChannelConcurrency)
		for _, ch := range channels {
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(ch string) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := e.cleanupChannel(ctx, shard, ch, cleanupKey, now); err != nil {
					if e.node.logEnabled(LogLevelError) {
						e.node.logger.log(newLogEntry(LogLevelError, "cleanup: failed to cleanup channel", map[string]any{
							"channel": ch,
							"error":   err.Error(),
						}))
					}
					if e.node.metrics != nil {
						e.node.metrics.incMapBrokerCleanupErrors(e.conf.Name)
					}
				}
			}(ch)
		}
		wg.Wait()
	}
}

// getChannelsForCleanup returns channels that have expired entries (score <= now).
func (e *RedisMapBroker) getChannelsForCleanup(ctx context.Context, client rueidis.Client, cleanupKey string, now int64) ([]string, error) {
	cmd := client.B().Zrangebyscore().Key(cleanupKey).Min("0").Max(strconv.FormatInt(now, 10)).Limit(0, cleanupChannelBatchSize).Build()
	result, err := client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// cleanupChannel runs the 2-phase cleanup for a single channel:
// Phase 1: find expired keys and read their state values (read-only Lua)
// Phase 2: construct removal protobufs in Go (with tags), then batch-remove atomically (Lua)
func (e *RedisMapBroker) cleanupChannel(ctx context.Context, shard *RedisShard, ch string, cleanupKey string, now int64) error {
	// Get channel options for this channel.
	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
	if err != nil {
		return err
	}

	// Phase 1: find expired keys and read their state values.
	expiredEntries, err := e.findExpiredKeys(ctx, shard, ch, now)
	if err != nil {
		return err
	}
	if len(expiredEntries) == 0 {
		return nil
	}

	// Phase 2: construct removal protobufs in Go (with tags from state), then batch-remove.
	removals := make([]cleanupRemovalEntry, 0, len(expiredEntries))
	for _, entry := range expiredEntries {
		// Extract tags from the stored publication protobuf.
		var tags map[string]string
		if len(entry.stateValue) > 0 {
			_, _, payload, parseErr := parseStateValue(entry.stateValue)
			if parseErr == nil && len(payload) > 0 {
				var pub protocol.Publication
				if unmarshalErr := pub.UnmarshalVT(payload); unmarshalErr == nil {
					tags = pub.Tags
				}
			}
		}

		// Construct removal protobuf with tags.
		removePub := &protocol.Publication{
			Key:     entry.key,
			Removed: true,
			Time:    now,
			Tags:    tags,
		}
		pubBytes, marshalErr := removePub.MarshalVT()
		if marshalErr != nil {
			continue
		}
		removals = append(removals, cleanupRemovalEntry{
			key:         entry.key,
			payload:     pubBytes,
			expireScore: entry.expireScore,
		})
	}

	if len(removals) == 0 {
		return nil
	}

	return e.batchRemoveExpired(ctx, shard, ch, cleanupKey, chOpts, removals)
}

// expiredKeyEntry holds data returned from the find-expired Lua script.
type expiredKeyEntry struct {
	key         string
	stateValue  []byte
	expireScore string
}

// cleanupRemovalEntry holds a pre-constructed removal protobuf for the batch-remove script.
type cleanupRemovalEntry struct {
	key         string
	payload     []byte
	expireScore string
}

// findExpiredKeys runs the read-only Lua script to find expired keys and their state values.
func (e *RedisMapBroker) findExpiredKeys(ctx context.Context, shard *RedisShard, ch string, now int64) ([]expiredKeyEntry, error) {
	result, err := e.findExpiredScript.Exec(ctx, shard.client,
		[]string{
			e.stateHashKey(shard, ch),   // KEYS[1]: state hash key
			e.stateExpireKey(shard, ch), // KEYS[2]: state expire zset key
		},
		[]string{
			strconv.FormatInt(now, 10),            // ARGV[1]: now
			strconv.Itoa(e.conf.CleanupBatchSize), // ARGV[2]: batch_size
		},
	).ToArray()
	if err != nil {
		return nil, err
	}

	// Parse triplets: (key, state_value, expire_score)
	if len(result) < 3 {
		return nil, nil
	}
	entries := make([]expiredKeyEntry, 0, len(result)/3)
	for i := 0; i+2 < len(result); i += 3 {
		key, _ := result[i].ToString()
		stateVal, _ := result[i+1].ToString()
		expireScore, _ := result[i+2].ToString()
		entries = append(entries, expiredKeyEntry{
			key:         key,
			stateValue:  []byte(stateVal),
			expireScore: expireScore,
		})
	}
	return entries, nil
}

// batchRemoveExpired runs the batch-remove Lua script to atomically delete expired entries,
// write removal events to stream, and publish via PUB/SUB.
func (e *RedisMapBroker) batchRemoveExpired(ctx context.Context, shard *RedisShard, ch string, cleanupKey string, chOpts MapChannelOptions, removals []cleanupRemovalEntry) error {
	// Determine publish command
	publishCommand := ""
	chID := ""
	if !e.conf.SkipPubSub {
		if e.useShardedPubSub(shard) {
			publishCommand = "SPUBLISH"
		} else {
			publishCommand = "PUBLISH"
		}
		chID = e.messageChannelID(shard, ch)
	}

	metaExpire := "0"
	if chOpts.MetaTTL > 0 {
		metaExpire = millis(chOpts.MetaTTL)
	}

	streamlessFlag := "0"
	if chOpts.Mode.IsEphemeral() {
		streamlessFlag = "1"
	}

	// Build ARGV: fixed args + triplets per entry
	argv := make([]string, 0, 9+len(removals)*3)
	argv = append(argv,
		strconv.Itoa(len(removals)),     // ARGV[1]: num_entries
		chID,                            // ARGV[2]: channel
		publishCommand,                  // ARGV[3]: publish_command
		strconv.Itoa(chOpts.StreamSize), // ARGV[4]: stream_size
		millis(chOpts.StreamTTL),        // ARGV[5]: stream_ttl
		metaExpire,                      // ARGV[6]: meta_expire
		e.node.ID(),                     // ARGV[7]: new_epoch_if_empty
		ch,                              // ARGV[8]: channel_for_cleanup
		streamlessFlag,                  // ARGV[9]: streamless
	)
	for _, r := range removals {
		argv = append(argv, r.key, convert.BytesToString(r.payload), r.expireScore)
	}

	result, err := e.batchRemoveScript.Exec(ctx, shard.client,
		[]string{
			e.stateHashKey(shard, ch),   // KEYS[1]
			e.stateExpireKey(shard, ch), // KEYS[2]
			e.streamKey(shard, ch),      // KEYS[3]
			e.metaKey(shard, ch),        // KEYS[4]
			cleanupKey,                  // KEYS[5]
			e.stateOrderKey(shard, ch),  // KEYS[6]
			e.stateMetaKey(shard, ch),   // KEYS[7]
		},
		argv,
	).ToArray()
	if err != nil {
		return err
	}
	if len(result) > 0 {
		if removedCount, parseErr := result[0].AsInt64(); parseErr == nil && removedCount > 0 {
			if e.node.metrics != nil {
				e.node.metrics.addMapBrokerCleanupKeysRemoved(e.conf.Name, removedCount)
			}
		}
	}
	return nil
}

// makePubSubCallbacks builds the pubSubCallbacks used by the pub/sub runner.
func (e *RedisMapBroker) makePubSubCallbacks(s *brokerShardWrapper) pubSubCallbacks {
	return pubSubCallbacks{
		handleMessage: func(isCluster bool, handler BrokerEventHandler, ch string, data []byte) error {
			return e.handleRedisClientMessage(isCluster, handler, ch, data)
		},
		shardChannelID: func(clusterIdx, psIdx int, sharded bool) string {
			return e.pubSubShardChannelID(clusterIdx, psIdx, sharded)
		},
		messageChannelID: func(ch string) string {
			return e.messageChannelID(s.shard, ch)
		},
		shardForChannel: func(ch string) *RedisShard {
			return e.getShard(ch).shard
		},
	}
}

func (e *RedisMapBroker) runPubSub(s *brokerShardWrapper, logFields map[string]any, eventHandler BrokerEventHandler, clusterShardIndex, psShardIndex int, useShardedPubSub bool, startOnce func(error)) {
	cb := e.makePubSubCallbacks(s)
	numPartitions := e.conf.NumShardedPubSubPartitions
	if numPartitions == 0 {
		numPartitions = 1
	}
	runPubSubLoop(
		s.shard,
		&s.subClientsMu,
		s.subClients,
		cb,
		e.node,
		e.conf.Name,
		e.conf.SubscribeOnReplica,
		e.conf.numPubSubProcessors,
		e.conf.numResubscribeShards,
		e.conf.numSubscribeShards,
		numPartitions,
		logFields,
		eventHandler,
		clusterShardIndex, psShardIndex,
		useShardedPubSub,
		startOnce,
	)
}

// pubSubPartitionHashTag returns the Redis Cluster hash tag for the given partition
// index. Used both when constructing PUB/SUB channel names (so messages route
// to a specific slot) and by alternative pub/sub strategies that need to
// determine slot ownership for a partition. Publisher and subscriber must
// agree on the scheme — keep this as the single source of truth.
//
// When UsePrecomputedPartitionTags is enabled, returns the precomputed tag
// for the index (selected for even slot distribution). Otherwise returns
// the bare integer index, preserving backward-compatible behaviour.
func (e *RedisMapBroker) pubSubPartitionHashTag(partitionIdx int) string {
	if e.partitionTags != nil {
		return e.partitionTags[partitionIdx]
	}
	return strconv.Itoa(partitionIdx)
}

func (e *RedisMapBroker) pubSubShardChannelID(clusterShardIndex int, psShardIndex int, useShardedPubSub bool) string {
	psShardStr := strconv.Itoa(psShardIndex)
	if !useShardedPubSub {
		var builder strings.Builder
		builder.Grow(len(e.shardChannel) + 1 + len(psShardStr))
		builder.WriteString(e.shardChannel)
		builder.WriteByte('.')
		builder.WriteString(psShardStr)
		return builder.String()
	}
	clusterShardStr := e.pubSubPartitionHashTag(clusterShardIndex)
	var builder strings.Builder
	capacity := len(e.shardChannel) + 1 + len(psShardStr) + 2 + len(clusterShardStr) + 1
	builder.Grow(capacity)
	builder.WriteString(e.shardChannel)
	builder.WriteByte('.')
	builder.WriteString(psShardStr)
	builder.WriteString(".{")
	builder.WriteString(clusterShardStr)
	builder.WriteByte('}')
	return builder.String()
}

func (e *RedisMapBroker) extractChannel(chID string) string {
	ch := strings.TrimPrefix(chID, e.messagePrefix)

	// Handle sharded PUB/SUB case: {idx}.channel
	if e.conf.NumShardedPubSubPartitions > 0 {
		if !strings.HasPrefix(ch, "{") {
			return "" // Invalid: expected {idx}.channel format
		}
		i := strings.Index(ch, ".")
		if i > 0 {
			return ch[i+1:]
		}
		return "" // Invalid: missing dot separator
	}

	// Non-cluster: plain channel name
	return ch
}

func (e *RedisMapBroker) handleRedisClientMessage(isCluster bool, eventHandler BrokerEventHandler, chID string, data []byte) error {
	// Parse message: supports backward compat (no prefix), non-delta, and delta formats
	offset, epoch, protobuf, isDelta, prevProtobuf, err := parseMessage(data)
	if err != nil {
		return fmt.Errorf("malformed pub/sub data: %w", err)
	}

	channel := e.extractChannel(chID)
	if channel == "" {
		return fmt.Errorf("unsupported channel: %s", chID)
	}

	sp := StreamPosition{Offset: offset, Epoch: epoch}

	// Unmarshal as Publication (unified format)
	var protoPub protocol.Publication
	err = protoPub.UnmarshalVT(protobuf)
	if err != nil {
		return fmt.Errorf("can not unmarshal value to Publication: %v", err)
	}

	if protoPub.Offset == 0 {
		protoPub.Offset = offset
	}

	// Regular publication
	var prevPubPtr *Publication
	if isDelta && len(prevProtobuf) > 0 {
		// Delta prev data is in state value format: "offset:epoch:protobuf".
		// Extract the protobuf payload.
		_, _, payload, parseErr := parseStateValue(prevProtobuf)
		if parseErr == nil {
			var prevPub protocol.Publication
			if err := prevPub.UnmarshalVT(payload); err == nil {
				prevPubPtr = pubFromProto(&prevPub)
			}
		}
	}

	_ = eventHandler.HandlePublication(channel, pubFromProto(&protoPub), sp, isDelta, prevPubPtr)
	return nil
}

// Subscribe to channels.
func (e *RedisMapBroker) Subscribe(channels ...string) error {
	if len(channels) == 0 {
		return nil
	}
	if len(channels) == 1 {
		return e.subscribe(e.getShard(channels[0]), channels[0])
	}
	return e.subscribeBatch(channels, false)
}

func (e *RedisMapBroker) subscribe(s *brokerShardWrapper, ch string) error {
	if e.node.logEnabled(LogLevelDebug) {
		e.node.logger.log(newLogEntry(LogLevelDebug, "subscribe node on channel", map[string]any{"channel": ch}))
	}
	psShardIndex := index(ch, e.conf.numSubscribeShards)
	clusterShardIndex := 0
	if e.useShardedPubSub(s.shard) {
		clusterShardIndex = consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
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
	if e.useShardedPubSub(s.shard) {
		err = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
	} else {
		err = conn.Do(context.Background(), conn.B().Subscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
	}
	return err
}

type mapBrokerConnKey struct {
	shardIdx        int
	clusterShardIdx int
	psShardIdx      int
}

type mapBrokerConnGroup struct {
	shard    *brokerShardWrapper
	channels []string
}

func (e *RedisMapBroker) subscribeBatch(channels []string, unsub bool) error {
	groups := make(map[mapBrokerConnKey]*mapBrokerConnGroup)
	for _, ch := range channels {
		var shardIdx int
		if len(e.shards) > 1 {
			shardIdx = consistentIndex(ch, len(e.shards))
		}
		s := e.shards[shardIdx]
		psShardIdx := index(ch, e.conf.numSubscribeShards)
		clusterShardIdx := 0
		if e.useShardedPubSub(s.shard) {
			clusterShardIdx = consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
		}
		clusterShardIdx = s.pubSubRunner.subClientsIndex(clusterShardIdx)
		key := mapBrokerConnKey{shardIdx: shardIdx, clusterShardIdx: clusterShardIdx, psShardIdx: psShardIdx}
		g, ok := groups[key]
		if !ok {
			g = &mapBrokerConnGroup{shard: s}
			groups[key] = g
		}
		g.channels = append(g.channels, ch)
	}
	// Track completed groups so we can roll back on partial failure.
	var completed []mapBrokerConnKey
	for key, g := range groups {
		s := g.shard
		s.subClientsMu.Lock()
		conn := s.subClients[key.clusterShardIdx][key.psShardIdx]
		if conn == nil {
			s.subClientsMu.Unlock()
			if !unsub {
				e.rollbackSubscribeBatch(groups, completed)
			}
			return errPubSubConnUnavailable
		}
		s.subClientsMu.Unlock()

		chIDs := make([]string, len(g.channels))
		for i, ch := range g.channels {
			chIDs[i] = e.messageChannelID(s.shard, ch)
		}

		var err error
		if e.useShardedPubSub(s.shard) {
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
				e.rollbackSubscribeBatch(groups, completed)
			}
			return err
		}
		completed = append(completed, key)
	}
	return nil
}

// rollbackSubscribeBatch unsubscribes channels from groups that were already
// successfully subscribed. Best-effort: errors are ignored.
func (e *RedisMapBroker) rollbackSubscribeBatch(groups map[mapBrokerConnKey]*mapBrokerConnGroup, completed []mapBrokerConnKey) {
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
			chIDs[i] = e.messageChannelID(s.shard, ch)
		}
		if e.useShardedPubSub(s.shard) {
			_ = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(chIDs...).Build()).Error()
		} else {
			_ = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(chIDs...).Build()).Error()
		}
	}
}

// Unsubscribe from channels.
func (e *RedisMapBroker) Unsubscribe(channels ...string) error {
	if len(channels) == 0 {
		return nil
	}
	if len(channels) == 1 {
		return e.unsubscribe(e.getShard(channels[0]), channels[0])
	}
	return e.subscribeBatch(channels, true)
}

func (e *RedisMapBroker) unsubscribe(s *brokerShardWrapper, ch string) error {
	if e.node.logEnabled(LogLevelDebug) {
		e.node.logger.log(newLogEntry(LogLevelDebug, "unsubscribe node from channel", map[string]any{"channel": ch}))
	}
	psShardIndex := index(ch, e.conf.numSubscribeShards)
	clusterShardIndex := 0
	if e.useShardedPubSub(s.shard) {
		clusterShardIndex = consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
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
	if e.useShardedPubSub(s.shard) {
		err = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
	} else {
		err = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
	}
	return err
}

// parseMessage parses message formats:
// 1. No prefix (backward compat): raw protobuf bytes
// 2. Non-delta: offset:epoch:protobuf
// 3. Delta: d:offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
// Returns offset, epoch, push bytes, delta flag, prev protobuf, and error if parsing fails.
func parseMessage(data []byte) (uint64, string, []byte, bool, []byte, error) {
	if len(data) == 0 {
		return 0, "", nil, false, nil, fmt.Errorf("empty message")
	}

	// Check for delta prefix
	if data[0] == 'd' && len(data) > 1 && data[1] == ':' {
		// Delta format: d:offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
		return parseDeltaMessage(data[2:]) // Skip "d:"
	}

	// Check if first byte is a digit (offset start) or colon separator
	if data[0] < '0' || data[0] > '9' {
		// No prefix - raw protobuf (backward compatibility)
		return 0, "", data, false, nil, nil
	}

	// Non-delta format: offset:epoch:protobuf
	firstColon := bytes.IndexByte(data, ':')
	if firstColon == -1 {
		// No colon found - treat as raw protobuf
		return 0, "", data, false, nil, nil
	}

	// Parse offset
	offset, err := strconv.ParseUint(convert.BytesToString(data[:firstColon]), 10, 64)
	if err != nil {
		// Failed to parse offset - treat as raw protobuf
		return 0, "", data, false, nil, nil
	}

	// Find second colon (after epoch)
	remaining := data[firstColon+1:]
	secondColon := bytes.IndexByte(remaining, ':')
	if secondColon == -1 {
		return 0, "", nil, false, nil, fmt.Errorf("missing epoch separator")
	}

	// Extract epoch
	epoch := convert.BytesToString(remaining[:secondColon])

	// Everything after second colon is protobuf bytes
	protobuf := remaining[secondColon+1:]

	return offset, epoch, protobuf, false, nil, nil
}

// parseDeltaMessage parses delta format: offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
func parseDeltaMessage(data []byte) (uint64, string, []byte, bool, []byte, error) {
	// Parse offset
	firstColon := bytes.IndexByte(data, ':')
	if firstColon == -1 {
		return 0, "", nil, false, nil, fmt.Errorf("delta: missing offset separator")
	}
	offset, err := strconv.ParseUint(convert.BytesToString(data[:firstColon]), 10, 64)
	if err != nil {
		return 0, "", nil, false, nil, fmt.Errorf("delta: invalid offset: %w", err)
	}

	// Parse epoch
	remaining := data[firstColon+1:]
	secondColon := bytes.IndexByte(remaining, ':')
	if secondColon == -1 {
		return 0, "", nil, false, nil, fmt.Errorf("delta: missing epoch separator")
	}
	epoch := convert.BytesToString(remaining[:secondColon])

	// Parse prev_len
	remaining = remaining[secondColon+1:]
	thirdColon := bytes.IndexByte(remaining, ':')
	if thirdColon == -1 {
		return 0, "", nil, false, nil, fmt.Errorf("delta: missing prev length separator")
	}
	prevLen, err := strconv.Atoi(convert.BytesToString(remaining[:thirdColon]))
	if err != nil {
		return 0, "", nil, false, nil, fmt.Errorf("delta: invalid prev length: %w", err)
	}

	// Extract prev_protobuf
	remaining = remaining[thirdColon+1:]
	if len(remaining) < prevLen {
		return 0, "", nil, false, nil, fmt.Errorf("delta: insufficient data for prev protobuf")
	}
	prevProtobuf := remaining[:prevLen]

	// Parse curr_len
	remaining = remaining[prevLen:]
	if len(remaining) == 0 || remaining[0] != ':' {
		return 0, "", nil, false, nil, fmt.Errorf("delta: missing curr length separator")
	}
	remaining = remaining[1:] // Skip ':'
	fourthColon := bytes.IndexByte(remaining, ':')
	if fourthColon == -1 {
		return 0, "", nil, false, nil, fmt.Errorf("delta: missing curr protobuf separator")
	}
	currLen, err := strconv.Atoi(convert.BytesToString(remaining[:fourthColon]))
	if err != nil {
		return 0, "", nil, false, nil, fmt.Errorf("delta: invalid curr length: %w", err)
	}

	// Extract curr_protobuf
	remaining = remaining[fourthColon+1:]
	if len(remaining) < currLen {
		return 0, "", nil, false, nil, fmt.Errorf("delta: insufficient data for curr protobuf")
	}
	currProtobuf := remaining[:currLen]

	return offset, epoch, currProtobuf, true, prevProtobuf, nil
}

// parseAddScriptResult parses the result from the add script.
func parseAddScriptResult(replies []rueidis.RedisMessage) (MapUpdateResult, error) {
	if len(replies) < 2 {
		return MapUpdateResult{}, fmt.Errorf("wrong number of replies from script: %d", len(replies))
	}
	offset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return MapUpdateResult{}, fmt.Errorf("could not parse offset: %w", err)
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return MapUpdateResult{}, fmt.Errorf("could not parse epoch: %w", err)
	}

	// Check for suppression reason (3rd value is the reason string, empty means not suppressed)
	var suppressReason SuppressReason
	if len(replies) >= 3 {
		reason, err := replies[2].ToString()
		if err == nil && reason != "" {
			suppressReason = SuppressReason(reason)
		}
	}

	result := MapUpdateResult{
		Position: StreamPosition{Offset: offset, Epoch: epoch},
	}
	if suppressReason != SuppressReasonNone {
		result.Suppressed = true
		result.SuppressReason = suppressReason

		// For CAS mismatch, parse and return current publication for immediate retry.
		// Client uses: CurrentPublication.Offset + Position.Epoch for next CAS attempt.
		if suppressReason == SuppressReasonPositionMismatch && len(replies) >= 4 {
			currentValue, err := replies[3].AsBytes()
			if err == nil && len(currentValue) > 0 {
				// Parse value: offset:epoch:payload
				entryOffset, _, payload, parseErr := parseStateValue(currentValue)
				if parseErr == nil {
					var protoPub protocol.Publication
					if protoPub.UnmarshalVT(payload) == nil {
						pub := pubFromProto(&protoPub)
						pub.Offset = entryOffset
						result.CurrentPublication = pub
					}
				}
			}
		}
	}
	return result, nil
}
