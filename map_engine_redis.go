package centrifuge

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	_ "embed"

	"github.com/centrifugal/centrifuge/internal/bpool"
	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/protocol"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/resp"
)

var (
	//go:embed internal/redis_lua/map_engine_add.lua
	brokerSnapshotPublishScriptSource string
	//go:embed internal/redis_lua/map_engine_read_ordered.lua
	brokerSnapshotReadOrderedScriptSource string
	//go:embed internal/redis_lua/map_engine_read_unordered.lua
	brokerSnapshotReadUnorderedScriptSource string
	//go:embed internal/redis_lua/map_engine_stream_read.lua
	brokerSnapshotReadStreamScriptSource string
	//go:embed internal/redis_lua/map_engine_read_meta.lua
	brokerSnapshotReadMetaScriptSource string
	//go:embed internal/redis_lua/map_engine_stats.lua
	brokerSnapshotStatsScriptSource string
	//go:embed internal/redis_lua/map_engine_cleanup.lua
	brokerSnapshotCleanupScriptSource string
)

type engineShardWrapper struct {
	shard               *RedisShard
	subClientsMu        sync.Mutex
	subClients          [][]rueidis.DedicatedClient
	pubSubStartChannels [][]*pubSubStart
}

// RedisMapEngine is a Redis-based MapEngine that provides support for:
// 1. Stateful channels - keyed state with revisions
// 2. Converged membership - presence with ordering and recovery
//
// Both features share the same abstraction:
// - Snapshot – a key-based snapshot
// - Stream - a log of changes – state and update revision for each key.
// - Revisions are ordered per channel
//
// Message Formats
// ===============
//
// This engine uses simplified message formats published by Lua scripts:
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
// - Snapshots (HSET): For keyed state - may keep latest protocol.Publication or custom state.
//
// Pagination:
// - Ordered snapshots use ZRANGEBYSCORE/ZRANGEBYLEX with LIMIT — exact page sizes.
// - Unordered snapshots use HSCAN with COUNT — COUNT is only a hint, Redis may return
//   more entries than requested (especially for small hashes in listpack encoding).
//   Callers should not rely on exact Limit enforcement for unordered reads.
type RedisMapEngine struct {
	node *Node
	conf RedisMapEngineConfig

	shards []*engineShardWrapper

	addScript           *rueidis.Lua
	readOrderedScript   *rueidis.Lua
	readUnorderedScript *rueidis.Lua
	readStreamScript    *rueidis.Lua
	readMetaScript      *rueidis.Lua
	presenceStatsScript *rueidis.Lua
	cleanupScript       *rueidis.Lua

	closeCh       chan struct{}
	closeOnce     sync.Once
	shardChannel  string
	messagePrefix string
}

var _ MapEngine = (*RedisMapEngine)(nil)

// RedisMapEngineConfig is a config for RedisMapEngine.
type RedisMapEngineConfig struct {
	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	Shards []*RedisShard
	// Prefix to use before every channel name and key in Redis.
	Prefix string
	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
	// it on the client side. This is useful for FIPS compliance.
	LoadSHA1 bool
	// IdempotentResultTTL is a time-to-live for idempotent result.
	IdempotentResultTTL time.Duration
	// SubscribeOnReplica allows subscribing on replica Redis nodes.
	SubscribeOnReplica bool
	// SkipPubSub enables mode when engine only works with data structures, without
	// publishing to channels and using PUB/SUB.
	SkipPubSub bool
	// NumShardedPubSubPartitions when greater than zero allows turning on a mode in which
	// engine will use Redis Cluster with sharded PUB/SUB feature available in
	// Redis >= 7: https://redis.io/docs/manual/pubsub/#sharded-pubsub
	NumShardedPubSubPartitions int
	// numSubscribeShards defines how many subscribe shards will be used.
	numSubscribeShards int
	// numResubscribeShards defines how many subscriber goroutines will be used for
	// resubscribing process for each subscribe shard.
	numResubscribeShards int
	// numPubSubProcessors allows configuring number of workers which will process
	// messages coming from Redis PUB/SUB.
	numPubSubProcessors int

	// CleanupInterval defines how often to run the cleanup worker that
	// generates remove events for expired keyed snapshot entries (presence and state).
	// Zero disables cleanup. Applies to all channels using TTL-based snapshots.
	CleanupInterval time.Duration
	// CleanupBatchSize defines max entries to process per channel per cleanup cycle.
	// Default is 100. Applies to all keyed snapshots (presence and state).
	CleanupBatchSize int

	// PresenceTTL is a time-to-live for presence information.
	PresenceTTL time.Duration
	// PresenceStreamSize defines the maximum number of presence events (joins/leaves)
	// to keep in the presence stream for recovery.
	PresenceStreamSize int
	// PresenceStreamTTL defines TTL for presence stream.
	PresenceStreamTTL time.Duration
	// PresenceMetaTTL defines TTL for presence stream metadata (offset, epoch).
	PresenceMetaTTL time.Duration
}

// NewRedisMapEngine initializes RedisMapEngine.
func NewRedisMapEngine(n *Node, conf RedisMapEngineConfig) (*RedisMapEngine, error) {
	if len(conf.Shards) == 0 {
		return nil, errors.New("snapshot engine: no shards provided")
	}

	if conf.SubscribeOnReplica {
		for i, s := range conf.Shards {
			if s.replicaClient == nil {
				return nil, fmt.Errorf("engine: SubscribeOnReplica enabled but no replica client initialized in shard[%d] (ReplicaClientEnabled option)", i)
			}
		}
	}

	if conf.Prefix == "" {
		conf.Prefix = "centrifuge"
	}
	if conf.PresenceTTL == 0 {
		conf.PresenceTTL = 60 * time.Second
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
		conf.numPubSubProcessors = 16 // Can be tuned
	}
	if conf.CleanupBatchSize == 0 {
		conf.CleanupBatchSize = 100
	}
	// CleanupInterval defaults to 0 (disabled). User must explicitly enable.
	// When enabled, it works for ALL keyed snapshots (presence and state with TTL).

	shardWrappers := make([]*engineShardWrapper, 0, len(conf.Shards))
	for _, s := range conf.Shards {
		shardWrappers = append(shardWrappers, &engineShardWrapper{shard: s})
	}

	e := &RedisMapEngine{
		node:   n,
		conf:   conf,
		shards: shardWrappers,
		addScript: rueidis.NewLuaScript(
			brokerSnapshotPublishScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readOrderedScript: rueidis.NewLuaScript(
			brokerSnapshotReadOrderedScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readUnorderedScript: rueidis.NewLuaScript(
			brokerSnapshotReadUnorderedScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readStreamScript: rueidis.NewLuaScript(
			brokerSnapshotReadStreamScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		readMetaScript: rueidis.NewLuaScript(
			brokerSnapshotReadMetaScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		presenceStatsScript: rueidis.NewLuaScript(
			brokerSnapshotStatsScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		cleanupScript: rueidis.NewLuaScript(
			brokerSnapshotCleanupScriptSource,
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
			return nil, errors.New("Redis Cluster requires sharded PUB/SUB (set NumShardedPubSubPartitions > 0)")
		}
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

		wrapper.subClients = subChannels
		wrapper.pubSubStartChannels = pubSubStartChannels
	}

	// Start background cleanup worker for guaranteed remove events on expiry.
	// Runs for both presence (PresenceStreamSize > 0) and keyed state (any channel using KeyTTL).
	// Since we can't detect keyed state usage at init time, we always run if cleanup is enabled.
	if e.conf.CleanupInterval > 0 {
		go e.runCleanupWorker(context.Background())
	}

	return e, nil
}

// parseSnapshotValue parses a snapshot value in format: offset:epoch:payload
// Returns offset, epoch, payload, and error if parsing fails.
func parseSnapshotValue(val []byte) (uint64, string, []byte, error) {
	if len(val) == 0 {
		return 0, "", nil, fmt.Errorf("empty snapshot value")
	}

	// Find first colon (offset separator)
	firstColon := bytes.IndexByte(val, ':')
	if firstColon == -1 {
		return 0, "", nil, fmt.Errorf("missing offset separator in snapshot value")
	}

	// Parse offset
	offset, err := strconv.ParseUint(convert.BytesToString(val[:firstColon]), 10, 64)
	if err != nil {
		return 0, "", nil, fmt.Errorf("invalid offset in snapshot value: %w", err)
	}

	// Find second colon (epoch separator)
	remaining := val[firstColon+1:]
	secondColon := bytes.IndexByte(remaining, ':')
	if secondColon == -1 {
		return 0, "", nil, fmt.Errorf("missing epoch separator in snapshot value")
	}

	// Extract epoch
	epoch := convert.BytesToString(remaining[:secondColon])

	// Everything after second colon is payload
	payload := remaining[secondColon+1:]

	return offset, epoch, payload, nil
}

func (e *RedisMapEngine) useShardedPubSub(s *RedisShard) bool {
	return s.isCluster && e.conf.NumShardedPubSubPartitions > 0
}

func (e *RedisMapEngine) getShard(channel string) *engineShardWrapper {
	if len(e.shards) == 1 {
		return e.shards[0]
	}
	return e.shards[consistentIndex(channel, len(e.shards))]
}

func (e *RedisMapEngine) streamKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":stream:")
}

func (e *RedisMapEngine) metaKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":meta:")
}

func (e *RedisMapEngine) snapshotHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:")
}

func (e *RedisMapEngine) snapshotOrderKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:order:")
}

func (e *RedisMapEngine) snapshotExpireKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:expire:")
}

func (e *RedisMapEngine) snapshotMetaKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:meta:")
}

// Presence-specific key functions removed - presence now uses generic snapshot keys:
// - snapshotHashKey instead of presenceHashKey
// - snapshotExpireKey instead of presenceExpireKey
// - streamKey instead of presenceStreamKey
// - metaKey instead of presenceMetaKey
// This unifies presence and keyed state - both are just keyed snapshots with different configs.

func (e *RedisMapEngine) aggregationZSetKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":aggregation:zset:")
}

func (e *RedisMapEngine) aggregationHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":aggregation:hash:")
}

func (e *RedisMapEngine) aggregationMappingHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":aggmap:")
}

func (e *RedisMapEngine) cleanupRegistrationKey(s *RedisShard) string {
	// Cleanup registration key is per-shard, not per-channel
	// Uses partition-based hash tag for cluster compatibility
	if !s.isCluster {
		return e.conf.Prefix + ":cleanup:channels"
	}
	if e.conf.NumShardedPubSubPartitions > 0 {
		// For sharded PUB/SUB, we have one cleanup key per partition
		// The partition index will be added by the caller
		return e.conf.Prefix + ":cleanup:channels"
	}
	return e.conf.Prefix + ":cleanup:channels"
}

func (e *RedisMapEngine) cleanupRegistrationKeyForChannel(s *RedisShard, ch string) string {
	// Get the cleanup registration key with proper hash tag for the channel's partition
	// Single registration ZSET works for ALL keyed snapshots (presence, state, etc.)
	if !s.isCluster {
		return e.conf.Prefix + ":cleanup:channels"
	}
	if e.conf.NumShardedPubSubPartitions > 0 {
		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
		return e.conf.Prefix + ":cleanup:channels:{" + strconv.Itoa(idx) + "}"
	}
	// For non-sharded cluster, use channel-based hashtag.
	return e.conf.Prefix + ":cleanup:channels:{" + ch + "}"
}

func (e *RedisMapEngine) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) string {
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

	var builder strings.Builder

	if e.conf.NumShardedPubSubPartitions > 0 {
		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
		idxStr := strconv.Itoa(idx)
		capacity := len(e.conf.Prefix) + 9 + 1 + len(idxStr) + 2 + len(ch) + 1 + len(idempotencyKey)
		builder.Grow(capacity)
		builder.WriteString(e.conf.Prefix)
		builder.WriteString(".result.{")
		builder.WriteString(idxStr)
		builder.WriteString("}.")
		builder.WriteString(ch)
		builder.WriteByte('.')
		builder.WriteString(idempotencyKey)
	} else {
		capacity := len(e.conf.Prefix) + 9 + 1 + len(ch) + 2 + len(idempotencyKey)
		builder.Grow(capacity)
		builder.WriteString(e.conf.Prefix)
		builder.WriteString(".result.{")
		builder.WriteString(ch)
		builder.WriteString("}.")
		builder.WriteString(idempotencyKey)
	}

	return builder.String()
}

// buildKey is a helper function to build Redis keys with proper cluster hash tag support
func (e *RedisMapEngine) buildKey(s *RedisShard, ch string, infix string) string {
	if !s.isCluster {
		var builder strings.Builder
		builder.Grow(len(e.conf.Prefix) + len(infix) + len(ch))
		builder.WriteString(e.conf.Prefix)
		builder.WriteString(infix)
		builder.WriteString(ch)
		return builder.String()
	}

	var builder strings.Builder

	if e.conf.NumShardedPubSubPartitions > 0 {
		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
		idxStr := strconv.Itoa(idx)
		capacity := len(e.conf.Prefix) + len(infix) + 1 + len(idxStr) + 2 + len(ch)
		builder.Grow(capacity)
		builder.WriteString(e.conf.Prefix)
		builder.WriteString(infix)
		builder.WriteByte('{')
		builder.WriteString(idxStr)
		builder.WriteString("}.")
		builder.WriteString(ch)
	} else {
		capacity := len(e.conf.Prefix) + len(infix) + 1 + len(ch) + 1
		builder.Grow(capacity)
		builder.WriteString(e.conf.Prefix)
		builder.WriteString(infix)
		builder.WriteByte('{')
		builder.WriteString(ch)
		builder.WriteByte('}')
	}

	return builder.String()
}

func (e *RedisMapEngine) messageChannelID(s *RedisShard, ch string) string {
	if !e.useShardedPubSub(s) {
		if s.isCluster {
			var builder strings.Builder
			builder.Grow(len(e.messagePrefix) + 1 + len(ch) + 1)
			builder.WriteString(e.messagePrefix)
			builder.WriteByte('{')
			builder.WriteString(ch)
			builder.WriteByte('}')
			return builder.String()
		}
		var builder strings.Builder
		builder.Grow(len(e.messagePrefix) + len(ch))
		builder.WriteString(e.messagePrefix)
		builder.WriteString(ch)
		return builder.String()
	}

	idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
	idxStr := strconv.Itoa(idx)

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

// Close closes the engine.
func (e *RedisMapEngine) Close(_ context.Context) error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
	return nil
}

func (e *RedisMapEngine) Clear(ctx context.Context, ch string, opts MapClearOptions) error {
	// TODO: implement.
	return nil
}

func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// Publish publishes data to a stateful channel with optional keyed state.
func (e *RedisMapEngine) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	// Apply channel options defaults from node config.
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL, opts.KeyTTL = applyChannelOptionsDefaults(
		opts.StreamSize, opts.StreamTTL, opts.MetaTTL, opts.KeyTTL,
		e.node.ResolveMapChannelOptions, ch,
	)

	// Fast path for non-history, non-idempotent, non-keyed publications.
	if opts.StreamSize == 0 && opts.StreamTTL == 0 && opts.IdempotencyKey == "" && key == "" {
		if e.conf.SkipPubSub {
			return MapPublishResult{}, nil
		}
		protoPub := &protocol.Publication{
			Data: opts.Data,
			Info: infoToProto(opts.ClientInfo),
			Tags: opts.Tags,
			Time: time.Now().UnixMilli(),
		}
		protoPub.Delta = opts.UseDelta

		push := &protocol.Push{
			Pub: protoPub,
		}
		pushBytes, err := push.MarshalVT()
		if err != nil {
			return MapPublishResult{}, err
		}

		payload := "0::" + convert.BytesToString(pushBytes)

		chID := e.messageChannelID(s.shard, ch)
		if e.useShardedPubSub(s.shard) {
			cmd := shardClient.B().Spublish().Channel(chID).Message(payload).Build()
			return MapPublishResult{}, shardClient.Do(ctx, cmd).Error()
		}
		cmd := shardClient.B().Publish().Channel(chID).Message(payload).Build()
		return MapPublishResult{}, shardClient.Do(ctx, cmd).Error()
	}

	now := time.Now().UnixMilli()

	// Stream publication (used for stream and pub/sub) - may have StreamData for incremental updates.
	streamData := opts.Data
	if len(opts.StreamData) > 0 {
		streamData = opts.StreamData
	}
	streamProtoPub := &protocol.Publication{
		Data:  streamData,
		Info:  infoToProto(opts.ClientInfo),
		Tags:  opts.Tags,
		Time:  now,
		Key:   key,
		Score: opts.Score,
	}
	streamBytes, err := streamProtoPub.MarshalVT()
	if err != nil {
		return MapPublishResult{}, err
	}

	// Snapshot publication (used for snapshot storage) - always uses full state Data.
	var snapshotBytes []byte
	if len(opts.StreamData) > 0 {
		// Different data for snapshot vs stream - need separate serialization.
		snapshotProtoPub := &protocol.Publication{
			Data:  opts.Data,
			Info:  infoToProto(opts.ClientInfo),
			Tags:  opts.Tags,
			Time:  now,
			Key:   key,
			Score: opts.Score,
		}
		snapshotBytes, err = snapshotProtoPub.MarshalVT()
		if err != nil {
			return MapPublishResult{}, err
		}
	}
	// If snapshotBytes is nil, Lua will use streamBytes for both.

	var resultKey string
	var resultExpire string
	if opts.IdempotencyKey != "" {
		resultKey = e.resultCacheKey(s.shard, ch, opts.IdempotencyKey)
		if opts.IdempotentResultTTL > 0 {
			resultExpire = strconv.Itoa(int(opts.IdempotentResultTTL.Seconds()))
		} else {
			resultExpire = strconv.Itoa(int(e.conf.IdempotentResultTTL.Seconds()))
		}
	}

	var streamKey, metaKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey, snapshotMetaKey string

	if opts.StreamSize > 0 && opts.StreamTTL > 0 {
		streamKey = e.streamKey(s.shard, ch)
		metaKey = e.metaKey(s.shard, ch)
	}

	if key != "" {
		snapshotHashKey = e.snapshotHashKey(s.shard, ch)
		snapshotMetaKey = e.snapshotMetaKey(s.shard, ch)
		if opts.Ordered {
			snapshotOrderKey = e.snapshotOrderKey(s.shard, ch)
		}
		snapshotExpireKey = e.snapshotExpireKey(s.shard, ch)
	}

	metaExpire := "0"
	if opts.MetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
	} else if e.node.config.HistoryMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
	}

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
	if opts.KeyTTL > 0 && key != "" && snapshotExpireKey != "" {
		cleanupRegKey = e.cleanupRegistrationKeyForChannel(s.shard, ch)
	}

	// Prepare ExpectedPosition arguments for CAS
	expectedOffset := ""
	expectedEpoch := ""
	if opts.ExpectedPosition != nil {
		expectedOffset = strconv.FormatUint(opts.ExpectedPosition.Offset, 10)
		expectedEpoch = opts.ExpectedPosition.Epoch
	}

	replies, err := e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, resultKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey,
			snapshotMetaKey, cleanupRegKey,
		},
		[]string{
			key,                                // message_key
			convert.BytesToString(streamBytes), // message_payload (Publication - for stream and publishing)
			strconv.Itoa(opts.StreamSize),
			strconv.FormatInt(int64(opts.StreamTTL.Seconds()), 10),
			chID, // channel (for Lua to publish)
			metaExpire,
			uuid.New().String(), // new_epoch_if_empty
			publishCommand,
			resultExpire,
			useDelta,
			version,
			opts.VersionEpoch,
			"0", // is_remove
			strconv.FormatInt(opts.Score, 10),
			strconv.FormatInt(int64(opts.KeyTTL.Seconds()), 10),
			"0",                                  // use_hexpire
			ch,                                   // channel_for_cleanup (for cleanup registration)
			string(opts.KeyMode),                 // key_mode
			boolToStr(opts.RefreshTTLOnSuppress), // refresh_ttl_on_suppress
			expectedOffset,                       // expected_offset (for CAS)
			expectedEpoch,                        // expected_epoch (for CAS)
			convert.BytesToString(snapshotBytes), // snapshot_payload (for snapshot storage, empty to use message_payload)
		},
	).ToArray()
	if err != nil {
		return MapPublishResult{}, err
	}

	return parseAddScriptResult(replies)
}

// Remove removes a key from keyed state snapshot.
func (e *RedisMapEngine) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	if key == "" {
		return MapPublishResult{}, fmt.Errorf("key is required for unpublish")
	}

	s := e.getShard(ch)
	shardClient := s.shard.client

	// Apply channel options defaults from node config.
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL, _ = applyChannelOptionsDefaults(
		opts.StreamSize, opts.StreamTTL, opts.MetaTTL, 0,
		e.node.ResolveMapChannelOptions, ch,
	)

	var streamKey, metaKey string
	if opts.StreamSize > 0 && opts.StreamTTL > 0 {
		streamKey = e.streamKey(s.shard, ch)
		metaKey = e.metaKey(s.shard, ch)
	}

	// For unpublish, we use snapshot keys to track which keys exist
	snapshotHashKey := e.snapshotHashKey(s.shard, ch)
	snapshotExpireKey := e.snapshotExpireKey(s.shard, ch)
	snapshotMetaKey := e.snapshotMetaKey(s.shard, ch)

	metaExpire := "0"
	if opts.MetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
	} else if e.node.config.HistoryMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
	}

	publishCommand := "PUBLISH"
	if e.useShardedPubSub(s.shard) {
		publishCommand = "SPUBLISH"
	}
	chID := e.messageChannelID(s.shard, ch)
	if e.conf.SkipPubSub {
		publishCommand = ""
		chID = ""
	}

	// Create a Publication with key and removed=true to signal removal
	protoPub := &protocol.Publication{
		Key:     key,
		Removed: true,
		Time:    time.Now().UnixMilli(),
	}
	pubBytes, err := protoPub.MarshalVT()
	if err != nil {
		return MapPublishResult{}, err
	}

	replies, err := e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, "", // Optional stream, no result key
			snapshotHashKey,
			"", // No order key for unpublish
			snapshotExpireKey,
			snapshotMetaKey,
			"", // No cleanup registration for unpublish
		},
		[]string{
			key,                             // message_key
			convert.BytesToString(pubBytes), // message_payload (Publication with Removed=true for stream)
			strconv.Itoa(opts.StreamSize),
			strconv.FormatInt(int64(opts.StreamTTL.Seconds()), 10),
			chID, // channel (for Lua to publish)
			metaExpire,
			e.node.ID(), // new_epoch_if_empty
			publishCommand,
			"", "0", "0", "", // result_key_expire, use_delta, version, version_epoch
			"1", // is_leave (this triggers removal)
			"0", // score
			"0", // map_member_ttl
			"0", // use_hexpire
			"",  // channel_for_cleanup (not used for unpublish)
			"",  // key_mode (not used for unpublish)
			"0", // refresh_ttl_on_suppress (not used for unpublish)
			"",  // expected_offset (not used for unpublish)
			"",  // expected_epoch (not used for unpublish)
			"",  // snapshot_payload (not used for unpublish)
		},
	).ToArray()
	if err != nil {
		return MapPublishResult{}, err
	}

	return parseAddScriptResult(replies)
}

// ReadState retrieves snapshot entries with per-entry revisions for a channel.
// Each entry includes its revision so client can filter: entry.Revision <= snapshot_revision.
// If opts.Revision is provided and epoch changed, returns empty entries.
// Returns entries, stream position, next cursor for pagination, and error.
// Cursor "0" or "" means end of iteration.
func (e *RedisMapEngine) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	// Handle single key lookup (Key filter)
	if opts.Key != "" {
		return e.readSingleKey(ctx, ch, opts)
	}
	if opts.Ordered {
		return e.readOrderedSnapshot(ctx, ch, opts)
	}
	return e.readUnorderedSnapshot(ctx, ch, opts)
}

func (e *RedisMapEngine) ReadStateZero(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	// Handle single key lookup (Key filter)
	if opts.Key != "" {
		return e.readSingleKey(ctx, ch, opts)
	}
	if opts.Ordered {
		return e.readOrderedSnapshot(ctx, ch, opts)
	}
	return e.readUnorderedSnapshotZero(ctx, ch, opts)
}

// readSingleKey retrieves a single key from the snapshot using HGET instead of HSCAN.
// This is more efficient for single key lookups and supports CAS read-modify-write patterns.
func (e *RedisMapEngine) readSingleKey(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	snapshotHashKey := e.snapshotHashKey(s.shard, ch)
	metaKey := e.metaKey(s.shard, ch)

	// Execute HGET and metadata read in pipeline
	cmds := make([]rueidis.Completed, 0, 2)
	cmds = append(cmds, shardClient.B().Hget().Key(snapshotHashKey).Field(opts.Key).Build())
	cmds = append(cmds, shardClient.B().Hmget().Key(metaKey).Field("s", "e").Build())

	results := shardClient.DoMulti(ctx, cmds...)

	// Parse HGET result
	valBytes, err := results[0].AsBytes()
	keyNotFound := rueidis.IsRedisNil(err)
	if err != nil && !keyNotFound {
		return nil, StreamPosition{}, "", err
	}

	// Parse metadata
	metaArr, err := results[1].ToArray()
	if err != nil {
		return nil, StreamPosition{}, "", err
	}

	var streamPos StreamPosition
	if len(metaArr) >= 2 {
		offsetVal, _ := metaArr[0].AsUint64()
		epochVal, _ := metaArr[1].ToString()
		streamPos = StreamPosition{Offset: offsetVal, Epoch: epochVal}
	}

	// Validate epoch if client provided snapshot revision
	if opts.Revision != nil && opts.Revision.Epoch != streamPos.Epoch {
		return nil, streamPos, "", ErrorUnrecoverablePosition
	}

	// Key not found - return empty
	if keyNotFound || len(valBytes) == 0 {
		return []*Publication{}, streamPos, "", nil
	}

	// Parse value: offset:epoch:payload
	entryOffset, _, payload, err := parseSnapshotValue(valBytes)
	if err != nil {
		return nil, streamPos, "", fmt.Errorf("failed to parse snapshot value: %w", err)
	}

	// Unmarshal Publication from protobuf payload
	var protoPub protocol.Publication
	if err := protoPub.UnmarshalVT(payload); err != nil {
		return nil, streamPos, "", fmt.Errorf("failed to unmarshal publication: %w", err)
	}

	pub := pubFromProto(&protoPub)
	pub.Key = opts.Key
	pub.Offset = entryOffset

	return []*Publication{pub}, streamPos, "", nil
}

func (e *RedisMapEngine) readUnorderedSnapshot(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	snapshotTTL := "0"
	if opts.StateTTL > 0 {
		snapshotTTL = strconv.FormatInt(int64(opts.StateTTL.Seconds()), 10)
	}

	// Cursor must be "0" to start for Redis HSCAN
	cursor := opts.Cursor
	if cursor == "" {
		cursor = "0"
	}

	replies, err := e.readUnorderedScript.Exec(ctx, shardClient,
		[]string{
			e.snapshotHashKey(s.shard, ch),
			e.snapshotExpireKey(s.shard, ch),
			e.metaKey(s.shard, ch),
			e.snapshotMetaKey(s.shard, ch),
		},
		[]string{
			cursor,
			strconv.Itoa(opts.Limit),
			strconv.FormatInt(time.Now().Unix(), 10),
			snapshotTTL,
			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10),
		},
	).ToArray()
	if err != nil {
		return nil, StreamPosition{}, "", err
	}
	if len(replies) < 4 {
		return nil, StreamPosition{}, "", errors.New("wrong number of replies")
	}

	streamOffset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, StreamPosition{}, "", err
	}
	streamEpoch, _ := replies[1].ToString()
	nextCursor, _ := replies[2].ToString()
	if nextCursor == "0" {
		nextCursor = "" // Convert Redis HSCAN "0" (complete) to empty string
	}
	dataArr, _ := replies[3].ToArray()

	streamPos := StreamPosition{Offset: streamOffset, Epoch: streamEpoch}

	// Validate epoch if client provided snapshot revision
	if opts.Revision != nil && opts.Revision.Epoch != streamEpoch {
		// Epoch changed, client needs to restart from beginning
		return nil, streamPos, nextCursor, ErrorUnrecoverablePosition
	}

	// Parse snapshot values with revisions
	pubs := make([]*Publication, 0, len(dataArr)/2)
	for i := 0; i < len(dataArr); i += 2 {
		key, _ := dataArr[i].ToString()
		val, _ := dataArr[i+1].AsBytes()

		// Parse value: offset:epoch:payload
		entryOffset, _, payload, err := parseSnapshotValue(val)
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
	return pubs, streamPos, nextCursor, nil
}

func (e *RedisMapEngine) readUnorderedSnapshotZero(
	ctx context.Context,
	ch string,
	opts MapReadStateOptions,
) ([]*Publication, StreamPosition, string, error) {
	s := e.getShard(ch)

	snapshotTTL := "0"
	if opts.StateTTL > 0 {
		snapshotTTL = strconv.FormatInt(int64(opts.StateTTL.Seconds()), 10)
	}

	cursor := opts.Cursor
	if cursor == "" {
		cursor = "0"
	}

	var (
		streamPos  StreamPosition
		nextCursor string
		pubs       []*Publication
	)

	err := e.readUnorderedScript.ExecWithReader(
		ctx,
		s.shard.client,
		[]string{
			e.snapshotHashKey(s.shard, ch),
			e.snapshotExpireKey(s.shard, ch),
			e.metaKey(s.shard, ch),
			e.snapshotMetaKey(s.shard, ch),
		},
		[]string{
			cursor,
			strconv.Itoa(opts.Limit),
			strconv.FormatInt(time.Now().Unix(), 10),
			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10),
			snapshotTTL,
		},
		func(r *bufio.Reader) error {
			rr := resp.NewReader(r)
			if err := rr.ExpectArrayWithLen(4); err != nil {
				return err
			}

			// --- reply[0]: offset ---
			offsetBytes, err := rr.ReadStringBytes()
			if err != nil {
				return err
			}
			if offsetBytes != nil {
				streamPos.Offset, err = strconv.ParseUint(
					unsafe.String(&offsetBytes[0], len(offsetBytes)), 10, 64,
				)
				if err != nil {
					return err
				}
			}

			// --- reply[1]: epoch ---
			epochBytes, err := rr.ReadStringBytes()
			if err != nil {
				return err
			}
			if epochBytes != nil {
				streamPos.Epoch = unsafe.String(&epochBytes[0], len(epochBytes))
			}

			// --- reply[2]: cursor ---
			cursorBytes, err := rr.ReadStringBytes()
			if err != nil && !errors.Is(err, rueidis.Nil) {
				return err
			}
			if cursorBytes != nil {
				nextCursor = unsafe.String(&cursorBytes[0], len(cursorBytes))
			}
			if nextCursor == "0" {
				nextCursor = "" // Convert Redis HSCAN "0" (complete) to empty string
			}

			// --- reply[3]: key-value array ---
			kvCount, err := rr.ExpectArray()
			if err != nil {
				return err
			}
			pubs = make([]*Publication, 0, kvCount/2)

			for i := int64(0); i < kvCount; i += 2 {
				// Read next key
				keyBytes, err := rr.ReadStringBytes()
				if err != nil {
					return err
				}
				// Make a safe copy
				key := convert.BytesToString(append([]byte(nil), keyBytes...))

				// Read corresponding value
				valBytes, err := rr.ReadStringBytes()
				if err != nil {
					return err
				}
				// Parse value: offset:epoch:payload
				entryOffset, _, payloadBytes, err := parseSnapshotValue(valBytes)
				if err != nil {
					// skip malformed entries
					continue
				}
				// Unmarshal Publication from protobuf payload
				var protoPub protocol.Publication
				if err := protoPub.UnmarshalVT(payloadBytes); err != nil {
					// Skip malformed entries
					continue
				}

				pub := pubFromProto(&protoPub)
				pub.Key = key
				pub.Offset = entryOffset
				pubs = append(pubs, pub)
			}

			return nil
		},
	)

	if err != nil {
		return nil, StreamPosition{}, "", err
	}

	// Validate snapshot revision (unchanged semantics)
	if opts.Revision != nil &&
		opts.Revision.Epoch != streamPos.Epoch {
		return nil, streamPos, nextCursor, ErrorUnrecoverablePosition
	}

	return pubs, streamPos, nextCursor, nil
}

// readOrderedSnapshot uses key-based cursor pagination for continuity.
// Cursor format: "score\x00key" to ensure no entries are skipped during concurrent modifications.
func (e *RedisMapEngine) readOrderedSnapshot(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	snapshotTTL := "0"
	if opts.StateTTL > 0 {
		snapshotTTL = strconv.FormatInt(int64(opts.StateTTL.Seconds()), 10)
	}

	// Parse cursor: "score\x00key" format
	cursorScore := ""
	cursorKey := ""
	if opts.Cursor != "" {
		cursorScore, cursorKey = parseOrderedCursor(opts.Cursor)
	}

	replies, err := e.readOrderedScript.Exec(ctx, shardClient,
		[]string{
			e.snapshotHashKey(s.shard, ch),
			e.snapshotOrderKey(s.shard, ch),
			e.snapshotExpireKey(s.shard, ch),
			e.metaKey(s.shard, ch),
			e.snapshotMetaKey(s.shard, ch),
		},
		[]string{
			strconv.Itoa(opts.Limit),                 // ARGV[1] = limit
			cursorScore,                              // ARGV[2] = cursor_score
			cursorKey,                                // ARGV[3] = cursor_key
			strconv.FormatInt(time.Now().Unix(), 10), // ARGV[4] = now
			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10), // ARGV[5] = meta_ttl
			snapshotTTL, // ARGV[6] = snapshot_ttl
		},
	).ToArray()
	if err != nil {
		return nil, StreamPosition{}, "", err
	}
	if len(replies) < 6 {
		return nil, StreamPosition{}, "", errors.New("wrong number of replies")
	}

	streamOffset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, StreamPosition{}, "", err
	}
	streamEpoch, _ := replies[1].ToString()
	keysArr, _ := replies[2].ToArray()
	valuesArr, _ := replies[3].ToArray()
	nextCursorScore, _ := replies[4].ToString()
	nextCursorKey, _ := replies[5].ToString()

	streamPos := StreamPosition{Offset: streamOffset, Epoch: streamEpoch}

	// Validate epoch if client provided snapshot revision
	if opts.Revision != nil && opts.Revision.Epoch != streamEpoch {
		// Epoch changed, client needs to restart from beginning
		return nil, streamPos, "", ErrorUnrecoverablePosition
	}

	// Parse snapshot values with revisions
	pubs := make([]*Publication, 0, len(keysArr))
	for i := 0; i < len(keysArr); i++ {
		key, _ := keysArr[i].ToString()
		val, _ := valuesArr[i].AsBytes()

		// Parse value: offset:epoch:payload
		entryOffset, _, payload, err := parseSnapshotValue(val)
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
		cursor = makeOrderedCursor(nextCursorScore, nextCursorKey)
	}

	return pubs, streamPos, cursor, nil
}

// makeOrderedCursor creates a cursor for ordered snapshots: "score\x00key"
func makeOrderedCursor(score, key string) string {
	return score + "\x00" + key
}

// parseOrderedCursor parses an ordered cursor into score and key strings.
func parseOrderedCursor(cursor string) (string, string) {
	for i := 0; i < len(cursor); i++ {
		if cursor[i] == '\x00' {
			return cursor[:i], cursor[i+1:]
		}
	}
	return "", ""
}

func (e *RedisMapEngine) ReadStreamZero(
	ctx context.Context,
	ch string,
	opts MapReadStreamOptions,
) ([]*Publication, StreamPosition, error) {
	s := e.getShard(ch)

	var includePubs = true
	var offset string

	if opts.Filter.Since != nil {
		if opts.Filter.Reverse {
			offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
			if offset == "0" {
				includePubs = false
			}
		} else {
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

	reverse := "0"
	if opts.Filter.Reverse {
		reverse = "1"
	}

	metaExpire := "0"
	if opts.MetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
	} else if e.node.config.HistoryMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
	}

	includePubsStr := "0"
	if includePubs {
		includePubsStr = "1"
	}

	var (
		streamPos StreamPosition
		pubs      []*Publication
	)

	err := e.readStreamScript.ExecWithReader(
		ctx,
		s.shard.client,
		[]string{
			e.streamKey(s.shard, ch),
			e.metaKey(s.shard, ch),
		},
		[]string{
			includePubsStr,
			offset,
			strconv.Itoa(limit),
			reverse,
			metaExpire,
			e.node.ID(),
		},
		func(r *bufio.Reader) error {
			rr := resp.NewReader(r)

			// ---- top-level reply ----
			n, err := rr.ExpectArray()
			if err != nil {
				return err
			}
			if n < 2 {
				return fmt.Errorf("wrong number of replies: %d", n)
			}

			// ---- reply[0]: top offset ----
			switch rr.PeekKind() {
			case resp.KindInt:
				v, err := rr.ReadInt64()
				if err != nil {
					return err
				}
				streamPos.Offset = uint64(v)
			case resp.KindString:
				buf, err := rr.ReadStringBytes()
				if err != nil {
					return err
				}
				v, err := strconv.ParseUint(
					unsafe.String(&buf[0], len(buf)),
					10,
					64,
				)
				if err != nil {
					return err
				}
				streamPos.Offset = v
			default:
				return fmt.Errorf("unexpected RESP kind for offset")
			}

			buf, err := rr.ReadStringBytes()
			if err != nil {
				return err
			}
			// Make a safe copy of the bytes.
			safeBuf := make([]byte, len(buf))
			copy(safeBuf, buf)
			streamPos.Epoch = convert.BytesToString(safeBuf)

			if !includePubs || n < 3 {
				return nil
			}

			// ---- reply[2]: publications ----
			pubCount, err := rr.ExpectArray()
			if err != nil {
				return err
			}

			pubs = make([]*Publication, 0, pubCount)

			for i := int64(0); i < pubCount; i++ {
				// entry = [id, fields]
				if _, err := rr.ExpectArray(); err != nil {
					return err
				}

				// ---- id ----
				idBuf, err := rr.ReadStringBytes()
				if err != nil {
					return err
				}

				idStr := unsafe.String(&idBuf[0], len(idBuf))
				hyphen := strings.IndexByte(idStr, '-')
				if hyphen <= 0 {
					return fmt.Errorf("invalid stream id")
				}

				pubOffset, err := strconv.ParseUint(idStr[:hyphen], 10, 64)
				if err != nil {
					return err
				}

				// ---- fields ----
				fieldCount, err := rr.ExpectArray()
				if err != nil {
					return err
				}

				var payload *bpool.ByteBuffer

				for j := int64(0); j < fieldCount; j += 2 {
					// key
					key, err := rr.ReadStringBytes()
					if err != nil {
						return err
					}
					isData := len(key) == 1 && key[0] == 'd'

					// value
					if isData {
						vbuf, err := rr.ReadStringBytes()
						if err != nil {
							return err
						}
						payload = bpool.GetByteBuffer(len(vbuf))
						payload.B = payload.B[:len(vbuf)]
						copy(payload.B, vbuf)
					} else {
						if err := rr.SkipValue(); err != nil {
							return err
						}
					}
				}

				if payload == nil {
					return errors.New("no payload data found in entry")
				}

				var protoPub protocol.Publication
				if err := protoPub.UnmarshalVT(payload.B); err != nil {
					bpool.PutByteBuffer(payload)
					return err
				}
				bpool.PutByteBuffer(payload)

				protoPub.Offset = pubOffset
				pubs = append(pubs, &Publication{
					Offset:  protoPub.Offset,
					Data:    protoPub.Data,
					Info:    infoFromProto(protoPub.GetInfo()),
					Tags:    protoPub.GetTags(),
					Time:    protoPub.Time,
					Key:     protoPub.GetKey(),
					Removed: protoPub.GetRemoved(),
					Score:   protoPub.GetScore(),
				})
			}
			return nil
		},
	)

	if err != nil {
		return nil, StreamPosition{}, err
	}

	return pubs, streamPos, nil
}

// ReadStreamZero2 is a 2-call version of ReadStreamZero with zero-alloc optimizations.
// Call 1: Get metadata (epoch, top_offset) using simpler Lua script with ExecWithReader
// Call 2: Read publications using native XRANGE/XREVRANGE with DoWithReader
// Key difference: Must filter out publications with offset > top_offset (non-atomic).
func (e *RedisMapEngine) ReadStreamZero2(ctx context.Context, ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error) {
	s := e.getShard(ch)

	// 1. Parse options (same as ReadStream/ReadStreamZero)
	var includePubs = true
	var offset string
	if opts.Filter.Since != nil {
		if opts.Filter.Reverse {
			offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
			if offset == "0" {
				includePubs = false
			}
		} else {
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
	if opts.MetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
	} else if e.node.config.HistoryMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
	}

	// 2. Call 1: Get metadata with ExecWithReader (zero-alloc)
	var streamPos StreamPosition

	err := e.readMetaScript.ExecWithReader(
		ctx,
		s.shard.client,
		[]string{e.metaKey(s.shard, ch)},
		[]string{metaExpire, e.node.ID()},
		func(r *bufio.Reader) error {
			rr := resp.NewReader(r)

			// Top-level must be an array of at least 2 elements
			n, err := rr.ExpectArray()
			if err != nil {
				return err
			}
			if n < 2 {
				return fmt.Errorf("wrong number of replies: %d", n)
			}

			// ---- reply[0]: top offset ----
			switch rr.PeekKind() {
			case resp.KindInt:
				v, err := rr.ReadInt64()
				if err != nil && !errors.Is(err, rueidis.Nil) {
					return err
				}
				streamPos.Offset = uint64(v)
			case resp.KindString:
				b, err := rr.ReadStringBytes()
				if err != nil {
					return err
				}
				if len(b) > 0 {
					v, err := strconv.ParseUint(unsafe.String(&b[0], len(b)), 10, 64)
					if err != nil {
						return err
					}
					streamPos.Offset = v
				}
			default:
				return fmt.Errorf("unexpected RESP type for offset: %q", rr.PeekKind())
			}

			// ---- reply[1]: epoch ----
			buf, err := rr.ReadStringBytes()
			if err != nil {
				return err
			}
			// Make a safe copy of the bytes.
			safeBuf := make([]byte, len(buf))
			copy(safeBuf, buf)
			streamPos.Epoch = convert.BytesToString(safeBuf)

			return nil
		},
	)
	if err != nil {
		return nil, StreamPosition{}, err
	}

	// 3. Early return if metadata-only
	if !includePubs {
		return nil, streamPos, nil
	}

	topOffset := streamPos.Offset

	// 4. Call 2: Execute XRANGE with DoWithReader (zero-alloc)
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

	var pubs []*Publication

	err = s.shard.client.DoWithReader(ctx, cmd, func(r *bufio.Reader) error {
		rr := resp.NewReader(r)

		// Top-level must be an array
		entryCount, err := rr.ExpectArray()
		if err != nil {
			return err
		}

		if entryCount > 0 {
			pubs = make([]*Publication, 0, entryCount)
		}

		for i := int64(0); i < entryCount; i++ {
			// Each entry = [id, fields]
			if _, err := rr.ExpectArray(); err != nil {
				return err
			}

			// ---- id ----
			idBytes, err := rr.ReadStringBytes()
			if err != nil {
				return err
			}
			idStr := unsafe.String(&idBytes[0], len(idBytes))
			hyphen := strings.IndexByte(idStr, '-')
			if hyphen <= 0 {
				return fmt.Errorf("invalid stream id")
			}

			pubOffset, err := strconv.ParseUint(idStr[:hyphen], 10, 64)
			if err != nil {
				return err
			}

			// Filter entries written after metadata read
			if pubOffset > topOffset {
				if err := rr.SkipValue(); err != nil { // skip fields array
					return err
				}
				continue
			}

			// ---- fields ----
			fieldCount, err := rr.ExpectArray()
			if err != nil {
				return err
			}

			var payload *bpool.ByteBuffer

			for j := int64(0); j < fieldCount; j += 2 {
				// key
				keyBytes, err := rr.ReadStringBytes()
				if err != nil {
					return err
				}
				isData := len(keyBytes) == 1 && keyBytes[0] == 'd'

				// value
				valBytes, err := rr.ReadStringBytes()
				if err != nil {
					return err
				}

				if isData {
					payload = bpool.GetByteBuffer(len(valBytes))
					payload.B = payload.B[:len(valBytes)]
					copy(payload.B, valBytes)
				}
			}

			if payload == nil {
				return errors.New("no payload data found in entry")
			}

			var protoPub protocol.Publication
			if err := protoPub.UnmarshalVT(payload.B); err != nil {
				bpool.PutByteBuffer(payload)
				return err
			}
			bpool.PutByteBuffer(payload)

			protoPub.Offset = pubOffset
			pubs = append(pubs, &Publication{
				Offset:  protoPub.Offset,
				Data:    protoPub.Data,
				Info:    infoFromProto(protoPub.GetInfo()),
				Tags:    protoPub.GetTags(),
				Time:    protoPub.Time,
				Key:     protoPub.GetKey(),
				Removed: protoPub.GetRemoved(),
				Score:   protoPub.GetScore(),
			})
		}

		return nil
	})

	if err != nil {
		return nil, StreamPosition{}, err
	}

	return pubs, streamPos, nil
}

// ReadStream retrieves publication stream for a channel.
func (e *RedisMapEngine) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error) {
	s := e.getShard(ch)

	var includePubs = true
	var offset string
	if opts.Filter.Since != nil {
		if opts.Filter.Reverse {
			offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
			if offset == "0" {
				includePubs = false
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
	if opts.MetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
	} else if e.node.config.HistoryMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
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
		return nil, StreamPosition{}, err
	}

	if len(replies) < 2 {
		return nil, StreamPosition{}, fmt.Errorf("wrong number of replies: %d", len(replies))
	}

	topOffset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, StreamPosition{}, fmt.Errorf("could not parse top offset: %w", err)
	}

	epoch, err := replies[1].ToString()
	if err != nil {
		return nil, StreamPosition{}, fmt.Errorf("could not parse epoch: %w", err)
	}

	streamPos := StreamPosition{Offset: topOffset, Epoch: epoch}
	if !includePubs || len(replies) < 3 {
		return nil, streamPos, nil
	}

	pubValues, err := replies[2].ToArray()
	if err != nil {
		return nil, StreamPosition{}, err
	}

	pubs := make([]*Publication, len(pubValues))
	for j, v := range pubValues {
		entry, err := v.ToArray()
		if err != nil {
			return nil, StreamPosition{}, err
		}
		if len(entry) != 2 {
			return nil, StreamPosition{}, fmt.Errorf("invalid publication format")
		}
		id, err := entry[0].ToString()
		if err != nil {
			return nil, StreamPosition{}, err
		}

		// entry[1] is an array of [field, value, field, value, ...]
		fieldsArr, err := entry[1].ToArray()
		if err != nil {
			return nil, StreamPosition{}, err
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
			return nil, StreamPosition{}, errors.New("no payload data found in entry")
		}

		hyphenPos := strings.Index(id, "-")
		if hyphenPos <= 0 {
			return nil, StreamPosition{}, fmt.Errorf("unexpected offset format: %s", id)
		}
		pubOffset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
		if err != nil {
			return nil, StreamPosition{}, err
		}

		var protoPub protocol.Publication
		err = protoPub.UnmarshalVT(payload)
		if err != nil {
			return nil, StreamPosition{}, fmt.Errorf("can not unmarshal value to Publication: %v", err)
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
	return pubs, streamPos, nil
}

// ReadStream2 is a 2-call version of ReadStream that splits metadata and publication reads.
// Call 1: Get metadata (epoch, top_offset) using simpler Lua script
// Call 2: Read publications using native XRANGE/XREVRANGE
// Key difference: Must filter out publications with offset > top_offset (non-atomic).
func (e *RedisMapEngine) ReadStream2(ctx context.Context, ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error) {
	s := e.getShard(ch)

	// 1. Parse options (same as ReadStream)
	var includePubs = true
	var offset string
	if opts.Filter.Since != nil {
		if opts.Filter.Reverse {
			offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
			if offset == "0" {
				includePubs = false
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
	if opts.MetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
	} else if e.node.config.HistoryMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
	}

	// 2. Call 1: Get metadata using new simpler Lua script
	metaReplies, err := e.readMetaScript.Exec(ctx, s.shard.client,
		[]string{e.metaKey(s.shard, ch)},
		[]string{metaExpire, e.node.ID()},
	).ToArray()
	if err != nil {
		return nil, StreamPosition{}, err
	}

	if len(metaReplies) < 2 {
		return nil, StreamPosition{}, fmt.Errorf("wrong number of replies: %d", len(metaReplies))
	}

	topOffset, err := metaReplies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, StreamPosition{}, fmt.Errorf("could not parse top offset: %w", err)
	}

	epoch, err := metaReplies[1].ToString()
	if err != nil {
		return nil, StreamPosition{}, fmt.Errorf("could not parse epoch: %w", err)
	}

	streamPos := StreamPosition{Offset: topOffset, Epoch: epoch}

	// 3. Early return if metadata-only
	if !includePubs {
		return nil, streamPos, nil
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
		return nil, StreamPosition{}, err
	}

	// 5. Parse stream entries and filter by topOffset
	pubs := make([]*Publication, 0, len(result))

	for _, v := range result {
		entry, err := v.ToArray()
		if err != nil {
			return nil, StreamPosition{}, err
		}
		if len(entry) != 2 {
			return nil, StreamPosition{}, fmt.Errorf("invalid publication format")
		}
		id, err := entry[0].ToString()
		if err != nil {
			return nil, StreamPosition{}, err
		}

		// Extract offset from stream ID "123-0" -> 123
		hyphenPos := strings.Index(id, "-")
		if hyphenPos <= 0 {
			return nil, StreamPosition{}, fmt.Errorf("unexpected offset format: %s", id)
		}
		pubOffset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
		if err != nil {
			return nil, StreamPosition{}, err
		}

		// CRITICAL: Filter entries written after metadata read
		if pubOffset > topOffset {
			continue
		}

		// entry[1] is an array of [field, value, field, value, ...]
		fieldsArr, err := entry[1].ToArray()
		if err != nil {
			return nil, StreamPosition{}, err
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
			return nil, StreamPosition{}, errors.New("no payload data found in entry")
		}

		var protoPub protocol.Publication
		err = protoPub.UnmarshalVT(payload)
		if err != nil {
			return nil, StreamPosition{}, fmt.Errorf("can not unmarshal value to Publication: %v", err)
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

	return pubs, streamPos, nil
}

// Stats returns short stats of current presence data.
// This is a read-only operation - cleanup is handled by the cleanup worker.
func (e *RedisMapEngine) Stats(ctx context.Context, ch string) (MapStats, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	replies, err := e.presenceStatsScript.Exec(ctx, shardClient,
		[]string{
			e.snapshotHashKey(s.shard, ch),
			e.aggregationHashKey(s.shard, ch),
		},
		nil, // No arguments needed for read-only stats
	).ToArray()
	if err != nil {
		return MapStats{}, err
	}
	if len(replies) != 2 {
		return MapStats{}, errors.New("wrong number of replies from script")
	}
	numKeys, err := replies[0].AsInt64()
	if err != nil {
		return MapStats{}, err
	}
	return MapStats{NumKeys: int(numKeys)}, nil
}

// ReadPresenceSnapshot retrieves presence snapshot with per-entry revisions for converged membership.
// Each entry includes its revision so client can filter: entry.Revision <= snapshot_revision.
// If opts.Revision is provided and epoch changed, returns empty entries.
// Returns Publications with Key=ClientID, Info=ClientInfo, Offset/Epoch for revision.
func (e *RedisMapEngine) ReadPresenceSnapshot(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, error) {
	// ReadPresenceSnapshot is just ReadState - it already returns Publications with Key and Offset set.
	// This reuses all the snapshot reading logic.
	pubs, streamPos, _, err := e.ReadState(ctx, ch, opts)
	if err != nil {
		return nil, StreamPosition{}, err
	}
	return pubs, streamPos, nil
}

// ReadPresenceStream retrieves presence event stream (joins/leaves) for a channel.
// Returns Publications with Info=ClientInfo and Removed flag (true for leave, false for join).
func (e *RedisMapEngine) ReadPresenceStream(ctx context.Context, ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error) {
	// ReadPresenceStream is literally just ReadStream - presence streams store Publications
	// Publication.Removed distinguishes join (false) from leave (true) events.
	return e.ReadStream(ctx, ch, opts)
}

// RegisterEventHandler registers a BrokerEventHandler to handle messages from Pub/Sub.
func (e *RedisMapEngine) RegisterEventHandler(h BrokerEventHandler) error {
	// Run all shards.
	for _, wrapper := range e.shards {
		err := e.runPubSubShard(wrapper, h)
		if err != nil {
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

func (e *RedisMapEngine) runPubSubShard(s *engineShardWrapper, h BrokerEventHandler) error {
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
func (e *RedisMapEngine) runForever(fn func()) {
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

// runCleanupWorker runs the background cleanup worker that generates LEAVE events
// for expired presence entries. This ensures guaranteed delivery of LEAVE events
// even when clients disconnect without explicit leave.
func (e *RedisMapEngine) runCleanupWorker(ctx context.Context) {
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
func (e *RedisMapEngine) runCleanupCycle(ctx context.Context) {
	now := time.Now().Unix()

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
func (e *RedisMapEngine) cleanupShard(ctx context.Context, shard *RedisShard, now int64) {
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
func (e *RedisMapEngine) cleanupPartition(ctx context.Context, shard *RedisShard, cleanupKey string, now int64) {
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
				}
			}(ch)
		}
		wg.Wait()
	}
}

// getChannelsForCleanup returns channels that have expired entries (score <= now).
func (e *RedisMapEngine) getChannelsForCleanup(ctx context.Context, client rueidis.Client, cleanupKey string, now int64) ([]string, error) {
	cmd := client.B().Zrangebyscore().Key(cleanupKey).Min("0").Max(strconv.FormatInt(now, 10)).Limit(0, cleanupChannelBatchSize).Build()
	result, err := client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// cleanupChannel runs the cleanup script for a single channel.
func (e *RedisMapEngine) cleanupChannel(ctx context.Context, shard *RedisShard, ch string, cleanupKey string, now int64) error {
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

	// Get channel options for this channel.
	opts := e.node.ResolveMapChannelOptions(ch)

	metaExpire := "0"
	if opts.MetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
	}

	_, err := e.cleanupScript.Exec(ctx, shard.client,
		[]string{
			e.snapshotHashKey(shard, ch),           // KEYS[1]: snapshot hash key
			e.snapshotExpireKey(shard, ch),         // KEYS[2]: snapshot expire zset key
			e.streamKey(shard, ch),                 // KEYS[3]: stream key
			e.metaKey(shard, ch),                   // KEYS[4]: stream meta key
			e.aggregationZSetKey(shard, ch),        // KEYS[5]: aggregation zset key
			e.aggregationHashKey(shard, ch),        // KEYS[6]: aggregation hash key
			cleanupKey,                             // KEYS[7]: cleanup registration zset key
			e.aggregationMappingHashKey(shard, ch), // KEYS[8]: aggregation mapping hash key
		},
		[]string{
			strconv.FormatInt(now, 10),            // ARGV[1]: now
			strconv.Itoa(e.conf.CleanupBatchSize), // ARGV[2]: batch_size
			chID,                                  // ARGV[3]: channel (for PUBLISH)
			publishCommand,                        // ARGV[4]: publish_command
			strconv.Itoa(opts.StreamSize),         // ARGV[5]: stream_size
			strconv.FormatInt(int64(opts.StreamTTL.Seconds()), 10), // ARGV[6]: stream_ttl
			metaExpire,  // ARGV[7]: meta_expire
			e.node.ID(), // ARGV[8]: new_epoch_if_empty
			"0",         // ARGV[9]: use_hexpire
			ch,          // ARGV[10]: channel_for_cleanup
			"1",         // ARGV[11]: force_consistency
		},
	).ToArray()
	return err
}

func (e *RedisMapEngine) runPubSub(s *engineShardWrapper, logFields map[string]any, eventHandler BrokerEventHandler, clusterShardIndex, psShardIndex int, useShardedPubSub bool, startOnce func(error)) {
	numProcessors := e.conf.numPubSubProcessors
	numResubscribeShards := e.conf.numResubscribeShards

	shardChannel := e.pubSubShardChannelID(clusterShardIndex, psShardIndex, useShardedPubSub)

	if e.node.logEnabled(LogLevelDebug) {
		debugLogValues := map[string]any{
			"num_processors": numProcessors,
		}
		if useShardedPubSub {
			debugLogValues["cluster_shard_index"] = clusterShardIndex
		}
		startLogFields := map[string]any{}
		for k, v := range logFields {
			startLogFields[k] = v
		}
		if s.shard.isCluster {
			startLogFields["cluster"] = true
		}
		combinedLogFields := make(map[string]any, len(startLogFields)+len(debugLogValues))
		for k, v := range startLogFields {
			combinedLogFields[k] = v
		}
		for k, v := range debugLogValues {
			combinedLogFields[k] = v
		}
		e.node.logger.log(newLogEntry(LogLevelDebug, "running Redis PUB/SUB", combinedLogFields))
		defer func() {
			e.node.logger.log(newLogEntry(LogLevelDebug, "stopping Redis PUB/SUB", combinedLogFields))
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
					err := e.handleRedisClientMessage(s.shard.isCluster, eventHandler, msg.Channel, convert.StringToBytes(msg.Message))
					if err != nil {
						e.node.logger.log(newErrorLogEntry(err, "error handling client message", logFields))
						continue
					}
				}
			}
		}(processingCh)
	}

	client := s.shard.client
	if e.conf.SubscribeOnReplica {
		client = s.shard.replicaClient
	}

	conn, cancel := client.Dedicate()
	defer cancel()
	defer conn.Close()

	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			select {
			case processors[index(msg.Channel, numProcessors)] <- msg:
			case <-done:
			}
		},
		OnSubscription: func(ps rueidis.PubSubSubscription) {
			if !useShardedPubSub {
				return
			}
			if ps.Kind == "sunsubscribe" && ps.Channel == shardChannel {
				e.node.logger.log(newLogEntry(LogLevelInfo, "pub/sub restart due to slot migration", logFields))
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
		e.node.logger.log(newErrorLogEntry(err, "pub/sub subscribe error", logFields))
		return
	}

	channels := e.node.Hub().Channels()

	var wg sync.WaitGroup
	started := time.Now()

	for i := 0; i < numResubscribeShards; i++ {
		wg.Add(1)
		go func(subscriberIndex int) {
			defer wg.Done()
			estimatedCap := len(channels) / e.conf.numResubscribeShards / e.conf.numSubscribeShards
			if useShardedPubSub {
				estimatedCap /= e.conf.NumShardedPubSubPartitions
			}
			chIDs := make([]string, 0, estimatedCap)

			for _, ch := range channels {
				if e.getShard(ch).shard == s.shard && ((useShardedPubSub && consistentIndex(ch, e.conf.NumShardedPubSubPartitions) == clusterShardIndex && index(ch, e.conf.numSubscribeShards) == psShardIndex && index(ch, e.conf.numResubscribeShards) == subscriberIndex) || (index(ch, e.conf.numSubscribeShards) == psShardIndex && index(ch, e.conf.numResubscribeShards) == subscriberIndex)) {
					chIDs = append(chIDs, e.messageChannelID(s.shard, ch))
				}
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
						e.node.logger.log(newErrorLogEntry(err, "error subscribing", logFields))
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
					e.node.logger.log(newErrorLogEntry(err, "error subscribing", logFields))
					closeDoneOnce()
					return
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		if len(channels) > 0 && e.node.logEnabled(LogLevelDebug) {
			e.logResubscribed(len(channels), time.Since(started), logFields)
		}
		select {
		case <-done:
			startOnce(errors.New("error resubscribing"))
		default:
			s.subClientsMu.Lock()
			s.subClients[clusterShardIndex][psShardIndex] = conn
			s.subClientsMu.Unlock()
			defer func() {
				s.subClientsMu.Lock()
				s.subClients[clusterShardIndex][psShardIndex] = nil
				s.subClientsMu.Unlock()
			}()
			startOnce(nil)
		}
		<-done
	}()

	select {
	case err = <-wait:
		startOnce(err)
		if err != nil {
			e.node.logger.log(newErrorLogEntry(err, "pub/sub connection error", logFields))
		}
	case <-done:
	case <-s.shard.closeCh:
	}
}

func (e *RedisMapEngine) logResubscribed(numChannels int, elapsed time.Duration, logFields map[string]any) {
	combinedLogFields := make(map[string]any, len(logFields)+2)
	for k, v := range logFields {
		combinedLogFields[k] = v
	}
	combinedLogFields["elapsed"] = elapsed.String()
	combinedLogFields["num_channels"] = numChannels
	e.node.logger.log(newLogEntry(LogLevelDebug, "resubscribed to channels", combinedLogFields))
}

func (e *RedisMapEngine) pubSubShardChannelID(clusterShardIndex int, psShardIndex int, useShardedPubSub bool) string {
	psShardStr := strconv.Itoa(psShardIndex)
	if !useShardedPubSub {
		var builder strings.Builder
		builder.Grow(len(e.shardChannel) + 1 + len(psShardStr))
		builder.WriteString(e.shardChannel)
		builder.WriteByte('.')
		builder.WriteString(psShardStr)
		return builder.String()
	}
	clusterShardStr := strconv.Itoa(clusterShardIndex)
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

func (e *RedisMapEngine) extractChannel(isCluster bool, chID string) string {
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

func (e *RedisMapEngine) handleRedisClientMessage(isCluster bool, eventHandler BrokerEventHandler, chID string, data []byte) error {
	// Parse message: supports backward compat (no prefix), non-delta, and delta formats
	offset, epoch, protobuf, isDelta, prevProtobuf, err := parseMessage(data)
	if err != nil {
		return fmt.Errorf("malformed pub/sub data: %w", err)
	}

	channel := e.extractChannel(isCluster, chID)
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
		var prevPub protocol.Publication
		if err := prevPub.UnmarshalVT(prevProtobuf); err == nil {
			prevPubPtr = pubFromProto(&prevPub)
		}
	}

	_ = eventHandler.HandlePublication(channel, pubFromProto(&protoPub), sp, isDelta, prevPubPtr)
	return nil
}

// Subscribe to a channel.
func (e *RedisMapEngine) Subscribe(ch string) error {
	s := e.getShard(ch)
	if e.node.logEnabled(LogLevelDebug) {
		e.node.logger.log(newLogEntry(LogLevelDebug, "subscribe node on channel", map[string]any{"channel": ch}))
	}
	psShardIndex := index(ch, e.conf.numSubscribeShards)
	var clusterShardIndex int
	if e.useShardedPubSub(s.shard) {
		clusterShardIndex = consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
	}

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

// Unsubscribe from a channel.
func (e *RedisMapEngine) Unsubscribe(ch string) error {
	s := e.getShard(ch)
	if e.node.logEnabled(LogLevelDebug) {
		e.node.logger.log(newLogEntry(LogLevelDebug, "unsubscribe node from channel", map[string]any{"channel": ch}))
	}
	psShardIndex := index(ch, e.conf.numSubscribeShards)
	var clusterShardIndex int
	if e.useShardedPubSub(s.shard) {
		clusterShardIndex = consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
	}

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
func parseAddScriptResult(replies []rueidis.RedisMessage) (MapPublishResult, error) {
	if len(replies) < 2 {
		return MapPublishResult{}, fmt.Errorf("wrong number of replies from script: %d", len(replies))
	}
	offset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return MapPublishResult{}, fmt.Errorf("could not parse offset: %w", err)
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return MapPublishResult{}, fmt.Errorf("could not parse epoch: %w", err)
	}

	// Check for suppression reason (3rd value is the reason string, empty means not suppressed)
	var suppressReason SuppressReason
	if len(replies) >= 3 {
		reason, err := replies[2].ToString()
		if err == nil && reason != "" {
			suppressReason = SuppressReason(reason)
		}
	}

	result := MapPublishResult{
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
				entryOffset, _, payload, parseErr := parseSnapshotValue(currentValue)
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

//// AddMember adds a client to converged membership (presence with ordering).
//func (e *RedisMapEngine) AddMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
//	// AddMember is just Publish with a Publication that has Key=clientID, Info=clientInfo, Data=nil
//	// This reuses all the Publish logic for keyed snapshots with aggregation support
//
//	protoPub := &protocol.Publication{
//		Key:  info.ClientID,
//		Info: infoToProto(&info),
//		Time: time.Now().UnixMilli(),
//	}
//	pubBytes, err := protoPub.MarshalVT()
//	if err != nil {
//		return err
//	}
//
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	// Optional presence stream for recovery
//	var streamKey, metaKey, metaExpire string
//	if e.conf.PresenceStreamSize > 0 {
//		streamKey = e.streamKey(s.shard, ch)
//		metaKey = e.metaKey(s.shard, ch)
//		if e.conf.PresenceMetaTTL > 0 {
//			metaExpire = strconv.Itoa(int(e.conf.PresenceMetaTTL.Seconds()))
//		} else {
//			metaExpire = "0"
//		}
//	}
//
//	publishCommand := ""
//	var chID string
//	if opts.Publish {
//		if e.useShardedPubSub(s.shard) {
//			publishCommand = "SPUBLISH"
//		} else {
//			publishCommand = "PUBLISH"
//		}
//		chID = e.messageChannelID(s.shard, ch)
//	}
//
//	// Use aggregation for user counting
//	aggregationKey := ""
//	aggregationValue := ""
//	if info.UserID != "" {
//		aggregationKey = "user_id"
//		aggregationValue = info.UserID
//	}
//
//	// Setup cleanup registration and aggregation mapping for presence with stream
//	cleanupRegKey := ""
//	aggMappingKey := ""
//	if e.conf.PresenceStreamSize > 0 {
//		cleanupRegKey = e.cleanupRegistrationKeyForChannel(s.shard, ch)
//		if info.UserID != "" {
//			aggMappingKey = e.aggregationMappingHashKey(s.shard, ch)
//		}
//	}
//
//	_, err = e.addScript.Exec(ctx, shardClient,
//		[]string{
//			streamKey, metaKey, "", // Optional presence stream, no result key
//			e.snapshotHashKey(s.shard, ch),
//			"", // No order key for presence
//			e.snapshotExpireKey(s.shard, ch),
//			e.aggregationZSetKey(s.shard, ch),
//			e.aggregationHashKey(s.shard, ch),
//			"",            // No snapshot meta key for presence (uses stream meta directly)
//			cleanupRegKey, // KEYS[10]: cleanup registration zset key
//			aggMappingKey, // KEYS[11]: aggregation mapping hash key
//		},
//		[]string{
//			info.ClientID,                   // message_key
//			convert.BytesToString(pubBytes), // message_payload (Publication for stream, publishing, and snapshot)
//			strconv.Itoa(e.conf.PresenceStreamSize),
//			strconv.FormatInt(int64(e.conf.PresenceStreamTTL.Seconds()), 10),
//			chID, // channel (for Lua to publish)
//			metaExpire,
//			e.node.ID(),
//			publishCommand,   // Lua will publish
//			"", "0", "0", "", // result_key_expire, use_delta, version, version_epoch
//			"0", // is_leave
//			"0", // score
//			strconv.FormatInt(int64(e.conf.PresenceTTL.Seconds()), 10),
//			"0",              // use_hexpire
//			aggregationKey,   // aggregation_key (e.g., "user_id")
//			aggregationValue, // aggregation_value (e.g., actual user ID)
//			"",               // message_key_payload (not used - message_payload is Publication)
//			ch,               // channel_for_cleanup (channel name for registration)
//		},
//	).ToArray()
//	return err
//}
//
//// RemoveMember removes a client from converged membership.
//func (e *RedisMapEngine) RemoveMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
//	// RemoveMember is just Publish with a Publication that has Removed=true
//	// This reuses all the Publish logic for keyed snapshots with aggregation support
//
//	protoPub := &protocol.Publication{
//		Key:     info.ClientID,
//		Info:    infoToProto(&info),
//		Removed: true,
//		Time:    time.Now().UnixMilli(),
//	}
//	pubBytes, err := protoPub.MarshalVT()
//	if err != nil {
//		return err
//	}
//
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	// Optional presence stream for recovery
//	var streamKey, metaKey, metaExpire string
//	if e.conf.PresenceStreamSize > 0 {
//		streamKey = e.streamKey(s.shard, ch)
//		metaKey = e.metaKey(s.shard, ch)
//		if e.conf.PresenceMetaTTL > 0 {
//			metaExpire = strconv.Itoa(int(e.conf.PresenceMetaTTL.Seconds()))
//		} else {
//			metaExpire = "0"
//		}
//	}
//
//	publishCommand := ""
//	var chID string
//	if opts.Publish {
//		if e.useShardedPubSub(s.shard) {
//			publishCommand = "SPUBLISH"
//		} else {
//			publishCommand = "PUBLISH"
//		}
//		chID = e.messageChannelID(s.shard, ch)
//	}
//
//	// Use aggregation for user counting
//	aggregationKey := ""
//	aggregationValue := ""
//	if info.UserID != "" {
//		aggregationKey = "user_id"
//		aggregationValue = info.UserID
//	}
//
//	// Cleanup registration for proper cleanup handling
//	cleanupRegKey := ""
//	aggMappingKey := ""
//	if e.conf.PresenceStreamSize > 0 {
//		cleanupRegKey = e.cleanupRegistrationKeyForChannel(s.shard, ch)
//		if info.UserID != "" {
//			aggMappingKey = e.aggregationMappingHashKey(s.shard, ch)
//		}
//	}
//
//	_, err = e.addScript.Exec(ctx, shardClient,
//		[]string{
//			streamKey, metaKey, "", // Optional presence stream, no result key
//			e.snapshotHashKey(s.shard, ch),
//			"", // No order key for presence
//			e.snapshotExpireKey(s.shard, ch),
//			e.aggregationZSetKey(s.shard, ch),
//			e.aggregationHashKey(s.shard, ch),
//			"",            // No snapshot meta key for presence
//			cleanupRegKey, // KEYS[10]: cleanup registration key (to update after remove)
//			aggMappingKey, // KEYS[11]: aggregation mapping hash key
//		},
//		[]string{
//			info.ClientID,                   // message_key
//			convert.BytesToString(pubBytes), // message_payload (Publication with Removed=true for stream and publishing)
//			strconv.Itoa(e.conf.PresenceStreamSize),
//			strconv.FormatInt(int64(e.conf.PresenceStreamTTL.Seconds()), 10),
//			chID, // channel (for Lua to publish)
//			metaExpire,
//			e.node.ID(),
//			publishCommand,   // Lua will publish
//			"", "0", "0", "", // result_key_expire, use_delta, version, version_epoch
//			"1", // is_leave
//			"0", // score
//			strconv.FormatInt(int64(e.conf.PresenceTTL.Seconds()), 10),
//			"0",              // use_hexpire
//			aggregationKey,   // aggregation_key
//			aggregationValue, // aggregation_value
//			"",               // message_key_payload (not used - message_payload is Publication)
//			ch,               // channel_for_cleanup (needed to update cleanup registration)
//		},
//	).ToArray()
//	return err
//}
//
//// Members returns actual presence information for a channel.
//func (e *RedisMapEngine) Members(ctx context.Context, ch string) (map[string]*ClientInfo, error) {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	metaTTL := "0"
//	if e.conf.PresenceMetaTTL > 0 {
//		metaTTL = strconv.Itoa(int(e.conf.PresenceMetaTTL.Seconds()))
//	}
//
//	replies, err := e.readUnorderedScript.Exec(ctx, shardClient,
//		[]string{
//			e.snapshotHashKey(s.shard, ch),   // hash_key
//			e.snapshotExpireKey(s.shard, ch), // expire_key
//			e.metaKey(s.shard, ch),           // meta_key
//			"",                               // snapshot_meta_key (not used for presence)
//		},
//		[]string{
//			"0", // cursor (start)
//			"0", // limit (0 = HGETALL)
//			strconv.FormatInt(time.Now().Unix(), 10),
//			metaTTL,
//			"0", // snapshot_ttl (don't refresh on read)
//		},
//	).ToArray()
//	if err != nil {
//		return nil, err
//	}
//	if len(replies) < 4 {
//		return nil, errors.New("wrong number of replies")
//	}
//
//	dataArr, _ := replies[3].ToArray()
//	if len(dataArr) == 0 {
//		return nil, nil
//	}
//
//	m := make(map[string]*ClientInfo, len(dataArr)/2)
//	for i := 0; i < len(dataArr); i += 2 {
//		k, _ := dataArr[i].ToString()
//		v, _ := dataArr[i+1].AsBytes()
//
//		// Parse snapshot value: offset:epoch:publication_bytes
//		_, _, payload, err := parseSnapshotValue(v)
//		if err != nil {
//			// Skip malformed entries
//			continue
//		}
//
//		var pub protocol.Publication
//		err = pub.UnmarshalVT(payload)
//		if err != nil {
//			return nil, err
//		}
//		if pub.Info != nil {
//			m[k] = infoFromProto(pub.Info)
//		}
//	}
//	return m, nil
//}
