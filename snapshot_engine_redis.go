package centrifuge

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/protocol"
	"github.com/redis/rueidis"
)

// RedisEngine PUB/SUB Message Formats
// ====================================
//
// This engine uses simplified message formats published by Lua scripts:
//
// 1. No prefix (backward compatibility):
//    - Raw protobuf bytes (Publication, Join, or Leave)
//    - Used by direct PUBLISH calls without Lua scripts
//
// 2. Non-delta publications/presence:
//    - Format: "offset:epoch:protobuf"
//    - Where protobuf is protocol.Push containing Publication/Join/Leave
//    - Example: "123:node-1:..." where ... is marshaled Push
//
// 3. Delta publications:
//    - Format: "d:offset:epoch:prev_len:prev_push:curr_len:curr_push"
//    - Where prev_push and curr_push are protocol.Push with Publication
//    - Example: "d:123:node-1:50:...:60:..." where ... are marshaled Push messages
//    - Enables atomic publishing of current + previous publication for delta compression
//    - Both prev and curr are fetched from stream which now stores Push (not Publication)
//
// Storage:
// - Streams (XADD): protocol.Push containing Publication (for pub history) or Join/Leave (for presence)
// - Snapshots (HSET): For keyed state - depends on use case; For presence - protocol.ClientInfo
//
// The Lua script (broker_snapshot_add.lua) is responsible for formatting messages
// before publishing them via PUBLISH/SPUBLISH commands.

var (
	//go:embed internal/redis_lua/broker_snapshot_add.lua
	brokerSnapshotPublishScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_read_ordered.lua
	brokerSnapshotReadOrderedScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_read_unordered.lua
	brokerSnapshotReadUnorderedScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_stream_read.lua
	brokerSnapshotReadStreamScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_presence_stats.lua
	brokerSnapshotPresenceStatsScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_cleanup.lua
	brokerSnapshotCleanupScriptSource string
)

type Engine interface {
	// Subscribe subscribes server to channel and returns error if it fails.
	Subscribe(ch string) error
	// Unsubscribe unsubscribes server from channel and returns error if it fails.
	Unsubscribe(ch string) error

	// Publish allows sending data into a channel. It can optionally use history and keyed snapshots.
	Publish(ctx context.Context, ch string, data []byte, opts EnginePublishOptions) (StreamPosition, bool, error)
	// Unpublish removes a key from keyed state snapshot. This is the counterpart to Publish()
	// for keyed state - it removes a specific key from the snapshot without affecting other keys.
	Unpublish(ctx context.Context, ch string, key string, opts EngineUnpublishOptions) (StreamPosition, error)
	// ReadStream retrieves publications from stream for a channel.
	ReadStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, StreamPosition, error)
	// ReadSnapshot retrieves a snapshot for a channel with per-entry revisions.
	// Each entry includes its revision so client can filter: entry.Revision <= snapshot_revision.
	// Returns entries, current stream position, next cursor for pagination, and error.
	// If opts.SnapshotRevision is provided and epoch changed, returns empty entries.
	// Cursor "0" or "" means end of iteration.
	ReadSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error)

	// AddMember adds a client to presence in a channel.
	AddMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error
	// RemoveMember removes a client from presence in a channel.
	RemoveMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error
	// Members returns actual presence information for a channel (simple query without revisions).
	Members(ctx context.Context, ch string) (map[string]*ClientInfo, error)
	// MemberStats returns short stats of current presence data.
	MemberStats(ctx context.Context, ch string) (EngineMemberStats, error)

	// ReadPresenceStream retrieves presence event stream (joins/leaves) for recovery.
	// Returns Publications with Info field (ClientInfo) and Removed flag (true for leave, false for join).
	ReadPresenceStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, StreamPosition, error)
	// ReadPresenceSnapshot retrieves presence snapshot with per-entry revisions for converged membership.
	// Returns Publications with Key=ClientID, Info=ClientInfo, and Offset/Epoch for revision tracking.
	// Presence doesn't use cursor pagination - returns all entries.
	ReadPresenceSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]*Publication, StreamPosition, error)
}

type engineShardWrapper struct {
	shard               *RedisShard
	subClientsMu        sync.Mutex
	subClients          [][]rueidis.DedicatedClient
	pubSubStartChannels [][]*pubSubStart
}

// EnginePublishOptions defines options for publishing.
type EnginePublishOptions struct {
	Key                 string
	Tags                map[string]string
	ClientInfo          *ClientInfo
	IdempotencyKey      string
	IdempotentResultTTL time.Duration
	StreamSize          int
	StreamTTL           time.Duration
	StreamMetaTTL       time.Duration
	UseDelta            bool
	Version             uint64
	VersionEpoch        string
	Ordered             bool
	Score               int64
	MemberTTL           time.Duration
}

// EnginePresenceOptions defines options for presence operations.
type EnginePresenceOptions struct {
	SendPush bool
}

// EngineUnpublishOptions defines options for unpublishing (removing a key from keyed state).
type EngineUnpublishOptions struct {
	// SendPush whether to publish removal notification to subscribers.
	SendPush bool
	// StreamSize for appending removal event to stream (0 to disable).
	StreamSize int
	// StreamTTL for stream entries.
	StreamTTL time.Duration
	// StreamMetaTTL for stream metadata.
	StreamMetaTTL time.Duration
}

type EngineMemberStats struct {
	NumClients int
	NumUsers   int
}

// ReadStreamOptions define some fields to alter ReadStream method behavior.
type ReadStreamOptions struct {
	// Filter for history publications.
	Filter HistoryFilter
	// MetaTTL allows overriding default (set in Config.HistoryMetaTTL) history
	// meta information expiration time.
	MetaTTL time.Duration
}

// ReadSnapshotOptions defines options for reading a snapshot.
type ReadSnapshotOptions struct {
	// SnapshotRevision is the revision client received during subscribe.
	// Server validates snapshot epoch matches this revision's epoch.
	// If epoch changed, server returns empty result forcing client to restart.
	SnapshotRevision *StreamPosition
	Ordered          bool
	Cursor           string
	Limit            int
	Offset           int
	SnapshotTTL      time.Duration
}

// SnapshotEntry represents a single entry in a snapshot with its revision.
// Client MUST filter entries where entry.Revision <= requested snapshot_revision.
type SnapshotEntry struct {
	Key      string
	Payload  []byte
	Revision StreamPosition // When this entry was written (offset, epoch)
}

// SnapshotEngine is a Redis-based engine that provides support for:
// 1. Stateful channels - keyed state with revisions
// 2. Converged membership - presence with ordering and recovery
//
// Both features share the same abstraction:
// - A keyed snapshot at some revision (epoch, offset)
// - A log of changes after that revision
// - Revisions are totally ordered per channel
//
// Key differences from RedisEngine:
// - Snapshot entries include revision (epoch, offset)
// - Clients can paginate snapshots and reconcile with live stream
// - Generic aggregations instead of hardcoded user tracking
type SnapshotEngine struct {
	node *Node
	conf SnapshotEngineConfig

	shards []*engineShardWrapper

	addScript           *rueidis.Lua
	readOrderedScript   *rueidis.Lua
	readUnorderedScript *rueidis.Lua
	readStreamScript    *rueidis.Lua
	presenceStatsScript *rueidis.Lua
	cleanupScript       *rueidis.Lua

	closeCh       chan struct{}
	closeOnce     sync.Once
	shardChannel  string
	messagePrefix string
}

var _ Engine = (*SnapshotEngine)(nil)

// SnapshotEngineConfig is a config for SnapshotEngine.
type SnapshotEngineConfig struct {
	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	Shards []*RedisShard
	// Prefix to use before every channel name and key in Redis.
	Prefix string
	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
	// it on the client side. This is useful for FIPS compliance.
	LoadSHA1 bool
	// PresenceTTL is a time-to-live for presence information.
	PresenceTTL time.Duration
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
	// PresenceStreamSize defines the maximum number of presence events (joins/leaves)
	// to keep in the presence stream for recovery.
	PresenceStreamSize int
	// PresenceStreamTTL defines TTL for presence stream.
	PresenceStreamTTL time.Duration
	// PresenceStreamMetaTTL defines TTL for presence stream metadata (offset, epoch).
	PresenceStreamMetaTTL time.Duration
	// CleanupInterval defines how often to run the cleanup worker that
	// generates remove events for expired keyed snapshot entries (presence and state).
	// Zero disables cleanup. Applies to all channels using TTL-based snapshots.
	CleanupInterval time.Duration
	// CleanupBatchSize defines max entries to process per channel per cleanup cycle.
	// Default is 1000. Applies to all keyed snapshots (presence and state).
	CleanupBatchSize int
}

// NewSnapshotEngine initializes SnapshotEngine.
func NewSnapshotEngine(n *Node, conf SnapshotEngineConfig) (*SnapshotEngine, error) {
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
		conf.CleanupBatchSize = 1000
	}
	// CleanupInterval defaults to 0 (disabled). User must explicitly enable.
	// When enabled, it works for ALL keyed snapshots (presence and state with TTL).

	shardWrappers := make([]*engineShardWrapper, 0, len(conf.Shards))
	for _, s := range conf.Shards {
		shardWrappers = append(shardWrappers, &engineShardWrapper{shard: s})
	}

	e := &SnapshotEngine{
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
		presenceStatsScript: rueidis.NewLuaScript(
			brokerSnapshotPresenceStatsScriptSource,
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

	for _, shardWrapper := range e.shards {
		shard := shardWrapper.shard
		if !shard.isCluster && e.conf.NumShardedPubSubPartitions > 0 {
			return nil, errors.New("can use sharded PUB/SUB feature (non-zero number of pub/sub partitions) only with Redis Cluster")
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

		shardWrapper.subClients = subChannels
		shardWrapper.pubSubStartChannels = pubSubStartChannels
	}

	// Start background cleanup worker for guaranteed remove events on expiry.
	// Runs for both presence (PresenceStreamSize > 0) and keyed state (any channel using MemberTTL).
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

func (e *SnapshotEngine) useShardedPubSub(s *RedisShard) bool {
	return s.isCluster && e.conf.NumShardedPubSubPartitions > 0
}

func (e *SnapshotEngine) getShard(channel string) *engineShardWrapper {
	if len(e.shards) == 1 {
		return e.shards[0]
	}
	return e.shards[consistentIndex(channel, len(e.shards))]
}

// Key generation methods (similar to RedisEngine but for new engine)

func (e *SnapshotEngine) streamKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":stream:")
}

func (e *SnapshotEngine) metaKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":meta:")
}

func (e *SnapshotEngine) snapshotHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:")
}

func (e *SnapshotEngine) snapshotOrderKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:order:")
}

func (e *SnapshotEngine) snapshotExpireKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:expire:")
}

func (e *SnapshotEngine) snapshotMetaKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":snapshot:meta:")
}

// Presence-specific key functions removed - presence now uses generic snapshot keys:
// - snapshotHashKey instead of presenceHashKey
// - snapshotExpireKey instead of presenceExpireKey
// - streamKey instead of presenceStreamKey
// - metaKey instead of presenceStreamMetaKey
// This unifies presence and keyed state - both are just keyed snapshots with different configs.

func (e *SnapshotEngine) aggregationZSetKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":aggregation:zset:")
}

func (e *SnapshotEngine) aggregationHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":aggregation:hash:")
}

func (e *SnapshotEngine) leavePayloadHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":leave:")
}

func (e *SnapshotEngine) aggregationMappingHashKey(s *RedisShard, ch string) string {
	return e.buildKey(s, ch, ":aggmap:")
}

func (e *SnapshotEngine) cleanupRegistrationKey(s *RedisShard) string {
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

func (e *SnapshotEngine) cleanupRegistrationKeyForChannel(s *RedisShard, ch string) string {
	// Get the cleanup registration key with proper hash tag for the channel's partition
	// Single registration ZSET works for ALL keyed snapshots (presence, state, etc.)
	if !s.isCluster {
		return e.conf.Prefix + ":cleanup:channels"
	}
	if e.conf.NumShardedPubSubPartitions > 0 {
		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
		return e.conf.Prefix + ":cleanup:channels:{" + strconv.Itoa(idx) + "}"
	}
	// For non-sharded cluster, use channel-based hash tag
	return e.conf.Prefix + ":cleanup:channels:{" + ch + "}"
}

func (e *SnapshotEngine) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) string {
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
func (e *SnapshotEngine) buildKey(s *RedisShard, ch string, infix string) string {
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

func (e *SnapshotEngine) messageChannelID(s *RedisShard, ch string) string {
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
func (e *SnapshotEngine) Close(_ context.Context) error {
	e.closeOnce.Do(func() {
		close(e.closeCh)
	})
	return nil
}

// Publish publishes data to a stateful channel with optional keyed state.
func (e *SnapshotEngine) Publish(ctx context.Context, ch string, data []byte, opts EnginePublishOptions) (StreamPosition, bool, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	// Fast path for non-history, non-idempotent, non-keyed publications.
	if opts.StreamSize == 0 && opts.StreamTTL == 0 && opts.IdempotencyKey == "" && opts.Key == "" {
		if e.conf.SkipPubSub {
			return StreamPosition{}, false, nil
		}
		protoPub := &protocol.Publication{
			Data: data,
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
			return StreamPosition{}, false, err
		}

		payload := "0::" + convert.BytesToString(pushBytes)

		chID := e.messageChannelID(s.shard, ch)
		if e.useShardedPubSub(s.shard) {
			cmd := shardClient.B().Spublish().Channel(chID).Message(payload).Build()
			return StreamPosition{}, false, shardClient.Do(ctx, cmd).Error()
		}
		cmd := shardClient.B().Publish().Channel(chID).Message(payload).Build()
		return StreamPosition{}, false, shardClient.Do(ctx, cmd).Error()
	}

	protoPub := &protocol.Publication{
		Data: data,
		Info: infoToProto(opts.ClientInfo),
		Tags: opts.Tags,
		Time: time.Now().UnixMilli(),
	}
	if opts.Key != "" {
		protoPub.Key = opts.Key
	}
	byteMessage, err := protoPub.MarshalVT()
	if err != nil {
		return StreamPosition{}, false, err
	}

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

	if opts.Key != "" {
		snapshotHashKey = e.snapshotHashKey(s.shard, ch)
		snapshotMetaKey = e.snapshotMetaKey(s.shard, ch)
		if opts.Ordered {
			snapshotOrderKey = e.snapshotOrderKey(s.shard, ch)
		}
		snapshotExpireKey = e.snapshotExpireKey(s.shard, ch)
	}

	metaExpire := "0"
	if opts.StreamMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.StreamMetaTTL.Seconds()))
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

	// Pre-construct remove publication for cleanup (enables guaranteed remove events on expiry)
	// Only needed if MemberTTL is set (keyed state with expiration)
	leaveHashKey := ""
	cleanupRegKey := ""
	var leaveBytes []byte
	if opts.MemberTTL > 0 && opts.Key != "" && snapshotExpireKey != "" {
		removePub := &protocol.Publication{
			Key:     opts.Key,
			Removed: true,
			Time:    time.Now().UnixMilli(),
		}
		leaveBytes, err = removePub.MarshalVT()
		if err != nil {
			return StreamPosition{}, false, err
		}
		leaveHashKey = e.buildKey(s.shard, ch, ":state:leave:")
		cleanupRegKey = e.cleanupRegistrationKeyForChannel(s.shard, ch)
	}

	replies, err := e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, resultKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey,
			"", "", snapshotMetaKey, // No aggregation keys for regular publish
			leaveHashKey, cleanupRegKey, "", // Leave hash and cleanup registration for keyed state, no aggregation mapping
		},
		[]string{
			opts.Key,                           // message_key
			convert.BytesToString(byteMessage), // message_payload (Publication - for stream, publishing, and snapshot)
			strconv.Itoa(opts.StreamSize),
			strconv.FormatInt(int64(opts.StreamTTL.Seconds()), 10),
			chID, // channel (for Lua to publish)
			metaExpire,
			e.node.ID(), // new_epoch_if_empty
			publishCommand,
			resultExpire,
			useDelta,
			version,
			opts.VersionEpoch,
			"0", // is_leave
			strconv.FormatInt(opts.Score, 10),
			strconv.FormatInt(int64(opts.MemberTTL.Seconds()), 10),
			"0", "", "", // use_hexpire, aggregation_key, aggregation_value
			"",                                // message_key_payload (not used - message_payload is Publication)
			convert.BytesToString(leaveBytes), // leave_payload (pre-constructed remove Publication)
			ch,                                // channel_for_cleanup (for cleanup registration)
		},
	).ToArray()
	if err != nil {
		return StreamPosition{}, false, err
	}

	return parseAddScriptResult(replies)
}

// Unpublish removes a key from keyed state snapshot.
func (e *SnapshotEngine) Unpublish(ctx context.Context, ch string, key string, opts EngineUnpublishOptions) (StreamPosition, error) {
	if key == "" {
		return StreamPosition{}, fmt.Errorf("key is required for unpublish")
	}

	s := e.getShard(ch)
	shardClient := s.shard.client

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
	if opts.StreamMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(opts.StreamMetaTTL.Seconds()))
	} else if e.node.config.HistoryMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
	}

	publishCommand := ""
	var chID string
	if opts.SendPush {
		if e.useShardedPubSub(s.shard) {
			publishCommand = "SPUBLISH"
		} else {
			publishCommand = "PUBLISH"
		}
		chID = e.messageChannelID(s.shard, ch)
	}
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
		return StreamPosition{}, err
	}

	replies, err := e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, "", // Optional stream, no result key
			snapshotHashKey,
			"", // No order key for unpublish
			snapshotExpireKey,
			"", "", snapshotMetaKey, // No aggregation keys for unpublish
			"", "", "", // No leave hash, cleanup registration, or aggregation mapping for unpublish
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
			"1",    // is_leave (this triggers removal)
			"0",    // score
			"0",    // keyed_member_ttl
			"0",    // use_hexpire
			"", "", // aggregation_key, aggregation_value
			"",     // message_key_payload (empty - we're removing)
			"", "", // leave_payload, channel_for_cleanup (not used for unpublish)
		},
	).ToArray()
	if err != nil {
		return StreamPosition{}, err
	}

	streamPos, _, err := parseAddScriptResult(replies)
	return streamPos, err
}

// ReadSnapshot retrieves snapshot entries with per-entry revisions for a channel.
// Each entry includes its revision so client can filter: entry.Revision <= snapshot_revision.
// If opts.SnapshotRevision is provided and epoch changed, returns empty entries.
// Returns entries, stream position, next cursor for pagination, and error.
// Cursor "0" or "" means end of iteration.
func (e *SnapshotEngine) ReadSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error) {
	if opts.Ordered {
		return e.readOrderedSnapshot(ctx, ch, opts)
	}
	return e.readUnorderedSnapshot(ctx, ch, opts)
}

func (e *SnapshotEngine) readUnorderedSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	snapshotTTL := "0"
	if opts.SnapshotTTL > 0 {
		snapshotTTL = strconv.FormatInt(int64(opts.SnapshotTTL.Seconds()), 10)
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
	dataArr, _ := replies[3].ToArray()

	streamPos := StreamPosition{Offset: streamOffset, Epoch: streamEpoch}

	// Validate epoch if client provided snapshot revision
	if opts.SnapshotRevision != nil && opts.SnapshotRevision.Epoch != streamEpoch {
		// Epoch changed - return empty entries, client must restart
		return nil, streamPos, nextCursor, nil
	}

	// Parse snapshot values with revisions
	entries := make([]SnapshotEntry, 0, len(dataArr)/2)
	for i := 0; i < len(dataArr); i += 2 {
		key, _ := dataArr[i].ToString()
		val, _ := dataArr[i+1].AsBytes()

		// Parse value: offset:epoch:payload
		entryOffset, entryEpoch, payload, err := parseSnapshotValue(val)
		if err != nil {
			// Skip malformed entries
			continue
		}

		entries = append(entries, SnapshotEntry{
			Key:     key,
			Payload: payload,
			Revision: StreamPosition{
				Offset: entryOffset,
				Epoch:  entryEpoch,
			},
		})
	}
	return entries, streamPos, nextCursor, nil
}

// readOrderedSnapshot uses offset-based pagination (not cursor), so returns empty cursor.
func (e *SnapshotEngine) readOrderedSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	snapshotTTL := "0"
	if opts.SnapshotTTL > 0 {
		snapshotTTL = strconv.FormatInt(int64(opts.SnapshotTTL.Seconds()), 10)
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
			strconv.Itoa(opts.Limit),                                             // ARGV[1] = limit
			strconv.Itoa(opts.Offset),                                            // ARGV[2] = offset
			strconv.FormatInt(time.Now().Unix(), 10),                             // ARGV[3] = now
			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10), // ARGV[4] = meta_ttl
			snapshotTTL, // ARGV[5] = snapshot_ttl
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
	keysArr, _ := replies[2].ToArray()
	valuesArr, _ := replies[3].ToArray()

	streamPos := StreamPosition{Offset: streamOffset, Epoch: streamEpoch}

	// Validate epoch if client provided snapshot revision
	if opts.SnapshotRevision != nil && opts.SnapshotRevision.Epoch != streamEpoch {
		// Epoch changed - return empty entries, client must restart
		return nil, streamPos, "", nil
	}

	// Parse snapshot values with revisions
	entries := make([]SnapshotEntry, 0, len(keysArr))
	for i := 0; i < len(keysArr); i++ {
		key, _ := keysArr[i].ToString()
		val, _ := valuesArr[i].AsBytes()

		// Parse value: offset:epoch:payload
		entryOffset, entryEpoch, payload, err := parseSnapshotValue(val)
		if err != nil {
			// Skip malformed entries
			continue
		}

		entries = append(entries, SnapshotEntry{
			Key:     key,
			Payload: payload,
			Revision: StreamPosition{
				Offset: entryOffset,
				Epoch:  entryEpoch,
			},
		})
	}

	return entries, streamPos, "", nil
}

// ReadStream retrieves publication stream for a channel.
func (e *SnapshotEngine) ReadStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, StreamPosition, error) {
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
			offset = strconv.FormatUint(opts.Filter.Since.Offset, 10)
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

	res, cleanup := e.readStreamScript.ExecUnsafe(ctx, s.shard.client,
		[]string{e.streamKey(s.shard, ch), e.metaKey(s.shard, ch)},
		[]string{
			includePubsStr,
			offset,
			strconv.Itoa(limit),
			reverse,
			metaExpire,
			e.node.ID(),
		},
	)
	defer cleanup()
	replies, err := res.ToArray()
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

	pubs := make([]*Publication, 0, len(pubValues))
	for _, v := range pubValues {
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

		// Find the "d" field which contains the payload
		var payload string
		for i := 0; i < len(fieldsArr)-1; i += 2 {
			fieldName, _ := fieldsArr[i].ToString()
			if fieldName == "d" {
				payload, _ = fieldsArr[i+1].ToString()
				break
			}
		}
		if payload == "" {
			continue // Skip entries without payload
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
		err = protoPub.UnmarshalVT(convert.StringToBytes(payload))
		if err != nil {
			return nil, StreamPosition{}, fmt.Errorf("can not unmarshal value to Publication: %v", err)
		}
		protoPub.Offset = pubOffset
		pubs = append(pubs, pubFromProto(&protoPub))
	}
	return pubs, streamPos, nil
}

// AddMember adds a client to converged membership (presence with ordering).
func (e *SnapshotEngine) AddMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
	// AddMember is just Publish with a Publication that has Key=clientID, Info=clientInfo, Data=nil
	// This reuses all the Publish logic for keyed snapshots with aggregation support

	protoPub := &protocol.Publication{
		Key:  info.ClientID,
		Info: infoToProto(&info),
		Time: time.Now().UnixMilli(),
	}
	pubBytes, err := protoPub.MarshalVT()
	if err != nil {
		return err
	}

	s := e.getShard(ch)
	shardClient := s.shard.client

	// Optional presence stream for recovery
	var streamKey, metaKey, metaExpire string
	if e.conf.PresenceStreamSize > 0 {
		streamKey = e.streamKey(s.shard, ch)
		metaKey = e.metaKey(s.shard, ch)
		if e.conf.PresenceStreamMetaTTL > 0 {
			metaExpire = strconv.Itoa(int(e.conf.PresenceStreamMetaTTL.Seconds()))
		} else {
			metaExpire = "0"
		}
	}

	publishCommand := ""
	var chID string
	if opts.SendPush {
		if e.useShardedPubSub(s.shard) {
			publishCommand = "SPUBLISH"
		} else {
			publishCommand = "PUBLISH"
		}
		chID = e.messageChannelID(s.shard, ch)
	}

	// Use aggregation for user counting
	aggregationKey := ""
	aggregationValue := ""
	if info.UserID != "" {
		aggregationKey = "user_id"
		aggregationValue = info.UserID
	}

	// Pre-construct LEAVE Publication for cleanup (enables guaranteed LEAVE events on expiry)
	// NOTE: The timestamp is set at join time, not when cleanup actually runs.
	// This means LEAVE events from cleanup may have timestamps in the past
	// (up to PresenceTTL seconds old). This is acceptable because:
	// 1. Clients use revision (epoch, offset) for ordering, not timestamps
	// 2. The exact leave time is when TTL expires, which this timestamp approximates
	leaveHashKey := ""
	cleanupRegKey := ""
	aggMappingKey := ""
	var leaveBytes []byte
	if e.conf.PresenceStreamSize > 0 {
		leavePub := &protocol.Publication{
			Key:     info.ClientID,
			Info:    infoToProto(&info),
			Removed: true,
			Time:    time.Now().UnixMilli(),
		}
		leaveBytes, err = leavePub.MarshalVT()
		if err != nil {
			return err
		}
		leaveHashKey = e.leavePayloadHashKey(s.shard, ch)
		cleanupRegKey = e.cleanupRegistrationKeyForChannel(s.shard, ch)
		if info.UserID != "" {
			aggMappingKey = e.aggregationMappingHashKey(s.shard, ch)
		}
	}

	_, err = e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, "", // Optional presence stream, no result key
			e.snapshotHashKey(s.shard, ch),
			"", // No order key for presence
			e.snapshotExpireKey(s.shard, ch),
			e.aggregationZSetKey(s.shard, ch),
			e.aggregationHashKey(s.shard, ch),
			"",            // No snapshot meta key for presence (uses stream meta directly)
			leaveHashKey,  // KEYS[10]: leave payload hash key
			cleanupRegKey, // KEYS[11]: cleanup registration zset key
			aggMappingKey, // KEYS[12]: aggregation mapping hash key
		},
		[]string{
			info.ClientID,                   // message_key
			convert.BytesToString(pubBytes), // message_payload (Publication for stream, publishing, and snapshot)
			strconv.Itoa(e.conf.PresenceStreamSize),
			strconv.FormatInt(int64(e.conf.PresenceStreamTTL.Seconds()), 10),
			chID, // channel (for Lua to publish)
			metaExpire,
			e.node.ID(),
			publishCommand,   // Lua will publish
			"", "0", "0", "", // result_key_expire, use_delta, version, version_epoch
			"0", // is_leave
			"0", // score
			strconv.FormatInt(int64(e.conf.PresenceTTL.Seconds()), 10),
			"0",                               // use_hexpire
			aggregationKey,                    // aggregation_key (e.g., "user_id")
			aggregationValue,                  // aggregation_value (e.g., actual user ID)
			"",                                // message_key_payload (not used - message_payload is Publication)
			convert.BytesToString(leaveBytes), // leave_payload (pre-constructed LEAVE Publication)
			ch,                                // channel_for_cleanup (channel name for registration)
		},
	).ToArray()
	return err
}

// RemoveMember removes a client from converged membership.
func (e *SnapshotEngine) RemoveMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
	// RemoveMember is just Publish with a Publication that has Removed=true
	// This reuses all the Publish logic for keyed snapshots with aggregation support

	protoPub := &protocol.Publication{
		Key:     info.ClientID,
		Info:    infoToProto(&info),
		Removed: true,
		Time:    time.Now().UnixMilli(),
	}
	pubBytes, err := protoPub.MarshalVT()
	if err != nil {
		return err
	}

	s := e.getShard(ch)
	shardClient := s.shard.client

	// Optional presence stream for recovery
	var streamKey, metaKey, metaExpire string
	if e.conf.PresenceStreamSize > 0 {
		streamKey = e.streamKey(s.shard, ch)
		metaKey = e.metaKey(s.shard, ch)
		if e.conf.PresenceStreamMetaTTL > 0 {
			metaExpire = strconv.Itoa(int(e.conf.PresenceStreamMetaTTL.Seconds()))
		} else {
			metaExpire = "0"
		}
	}

	publishCommand := ""
	var chID string
	if opts.SendPush {
		if e.useShardedPubSub(s.shard) {
			publishCommand = "SPUBLISH"
		} else {
			publishCommand = "PUBLISH"
		}
		chID = e.messageChannelID(s.shard, ch)
	}

	// Use aggregation for user counting
	aggregationKey := ""
	aggregationValue := ""
	if info.UserID != "" {
		aggregationKey = "user_id"
		aggregationValue = info.UserID
	}

	// Leave hash key and cleanup registration for proper cleanup handling
	leaveHashKey := ""
	cleanupRegKey := ""
	aggMappingKey := ""
	if e.conf.PresenceStreamSize > 0 {
		leaveHashKey = e.leavePayloadHashKey(s.shard, ch)
		cleanupRegKey = e.cleanupRegistrationKeyForChannel(s.shard, ch)
		if info.UserID != "" {
			aggMappingKey = e.aggregationMappingHashKey(s.shard, ch)
		}
	}

	_, err = e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, "", // Optional presence stream, no result key
			e.snapshotHashKey(s.shard, ch),
			"", // No order key for presence
			e.snapshotExpireKey(s.shard, ch),
			e.aggregationZSetKey(s.shard, ch),
			e.aggregationHashKey(s.shard, ch),
			"",            // No snapshot meta key for presence
			leaveHashKey,  // KEYS[10]: leave payload hash key (to clean up)
			cleanupRegKey, // KEYS[11]: cleanup registration key (to update after remove)
			aggMappingKey, // KEYS[12]: aggregation mapping hash key
		},
		[]string{
			info.ClientID,                   // message_key
			convert.BytesToString(pubBytes), // message_payload (Publication with Removed=true for stream and publishing)
			strconv.Itoa(e.conf.PresenceStreamSize),
			strconv.FormatInt(int64(e.conf.PresenceStreamTTL.Seconds()), 10),
			chID, // channel (for Lua to publish)
			metaExpire,
			e.node.ID(),
			publishCommand,   // Lua will publish
			"", "0", "0", "", // result_key_expire, use_delta, version, version_epoch
			"1", // is_leave
			"0", // score
			strconv.FormatInt(int64(e.conf.PresenceTTL.Seconds()), 10),
			"0",              // use_hexpire
			aggregationKey,   // aggregation_key
			aggregationValue, // aggregation_value
			"",               // message_key_payload (not used - message_payload is Publication)
			"",               // leave_payload (empty - not needed for explicit leave)
			ch,               // channel_for_cleanup (needed to update cleanup registration)
		},
	).ToArray()
	return err
}

// Members returns actual presence information for a channel.
func (e *SnapshotEngine) Members(ctx context.Context, ch string) (map[string]*ClientInfo, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	metaTTL := "0"
	if e.conf.PresenceStreamMetaTTL > 0 {
		metaTTL = strconv.Itoa(int(e.conf.PresenceStreamMetaTTL.Seconds()))
	}

	replies, err := e.readUnorderedScript.Exec(ctx, shardClient,
		[]string{
			e.snapshotHashKey(s.shard, ch),   // hash_key
			e.snapshotExpireKey(s.shard, ch), // expire_key
			e.metaKey(s.shard, ch),           // meta_key
			"",                               // snapshot_meta_key (not used for presence)
		},
		[]string{
			"0", // cursor (start)
			"0", // limit (0 = HGETALL)
			strconv.FormatInt(time.Now().Unix(), 10),
			metaTTL,
			"0", // snapshot_ttl (don't refresh on read)
		},
	).ToArray()
	if err != nil {
		return nil, err
	}
	if len(replies) < 4 {
		return nil, errors.New("wrong number of replies")
	}

	dataArr, _ := replies[3].ToArray()
	if len(dataArr) == 0 {
		return nil, nil
	}

	m := make(map[string]*ClientInfo, len(dataArr)/2)
	for i := 0; i < len(dataArr); i += 2 {
		k, _ := dataArr[i].ToString()
		v, _ := dataArr[i+1].AsBytes()

		// Parse snapshot value: offset:epoch:publication_bytes
		_, _, payload, err := parseSnapshotValue(v)
		if err != nil {
			// Skip malformed entries
			continue
		}

		var pub protocol.Publication
		err = pub.UnmarshalVT(payload)
		if err != nil {
			return nil, err
		}
		if pub.Info != nil {
			m[k] = infoFromProto(pub.Info)
		}
	}
	return m, nil
}

// MemberStats returns short stats of current presence data.
func (e *SnapshotEngine) MemberStats(ctx context.Context, ch string) (EngineMemberStats, error) {
	s := e.getShard(ch)
	shardClient := s.shard.client

	now := time.Now().Unix()

	replies, err := e.presenceStatsScript.Exec(ctx, shardClient,
		[]string{
			e.snapshotHashKey(s.shard, ch),
			e.snapshotExpireKey(s.shard, ch),
			e.aggregationZSetKey(s.shard, ch),
			e.aggregationHashKey(s.shard, ch),
		},
		[]string{
			strconv.FormatInt(now, 10),
			"0", // use_hexpire
		},
	).ToArray()
	if err != nil {
		return EngineMemberStats{}, err
	}
	if len(replies) != 2 {
		return EngineMemberStats{}, errors.New("wrong number of replies from script")
	}
	numClients, err := replies[0].AsInt64()
	if err != nil {
		return EngineMemberStats{}, err
	}
	numUsers, err := replies[1].AsInt64()
	if err != nil {
		return EngineMemberStats{}, err
	}
	return EngineMemberStats{NumClients: int(numClients), NumUsers: int(numUsers)}, nil
}

// ReadPresenceSnapshot retrieves presence snapshot with per-entry revisions for converged membership.
// Each entry includes its revision so client can filter: entry.Revision <= snapshot_revision.
// If opts.SnapshotRevision is provided and epoch changed, returns empty entries.
// Returns Publications with Key=ClientID, Info=ClientInfo, Offset/Epoch for revision.
func (e *SnapshotEngine) ReadPresenceSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]*Publication, StreamPosition, error) {
	// ReadPresenceSnapshot is just ReadSnapshot with SnapshotEntry → Publication unmarshaling
	// This reuses all the snapshot reading logic

	snapshotEntries, streamPos, _, err := e.ReadSnapshot(ctx, ch, opts)
	if err != nil {
		return nil, StreamPosition{}, err
	}

	pubs := make([]*Publication, 0, len(snapshotEntries))
	for _, entry := range snapshotEntries {
		// Unmarshal Publication from snapshot payload
		var protoPub protocol.Publication
		err := protoPub.UnmarshalVT(entry.Payload)
		if err != nil {
			// Skip malformed entries
			continue
		}

		// Convert to Publication with revision info
		pub := pubFromProto(&protoPub)
		pub.Offset = entry.Revision.Offset
		// Ensure Key matches the Redis hash key (should already be set from protoPub, but be explicit)
		if pub.Key == "" {
			pub.Key = entry.Key
		}
		pubs = append(pubs, pub)
	}
	return pubs, streamPos, nil
}

// ReadPresenceStream retrieves presence event stream (joins/leaves) for a channel.
// Returns Publications with Info=ClientInfo and Removed flag (true for leave, false for join).
func (e *SnapshotEngine) ReadPresenceStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, StreamPosition, error) {
	// ReadPresenceStream is literally just ReadStream - presence streams store Publications
	// Publication.Removed distinguishes join (false) from leave (true)

	if e.conf.PresenceStreamSize == 0 {
		return nil, StreamPosition{}, nil
	}

	return e.ReadStream(ctx, ch, opts)
}

// RegisterEventHandler registers a BrokerEventHandler to handle messages from Pub/Sub.
func (e *SnapshotEngine) RegisterEventHandler(h BrokerEventHandler) error {
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

func (e *SnapshotEngine) runPubSubShard(s *engineShardWrapper, h BrokerEventHandler) error {
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
func (e *SnapshotEngine) runForever(fn func()) {
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

// runCleanupWorker runs the background cleanup worker that generates LEAVE events
// for expired presence entries. This ensures guaranteed delivery of LEAVE events
// even when clients disconnect without explicit leave.
func (e *SnapshotEngine) runCleanupWorker(ctx context.Context) {
	if e.conf.CleanupInterval <= 0 || e.conf.PresenceStreamSize <= 0 {
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

// runCleanupCycle processes all shards and cleans up expired presence entries.
func (e *SnapshotEngine) runCleanupCycle(ctx context.Context) {
	now := time.Now().Unix()

	for _, shardWrapper := range e.shards {
		if ctx.Err() != nil {
			return
		}

		shard := shardWrapper.shard

		// Get cleanup registration key(s) based on cluster configuration
		var cleanupKeys []string
		if e.useShardedPubSub(shard) {
			// For sharded PUB/SUB, we have multiple cleanup keys
			for i := 0; i < e.conf.NumShardedPubSubPartitions; i++ {
				cleanupKeys = append(cleanupKeys, e.conf.Prefix+":cleanup:channels:{"+strconv.Itoa(i)+"}")
			}
		} else {
			cleanupKeys = []string{e.conf.Prefix + ":cleanup:channels"}
		}

		for _, cleanupKey := range cleanupKeys {
			if ctx.Err() != nil {
				return
			}

			// Get channels that need cleanup (score < now)
			channels, err := e.getChannelsForCleanup(ctx, shard.client, cleanupKey, now)
			if err != nil {
				if e.node.logEnabled(LogLevelError) {
					e.node.logger.log(newLogEntry(LogLevelError, "cleanup: failed to get channels", map[string]any{
						"error": err.Error(),
					}))
				}
				continue
			}

			for _, ch := range channels {
				if ctx.Err() != nil {
					return
				}

				err := e.cleanupChannel(ctx, shard, ch, cleanupKey, now)
				if err != nil {
					if e.node.logEnabled(LogLevelError) {
						e.node.logger.log(newLogEntry(LogLevelError, "cleanup: failed to cleanup channel", map[string]any{
							"channel": ch,
							"error":   err.Error(),
						}))
					}
				}
			}
		}
	}
}

// getChannelsForCleanup returns channels that have expired entries.
func (e *SnapshotEngine) getChannelsForCleanup(ctx context.Context, client rueidis.Client, cleanupKey string, now int64) ([]string, error) {
	// Get channels with score <= now (have expired entries)
	cmd := client.B().Zrangebyscore().Key(cleanupKey).Min("0").Max(strconv.FormatInt(now, 10)).Limit(0, 100).Build()
	result, err := client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// cleanupChannel runs the cleanup script for a single channel.
func (e *SnapshotEngine) cleanupChannel(ctx context.Context, shard *RedisShard, ch string, cleanupKey string, now int64) error {
	s := e.getShard(ch)

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
	if e.conf.PresenceStreamMetaTTL > 0 {
		metaExpire = strconv.Itoa(int(e.conf.PresenceStreamMetaTTL.Seconds()))
	}

	_, err := e.cleanupScript.Exec(ctx, shard.client,
		[]string{
			e.snapshotHashKey(shard, ch),           // KEYS[1]: snapshot hash key
			e.snapshotExpireKey(shard, ch),         // KEYS[2]: snapshot expire zset key
			e.streamKey(shard, ch),                 // KEYS[3]: stream key
			e.metaKey(shard, ch),                   // KEYS[4]: stream meta key
			e.aggregationZSetKey(s.shard, ch),      // KEYS[5]: aggregation zset key
			e.aggregationHashKey(s.shard, ch),      // KEYS[6]: aggregation hash key
			cleanupKey,                             // KEYS[7]: cleanup registration zset key
			e.leavePayloadHashKey(shard, ch),       // KEYS[8]: leave payload hash key
			e.aggregationMappingHashKey(shard, ch), // KEYS[9]: aggregation mapping hash key
		},
		[]string{
			strconv.FormatInt(now, 10),              // ARGV[1]: now
			strconv.Itoa(e.conf.CleanupBatchSize),   // ARGV[2]: batch_size
			chID,                                    // ARGV[3]: channel (for PUBLISH)
			publishCommand,                          // ARGV[4]: publish_command
			strconv.Itoa(e.conf.PresenceStreamSize), // ARGV[5]: stream_size
			strconv.FormatInt(int64(e.conf.PresenceStreamTTL.Seconds()), 10), // ARGV[6]: stream_ttl
			metaExpire,  // ARGV[7]: meta_expire
			e.node.ID(), // ARGV[8]: new_epoch_if_empty
			"0",         // ARGV[9]: use_hexpire
			ch,          // ARGV[10]: channel_for_cleanup
		},
	).ToArray()

	return err
}

func (e *SnapshotEngine) runPubSub(s *engineShardWrapper, logFields map[string]any, eventHandler BrokerEventHandler, clusterShardIndex, psShardIndex int, useShardedPubSub bool, startOnce func(error)) {
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

func (e *SnapshotEngine) logResubscribed(numChannels int, elapsed time.Duration, logFields map[string]any) {
	combinedLogFields := make(map[string]any, len(logFields)+2)
	for k, v := range logFields {
		combinedLogFields[k] = v
	}
	combinedLogFields["elapsed"] = elapsed.String()
	combinedLogFields["num_channels"] = numChannels
	e.node.logger.log(newLogEntry(LogLevelDebug, "resubscribed to channels", combinedLogFields))
}

func (e *SnapshotEngine) pubSubShardChannelID(clusterShardIndex int, psShardIndex int, useShardedPubSub bool) string {
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

func (e *SnapshotEngine) extractChannel(isCluster bool, chID string) string {
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

func (e *SnapshotEngine) handleRedisClientMessage(isCluster bool, eventHandler BrokerEventHandler, chID string, data []byte) error {
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

	// Check if this is a presence event (has Info field but no Data)
	if protoPub.Info != nil && len(protoPub.Data) == 0 {
		// Presence event: join or leave based on Removed flag
		info := infoFromProto(protoPub.Info)
		if protoPub.Removed {
			_ = eventHandler.HandleLeave(channel, info)
		} else {
			_ = eventHandler.HandleJoin(channel, info)
		}
		return nil
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
func (e *SnapshotEngine) Subscribe(ch string) error {
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
func (e *SnapshotEngine) Unsubscribe(ch string) error {
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
func parseAddScriptResult(replies []rueidis.RedisMessage) (StreamPosition, bool, error) {
	if len(replies) < 2 {
		return StreamPosition{}, false, fmt.Errorf("wrong number of replies from script: %d", len(replies))
	}
	offset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return StreamPosition{}, false, fmt.Errorf("could not parse offset: %w", err)
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return StreamPosition{}, false, fmt.Errorf("could not parse epoch: %w", err)
	}
	fromCache := false
	if len(replies) >= 3 {
		fromCacheVal, err := replies[2].ToString()
		if err == nil && fromCacheVal == "1" {
			fromCache = true
		}
	}
	return StreamPosition{Offset: offset, Epoch: epoch}, fromCache, nil
}
