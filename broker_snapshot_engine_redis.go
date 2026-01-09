package centrifuge

import (
	"context"
	_ "embed"
	"sync"
	"time"

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
	//go:embed internal/redis_lua/broker_snapshot_history_get.lua
	brokerSnapshotHistoryGetScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_presence_stats.lua
	brokerSnapshotPresenceStatsScriptSource string
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
	ReadPresenceStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*PresenceStreamEvent, StreamPosition, error)
	// ReadPresenceSnapshot retrieves presence snapshot with per-entry revisions for converged membership.
	// Each entry includes its revision so client can filter: entry.Revision <= snapshot_revision.
	// Presence doesn't use cursor pagination - returns all entries.
	ReadPresenceSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]PresenceSnapshotEntry, StreamPosition, error)
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

// PresenceSnapshotEntry represents a presence entry with its revision.
type PresenceSnapshotEntry struct {
	ClientID string
	Info     *ClientInfo
	Revision StreamPosition // When this presence entry was written
}

// PresenceStreamEvent represents a join or leave event with positioning information.
// Only available when PresenceStreamSize > 0 in engine config.
type PresenceStreamEvent struct {
	Type   string      // "join" or "leave"
	Info   *ClientInfo // Client information
	Offset uint64      // Sequential offset in presence stream
}

//
//// RedisEngine is a Redis-based engine that provides channel-centric APIs
//// for history, presence, and keyed snapshots using modern Lua scripts.
//type RedisEngine struct {
//	node *Node
//	conf RedisEngineConfig
//
//	shards []*engineShardWrapper
//
//	addScript           *rueidis.Lua
//	readOrderedScript   *rueidis.Lua
//	readUnorderedScript *rueidis.Lua
//	historyGetScript    *rueidis.Lua
//	presenceGetScript   *rueidis.Lua
//	presenceStatsScript *rueidis.Lua
//
//	closeCh       chan struct{}
//	closeOnce     sync.Once
//	shardChannel  string
//	messagePrefix string
//}
//
//var _ Engine = (*RedisEngine)(nil)
//
//// RedisEngineConfig is a config for RedisEngine.
//type RedisEngineConfig struct {
//	// Shards is a slice of RedisShard to use. At least one shard must be provided.
//	Shards []*RedisShard
//	// Prefix to use before every channel name and key in Redis.
//	Prefix string
//	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
//	// it on the client side. This is useful for FIPS compliance.
//	LoadSHA1 bool
//	// PresenceTTL is a time-to-live for presence information.
//	PresenceTTL time.Duration
//	// IdempotentResultTTL is a time-to-live for idempotent result.
//	IdempotentResultTTL time.Duration
//	// SubscribeOnReplica allows subscribing on replica Redis nodes.
//	SubscribeOnReplica bool
//	// SkipPubSub enables mode when Redis engine only works with data structures, without
//	// publishing to channels and using PUB/SUB.
//	SkipPubSub bool
//	// NumShardedPubSubPartitions when greater than zero allows turning on a mode in which
//	// engine will use Redis Cluster with sharded PUB/SUB feature available in
//	// Redis >= 7: https://redis.io/docs/manual/pubsub/#sharded-pubsub
//	NumShardedPubSubPartitions int
//	// numSubscribeShards defines how many subscribe shards will be used.
//	// Each subscribe shard uses a dedicated connection to Redis for making subscriptions.
//	// Zero value means 1.
//	numSubscribeShards int
//	// numResubscribeShards defines how many subscriber goroutines will be used for
//	// resubscribing process for each subscribe shard. Zero value tells
//	// Centrifuge to use 16 subscriber goroutines per subscribe shard.
//	numResubscribeShards int
//	// numPubSubProcessors allows configuring number of workers which will process
//	// messages coming from Redis PUB/SUB. Zero value tells Centrifuge to use the
//	// number calculated as:
//	// runtime.NumCPU / numSubscribeShards / NumShardedPubSubPartitions (if used) (minimum 1).
//	numPubSubProcessors int
//	// PresenceStreamSize defines the maximum number of presence events (joins/leaves)
//	// to keep in the presence stream for recovery. Zero value (default) means no
//	// presence history - presence events are only published in real-time without
//	// persistence. When set to a positive value, presence events get their own
//	// independent offset sequence separate from publications, enabling presence
//	// event recovery via ReadPresenceStream.
//	// Trade-off: enabling this doubles Redis memory usage for channels with presence
//	// (separate stream for presence events), but provides guaranteed delivery and
//	// recovery for join/leave events.
//	PresenceStreamSize int
//	// PresenceStreamTTL defines TTL for presence stream. Zero value means presence
//	// stream entries don't expire. Only used when PresenceStreamSize > 0.
//	PresenceStreamTTL time.Duration
//	// PresenceStreamMetaTTL defines TTL for presence stream metadata (offset, epoch).
//	// Zero value means metadata doesn't expire. Only used when PresenceStreamSize > 0.
//	PresenceStreamMetaTTL time.Duration
//}
//
//// NewRedisEngine initializes RedisEngine.
//func NewRedisEngine(n *Node, conf RedisEngineConfig) (*RedisEngine, error) {
//	if len(conf.Shards) == 0 {
//		return nil, errors.New("redis engine: no shards provided")
//	}
//
//	if conf.SubscribeOnReplica {
//		for i, s := range conf.Shards {
//			if s.replicaClient == nil {
//				return nil, fmt.Errorf("engine: SubscribeOnReplica enabled but no replica client initialized in shard[%d] (ReplicaClientEnabled option)", i)
//			}
//		}
//	}
//
//	if conf.Prefix == "" {
//		conf.Prefix = "centrifuge"
//	}
//	if conf.PresenceTTL == 0 {
//		conf.PresenceTTL = 60 * time.Second
//	}
//	if conf.IdempotentResultTTL == 0 {
//		conf.IdempotentResultTTL = 5 * time.Minute
//	}
//
//	if conf.numSubscribeShards == 0 {
//		conf.numSubscribeShards = 1
//	}
//	if conf.numResubscribeShards == 0 {
//		conf.numResubscribeShards = 16
//	}
//	if conf.numPubSubProcessors == 0 {
//		conf.numPubSubProcessors = runtime.NumCPU() / conf.numSubscribeShards
//		if conf.NumShardedPubSubPartitions > 0 {
//			conf.numPubSubProcessors /= conf.NumShardedPubSubPartitions
//		}
//		if conf.numPubSubProcessors < 1 {
//			conf.numPubSubProcessors = 1
//		}
//	}
//
//	shardWrappers := make([]*engineShardWrapper, 0, len(conf.Shards))
//	for _, s := range conf.Shards {
//		shardWrappers = append(shardWrappers, &engineShardWrapper{shard: s})
//	}
//
//	e := &RedisEngine{
//		node:   n,
//		conf:   conf,
//		shards: shardWrappers,
//		addScript: rueidis.NewLuaScript(
//			brokerSnapshotPublishScriptSource,
//			rueidis.WithLoadSHA1(conf.LoadSHA1),
//		),
//		readOrderedScript: rueidis.NewLuaScript(
//			brokerSnapshotReadOrderedScriptSource,
//			rueidis.WithLoadSHA1(conf.LoadSHA1),
//		),
//		readUnorderedScript: rueidis.NewLuaScript(
//			brokerSnapshotReadUnorderedScriptSource,
//			rueidis.WithLoadSHA1(conf.LoadSHA1),
//		),
//		historyGetScript: rueidis.NewLuaScript(
//			brokerSnapshotHistoryGetScriptSource,
//			rueidis.WithLoadSHA1(conf.LoadSHA1),
//		),
//		presenceGetScript: rueidis.NewLuaScript(
//			brokerSnapshotPresenceGetScriptSource,
//			rueidis.WithLoadSHA1(conf.LoadSHA1),
//		),
//		presenceStatsScript: rueidis.NewLuaScript(
//			brokerSnapshotPresenceStatsScriptSource,
//			rueidis.WithLoadSHA1(conf.LoadSHA1),
//		),
//		closeCh: make(chan struct{}),
//	}
//	e.shardChannel = conf.Prefix + redisPubSubShardChannelSuffix
//	e.messagePrefix = conf.Prefix + redisClientChannelPrefix
//
//	for _, shardWrapper := range e.shards {
//		shard := shardWrapper.shard
//		if !shard.isCluster && e.conf.NumShardedPubSubPartitions > 0 {
//			return nil, errors.New("can use sharded PUB/SUB feature (non-zero number of pub/sub partitions) only with Redis Cluster")
//		}
//		subChannels := make([][]rueidis.DedicatedClient, 0)
//		pubSubStartChannels := make([][]*pubSubStart, 0)
//
//		if e.useShardedPubSub(shard) {
//			for i := 0; i < e.conf.NumShardedPubSubPartitions; i++ {
//				subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
//				pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
//			}
//		} else {
//			subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
//			pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
//		}
//
//		for i := 0; i < len(subChannels); i++ {
//			for j := 0; j < e.conf.numSubscribeShards; j++ {
//				subChannels[i] = append(subChannels[i], nil)
//				pubSubStartChannels[i] = append(pubSubStartChannels[i], &pubSubStart{errCh: make(chan error, 1)})
//			}
//		}
//
//		shardWrapper.subClients = subChannels
//		shardWrapper.pubSubStartChannels = pubSubStartChannels
//	}
//
//	return e, nil
//}
//
//func (e *RedisEngine) useShardedPubSub(s *RedisShard) bool {
//	return s.isCluster && e.conf.NumShardedPubSubPartitions > 0
//}
//
//func (e *RedisEngine) getShard(channel string) *engineShardWrapper {
//	if len(e.shards) == 1 {
//		return e.shards[0]
//	}
//	return e.shards[consistentIndex(channel, len(e.shards))]
//}
//
//func (e *RedisEngine) historyStreamKey(s *RedisShard, ch string) string {
//	if !s.isCluster {
//		// Fast path: prefix + ":stream:" + ch
//		var builder strings.Builder
//		builder.Grow(len(e.conf.Prefix) + 8 + len(ch))
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(":stream:")
//		builder.WriteString(ch)
//		return builder.String()
//	}
//
//	var builder strings.Builder
//
//	if e.conf.NumShardedPubSubPartitions > 0 {
//		// Sharded cluster: prefix + ":stream:" + "{" + idx + "}." + ch
//		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
//		idxStr := strconv.Itoa(idx)
//		capacity := len(e.conf.Prefix) + 8 + 1 + len(idxStr) + 2 + len(ch)
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(":stream:{")
//		builder.WriteString(idxStr)
//		builder.WriteString("}.")
//		builder.WriteString(ch)
//	} else {
//		// Non-sharded cluster: prefix + ":stream:" + "{" + ch + "}"
//		capacity := len(e.conf.Prefix) + 8 + 1 + len(ch) + 1
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(":stream:{")
//		builder.WriteString(ch)
//		builder.WriteByte('}')
//	}
//
//	return builder.String()
//}
//
//func (e *RedisEngine) metaKey(s *RedisShard, ch string) string {
//	if !s.isCluster {
//		var builder strings.Builder
//		builder.Grow(len(e.conf.Prefix) + 6 + len(ch))
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(":meta:")
//		builder.WriteString(ch)
//		return builder.String()
//	}
//
//	var builder strings.Builder
//
//	if e.conf.NumShardedPubSubPartitions > 0 {
//		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
//		idxStr := strconv.Itoa(idx)
//		capacity := len(e.conf.Prefix) + 6 + 1 + len(idxStr) + 2 + len(ch)
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(":meta:{")
//		builder.WriteString(idxStr)
//		builder.WriteString("}.")
//		builder.WriteString(ch)
//	} else {
//		capacity := len(e.conf.Prefix) + 6 + 1 + len(ch) + 1
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(":meta:{")
//		builder.WriteString(ch)
//		builder.WriteByte('}')
//	}
//
//	return builder.String()
//}
//
//func (e *RedisEngine) snapshotHashKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":snapshot:")
//}
//
//func (e *RedisEngine) snapshotOrderKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":snapshot:order:")
//}
//
//func (e *RedisEngine) snapshotExpireKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":snapshot:expire:")
//}
//
//func (e *RedisEngine) snapshotMetaKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":snapshot:meta:")
//}
//
//// buildKey is a helper function to build Redis keys with proper cluster hash tag support
//func (e *RedisEngine) buildKey(s *RedisShard, ch string, infix string) string {
//	if !s.isCluster {
//		var builder strings.Builder
//		builder.Grow(len(e.conf.Prefix) + len(infix) + len(ch))
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(infix)
//		builder.WriteString(ch)
//		return builder.String()
//	}
//
//	var builder strings.Builder
//
//	if e.conf.NumShardedPubSubPartitions > 0 {
//		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
//		idxStr := strconv.Itoa(idx)
//		capacity := len(e.conf.Prefix) + len(infix) + 1 + len(idxStr) + 2 + len(ch)
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(infix)
//		builder.WriteByte('{')
//		builder.WriteString(idxStr)
//		builder.WriteString("}.")
//		builder.WriteString(ch)
//	} else {
//		capacity := len(e.conf.Prefix) + len(infix) + 1 + len(ch) + 1
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(infix)
//		builder.WriteByte('{')
//		builder.WriteString(ch)
//		builder.WriteByte('}')
//	}
//
//	return builder.String()
//}
//
//func (e *RedisEngine) presenceHashKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":presence:snapshot:")
//}
//
//func (e *RedisEngine) presenceExpireKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":presence:expire:")
//}
//
//func (e *RedisEngine) presenceStreamKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":presence:stream:")
//}
//
//func (e *RedisEngine) presenceStreamMetaKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":presence:meta:")
//}
//
//func (e *RedisEngine) userZSetKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":user:zset:")
//}
//
//func (e *RedisEngine) userHashKey(s *RedisShard, ch string) string {
//	return e.buildKey(s, ch, ":user:hash:")
//}
//
//func (e *RedisEngine) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) string {
//	if !s.isCluster {
//		// Fast path: prefix + ".result." + ch + "." + idempotencyKey
//		var builder strings.Builder
//		builder.Grow(len(e.conf.Prefix) + 9 + len(ch) + 1 + len(idempotencyKey))
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(".result.")
//		builder.WriteString(ch)
//		builder.WriteByte('.')
//		builder.WriteString(idempotencyKey)
//		return builder.String()
//	}
//
//	var builder strings.Builder
//
//	if e.conf.NumShardedPubSubPartitions > 0 {
//		// Sharded cluster: prefix + ".result." + "{" + idx + "}." + ch + "." + idempotencyKey
//		idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
//		idxStr := strconv.Itoa(idx)
//		capacity := len(e.conf.Prefix) + 9 + 1 + len(idxStr) + 2 + len(ch) + 1 + len(idempotencyKey)
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(".result.{")
//		builder.WriteString(idxStr)
//		builder.WriteString("}.")
//		builder.WriteString(ch)
//		builder.WriteByte('.')
//		builder.WriteString(idempotencyKey)
//	} else {
//		// Non-sharded cluster: prefix + ".result." + "{" + ch + "}" + "." + idempotencyKey
//		capacity := len(e.conf.Prefix) + 9 + 1 + len(ch) + 2 + len(idempotencyKey)
//		builder.Grow(capacity)
//		builder.WriteString(e.conf.Prefix)
//		builder.WriteString(".result.{")
//		builder.WriteString(ch)
//		builder.WriteString("}.")
//		builder.WriteString(idempotencyKey)
//	}
//
//	return builder.String()
//}
//
//func (e *RedisEngine) messageChannelID(s *RedisShard, ch string) string {
//	if !e.useShardedPubSub(s) {
//		// In cluster mode, always use hash tags to ensure channels and keys (history,
//		// result cache) are in the same hash slot. This is required for Lua scripts in
//		// serverless Redis (e.g., AWS Elasticache Serverless) where PUBLISH commands
//		// inside Lua scripts are slot-affecting. Standard Redis Cluster works fine with
//		// this since PUBLISH is broadcast to all nodes regardless of hash slot.
//		if s.isCluster {
//			// Build: messagePrefix + "{" + ch + "}"
//			var builder strings.Builder
//			builder.Grow(len(e.messagePrefix) + 1 + len(ch) + 1)
//			builder.WriteString(e.messagePrefix)
//			builder.WriteByte('{')
//			builder.WriteString(ch)
//			builder.WriteByte('}')
//			return builder.String()
//		}
//		// Non-cluster path: no hash tags needed
//		var builder strings.Builder
//		builder.Grow(len(e.messagePrefix) + len(ch))
//		builder.WriteString(e.messagePrefix)
//		builder.WriteString(ch)
//		return builder.String()
//	}
//
//	// Sharded pub/sub path: uses partition-based hash tags
//	idx := consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
//	idxStr := strconv.Itoa(idx)
//
//	// Pre-calculate capacity: messagePrefix + "{" + idxStr + "}." + ch
//	capacity := len(e.messagePrefix) + 1 + len(idxStr) + 2 + len(ch)
//
//	var builder strings.Builder
//	builder.Grow(capacity)
//	builder.WriteString(e.messagePrefix)
//	builder.WriteByte('{')
//	builder.WriteString(idxStr)
//	builder.WriteString("}.")
//	builder.WriteString(ch)
//
//	return builder.String()
//}
//
//func (e *RedisEngine) Publish(ctx context.Context, ch string, data []byte, opts EnginePublishOptions) (StreamPosition, bool, error) {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	// Fast path for non-history, non-idempotent, non-keyed publications.
//	if opts.StreamSize == 0 && opts.StreamTTL == 0 && opts.IdempotencyKey == "" && opts.Key == "" {
//		if e.conf.SkipPubSub {
//			// Nothing to do if PUB/SUB is skipped and there's no history/idempotency/keyed state
//			return StreamPosition{}, false, nil
//		}
//		protoPub := &protocol.Publication{
//			Data: data,
//			Info: infoToProto(opts.ClientInfo),
//			Tags: opts.Tags,
//			Time: time.Now().UnixMilli(),
//		}
//		protoPub.Delta = opts.UseDelta
//
//		// Wrap in Push for unified message format
//		push := &protocol.Push{
//			Pub: protoPub,
//		}
//		pushBytes, err := push.MarshalVT()
//		if err != nil {
//			return StreamPosition{}, false, err
//		}
//
//		// Format: offset:epoch:push_bytes (no stream = offset 0, empty epoch)
//		payload := "0::" + convert.BytesToString(pushBytes)
//
//		chID := e.messageChannelID(s.shard, ch)
//		if e.useShardedPubSub(s.shard) {
//			cmd := shardClient.B().Spublish().Channel(chID).Message(payload).Build()
//			return StreamPosition{}, false, shardClient.Do(ctx, cmd).Error()
//		}
//		cmd := shardClient.B().Publish().Channel(chID).Message(payload).Build()
//		return StreamPosition{}, false, shardClient.Do(ctx, cmd).Error()
//	}
//
//	protoPub := &protocol.Publication{
//		Data: data,
//		Info: infoToProto(opts.ClientInfo),
//		Tags: opts.Tags,
//		Time: time.Now().UnixMilli(),
//	}
//	byteMessage, err := protoPub.MarshalVT()
//	if err != nil {
//		return StreamPosition{}, false, err
//	}
//
//	var resultKey string
//	var resultExpire string
//	if opts.IdempotencyKey != "" {
//		resultKey = e.resultCacheKey(s.shard, ch, opts.IdempotencyKey)
//		if opts.IdempotentResultTTL > 0 {
//			resultExpire = strconv.Itoa(int(opts.IdempotentResultTTL.Seconds()))
//		} else {
//			resultExpire = strconv.Itoa(int(e.conf.IdempotentResultTTL.Seconds()))
//		}
//	}
//
//	var streamKey, metaKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey, snapshotMetaKey string
//
//	if opts.StreamSize > 0 && opts.StreamTTL > 0 {
//		streamKey = e.historyStreamKey(s.shard, ch)
//		metaKey = e.metaKey(s.shard, ch)
//	}
//
//	if opts.Key != "" {
//		snapshotHashKey = e.snapshotHashKey(s.shard, ch)
//		snapshotMetaKey = e.snapshotMetaKey(s.shard, ch)
//		if opts.Ordered {
//			snapshotOrderKey = e.snapshotOrderKey(s.shard, ch)
//		}
//		snapshotExpireKey = e.snapshotExpireKey(s.shard, ch)
//	}
//
//	metaExpire := "0"
//	if opts.StreamMetaTTL > 0 {
//		metaExpire = strconv.Itoa(int(opts.StreamMetaTTL.Seconds()))
//	} else if e.node.config.HistoryMetaTTL > 0 {
//		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
//	}
//
//	useDelta := "0"
//	if opts.UseDelta {
//		useDelta = "1"
//	}
//
//	version := "0"
//	if opts.Version > 0 {
//		version = strconv.FormatUint(opts.Version, 10)
//	}
//
//	publishCommand := "PUBLISH"
//	if e.useShardedPubSub(s.shard) {
//		publishCommand = "SPUBLISH"
//	}
//	if e.conf.SkipPubSub {
//		publishCommand = ""
//	}
//
//	chID := e.messageChannelID(s.shard, ch)
//	if e.conf.SkipPubSub {
//		chID = ""
//	}
//
//	// Prepare Push message for publishing
//	push := &protocol.Push{
//		Pub: protoPub,
//	}
//	pushBytes, err := push.MarshalVT()
//	if err != nil {
//		return StreamPosition{}, false, err
//	}
//
//	// Lua script will publish message_payload in format:
//	// - Non-delta: offset:epoch:push_protobuf
//	// - Delta: d:offset:epoch:prev_len:prev_publication:curr_len:curr_push
//	//   (prev comes from stream as Publication, curr is Push)
//	replies, err := e.addScript.Exec(ctx, shardClient,
//		[]string{
//			streamKey, metaKey, resultKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey,
//			"", "", snapshotMetaKey, // No presence keys.
//		},
//		[]string{
//			opts.Key,                         // message_key
//			convert.BytesToString(pushBytes), // message_payload (Push - for stream and publishing)
//			strconv.Itoa(opts.StreamSize),
//			strconv.FormatInt(int64(opts.StreamTTL.Seconds()), 10),
//			chID, // channel (for Lua to publish)
//			metaExpire,
//			e.node.ID(), // new_epoch_if_empty
//			publishCommand,
//			resultExpire,
//			useDelta,
//			version,
//			opts.VersionEpoch,
//			"0", // is_leave
//			strconv.FormatInt(opts.Score, 10),
//			strconv.FormatInt(int64(opts.MemberTTL.Seconds()), 10),
//			"0", "0", "", // use_hexpire, track_user, user_id
//			convert.BytesToString(byteMessage), // message_key_payload (Publication - for snapshot if needed)
//		},
//	).ToArray()
//	if err != nil {
//		return StreamPosition{}, false, err
//	}
//
//	return parseAddScriptResult(replies)
//}
//
//func (e *RedisEngine) AddMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	infoBytes, err := infoToProto(&info).MarshalVT()
//	if err != nil {
//		return err
//	}
//
//	// Prepare Push message for publishing and stream storage
//	push := &protocol.Push{
//		Join: &protocol.Join{
//			Info: infoToProto(&info),
//		},
//	}
//	pushBytes, err := push.MarshalVT()
//	if err != nil {
//		return err
//	}
//
//	// Optional presence stream for recovery (independent offset sequence)
//	var streamKey, metaKey, metaExpire string
//	if e.conf.PresenceStreamSize > 0 {
//		streamKey = e.presenceStreamKey(s.shard, ch)
//		metaKey = e.presenceStreamMetaKey(s.shard, ch)
//		if e.conf.PresenceStreamMetaTTL > 0 {
//			metaExpire = strconv.Itoa(int(e.conf.PresenceStreamMetaTTL.Seconds()))
//		} else {
//			metaExpire = "0"
//		}
//	}
//
//	publishCommand := ""
//	var chID string
//	if opts.SendPush {
//		if e.useShardedPubSub(s.shard) {
//			publishCommand = "SPUBLISH"
//		} else {
//			publishCommand = "PUBLISH"
//		}
//		chID = e.messageChannelID(s.shard, ch)
//	}
//
//	// Lua script will publish in format: offset:epoch:push_protobuf
//	_, err = e.addScript.Exec(ctx, shardClient,
//		[]string{
//			streamKey, metaKey, "", // Optional presence stream, no result key
//			e.presenceHashKey(s.shard, ch),
//			"", // No order key for presence
//			e.presenceExpireKey(s.shard, ch),
//			e.userZSetKey(s.shard, ch),
//			e.userHashKey(s.shard, ch),
//			"", // No snapshot meta key
//		},
//		[]string{
//			info.ClientID,                    // message_key
//			convert.BytesToString(pushBytes), // message_payload (Push for stream)
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
//			"0", // use_hexpire
//			"1", // track_user
//			info.UserID,
//			convert.BytesToString(infoBytes), // message_key_payload (for snapshot - ClientInfo)
//		},
//	).ToArray()
//	return err
//}
//
//func (e *RedisEngine) RemoveMember(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	// Prepare Push message for publishing and stream storage
//	push := &protocol.Push{
//		Leave: &protocol.Leave{
//			Info: infoToProto(&info),
//		},
//	}
//	pushBytes, err := push.MarshalVT()
//	if err != nil {
//		return err
//	}
//
//	// Optional presence stream for recovery (independent offset sequence)
//	var streamKey, metaKey, metaExpire string
//	if e.conf.PresenceStreamSize > 0 {
//		streamKey = e.presenceStreamKey(s.shard, ch)
//		metaKey = e.presenceStreamMetaKey(s.shard, ch)
//		if e.conf.PresenceStreamMetaTTL > 0 {
//			metaExpire = strconv.Itoa(int(e.conf.PresenceStreamMetaTTL.Seconds()))
//		} else {
//			metaExpire = "0"
//		}
//	}
//
//	publishCommand := ""
//	var chID string
//	if opts.SendPush {
//		if e.useShardedPubSub(s.shard) {
//			publishCommand = "SPUBLISH"
//		} else {
//			publishCommand = "PUBLISH"
//		}
//		chID = e.messageChannelID(s.shard, ch)
//	}
//
//	// Lua script will publish in format: offset:epoch:push_protobuf
//	_, err = e.addScript.Exec(ctx, shardClient,
//		[]string{
//			streamKey, metaKey, "", // Optional presence stream, no result key
//			e.presenceHashKey(s.shard, ch),
//			"", // No order key for presence
//			e.presenceExpireKey(s.shard, ch),
//			e.userZSetKey(s.shard, ch),
//			e.userHashKey(s.shard, ch),
//			"", // No snapshot meta key
//		},
//		[]string{
//			info.ClientID,                    // message_key
//			convert.BytesToString(pushBytes), // message_payload (Push for stream)
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
//			"0", // use_hexpire
//			"1", // track_user
//			info.UserID,
//			"", // message_key_payload (empty - leave doesn't store in snapshot)
//		},
//	).ToArray()
//	return err
//}
//
//// ReadStream retrieves publication stream for a channel.
//func (e *RedisEngine) ReadStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, StreamPosition, error) {
//	s := e.getShard(ch)
//
//	var includePubs = true
//	var offset string
//	if opts.Filter.Since != nil {
//		if opts.Filter.Reverse {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
//			if offset == "0" {
//				includePubs = false
//			}
//		} else {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset, 10)
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
//		limit = -1
//	}
//
//	reverse := "0"
//	if opts.Filter.Reverse {
//		reverse = "1"
//	}
//
//	metaExpire := "0"
//	if opts.MetaTTL > 0 {
//		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
//	} else if e.node.config.HistoryMetaTTL > 0 {
//		metaExpire = strconv.Itoa(int(e.node.config.HistoryMetaTTL.Seconds()))
//	}
//
//	includePubsStr := "0"
//	if includePubs {
//		includePubsStr = "1"
//	}
//
//	replies, err := e.historyGetScript.Exec(ctx, s.shard.client,
//		[]string{e.historyStreamKey(s.shard, ch), e.metaKey(s.shard, ch)},
//		[]string{
//			includePubsStr,
//			offset,
//			strconv.Itoa(limit),
//			reverse,
//			metaExpire,
//			e.node.ID(),
//		},
//	).ToArray()
//	if err != nil {
//		return nil, StreamPosition{}, err
//	}
//
//	if len(replies) < 2 {
//		return nil, StreamPosition{}, fmt.Errorf("wrong number of replies: %d", len(replies))
//	}
//
//	topOffset, err := replies[0].AsUint64()
//	if err != nil && !rueidis.IsRedisNil(err) {
//		return nil, StreamPosition{}, fmt.Errorf("could not parse top offset: %w", err)
//	}
//
//	epoch, err := replies[1].ToString()
//	if err != nil {
//		return nil, StreamPosition{}, fmt.Errorf("could not parse epoch: %w", err)
//	}
//
//	streamPos := StreamPosition{Offset: topOffset, Epoch: epoch}
//	if !includePubs || len(replies) < 3 {
//		return nil, streamPos, nil
//	}
//
//	pubValues, err := replies[2].ToArray()
//	if err != nil {
//		return nil, StreamPosition{}, err
//	}
//
//	pubs := make([]*Publication, 0, len(pubValues))
//	for _, v := range pubValues {
//		entry, err := v.ToArray()
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//		if len(entry) != 2 {
//			return nil, StreamPosition{}, fmt.Errorf("invalid publication format")
//		}
//		id, err := entry[0].ToString()
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//		payload, err := entry[1].ToString()
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//
//		hyphenPos := strings.Index(id, "-")
//		if hyphenPos <= 0 {
//			return nil, StreamPosition{}, fmt.Errorf("unexpected offset format: %s", id)
//		}
//		pubOffset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//
//		// Stream now contains Push instead of Publication
//		var push protocol.Push
//		err = push.UnmarshalVT(convert.StringToBytes(payload))
//		if err != nil {
//			return nil, StreamPosition{}, fmt.Errorf("can not unmarshal value to Push: %v", err)
//		}
//		if push.Pub != nil {
//			pub := push.Pub
//			pub.Offset = pubOffset
//			pubs = append(pubs, pubFromProto(pub))
//		}
//	}
//	return pubs, streamPos, nil
//}
//
//func (e *RedisEngine) Members(ctx context.Context, ch string) (map[string]*ClientInfo, error) {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	now := time.Now().Unix()
//
//	res, err := e.presenceGetScript.Exec(ctx, shardClient,
//		[]string{
//			e.presenceExpireKey(s.shard, ch),
//			e.presenceHashKey(s.shard, ch),
//		},
//		[]string{
//			strconv.FormatInt(now, 10),
//			"0", // use_hexpire
//		},
//	).AsStrMap()
//	if err != nil {
//		return nil, err
//	}
//	if len(res) == 0 {
//		return nil, nil
//	}
//	m := make(map[string]*ClientInfo, len(res))
//	for k, v := range res {
//		var info protocol.ClientInfo
//		err := info.UnmarshalVT([]byte(v))
//		if err != nil {
//			return nil, err
//		}
//		m[k] = infoFromProto(&info)
//	}
//	return m, nil
//}
//
//func (e *RedisEngine) MemberStats(ctx context.Context, ch string) (MemberStats, error) {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	now := time.Now().Unix()
//
//	replies, err := e.presenceStatsScript.Exec(ctx, shardClient,
//		[]string{
//			e.presenceExpireKey(s.shard, ch),
//			e.presenceHashKey(s.shard, ch),
//			e.userZSetKey(s.shard, ch),
//			e.userHashKey(s.shard, ch),
//		},
//		[]string{
//			strconv.FormatInt(now, 10),
//			"0", // use_hexpire
//		},
//	).ToArray()
//	if err != nil {
//		return MemberStats{}, err
//	}
//	if len(replies) != 2 {
//		return MemberStats{}, errors.New("wrong number of replies from script")
//	}
//	numClients, err := replies[0].AsInt64()
//	if err != nil {
//		return MemberStats{}, err
//	}
//	numUsers, err := replies[1].AsInt64()
//	if err != nil {
//		return MemberStats{}, err
//	}
//	return MemberStats{NumClients: int(numClients), NumUsers: int(numUsers)}, nil
//}
//
//// PresenceStreamEvent represents a join or leave event with positioning information.
//// Only available when PresenceStreamSize > 0 in RedisEngineConfig.
//type PresenceStreamEvent struct {
//	Type   string      // "join" or "leave"
//	Info   *ClientInfo // Client information
//	Offset uint64      // Sequential offset in presence stream
//}
//
//// ReadPresenceStream retrieves presence event stream (joins/leaves) for a channel.
//// This method only works when PresenceStreamSize > 0 in RedisEngineConfig, which
//// enables a separate offset sequence for presence events independent of publications.
////
//// Use this for:
//// - Recovering missed join/leave events after reconnection
//// - Auditing presence changes with guaranteed delivery
//// - Tracking who joined/left with exact ordering
////
//// Note: This uses the same Lua script as ReadStream but targets presence-specific keys.
//func (e *RedisEngine) ReadPresenceStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*PresenceStreamEvent, StreamPosition, error) {
//	if e.conf.PresenceStreamSize == 0 {
//		// Members stream not enabled - return empty
//		return nil, StreamPosition{}, nil
//	}
//
//	s := e.getShard(ch)
//
//	var includePubs = true
//	var offset string
//	if opts.Filter.Since != nil {
//		if opts.Filter.Reverse {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
//			if offset == "0" {
//				includePubs = false
//			}
//		} else {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset, 10)
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
//		limit = -1
//	}
//
//	reverse := "0"
//	if opts.Filter.Reverse {
//		reverse = "1"
//	}
//
//	metaExpire := "0"
//	if opts.MetaTTL > 0 {
//		metaExpire = strconv.Itoa(int(opts.MetaTTL.Seconds()))
//	} else if e.conf.PresenceStreamMetaTTL > 0 {
//		metaExpire = strconv.Itoa(int(e.conf.PresenceStreamMetaTTL.Seconds()))
//	}
//
//	includePubsStr := "0"
//	if includePubs {
//		includePubsStr = "1"
//	}
//
//	// Use presence stream keys instead of regular stream keys
//	replies, err := e.historyGetScript.Exec(ctx, s.shard.client,
//		[]string{e.presenceStreamKey(s.shard, ch), e.presenceStreamMetaKey(s.shard, ch)},
//		[]string{
//			includePubsStr,
//			offset,
//			strconv.Itoa(limit),
//			reverse,
//			metaExpire,
//			e.node.ID(),
//		},
//	).ToArray()
//	if err != nil {
//		return nil, StreamPosition{}, err
//	}
//
//	if len(replies) < 2 {
//		return nil, StreamPosition{}, fmt.Errorf("wrong number of replies: %d", len(replies))
//	}
//
//	topOffset, err := replies[0].AsUint64()
//	if err != nil && !rueidis.IsRedisNil(err) {
//		return nil, StreamPosition{}, fmt.Errorf("could not parse top offset: %w", err)
//	}
//
//	epoch, err := replies[1].ToString()
//	if err != nil {
//		return nil, StreamPosition{}, fmt.Errorf("could not parse epoch: %w", err)
//	}
//
//	streamPos := StreamPosition{Offset: topOffset, Epoch: epoch}
//	if !includePubs || len(replies) < 3 {
//		return nil, streamPos, nil
//	}
//
//	pubValues, err := replies[2].ToArray()
//	if err != nil {
//		return nil, StreamPosition{}, err
//	}
//
//	events := make([]*PresenceStreamEvent, 0, len(pubValues))
//	for _, v := range pubValues {
//		entry, err := v.ToArray()
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//		if len(entry) != 2 {
//			return nil, StreamPosition{}, fmt.Errorf("invalid presence event format")
//		}
//		id, err := entry[0].ToString()
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//		payload, err := entry[1].ToString()
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//
//		hyphenPos := strings.Index(id, "-")
//		if hyphenPos <= 0 {
//			return nil, StreamPosition{}, fmt.Errorf("unexpected offset format: %s", id)
//		}
//		eventOffset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//
//		// The payload is a Push message (Join or Leave)
//		var push protocol.Push
//		err = push.UnmarshalVT(convert.StringToBytes(payload))
//		if err != nil {
//			return nil, StreamPosition{}, fmt.Errorf("can not unmarshal value to Push: %v", err)
//		}
//
//		var eventType string
//		var info *ClientInfo
//		if push.Join != nil {
//			eventType = "join"
//			info = infoFromProto(push.Join.Info)
//		} else if push.Leave != nil {
//			eventType = "leave"
//			info = infoFromProto(push.Leave.Info)
//		} else {
//			// Skip if neither join nor leave
//			continue
//		}
//
//		events = append(events, &PresenceStreamEvent{
//			Type:   eventType,
//			Info:   info,
//			Offset: eventOffset,
//		})
//	}
//	return events, streamPos, nil
//}
//
//func (e *RedisEngine) ReadSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error) {
//	if opts.Ordered {
//		return e.readOrderedSnapshot(ctx, ch, opts)
//	}
//	return e.readUnorderedSnapshot(ctx, ch, opts)
//}
//
//func (e *RedisEngine) readUnorderedSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, string, error) {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	snapshotTTL := "0"
//	if opts.SnapshotTTL > 0 {
//		snapshotTTL = strconv.FormatInt(int64(opts.SnapshotTTL.Seconds()), 10)
//	}
//
//	// Cursor must be "0" to start for Redis HSCAN
//	cursor := opts.Cursor
//	if cursor == "" {
//		cursor = "0"
//	}
//
//	replies, err := e.readUnorderedScript.Exec(ctx, shardClient,
//		[]string{
//			e.snapshotHashKey(s.shard, ch),
//			e.snapshotExpireKey(s.shard, ch),
//			e.metaKey(s.shard, ch),
//			e.snapshotMetaKey(s.shard, ch),
//		},
//		[]string{
//			cursor,
//			strconv.Itoa(opts.Limit),
//			strconv.FormatInt(time.Now().Unix(), 10),
//			snapshotTTL,
//			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10),
//		},
//	).ToArray()
//	if err != nil {
//		return nil, StreamPosition{}, "", err
//	}
//	if len(replies) < 4 {
//		return nil, StreamPosition{}, "", errors.New("wrong number of replies")
//	}
//
//	offset, err := replies[0].AsUint64()
//	if err != nil && !rueidis.IsRedisNil(err) {
//		return nil, StreamPosition{}, "", err
//	}
//	epoch, _ := replies[1].ToString()
//	nextCursor, _ := replies[2].ToString()
//	dataArr, _ := replies[3].ToArray()
//
//	// Note: RedisEngine does not store revision with payload (legacy format)
//	// Returns entries with zero revision - use SnapshotEngine for proper revision support
//	entries := make([]SnapshotEntry, 0, len(dataArr)/2)
//	for i := 0; i < len(dataArr); i += 2 {
//		key, _ := dataArr[i].ToString()
//		val, _ := dataArr[i+1].AsBytes()
//		entries = append(entries, SnapshotEntry{
//			Key:      key,
//			Payload:  val,
//			Revision: StreamPosition{}, // Legacy format - no per-entry revision
//		})
//	}
//	return entries, StreamPosition{Offset: offset, Epoch: epoch}, nextCursor, nil
//}
//
//func (e *RedisEngine) readOrderedSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]SnapshotEntry, StreamPosition, error) {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	snapshotTTL := "0"
//	if opts.SnapshotTTL > 0 {
//		snapshotTTL = strconv.FormatInt(int64(opts.SnapshotTTL.Seconds()), 10)
//	}
//
//	replies, err := e.readOrderedScript.Exec(ctx, shardClient,
//		[]string{
//			e.snapshotHashKey(s.shard, ch),
//			e.snapshotOrderKey(s.shard, ch),
//			e.snapshotExpireKey(s.shard, ch),
//			e.metaKey(s.shard, ch),
//		},
//		[]string{
//			strconv.Itoa(opts.Offset),
//			strconv.Itoa(opts.Limit),
//			strconv.FormatInt(time.Now().Unix(), 10),
//			snapshotTTL,
//			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10),
//		},
//	).ToArray()
//	if err != nil {
//		return nil, StreamPosition{}, err
//	}
//	if len(replies) < 4 {
//		return nil, StreamPosition{}, errors.New("wrong number of replies")
//	}
//
//	offset, err := replies[0].AsUint64()
//	if err != nil && !rueidis.IsRedisNil(err) {
//		return nil, StreamPosition{}, err
//	}
//	epoch, _ := replies[1].ToString()
//	keysArr, _ := replies[2].ToArray()
//	valuesArr, _ := replies[3].ToArray()
//
//	// Note: RedisEngine does not store revision with payload (legacy format)
//	// Returns entries with zero revision - use SnapshotEngine for proper revision support
//	entries := make([]SnapshotEntry, 0, len(keysArr))
//	for i := 0; i < len(keysArr); i++ {
//		key, _ := keysArr[i].ToString()
//		val, _ := valuesArr[i].AsBytes()
//		entries = append(entries, SnapshotEntry{
//			Key:      key,
//			Payload:  val,
//			Revision: StreamPosition{}, // Legacy format - no per-entry revision
//		})
//	}
//
//	return entries, StreamPosition{Offset: offset, Epoch: epoch}, nil
//}
//
//// ReadPresenceSnapshot retrieves presence snapshot with per-entry revisions.
//// Note: RedisEngine uses legacy format without per-entry revisions.
//// Use SnapshotEngine for proper converged membership with revisions.
//func (e *RedisEngine) ReadPresenceSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) ([]PresenceSnapshotEntry, StreamPosition, error) {
//	s := e.getShard(ch)
//	shardClient := s.shard.client
//
//	now := time.Now().Unix()
//
//	res, err := e.presenceGetScript.Exec(ctx, shardClient,
//		[]string{
//			e.presenceExpireKey(s.shard, ch),
//			e.presenceHashKey(s.shard, ch),
//		},
//		[]string{
//			strconv.FormatInt(now, 10),
//			"0", // use_hexpire
//		},
//	).AsStrMap()
//	if err != nil {
//		return nil, StreamPosition{}, err
//	}
//	if len(res) == 0 {
//		return nil, StreamPosition{}, nil
//	}
//
//	// Note: RedisEngine does not store revision with presence entries (legacy format)
//	// Returns entries with zero revision - use SnapshotEngine for proper revision support
//	entries := make([]PresenceSnapshotEntry, 0, len(res))
//	for clientID, v := range res {
//		var info protocol.ClientInfo
//		err := info.UnmarshalVT([]byte(v))
//		if err != nil {
//			return nil, StreamPosition{}, err
//		}
//		entries = append(entries, PresenceSnapshotEntry{
//			ClientID: clientID,
//			Info:     infoFromProto(&info),
//			Revision: StreamPosition{}, // Legacy format - no per-entry revision
//		})
//	}
//	return entries, StreamPosition{}, nil
//}
//
//// RegisterEventHandler registers a BrokerEventHandler to handle messages from Pub/Sub.
//func (e *RedisEngine) RegisterEventHandler(h BrokerEventHandler) error {
//	// Run all shards.
//	for _, wrapper := range e.shards {
//		err := e.runPubSubShard(wrapper, h)
//		if err != nil {
//			return err
//		}
//	}
//	for i := 0; i < len(e.shards); i++ {
//		for j := 0; j < len(e.shards[i].pubSubStartChannels); j++ {
//			for k := 0; k < len(e.shards[i].pubSubStartChannels[j]); k++ {
//				if !e.conf.SkipPubSub {
//					<-e.shards[i].pubSubStartChannels[j][k].errCh
//				}
//			}
//		}
//	}
//	return nil
//}
//
//func (e *RedisEngine) runPubSubShard(s *engineShardWrapper, h BrokerEventHandler) error {
//	if e.conf.SkipPubSub {
//		return nil
//	}
//
//	for i := 0; i < len(s.subClients); i++ { // Cluster shards.
//		clusterShardIndex := i
//		for j := 0; j < len(s.subClients[i]); j++ { // PUB/SUB shards.
//			pubSubShardIndex := j
//			logFields := map[string]any{
//				"shard":          s.shard.string(),
//				"pub_sub_shard":  pubSubShardIndex,
//				"cluster_shards": len(s.subClients),
//				"pub_sub_shards": len(s.subClients[i]),
//			}
//			go e.runForever(func() {
//				select {
//				case <-e.closeCh:
//					return
//				default:
//				}
//				e.runPubSub(s, logFields, h, clusterShardIndex, pubSubShardIndex, e.useShardedPubSub(s.shard), func(err error) {
//					s.pubSubStartChannels[clusterShardIndex][pubSubShardIndex].once.Do(func() {
//						s.pubSubStartChannels[clusterShardIndex][pubSubShardIndex].errCh <- err
//					})
//				})
//			})
//		}
//	}
//	return nil
//}
//
//// runForever keeps another function running indefinitely.
//// The reason this loop is not inside the function itself is
//// so that defer can be used to clean-up nicely.
//func (e *RedisEngine) runForever(fn func()) {
//	for {
//		select {
//		case <-e.closeCh:
//			return
//		default:
//		}
//		fn()
//		select {
//		case <-e.closeCh:
//			return
//		case <-time.After(250 * time.Millisecond):
//		}
//	}
//}
//
//func (e *RedisEngine) runPubSub(s *engineShardWrapper, logFields map[string]any, eventHandler BrokerEventHandler, clusterShardIndex, psShardIndex int, useShardedPubSub bool, startOnce func(error)) {
//	numProcessors := e.conf.numPubSubProcessors
//	numResubscribeShards := e.conf.numResubscribeShards
//
//	shardChannel := e.pubSubShardChannelID(clusterShardIndex, psShardIndex, useShardedPubSub)
//
//	if e.node.logEnabled(LogLevelDebug) {
//		// Prepare log fields with care to avoid data races.
//		debugLogValues := map[string]any{
//			"num_processors": numProcessors,
//		}
//		if useShardedPubSub {
//			debugLogValues["cluster_shard_index"] = clusterShardIndex
//		}
//		startLogFields := map[string]any{}
//		for k, v := range logFields {
//			startLogFields[k] = v
//		}
//		if s.shard.isCluster {
//			startLogFields["cluster"] = true
//		}
//		combinedLogFields := make(map[string]any, len(startLogFields)+len(debugLogValues))
//		for k, v := range startLogFields {
//			combinedLogFields[k] = v
//		}
//		for k, v := range debugLogValues {
//			combinedLogFields[k] = v
//		}
//		e.node.logger.log(newLogEntry(LogLevelDebug, "running Redis PUB/SUB", combinedLogFields))
//		defer func() {
//			e.node.logger.log(newLogEntry(LogLevelDebug, "stopping Redis PUB/SUB", combinedLogFields))
//		}()
//	}
//
//	done := make(chan struct{})
//	var doneOnce sync.Once
//	closeDoneOnce := func() {
//		doneOnce.Do(func() {
//			close(done)
//		})
//	}
//	defer closeDoneOnce()
//
//	processors := make(map[int]chan rueidis.PubSubMessage)
//	for i := 0; i < numProcessors; i++ {
//		processingCh := make(chan rueidis.PubSubMessage, pubSubProcessorBufferSize)
//		processors[i] = processingCh
//		go func(ch chan rueidis.PubSubMessage) {
//			for {
//				select {
//				case <-done:
//					return
//				case msg := <-ch:
//					err := e.handleRedisClientMessage(s.shard.isCluster, eventHandler, msg.Channel, convert.StringToBytes(msg.Message))
//					if err != nil {
//						e.node.logger.log(newErrorLogEntry(err, "error handling client message", logFields))
//						continue
//					}
//				}
//			}
//		}(processingCh)
//	}
//
//	client := s.shard.client
//	if e.conf.SubscribeOnReplica {
//		client = s.shard.replicaClient
//	}
//
//	conn, cancel := client.Dedicate()
//	defer cancel()
//	defer conn.Close()
//
//	wait := conn.SetPubSubHooks(rueidis.PubSubHooks{
//		OnMessage: func(msg rueidis.PubSubMessage) {
//			select {
//			case processors[index(msg.Channel, numProcessors)] <- msg:
//			case <-done:
//			}
//		},
//		OnSubscription: func(ps rueidis.PubSubSubscription) {
//			if !useShardedPubSub {
//				return
//			}
//			if ps.Kind == "sunsubscribe" && ps.Channel == shardChannel {
//				// Helps to handle slot migration.
//				e.node.logger.log(newLogEntry(LogLevelInfo, "pub/sub restart due to slot migration", logFields))
//				closeDoneOnce()
//			}
//		},
//	})
//
//	var err error
//	if useShardedPubSub {
//		err = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(shardChannel).Build()).Error()
//	} else {
//		err = conn.Do(context.Background(), conn.B().Subscribe().Channel(shardChannel).Build()).Error()
//	}
//	if err != nil {
//		startOnce(err)
//		e.node.logger.log(newErrorLogEntry(err, "pub/sub subscribe error", logFields))
//		return
//	}
//
//	channels := e.node.Hub().Channels()
//
//	var wg sync.WaitGroup
//	started := time.Now()
//
//	for i := 0; i < numResubscribeShards; i++ {
//		wg.Add(1)
//		go func(subscriberIndex int) {
//			defer wg.Done()
//			estimatedCap := len(channels) / e.conf.numResubscribeShards / e.conf.numSubscribeShards
//			if useShardedPubSub {
//				estimatedCap /= e.conf.NumShardedPubSubPartitions
//			}
//			chIDs := make([]string, 0, estimatedCap)
//
//			for _, ch := range channels {
//				if e.getShard(ch).shard == s.shard && ((useShardedPubSub && consistentIndex(ch, e.conf.NumShardedPubSubPartitions) == clusterShardIndex && index(ch, e.conf.numSubscribeShards) == psShardIndex && index(ch, e.conf.numResubscribeShards) == subscriberIndex) || (index(ch, e.conf.numSubscribeShards) == psShardIndex && index(ch, e.conf.numResubscribeShards) == subscriberIndex)) {
//					chIDs = append(chIDs, e.messageChannelID(s.shard, ch))
//				}
//			}
//
//			subscribeBatch := func(batch []string) error {
//				if useShardedPubSub {
//					return conn.Do(context.Background(), conn.B().Ssubscribe().Channel(batch...).Build()).Error()
//				}
//				return conn.Do(context.Background(), conn.B().Subscribe().Channel(batch...).Build()).Error()
//			}
//
//			batch := make([]string, 0, redisSubscribeBatchLimit)
//
//			for i, ch := range chIDs {
//				if len(batch) > 0 && i%redisSubscribeBatchLimit == 0 {
//					err := subscribeBatch(batch)
//					if err != nil {
//						e.node.logger.log(newErrorLogEntry(err, "error subscribing", logFields))
//						closeDoneOnce()
//						return
//					}
//					batch = batch[:0]
//				}
//				batch = append(batch, ch)
//			}
//			if len(batch) > 0 {
//				err := subscribeBatch(batch)
//				if err != nil {
//					e.node.logger.log(newErrorLogEntry(err, "error subscribing", logFields))
//					closeDoneOnce()
//					return
//				}
//			}
//		}(i)
//	}
//
//	go func() {
//		wg.Wait()
//		if len(channels) > 0 && e.node.logEnabled(LogLevelDebug) {
//			e.logResubscribed(len(channels), time.Since(started), logFields)
//		}
//		select {
//		case <-done:
//			startOnce(errors.New("error resubscribing"))
//		default:
//			s.subClientsMu.Lock()
//			s.subClients[clusterShardIndex][psShardIndex] = conn
//			s.subClientsMu.Unlock()
//			defer func() {
//				s.subClientsMu.Lock()
//				s.subClients[clusterShardIndex][psShardIndex] = nil
//				s.subClientsMu.Unlock()
//			}()
//			startOnce(nil)
//		}
//		<-done
//	}()
//
//	select {
//	case err = <-wait:
//		startOnce(err)
//		if err != nil {
//			e.node.logger.log(newErrorLogEntry(err, "pub/sub connection error", logFields))
//		}
//	case <-done:
//	case <-s.shard.closeCh:
//	}
//}
//
//func (e *RedisEngine) logResubscribed(numChannels int, elapsed time.Duration, logFields map[string]any) {
//	combinedLogFields := make(map[string]any, len(logFields)+2)
//	for k, v := range logFields {
//		combinedLogFields[k] = v
//	}
//	combinedLogFields["elapsed"] = elapsed.String()
//	combinedLogFields["num_channels"] = numChannels
//	e.node.logger.log(newLogEntry(LogLevelDebug, "resubscribed to channels", combinedLogFields))
//}
//
//func (e *RedisEngine) Close(_ context.Context) error {
//	e.closeOnce.Do(func() {
//		close(e.closeCh)
//	})
//	return nil
//}
//
//// Subscribe to a channel.
//func (e *RedisEngine) Subscribe(ch string) error {
//	s := e.getShard(ch)
//	if e.node.logEnabled(LogLevelDebug) {
//		e.node.logger.log(newLogEntry(LogLevelDebug, "subscribe node on channel", map[string]any{"channel": ch}))
//	}
//	psShardIndex := index(ch, e.conf.numSubscribeShards)
//	var clusterShardIndex int
//	if e.useShardedPubSub(s.shard) {
//		clusterShardIndex = consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
//	}
//
//	s.subClientsMu.Lock()
//	conn := s.subClients[clusterShardIndex][psShardIndex]
//	if conn == nil {
//		s.subClientsMu.Unlock()
//		return errPubSubConnUnavailable
//	}
//	s.subClientsMu.Unlock()
//
//	var err error
//	if e.useShardedPubSub(s.shard) {
//		err = conn.Do(context.Background(), conn.B().Ssubscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
//	} else {
//		err = conn.Do(context.Background(), conn.B().Subscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
//	}
//	return err
//}
//
//// Unsubscribe from a channel.
//func (e *RedisEngine) Unsubscribe(ch string) error {
//	s := e.getShard(ch)
//	if e.node.logEnabled(LogLevelDebug) {
//		e.node.logger.log(newLogEntry(LogLevelDebug, "unsubscribe node from channel", map[string]any{"channel": ch}))
//	}
//	psShardIndex := index(ch, e.conf.numSubscribeShards)
//	var clusterShardIndex int
//	if e.useShardedPubSub(s.shard) {
//		clusterShardIndex = consistentIndex(ch, e.conf.NumShardedPubSubPartitions)
//	}
//
//	s.subClientsMu.Lock()
//	conn := s.subClients[clusterShardIndex][psShardIndex]
//	if conn == nil {
//		s.subClientsMu.Unlock()
//		return errPubSubConnUnavailable
//	}
//	s.subClientsMu.Unlock()
//
//	var err error
//	if e.useShardedPubSub(s.shard) {
//		err = conn.Do(context.Background(), conn.B().Sunsubscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
//	} else {
//		err = conn.Do(context.Background(), conn.B().Unsubscribe().Channel(e.messageChannelID(s.shard, ch)).Build()).Error()
//	}
//	return err
//}
//
//func (e *RedisEngine) pubSubShardChannelID(clusterShardIndex int, psShardIndex int, useShardedPubSub bool) string {
//	psShardStr := strconv.Itoa(psShardIndex)
//	if !useShardedPubSub {
//		var builder strings.Builder
//		builder.Grow(len(e.shardChannel) + 1 + len(psShardStr))
//		builder.WriteString(e.shardChannel)
//		builder.WriteByte('.')
//		builder.WriteString(psShardStr)
//		return builder.String()
//	}
//	clusterShardStr := strconv.Itoa(clusterShardIndex)
//	var builder strings.Builder
//	capacity := len(e.shardChannel) + 1 + len(psShardStr) + 2 + len(clusterShardStr) + 1
//	builder.Grow(capacity)
//	builder.WriteString(e.shardChannel)
//	builder.WriteByte('.')
//	builder.WriteString(psShardStr)
//	builder.WriteString(".{")
//	builder.WriteString(clusterShardStr)
//	builder.WriteByte('}')
//	return builder.String()
//}
//
//func (e *RedisEngine) extractChannel(isCluster bool, chID string) string {
//	ch := strings.TrimPrefix(chID, e.messagePrefix)
//
//	// Handle sharded PUB/SUB case: {idx}.channel
//	if e.conf.NumShardedPubSubPartitions > 0 {
//		if !strings.HasPrefix(ch, "{") {
//			return "" // Invalid: expected {idx}.channel format
//		}
//		i := strings.Index(ch, ".")
//		if i > 0 {
//			return ch[i+1:]
//		}
//		return "" // Invalid: missing dot separator
//	}
//
//	// Handle cluster case: must have {channel} format
//	if isCluster {
//		channelLen := len(ch)
//		if channelLen < 2 || ch[0] != '{' || ch[channelLen-1] != '}' {
//			return ""
//		}
//		return ch[1 : channelLen-1]
//	}
//
//	// Non-cluster: plain channel name
//	return ch
//}
//
//func (e *RedisEngine) handleRedisClientMessage(isCluster bool, eventHandler BrokerEventHandler, chID string, data []byte) error {
//	// Parse message: supports backward compat (no prefix), non-delta, and delta formats
//	offset, epoch, protobuf, isDelta, prevProtobuf, err := parseMessage(data)
//	if err != nil {
//		return fmt.Errorf("malformed pub/sub data: %w", err)
//	}
//
//	channel := e.extractChannel(isCluster, chID)
//	if channel == "" {
//		return fmt.Errorf("unsupported channel: %s", chID)
//	}
//
//	sp := StreamPosition{Offset: offset, Epoch: epoch}
//
//	// Try to unmarshal as Push first (new format)
//	var push protocol.Push
//	err = push.UnmarshalVT(protobuf)
//	if err == nil {
//		// Successfully parsed as Push - handle based on type
//		if push.Pub != nil {
//			pub := push.Pub
//			if pub.Offset == 0 {
//				pub.Offset = offset
//			}
//
//			var prevPubPtr *Publication
//			if isDelta && len(prevProtobuf) > 0 {
//				var prevPush protocol.Push
//				if err := prevPush.UnmarshalVT(prevProtobuf); err == nil && prevPush.Pub != nil {
//					prevPubPtr = pubFromProto(prevPush.Pub)
//				}
//			}
//
//			_ = eventHandler.HandlePublication(channel, pubFromProto(pub), sp, isDelta, prevPubPtr)
//		} else if push.Join != nil {
//			_ = eventHandler.HandleJoin(channel, infoFromProto(push.Join.Info))
//		} else if push.Leave != nil {
//			_ = eventHandler.HandleLeave(channel, infoFromProto(push.Leave.Info))
//		}
//		return nil
//	}
//
//	// Fallback: try to unmarshal as Publication directly (backward compatibility)
//	var pub protocol.Publication
//	err = pub.UnmarshalVT(protobuf)
//	if err == nil {
//		if pub.Offset == 0 {
//			pub.Offset = offset
//		}
//
//		var prevPubPtr *Publication
//		if isDelta && len(prevProtobuf) > 0 {
//			var prevPub protocol.Publication
//			if err := prevPub.UnmarshalVT(prevProtobuf); err == nil {
//				prevPubPtr = pubFromProto(&prevPub)
//			}
//		}
//
//		_ = eventHandler.HandlePublication(channel, pubFromProto(&pub), sp, isDelta, prevPubPtr)
//		return nil
//	}
//
//	// Fallback: try ClientInfo for join/leave (backward compatibility - though these should have Push wrapper)
//	var info protocol.ClientInfo
//	err = info.UnmarshalVT(protobuf)
//	if err == nil {
//		// Can't distinguish join vs leave without wrapper, default to join
//		_ = eventHandler.HandleJoin(channel, infoFromProto(&info))
//		return nil
//	}
//
//	return fmt.Errorf("failed to unmarshal protobuf as any known type")
//}
//
//// parseMessage parses message formats:
//// 1. No prefix (backward compat): raw protobuf bytes
//// 2. Non-delta: offset:epoch:protobuf
//// 3. Delta: d:offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
//// Returns offset, epoch, push bytes, delta flag, prev protobuf, and error if parsing fails.
//func parseMessage(data []byte) (uint64, string, []byte, bool, []byte, error) {
//	if len(data) == 0 {
//		return 0, "", nil, false, nil, fmt.Errorf("empty message")
//	}
//
//	// Check for delta prefix
//	if data[0] == 'd' && len(data) > 1 && data[1] == ':' {
//		// Delta format: d:offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
//		return parseDeltaMessage(data[2:]) // Skip "d:"
//	}
//
//	// Check if first byte is a digit (offset start) or colon separator
//	if data[0] < '0' || data[0] > '9' {
//		// No prefix - raw protobuf (backward compatibility)
//		return 0, "", data, false, nil, nil
//	}
//
//	// Non-delta format: offset:epoch:protobuf
//	firstColon := bytes.IndexByte(data, ':')
//	if firstColon == -1 {
//		// No colon found - treat as raw protobuf
//		return 0, "", data, false, nil, nil
//	}
//
//	// Parse offset
//	offset, err := strconv.ParseUint(convert.BytesToString(data[:firstColon]), 10, 64)
//	if err != nil {
//		// Failed to parse offset - treat as raw protobuf
//		return 0, "", data, false, nil, nil
//	}
//
//	// Find second colon (after epoch)
//	remaining := data[firstColon+1:]
//	secondColon := bytes.IndexByte(remaining, ':')
//	if secondColon == -1 {
//		return 0, "", nil, false, nil, fmt.Errorf("missing epoch separator")
//	}
//
//	// Extract epoch
//	epoch := convert.BytesToString(remaining[:secondColon])
//
//	// Everything after second colon is protobuf bytes
//	protobuf := remaining[secondColon+1:]
//
//	return offset, epoch, protobuf, false, nil, nil
//}
//
//// parseDeltaMessage parses delta format: offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
//func parseDeltaMessage(data []byte) (uint64, string, []byte, bool, []byte, error) {
//	// Parse offset
//	firstColon := bytes.IndexByte(data, ':')
//	if firstColon == -1 {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: missing offset separator")
//	}
//	offset, err := strconv.ParseUint(convert.BytesToString(data[:firstColon]), 10, 64)
//	if err != nil {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: invalid offset: %w", err)
//	}
//
//	// Parse epoch
//	remaining := data[firstColon+1:]
//	secondColon := bytes.IndexByte(remaining, ':')
//	if secondColon == -1 {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: missing epoch separator")
//	}
//	epoch := convert.BytesToString(remaining[:secondColon])
//
//	// Parse prev_len
//	remaining = remaining[secondColon+1:]
//	thirdColon := bytes.IndexByte(remaining, ':')
//	if thirdColon == -1 {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: missing prev length separator")
//	}
//	prevLen, err := strconv.Atoi(convert.BytesToString(remaining[:thirdColon]))
//	if err != nil {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: invalid prev length: %w", err)
//	}
//
//	// Extract prev_protobuf
//	remaining = remaining[thirdColon+1:]
//	if len(remaining) < prevLen {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: insufficient data for prev protobuf")
//	}
//	prevProtobuf := remaining[:prevLen]
//
//	// Parse curr_len
//	remaining = remaining[prevLen:]
//	if len(remaining) == 0 || remaining[0] != ':' {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: missing curr length separator")
//	}
//	remaining = remaining[1:] // Skip ':'
//	fourthColon := bytes.IndexByte(remaining, ':')
//	if fourthColon == -1 {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: missing curr protobuf separator")
//	}
//	currLen, err := strconv.Atoi(convert.BytesToString(remaining[:fourthColon]))
//	if err != nil {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: invalid curr length: %w", err)
//	}
//
//	// Extract curr_protobuf
//	remaining = remaining[fourthColon+1:]
//	if len(remaining) < currLen {
//		return 0, "", nil, false, nil, fmt.Errorf("delta: insufficient data for curr protobuf")
//	}
//	currProtobuf := remaining[:currLen]
//
//	return offset, epoch, currProtobuf, true, prevProtobuf, nil
//}
//
//func parseAddScriptResult(replies []rueidis.RedisMessage) (StreamPosition, bool, error) {
//	if len(replies) < 2 {
//		return StreamPosition{}, false, fmt.Errorf("wrong number of replies from script: %d", len(replies))
//	}
//	offset, err := replies[0].AsUint64()
//	if err != nil && !rueidis.IsRedisNil(err) {
//		return StreamPosition{}, false, fmt.Errorf("could not parse offset: %w", err)
//	}
//	epoch, err := replies[1].ToString()
//	if err != nil {
//		return StreamPosition{}, false, fmt.Errorf("could not parse epoch: %w", err)
//	}
//	fromCache := false
//	if len(replies) >= 3 {
//		fromCacheVal, err := replies[2].ToString()
//		if err == nil && fromCacheVal == "1" {
//			fromCache = true
//		}
//	}
//	return StreamPosition{Offset: offset, Epoch: epoch}, fromCache, nil
//}
