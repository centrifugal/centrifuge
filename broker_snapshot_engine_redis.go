package centrifuge

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"
	"github.com/centrifugal/protocol"
	"github.com/redis/rueidis"
)

var (
	//go:embed internal/redis_lua/broker_snapshot_add.lua
	brokerSnapshotPublishScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_read_ordered.lua
	brokerSnapshotReadOrderedScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_read_unordered.lua
	brokerSnapshotReadUnorderedScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_history_get.lua
	brokerSnapshotHistoryGetScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_presence_get.lua
	brokerSnapshotPresenceGetScriptSource string
	//go:embed internal/redis_lua/broker_snapshot_presence_stats.lua
	brokerSnapshotPresenceStatsScriptSource string
)

// Engine is an interface for channel operations.
type Engine interface {
	// Publish allows sending data into a channel. It can optionally use history and keyed snapshots.
	Publish(ctx context.Context, ch string, data []byte, opts EnginePublishOptions) (EngineStreamPosition, bool, error)
	// ReadStream retrieves publications from stream for a channel.
	ReadStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, EngineStreamPosition, error)
	// ReadSnapshot retrieves a snapshot for a channel.
	ReadSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) (map[string][]byte, EngineStreamPosition, error)

	// AddPresence adds a client to presence in a channel.
	AddPresence(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error
	// RemovePresence removes a client from presence in a channel.
	RemovePresence(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error
	// Presence returns actual presence information for a channel.
	Presence(ctx context.Context, ch string) (map[string]*ClientInfo, error)
	// PresenceStats returns short stats of current presence data.
	PresenceStats(ctx context.Context, ch string) (PresenceStats, error)
}

// EngineStreamPosition contains fields to describe position in stream.
// At moment this is used for automatic recovery mechanics. More info about stream
// recovery in Centrifugo docs: https://centrifugal.dev/docs/server/history_and_recovery.
type EngineStreamPosition struct {
	// Offset defines publication incremental offset inside a stream.
	Offset uint64 `json:"offset,omitempty"`
	// Epoch allows handling situations when storage
	// lost stream entirely for some reason (expired or lost after restart) and we
	// want to track this fact to prevent successful recovery from another stream.
	// I.e. for example we have a stream [1, 2, 3], then it's lost and new stream
	// contains [1, 2, 3, 4], client that recovers from position 3 will only receive
	// publication 4 missing 1, 2, 3 from new stream. With epoch, we can tell client
	// that correct recovery is not possible.
	Epoch string `json:"epoch,omitempty"`
	// Cursor for snapshot iteration.
	Cursor string `json:"cursor,omitempty"`
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
	Ordered     bool
	Cursor      string
	Limit       int
	Offset      int
	SnapshotTTL time.Duration
}

// RedisEngine is a Redis-based engine that provides channel-centric APIs
// for history, presence, and keyed snapshots using modern Lua scripts.
type RedisEngine struct {
	node *Node
	conf RedisEngineConfig

	shards []*RedisShard

	addScript           *rueidis.Lua
	readOrderedScript   *rueidis.Lua
	readUnorderedScript *rueidis.Lua
	historyGetScript    *rueidis.Lua
	presenceGetScript   *rueidis.Lua
	presenceStatsScript *rueidis.Lua
}

var _ Engine = (*RedisEngine)(nil)

// RedisEngineConfig is a config for RedisEngine.
type RedisEngineConfig struct {
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
}

// NewRedisEngine initializes RedisEngine.
func NewRedisEngine(n *Node, conf RedisEngineConfig) (*RedisEngine, error) {
	if len(conf.Shards) == 0 {
		return nil, errors.New("redis engine: no shards provided")
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

	e := &RedisEngine{
		node:   n,
		conf:   conf,
		shards: conf.Shards,
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
		historyGetScript: rueidis.NewLuaScript(
			brokerSnapshotHistoryGetScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		presenceGetScript: rueidis.NewLuaScript(
			brokerSnapshotPresenceGetScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
		presenceStatsScript: rueidis.NewLuaScript(
			brokerSnapshotPresenceStatsScriptSource,
			rueidis.WithLoadSHA1(conf.LoadSHA1),
		),
	}
	return e, nil
}

func (e *RedisEngine) getShard(channel string) *RedisShard {
	if len(e.shards) == 1 {
		return e.shards[0]
	}
	return e.shards[consistentIndex(channel, len(e.shards))]
}

func (e *RedisEngine) historyStreamKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":stream:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) metaKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":meta:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) snapshotHashKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":snapshot:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) snapshotOrderKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":snapshot:order:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) snapshotExpireKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":snapshot:expire:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) snapshotMetaKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":snapshot:meta:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) presenceHashKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":presence:snapshot:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) presenceExpireKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":presence:expire:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) userZSetKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":user:zset:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) userHashKey(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(":user:hash:")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(".result.")
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}.")
	} else {
		builder.WriteString(ch)
		builder.WriteString(".")
	}
	builder.WriteString(idempotencyKey)
	return builder.String()
}

func (e *RedisEngine) messageChannelID(s *RedisShard, ch string) string {
	var builder strings.Builder
	builder.WriteString(e.conf.Prefix)
	builder.WriteString(redisClientChannelPrefix)
	if s.isCluster {
		builder.WriteString("{")
		builder.WriteString(ch)
		builder.WriteString("}")
	} else {
		builder.WriteString(ch)
	}
	return builder.String()
}

func (e *RedisEngine) Publish(ctx context.Context, ch string, data []byte, opts EnginePublishOptions) (EngineStreamPosition, bool, error) {
	s := e.getShard(ch)
	shardClient := s.client

	// Fast path for non-history, non-idempotent, non-keyed publications.
	if opts.StreamSize == 0 && opts.StreamTTL == 0 && opts.IdempotencyKey == "" && opts.Key == "" {
		protoPub := &protocol.Publication{
			Data: data,
			Info: infoToProto(opts.ClientInfo),
			Tags: opts.Tags,
			Time: time.Now().UnixMilli(),
		}
		protoPub.Delta = opts.UseDelta
		byteMessage, err := protoPub.MarshalVT()
		if err != nil {
			return EngineStreamPosition{}, false, err
		}
		cmd := shardClient.B().Publish().Channel(e.messageChannelID(s, ch)).Message(convert.BytesToString(byteMessage)).Build()
		return EngineStreamPosition{}, false, shardClient.Do(ctx, cmd).Error()
	}

	protoPub := &protocol.Publication{
		Data: data,
		Info: infoToProto(opts.ClientInfo),
		Tags: opts.Tags,
		Time: time.Now().UnixMilli(),
	}
	byteMessage, err := protoPub.MarshalVT()
	if err != nil {
		return EngineStreamPosition{}, false, err
	}

	var resultKey string
	var resultExpire string
	if opts.IdempotencyKey != "" {
		resultKey = e.resultCacheKey(s, ch, opts.IdempotencyKey)
		if opts.IdempotentResultTTL > 0 {
			resultExpire = strconv.Itoa(int(opts.IdempotentResultTTL.Seconds()))
		} else {
			resultExpire = strconv.Itoa(int(e.conf.IdempotentResultTTL.Seconds()))
		}
	}

	var streamKey, metaKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey, snapshotMetaKey string

	if opts.StreamSize > 0 && opts.StreamTTL > 0 {
		streamKey = e.historyStreamKey(s, ch)
		metaKey = e.metaKey(s, ch)
	}

	if opts.Key != "" {
		snapshotHashKey = e.snapshotHashKey(s, ch)
		snapshotMetaKey = e.snapshotMetaKey(s, ch)
		if opts.Ordered {
			snapshotOrderKey = e.snapshotOrderKey(s, ch)
		}
		snapshotExpireKey = e.snapshotExpireKey(s, ch)
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

	replies, err := e.addScript.Exec(ctx, shardClient,
		[]string{
			streamKey, metaKey, resultKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey,
			"", "", snapshotMetaKey, // No presence keys.
		},
		[]string{
			opts.Key, // message_key
			convert.BytesToString(byteMessage),
			strconv.Itoa(opts.StreamSize),
			strconv.FormatInt(int64(opts.StreamTTL.Seconds()), 10),
			e.messageChannelID(s, ch), // channel
			metaExpire,
			e.node.ID(), // new_epoch_if_empty
			"PUBLISH",   // publish_command
			resultExpire,
			useDelta,
			version,
			opts.VersionEpoch,
			"0", // is_leave
			strconv.FormatInt(opts.Score, 10),
			strconv.FormatInt(int64(opts.MemberTTL.Seconds()), 10),
			"0", "0", "", // use_hexpire, track_user, user_id
		},
	).ToArray()
	if err != nil {
		return EngineStreamPosition{}, false, err
	}

	return parseAddScriptResult(replies)
}

func (e *RedisEngine) AddPresence(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
	s := e.getShard(ch)
	shardClient := s.client

	var err error
	var byteMessage []byte
	var chID string
	if opts.SendPush {
		push := &protocol.Push{
			Join: &protocol.Join{
				Info: infoToProto(&info),
			},
		}
		byteMessage, err = push.MarshalVT()
		if err != nil {
			return err
		}
		chID = e.messageChannelID(s, ch)
	}

	infoBytes, err := infoToProto(&info).MarshalVT()
	if err != nil {
		return err
	}

	_, err = e.addScript.Exec(ctx, shardClient,
		[]string{
			"", "", "", // No stream, no result key
			e.presenceHashKey(s, ch),
			"", // No order key for presence
			e.presenceExpireKey(s, ch),
			e.userZSetKey(s, ch),
			e.userHashKey(s, ch),
			"", // No snapshot meta key
		},
		[]string{
			info.ClientID,                      // message_key
			convert.BytesToString(byteMessage), // message_payload
			"0", "0",                           // stream_size, stream_ttl
			chID, // channel
			"0",  // meta_expire
			e.node.ID(),
			"", "", "0", "0", "",
			"0", // is_leave
			"0",
			strconv.FormatInt(int64(e.conf.PresenceTTL.Seconds()), 10),
			"0",
			"1", // track_user
			info.UserID,
			convert.BytesToString(infoBytes),
		},
	).ToArray()
	return err
}

func (e *RedisEngine) RemovePresence(ctx context.Context, ch string, info ClientInfo, opts EnginePresenceOptions) error {
	s := e.getShard(ch)
	shardClient := s.client

	var err error
	var byteMessage []byte
	var chID string
	if opts.SendPush {
		push := &protocol.Push{
			Leave: &protocol.Leave{
				Info: infoToProto(&info),
			},
		}
		byteMessage, err = push.MarshalVT()
		if err != nil {
			return err
		}
		chID = e.messageChannelID(s, ch)
	}

	_, err = e.addScript.Exec(ctx, shardClient,
		[]string{
			"", "", "", // No stream, no result key
			e.presenceHashKey(s, ch),
			"", // No order key for presence
			e.presenceExpireKey(s, ch),
			e.userZSetKey(s, ch),
			e.userHashKey(s, ch),
			"", // No snapshot meta key
		},
		[]string{
			info.ClientID,                      // message_key
			convert.BytesToString(byteMessage), // message_payload
			"0", "0",                           // stream_size, stream_ttl
			chID, // channel
			"0",  // meta_expire
			e.node.ID(),
			"", "", "0", "0", "",
			"1", // is_leave
			"0", "0", "0",
			"1",         // track_user
			info.UserID, // user_id
			"",
		},
	).ToArray()

	return err
}

// ReadStream retrieves publication stream for a channel.
func (e *RedisEngine) ReadStream(ctx context.Context, ch string, opts ReadStreamOptions) ([]*Publication, EngineStreamPosition, error) {
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
		limit = -1
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

	replies, err := e.historyGetScript.Exec(ctx, s.client,
		[]string{e.historyStreamKey(s, ch), e.metaKey(s, ch)},
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
		return nil, EngineStreamPosition{}, err
	}

	if len(replies) < 2 {
		return nil, EngineStreamPosition{}, fmt.Errorf("wrong number of replies: %d", len(replies))
	}

	topOffset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, EngineStreamPosition{}, fmt.Errorf("could not parse top offset: %w", err)
	}

	epoch, err := replies[1].ToString()
	if err != nil {
		return nil, EngineStreamPosition{}, fmt.Errorf("could not parse epoch: %w", err)
	}

	streamPos := EngineStreamPosition{Offset: topOffset, Epoch: epoch}
	if !includePubs || len(replies) < 3 {
		return nil, streamPos, nil
	}

	pubValues, err := replies[2].ToArray()
	if err != nil {
		return nil, EngineStreamPosition{}, err
	}

	pubs := make([]*Publication, 0, len(pubValues))
	for _, v := range pubValues {
		entry, err := v.ToArray()
		if err != nil {
			return nil, EngineStreamPosition{}, err
		}
		if len(entry) != 2 {
			return nil, EngineStreamPosition{}, fmt.Errorf("invalid publication format")
		}
		id, err := entry[0].ToString()
		if err != nil {
			return nil, EngineStreamPosition{}, err
		}
		payload, err := entry[1].ToString()
		if err != nil {
			return nil, EngineStreamPosition{}, err
		}

		hyphenPos := strings.Index(id, "-")
		if hyphenPos <= 0 {
			return nil, EngineStreamPosition{}, fmt.Errorf("unexpected offset format: %s", id)
		}
		pubOffset, err := strconv.ParseUint(id[:hyphenPos], 10, 64)
		if err != nil {
			return nil, EngineStreamPosition{}, err
		}

		var pub protocol.Publication
		err = pub.UnmarshalVT(convert.StringToBytes(payload))
		if err != nil {
			return nil, EngineStreamPosition{}, fmt.Errorf("can not unmarshal value to Publication: %v", err)
		}
		pub.Offset = pubOffset
		pubs = append(pubs, pubFromProto(&pub))
	}
	return pubs, streamPos, nil
}

func (e *RedisEngine) Presence(ctx context.Context, ch string) (map[string]*ClientInfo, error) {
	s := e.getShard(ch)
	shardClient := s.client

	now := time.Now().Unix()

	res, err := e.presenceGetScript.Exec(ctx, shardClient,
		[]string{
			e.presenceExpireKey(s, ch),
			e.presenceHashKey(s, ch),
		},
		[]string{
			strconv.FormatInt(now, 10),
			"0", // use_hexpire
		},
	).AsStrMap()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	m := make(map[string]*ClientInfo, len(res))
	for k, v := range res {
		var info protocol.ClientInfo
		err := info.UnmarshalVT([]byte(v))
		if err != nil {
			return nil, err
		}
		m[k] = infoFromProto(&info)
	}
	return m, nil
}

func (e *RedisEngine) PresenceStats(ctx context.Context, ch string) (PresenceStats, error) {
	s := e.getShard(ch)
	shardClient := s.client

	now := time.Now().Unix()

	replies, err := e.presenceStatsScript.Exec(ctx, shardClient,
		[]string{
			e.presenceExpireKey(s, ch),
			e.presenceHashKey(s, ch),
			e.userZSetKey(s, ch),
			e.userHashKey(s, ch),
		},
		[]string{
			strconv.FormatInt(now, 10),
			"0", // use_hexpire
		},
	).ToArray()
	if err != nil {
		return PresenceStats{}, err
	}
	if len(replies) != 2 {
		return PresenceStats{}, errors.New("wrong number of replies from script")
	}
	numClients, err := replies[0].AsInt64()
	if err != nil {
		return PresenceStats{}, err
	}
	numUsers, err := replies[1].AsInt64()
	if err != nil {
		return PresenceStats{}, err
	}
	return PresenceStats{NumClients: int(numClients), NumUsers: int(numUsers)}, nil
}

func (e *RedisEngine) ReadSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) (map[string][]byte, EngineStreamPosition, error) {
	if opts.Ordered {
		return e.readOrderedSnapshot(ctx, ch, opts)
	}
	return e.readUnorderedSnapshot(ctx, ch, opts)
}

func (e *RedisEngine) readUnorderedSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) (map[string][]byte, EngineStreamPosition, error) {
	s := e.getShard(ch)
	shardClient := s.client

	snapshotTTL := "0"
	if opts.SnapshotTTL > 0 {
		snapshotTTL = strconv.FormatInt(int64(opts.SnapshotTTL.Seconds()), 10)
	}

	replies, err := e.readUnorderedScript.Exec(ctx, shardClient,
		[]string{
			e.snapshotHashKey(s, ch),
			e.snapshotExpireKey(s, ch),
			e.metaKey(s, ch),
			e.snapshotMetaKey(s, ch),
		},
		[]string{
			opts.Cursor,
			strconv.Itoa(opts.Limit),
			strconv.FormatInt(time.Now().Unix(), 10),
			snapshotTTL,
			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10),
		},
	).ToArray()
	if err != nil {
		return nil, EngineStreamPosition{}, err
	}
	if len(replies) < 4 {
		return nil, EngineStreamPosition{}, errors.New("wrong number of replies")
	}

	offset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, EngineStreamPosition{}, err
	}
	epoch, _ := replies[1].ToString()
	cursor, _ := replies[2].ToString()
	dataArr, _ := replies[3].ToArray()

	data := make(map[string][]byte, len(dataArr)/2)
	for i := 0; i < len(dataArr); i += 2 {
		key, _ := dataArr[i].ToString()
		val, _ := dataArr[i+1].AsBytes()
		data[key] = val
	}
	return data, EngineStreamPosition{Offset: offset, Epoch: epoch, Cursor: cursor}, nil
}

func (e *RedisEngine) readOrderedSnapshot(ctx context.Context, ch string, opts ReadSnapshotOptions) (map[string][]byte, EngineStreamPosition, error) {
	s := e.getShard(ch)
	shardClient := s.client

	snapshotTTL := "0"
	if opts.SnapshotTTL > 0 {
		snapshotTTL = strconv.FormatInt(int64(opts.SnapshotTTL.Seconds()), 10)
	}

	replies, err := e.readOrderedScript.Exec(ctx, shardClient,
		[]string{
			e.snapshotHashKey(s, ch),
			e.snapshotOrderKey(s, ch),
			e.snapshotExpireKey(s, ch),
			e.metaKey(s, ch),
		},
		[]string{
			strconv.Itoa(opts.Offset),
			strconv.Itoa(opts.Limit),
			strconv.FormatInt(time.Now().Unix(), 10),
			snapshotTTL,
			strconv.FormatInt(int64(e.node.config.HistoryMetaTTL.Seconds()), 10),
		},
	).ToArray()
	if err != nil {
		return nil, EngineStreamPosition{}, err
	}
	if len(replies) < 4 {
		return nil, EngineStreamPosition{}, errors.New("wrong number of replies")
	}

	offset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return nil, EngineStreamPosition{}, err
	}
	epoch, _ := replies[1].ToString()
	keysArr, _ := replies[2].ToArray()
	valuesArr, _ := replies[3].ToArray()

	data := make(map[string][]byte, len(keysArr))
	for i := 0; i < len(keysArr); i++ {
		key, _ := keysArr[i].ToString()
		val, _ := valuesArr[i].AsBytes()
		data[key] = val
	}

	return data, EngineStreamPosition{Offset: offset, Epoch: epoch}, nil
}

func parseAddScriptResult(replies []rueidis.RedisMessage) (EngineStreamPosition, bool, error) {
	if len(replies) < 2 {
		return EngineStreamPosition{}, false, fmt.Errorf("wrong number of replies from script: %d", len(replies))
	}
	offset, err := replies[0].AsUint64()
	if err != nil && !rueidis.IsRedisNil(err) {
		return EngineStreamPosition{}, false, fmt.Errorf("could not parse offset: %w", err)
	}
	epoch, err := replies[1].ToString()
	if err != nil {
		return EngineStreamPosition{}, false, fmt.Errorf("could not parse epoch: %w", err)
	}
	fromCache := false
	if len(replies) >= 3 {
		fromCacheVal, err := replies[2].ToString()
		if err == nil && fromCacheVal == "1" {
			fromCache = true
		}
	}
	return EngineStreamPosition{Offset: offset, Epoch: epoch}, fromCache, nil
}
