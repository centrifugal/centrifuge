package centrifuge

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	_ "embed"

	"github.com/centrifugal/centrifuge/internal/convert"

	"github.com/centrifugal/protocol"
	"github.com/redis/rueidis"
)

var _ PresenceManager = (*RedisPresenceManager)(nil)

// RedisPresenceManager keeps presence in Redis thus allows scaling nodes.
type RedisPresenceManager struct {
	node              *Node
	config            RedisPresenceManagerConfig
	shards            []*RedisShard
	sharding          bool
	addPresenceScript *rueidis.Lua
	remPresenceScript *rueidis.Lua
	presenceScript    *rueidis.Lua
}

// RedisPresenceManagerConfig is a config for RedisPresenceManager.
type RedisPresenceManagerConfig struct {
	// Prefix to use before every channel name and key in Redis. By default,
	// "centrifuge" prefix will be used.
	Prefix string

	// PresenceTTL is an interval how long to consider presence info
	// valid after receiving presence update. This allows to automatically
	// clean up unnecessary presence entries after TTL passed. Zero value
	// means 60 seconds.
	PresenceTTL time.Duration

	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	// Data will be consistently sharded by channel over provided Redis shards.
	Shards []*RedisShard
}

var (
	//go:embed internal/redis_lua/presence_add.lua
	addPresenceScriptSource string

	//go:embed internal/redis_lua/presence_rem.lua
	remPresenceScriptSource string

	//go:embed internal/redis_lua/presence_get.lua
	presenceScriptSource string
)

// NewRedisPresenceManager creates new RedisPresenceManager.
func NewRedisPresenceManager(n *Node, config RedisPresenceManagerConfig) (*RedisPresenceManager, error) {
	if len(config.Shards) == 0 {
		return nil, errors.New("presence: no Redis shards provided in configuration")
	}

	if len(config.Shards) > 1 {
		n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("presence: Redis sharding enabled: %d shards", len(config.Shards))))
	}

	if config.Prefix == "" {
		config.Prefix = "centrifuge"
	}

	if config.PresenceTTL == 0 {
		config.PresenceTTL = 60 * time.Second
	}

	m := &RedisPresenceManager{
		node:     n,
		shards:   config.Shards,
		config:   config,
		sharding: len(config.Shards) > 1,

		addPresenceScript: rueidis.NewLuaScript(addPresenceScriptSource),
		remPresenceScript: rueidis.NewLuaScript(remPresenceScriptSource),
		presenceScript:    rueidis.NewLuaScript(presenceScriptSource),
	}
	return m, nil
}

func (m *RedisPresenceManager) Close(_ context.Context) error {
	return nil
}

func (m *RedisPresenceManager) getShard(channel string) *RedisShard {
	if !m.sharding {
		return m.shards[0]
	}
	return m.shards[consistentIndex(channel, len(m.shards))]
}

// AddPresence - see PresenceManager interface description.
func (m *RedisPresenceManager) AddPresence(ch string, uid string, info *ClientInfo) error {
	return m.addPresence(m.getShard(ch), ch, uid, info)
}

func (m *RedisPresenceManager) addPresenceScriptKeysArgs(s *RedisShard, ch string, uid string, info *ClientInfo) ([]string, []string, error) {
	expire := int(m.config.PresenceTTL.Seconds())
	infoBytes, err := infoToProto(info).MarshalVT()
	if err != nil {
		return nil, nil, err
	}
	expireAt := time.Now().Unix() + int64(expire)
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)

	keys := []string{string(setKey), string(hashKey)}
	args := []string{strconv.Itoa(expire), strconv.FormatInt(expireAt, 10), uid, convert.BytesToString(infoBytes)}
	return keys, args, nil
}

func (m *RedisPresenceManager) addPresence(s *RedisShard, ch string, uid string, info *ClientInfo) error {
	keys, args, err := m.addPresenceScriptKeysArgs(s, ch, uid, info)
	if err != nil {
		return err
	}
	resp := m.addPresenceScript.Exec(context.Background(), s.client, keys, args)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil
	}
	return resp.Error()
}

// RemovePresence - see PresenceManager interface description.
func (m *RedisPresenceManager) RemovePresence(ch string, uid string) error {
	return m.removePresence(m.getShard(ch), ch, uid)
}

func (m *RedisPresenceManager) removePresenceScriptKeysArgs(s *RedisShard, ch string, uid string) ([]string, []string, error) {
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)
	keys := []string{string(setKey), string(hashKey)}
	args := []string{uid}
	return keys, args, nil
}

func (m *RedisPresenceManager) removePresence(s *RedisShard, ch string, uid string) error {
	keys, args, err := m.removePresenceScriptKeysArgs(s, ch, uid)
	if err != nil {
		return err
	}
	resp := m.remPresenceScript.Exec(context.Background(), s.client, keys, args)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil
	}
	return resp.Error()
}

// Presence - see PresenceManager interface description.
func (m *RedisPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	return m.presence(m.getShard(ch), ch)
}

func (m *RedisPresenceManager) presenceScriptKeysArgs(s *RedisShard, ch string) ([]string, []string, error) {
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)
	now := int(time.Now().Unix())
	keys := []string{string(setKey), string(hashKey)}
	args := []string{strconv.Itoa(now)}
	return keys, args, nil
}

func (m *RedisPresenceManager) presence(s *RedisShard, ch string) (map[string]*ClientInfo, error) {
	keys, args, err := m.presenceScriptKeysArgs(s, ch)
	if err != nil {
		return nil, err
	}
	resp, err := m.presenceScript.Exec(context.Background(), s.client, keys, args).ToArray()
	if err != nil {
		return nil, err
	}
	return mapStringClientInfo(resp)
}

func mapStringClientInfo(result []rueidis.RedisMessage) (map[string]*ClientInfo, error) {
	if len(result)%2 != 0 {
		return nil, errors.New("mapStringClientInfo expects even number of values result")
	}
	m := make(map[string]*ClientInfo, len(result)/2)
	for i := 0; i < len(result); i += 2 {
		key, err := result[i].ToString()
		if err != nil {
			return nil, errors.New("key is not string")
		}
		value, err := result[i+1].ToString()
		if err != nil {
			return nil, errors.New("value is not string")
		}
		var f protocol.ClientInfo
		err = f.UnmarshalVT(convert.StringToBytes(value))
		if err != nil {
			return nil, errors.New("can not unmarshal value to ClientInfo")
		}
		m[key] = infoFromProto(&f)
	}
	return m, nil
}

// PresenceStats - see PresenceManager interface description.
func (m *RedisPresenceManager) PresenceStats(ch string) (PresenceStats, error) {
	presence, err := m.Presence(ch)
	if err != nil {
		return PresenceStats{}, err
	}

	numClients := len(presence)
	numUsers := 0
	uniqueUsers := map[string]struct{}{}

	for _, info := range presence {
		userID := info.UserID
		if _, ok := uniqueUsers[userID]; !ok {
			uniqueUsers[userID] = struct{}{}
			numUsers++
		}
	}

	return PresenceStats{
		NumClients: numClients,
		NumUsers:   numUsers,
	}, nil
}

func (m *RedisPresenceManager) presenceHashKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		ch = "{" + ch + "}"
	}
	return channelID(m.config.Prefix + ".presence.data." + ch)
}

func (m *RedisPresenceManager) presenceSetKey(s *RedisShard, ch string) channelID {
	if s.useCluster {
		ch = "{" + ch + "}"
	}
	return channelID(m.config.Prefix + ".presence.expire." + ch)
}
