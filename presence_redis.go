package centrifuge

import (
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

var _ PresenceManager = (*RedisPresenceManager)(nil)

// DefaultRedisPresenceTTL is a default value for presence TTL in Redis.
const DefaultRedisPresenceTTL = 60 * time.Second

// RedisPresenceManager keeps presence in Redis thus allows scaling nodes.
type RedisPresenceManager struct {
	node              *Node
	sharding          bool
	config            RedisPresenceManagerConfig
	shards            []*RedisShard
	addPresenceScript *redis.Script
	remPresenceScript *redis.Script
	presenceScript    *redis.Script
}

// RedisPresenceManagerConfig is a config for RedisPresenceManager.
type RedisPresenceManagerConfig struct {
	// Prefix to use before every channel name and key in Redis.
	Prefix string

	// PresenceTTL is an interval how long to consider presence info
	// valid after receiving presence update. This allows to automatically
	// clean up unnecessary presence entries after TTL passed. Zero value
	// means that DefaultRedisPresenceTTL will be used.
	PresenceTTL time.Duration

	// Shards is a list of Redis shards to use.
	Shards []*RedisShard
}

const (
	// Add/update client presence information.
	// KEYS[1] - presence set key
	// KEYS[2] - presence hash key
	// ARGV[1] - key expire seconds
	// ARGV[2] - expire at for set member
	// ARGV[3] - client ID
	// ARGV[4] - info payload
	addPresenceSource = `
redis.call("zadd", KEYS[1], ARGV[2], ARGV[3])
redis.call("hset", KEYS[2], ARGV[3], ARGV[4])
redis.call("expire", KEYS[1], ARGV[1])
redis.call("expire", KEYS[2], ARGV[1])
	`

	// Remove client presence.
	// KEYS[1] - presence set key
	// KEYS[2] - presence hash key
	// ARGV[1] - client ID
	remPresenceSource = `
redis.call("hdel", KEYS[2], ARGV[1])
redis.call("zrem", KEYS[1], ARGV[1])
	`

	// Get presence information.
	// KEYS[1] - presence set key
	// KEYS[2] - presence hash key
	// ARGV[1] - current timestamp in seconds
	presenceSource = `
local expired = redis.call("zrangebyscore", KEYS[1], "0", ARGV[1])
if #expired > 0 then
  for num = 1, #expired do
    redis.call("hdel", KEYS[2], expired[num])
  end
  redis.call("zremrangebyscore", KEYS[1], "0", ARGV[1])
end
return redis.call("hgetall", KEYS[2])
	`
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
		config.Prefix = defaultPrefix
	}

	m := &RedisPresenceManager{
		node:              n,
		shards:            config.Shards,
		config:            config,
		sharding:          len(config.Shards) > 1,
		addPresenceScript: redis.NewScript(2, addPresenceSource),
		remPresenceScript: redis.NewScript(2, remPresenceSource),
		presenceScript:    redis.NewScript(2, presenceSource),
	}

	for i := range config.Shards {
		config.Shards[i].registerScripts(
			m.addPresenceScript,
			m.remPresenceScript,
			m.presenceScript,
		)
	}

	return m, nil
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

func (m *RedisPresenceManager) addPresence(s *RedisShard, ch string, uid string, info *ClientInfo) error {
	presenceTTL := m.config.PresenceTTL
	if presenceTTL == 0 {
		presenceTTL = DefaultRedisPresenceTTL
	}
	expire := int(presenceTTL.Seconds())
	infoJSON, err := infoToProto(info).Marshal()
	if err != nil {
		return err
	}
	expireAt := time.Now().Unix() + int64(expire)
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)
	dr := s.newDataRequest("", m.addPresenceScript, setKey, []interface{}{setKey, hashKey, expire, expireAt, uid, infoJSON})
	resp := s.getDataResponse(dr)
	return resp.err
}

// RemovePresence - see PresenceManager interface description.
func (m *RedisPresenceManager) RemovePresence(ch string, uid string) error {
	return m.removePresence(m.getShard(ch), ch, uid)
}

func (m *RedisPresenceManager) removePresence(s *RedisShard, ch string, uid string) error {
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)
	dr := s.newDataRequest("", m.remPresenceScript, setKey, []interface{}{setKey, hashKey, uid})
	resp := s.getDataResponse(dr)
	return resp.err
}

// Presence - see PresenceManager interface description.
func (m *RedisPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	return m.presence(m.getShard(ch), ch)
}

// Presence - see PresenceManager interface description.
func (m *RedisPresenceManager) presence(s *RedisShard, ch string) (map[string]*ClientInfo, error) {
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)
	now := int(time.Now().Unix())
	dr := s.newDataRequest("", m.presenceScript, setKey, []interface{}{setKey, hashKey, now})
	resp := s.getDataResponse(dr)
	if resp.err != nil {
		return nil, resp.err
	}
	return mapStringClientInfo(resp.reply, nil)
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
