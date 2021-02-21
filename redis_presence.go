package centrifuge

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge/internal/timers"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

var _ PresenceManager = (*RedisPresenceManager)(nil)

// DefaultRedisPresenceTTL is a default value for presence TTL in Redis.
const DefaultRedisPresenceTTL = 60 * time.Second

// RedisPresenceManager ...
type RedisPresenceManager struct {
	node              *Node
	sharding          bool
	config            RedisPresenceManagerConfig
	shards            []*RedisShard
	addPresenceScript *redis.Script
	remPresenceScript *redis.Script
	presenceScript    *redis.Script
}

type RedisPresenceManagerConfig struct {
	// Prefix to use before every channel name and key in Redis.
	Prefix string

	// PresenceTTL is an interval how long to consider presence info
	// valid after receiving presence update. This allows to automatically
	// clean up unnecessary presence entries after TTL passed.
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

	for _, shard := range config.Shards {
		if !shard.useCluster {
			// Only need data pipeline in non-cluster scenario.
			go runForever(func() {
				m.runDataPipeline(shard)
			})
		}
	}

	return m, nil
}

func (m *RedisPresenceManager) getShard(channel string) *RedisShard {
	if !m.sharding {
		return m.shards[0]
	}
	return m.shards[consistentIndex(channel, len(m.shards))]
}

func (m *RedisPresenceManager) getDataResponse(s *RedisShard, r dataRequest) *dataResponse {
	if s.useCluster {
		reply, err := m.processClusterDataRequest(s, r)
		return &dataResponse{
			reply: reply,
			err:   err,
		}
	}
	select {
	case s.dataCh <- r:
	default:
		timer := timers.AcquireTimer(s.readTimeout())
		defer timers.ReleaseTimer(timer)
		select {
		case s.dataCh <- r:
		case <-timer.C:
			return &dataResponse{nil, errRedisOpTimeout}
		}
	}
	return r.result()
}

func (m *RedisPresenceManager) runDataPipeline(s *RedisShard) {
	conn := s.pool.Get()
	scripts := []*redis.Script{
		m.addPresenceScript,
		m.remPresenceScript,
		m.presenceScript,
	}
	for _, script := range scripts {
		err := script.Load(conn)
		if err != nil {
			m.node.Log(NewLogEntry(LogLevelError, "error loading Lua script", map[string]interface{}{"error": err.Error()}))
			// Can not proceed if script has not been loaded.
			_ = conn.Close()
			return
		}
	}
	_ = conn.Close()

	var drs []dataRequest

	for dr := range s.dataCh {
		drs = append(drs, dr)
	loop:
		for len(drs) < redisDataBatchLimit {
			select {
			case req := <-s.dataCh:
				drs = append(drs, req)
			default:
				break loop
			}
		}

		conn := s.pool.Get()

		for i := range drs {
			switch drs[i].op {
			case dataOpAddPresence:
				_ = m.addPresenceScript.SendHash(conn, drs[i].args...)
			case dataOpRemovePresence:
				_ = m.remPresenceScript.SendHash(conn, drs[i].args...)
			case dataOpPresence:
				_ = m.presenceScript.SendHash(conn, drs[i].args...)
			}
		}

		err := conn.Flush()
		if err != nil {
			for i := range drs {
				drs[i].done(nil, err)
			}
			m.node.Log(NewLogEntry(LogLevelError, "error flushing data pipeline", map[string]interface{}{"error": err.Error()}))
			_ = conn.Close()
			return
		}
		var noScriptError bool
		for i := range drs {
			reply, err := conn.Receive()
			if err != nil {
				// Check for NOSCRIPT error. In normal circumstances this should never happen.
				// The only possible situation is when Redis scripts were flushed. In this case
				// we will return from this func and load publish script from scratch.
				// Redigo does the same check but for single EVALSHA command: see
				// https://github.com/garyburd/redigo/blob/master/redis/script.go#L64
				if e, ok := err.(redis.Error); ok && strings.HasPrefix(string(e), "NOSCRIPT ") {
					noScriptError = true
				}
			}
			drs[i].done(reply, err)
		}
		if conn.Err() != nil {
			_ = conn.Close()
			return
		}
		if noScriptError {
			// Start this func from the beginning and LOAD missing script.
			_ = conn.Close()
			return
		}
		_ = conn.Close()
		drs = nil
	}
}

func (m *RedisPresenceManager) processClusterDataRequest(s *RedisShard, dr dataRequest) (interface{}, error) {
	conn := s.pool.Get()
	defer func() { _ = conn.Close() }()

	var err error

	var key string
	switch dr.op {
	case dataOpAddPresence, dataOpRemovePresence, dataOpPresence:
		key = fmt.Sprintf("%s", dr.args[0])
	default:
	}
	if key != "" {
		if c, ok := conn.(*redisc.Conn); ok {
			err := c.Bind(key)
			if err != nil {
				return nil, err
			}
		}
	}

	// Handle redirects automatically.
	conn, err = redisc.RetryConn(conn, 3, 50*time.Millisecond)
	if err != nil {
		return nil, err
	}

	var reply interface{}

	switch dr.op {
	case dataOpAddPresence:
		reply, err = m.addPresenceScript.Do(conn, dr.args...)
	case dataOpRemovePresence:
		reply, err = m.remPresenceScript.Do(conn, dr.args...)
	case dataOpPresence:
		reply, err = m.presenceScript.Do(conn, dr.args...)
	}
	return reply, err
}

// AddPresence - see engine interface description.
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
	dr := newDataRequest(dataOpAddPresence, []interface{}{setKey, hashKey, expire, expireAt, uid, infoJSON})
	resp := m.getDataResponse(s, dr)
	return resp.err
}

// RemovePresence - see engine interface description.
func (m *RedisPresenceManager) RemovePresence(ch string, uid string) error {
	return m.removePresence(m.getShard(ch), ch, uid)
}

func (m *RedisPresenceManager) removePresence(s *RedisShard, ch string, uid string) error {
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)
	dr := newDataRequest(dataOpRemovePresence, []interface{}{setKey, hashKey, uid})
	resp := m.getDataResponse(s, dr)
	return resp.err
}

// Presence - see engine interface description.
func (m *RedisPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	return m.presence(m.getShard(ch), ch)
}

// Presence - see engine interface description.
func (m *RedisPresenceManager) presence(s *RedisShard, ch string) (map[string]*ClientInfo, error) {
	hashKey := m.presenceHashKey(s, ch)
	setKey := m.presenceSetKey(s, ch)
	now := int(time.Now().Unix())
	dr := newDataRequest(dataOpPresence, []interface{}{setKey, hashKey, now})
	resp := m.getDataResponse(s, dr)
	if resp.err != nil {
		return nil, resp.err
	}
	return mapStringClientInfo(resp.reply, nil)
}

// PresenceStats - see engine interface description.
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
