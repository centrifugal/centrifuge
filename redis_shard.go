package centrifuge

import (
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

type (
	// channelID is unique channel identifier in Redis.
	channelID string
)

var errRedisOpTimeout = errors.New("operation timed out")

const (
	defaultPrefix         = "centrifuge"
	defaultReadTimeout    = time.Second
	defaultWriteTimeout   = time.Second
	defaultConnectTimeout = time.Second
	defaultPoolSize       = 128
)

type redisConnPool interface {
	Get() redis.Conn
}

type RedisShard struct {
	config     RedisShardConfig
	pool       redisConnPool
	subCh      chan subRequest
	pubCh      chan pubRequest
	dataCh     chan dataRequest
	useCluster bool
}

// NewRedisShard initializes new Redis shard.
func NewRedisShard(n *Node, conf RedisShardConfig) (*RedisShard, error) {
	shard := &RedisShard{
		config: conf,
	}
	pool, err := newPool(shard, n, conf)
	if err != nil {
		return nil, err
	}
	shard.pool = pool
	shard.subCh = make(chan subRequest)
	shard.pubCh = make(chan pubRequest)
	shard.dataCh = make(chan dataRequest)
	return shard, nil
}

// RedisShardConfig contains Redis connection options.
type RedisShardConfig struct {
	// Host is Redis server host.
	Host string
	// Port is Redis server port.
	Port int
	// SentinelAddrs is a slice of Sentinel addresses.
	SentinelAddrs []string
	// SentinelMasterName is a name of Redis instance master Sentinel monitors.
	SentinelMasterName string
	// SentinelPassword is a password for Sentinel. Works with Sentinel >= 5.0.1.
	SentinelPassword string
	// ClusterAddrs is a slice of seed cluster addrs for this shard.
	ClusterAddrs []string
	// DB is Redis database number. If not set then database 0 used.
	DB int
	// Password is password to use when connecting to Redis database. If empty then password not used.
	Password string
	// Whether to use TLS connection or not.
	UseTLS bool
	// Whether to skip hostname verification as part of TLS handshake.
	TLSSkipVerify bool
	// Connection TLS configuration.
	TLSConfig *tls.Config
	// IdleTimeout is timeout after which idle connections to Redis will be closed.
	IdleTimeout time.Duration
	// ReadTimeout is a timeout on read operations. Note that at moment it should be greater
	// than node ping publish interval in order to prevent timing out Pub/Sub connection's
	// Receive call.
	ReadTimeout time.Duration
	// WriteTimeout is a timeout on write operations.
	WriteTimeout time.Duration
	// ConnectTimeout is a timeout on connect operation.
	ConnectTimeout time.Duration
}

type pubRequest struct {
	channel channelID
	message []byte
	err     chan error
}

func (pr *pubRequest) done(err error) {
	pr.err <- err
}

type dataOp int

const (
	dataOpAddPresence dataOp = iota
	dataOpRemovePresence
	dataOpPresence
	dataOpHistory
	dataOpAddHistory
	dataOpHistoryRemove
)

type dataResponse struct {
	reply interface{}
	err   error
}

type dataRequest struct {
	op   dataOp
	args []interface{}
	resp chan *dataResponse
}

func newDataRequest(op dataOp, args []interface{}) dataRequest {
	return dataRequest{op: op, args: args, resp: make(chan *dataResponse, 1)}
}

func (dr *dataRequest) done(reply interface{}, err error) {
	if dr.resp == nil {
		return
	}
	dr.resp <- &dataResponse{reply: reply, err: err}
}

func (dr *dataRequest) result() *dataResponse {
	if dr.resp == nil {
		// No waiting, as caller didn't care about response.
		return &dataResponse{}
	}
	return <-dr.resp
}

// subRequest is an internal request to subscribe or unsubscribe from one or more channels.
type subRequest struct {
	channels  []channelID
	subscribe bool
	err       chan error
}

// newSubRequest creates a new request to subscribe or unsubscribe form a channel.
func newSubRequest(chIDs []channelID, subscribe bool) subRequest {
	return subRequest{
		channels:  chIDs,
		subscribe: subscribe,
		err:       make(chan error, 1),
	}
}

// done should only be called once for subRequest.
func (sr *subRequest) done(err error) {
	sr.err <- err
}

func (sr *subRequest) result() error {
	return <-sr.err
}

func makePoolFactory(s *RedisShard, n *Node, conf RedisShardConfig) func(addr string, options ...redis.DialOption) (*redis.Pool, error) {
	password := conf.Password
	db := conf.DB

	useSentinel := conf.SentinelMasterName != "" && len(conf.SentinelAddrs) > 0

	var lastMu sync.Mutex
	var lastMaster string

	poolSize := defaultPoolSize
	maxIdle := poolSize

	var sntnl *sentinel.Sentinel
	if useSentinel {
		sntnl = &sentinel.Sentinel{
			Addrs:      conf.SentinelAddrs,
			MasterName: conf.SentinelMasterName,
			Dial: func(addr string) (redis.Conn, error) {
				timeout := 300 * time.Millisecond
				opts := []redis.DialOption{
					redis.DialConnectTimeout(timeout),
					redis.DialReadTimeout(timeout),
					redis.DialWriteTimeout(timeout),
				}
				c, err := redis.Dial("tcp", addr, opts...)
				if err != nil {
					n.Log(NewLogEntry(LogLevelError, "error dialing to Sentinel", map[string]interface{}{"error": err.Error()}))
					return nil, err
				}
				if conf.SentinelPassword != "" {
					if _, err := c.Do("AUTH", conf.SentinelPassword); err != nil {
						_ = c.Close()
						n.Log(NewLogEntry(LogLevelError, "error auth in Redis Sentinel", map[string]interface{}{"error": err.Error()}))
						return nil, err
					}
				}
				return c, nil
			},
		}

		// Periodically discover new Sentinels.
		go func() {
			if err := sntnl.Discover(); err != nil {
				n.Log(NewLogEntry(LogLevelError, "error discover Sentinel", map[string]interface{}{"error": err.Error()}))
			}
			for {
				<-time.After(30 * time.Second)
				if err := sntnl.Discover(); err != nil {
					n.Log(NewLogEntry(LogLevelError, "error discover Sentinel", map[string]interface{}{"error": err.Error()}))
				}
			}
		}()
	}

	return func(serverAddr string, dialOpts ...redis.DialOption) (*redis.Pool, error) {
		pool := &redis.Pool{
			MaxIdle:     maxIdle,
			MaxActive:   poolSize,
			Wait:        true,
			IdleTimeout: conf.IdleTimeout,
			Dial: func() (redis.Conn, error) {
				var c redis.Conn
				if useSentinel {
					masterAddr, err := sntnl.MasterAddr()
					if err != nil {
						return nil, err
					}
					lastMu.Lock()
					if masterAddr != lastMaster {
						n.Log(NewLogEntry(LogLevelInfo, "Redis master discovered", map[string]interface{}{"addr": masterAddr}))
						lastMaster = masterAddr
					}
					lastMu.Unlock()
					c, err = redis.Dial("tcp", masterAddr, dialOpts...)
					if err != nil {
						n.Log(NewLogEntry(LogLevelError, "error dialing to Redis", map[string]interface{}{"error": err.Error(), "addr": masterAddr}))
						return nil, err
					}
				} else {
					var err error
					c, err = redis.Dial("tcp", serverAddr, dialOpts...)
					if err != nil {
						n.Log(NewLogEntry(LogLevelError, "error dialing to Redis", map[string]interface{}{"error": err.Error(), "addr": serverAddr}))
						return nil, err
					}
				}

				if password != "" {
					if _, err := c.Do("AUTH", password); err != nil {
						_ = c.Close()
						n.Log(NewLogEntry(LogLevelError, "error auth in Redis", map[string]interface{}{"error": err.Error()}))
						return nil, err
					}
				}

				if db != 0 {
					if _, err := c.Do("SELECT", db); err != nil {
						_ = c.Close()
						n.Log(NewLogEntry(LogLevelError, "error selecting Redis db", map[string]interface{}{"error": err.Error()}))
						return nil, err
					}
				}
				return c, nil
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if useSentinel {
					if !sentinel.TestRole(c, "master") {
						return errors.New("failed master role check")
					}
					return nil
				}
				if s.useCluster {
					// No need in this optimization outside cluster
					// use case due to utilization of pipelining.
					if time.Since(t) < time.Second {
						return nil
					}
				}
				_, err := c.Do("PING")
				return err
			},
		}
		return pool, nil
	}
}

func getDialOpts(conf RedisShardConfig) []redis.DialOption {
	var readTimeout = defaultReadTimeout
	if conf.ReadTimeout != 0 {
		readTimeout = conf.ReadTimeout
	}
	var writeTimeout = defaultWriteTimeout
	if conf.WriteTimeout != 0 {
		writeTimeout = conf.WriteTimeout
	}
	var connectTimeout = defaultConnectTimeout
	if conf.ConnectTimeout != 0 {
		connectTimeout = conf.ConnectTimeout
	}

	dialOpts := []redis.DialOption{
		redis.DialConnectTimeout(connectTimeout),
		redis.DialReadTimeout(readTimeout),
		redis.DialWriteTimeout(writeTimeout),
	}
	if conf.UseTLS {
		dialOpts = append(dialOpts, redis.DialUseTLS(true))
		if conf.TLSConfig != nil {
			dialOpts = append(dialOpts, redis.DialTLSConfig(conf.TLSConfig))
		}
		if conf.TLSSkipVerify {
			dialOpts = append(dialOpts, redis.DialTLSSkipVerify(true))
		}
	}
	return dialOpts
}

func newPool(s *RedisShard, n *Node, conf RedisShardConfig) (redisConnPool, error) {
	host := conf.Host
	port := conf.Port
	password := conf.Password
	db := conf.DB

	useSentinel := conf.SentinelMasterName != "" && len(conf.SentinelAddrs) > 0
	usingPassword := password != ""

	poolFactory := makePoolFactory(s, n, conf)

	if !s.useCluster {
		serverAddr := net.JoinHostPort(host, strconv.Itoa(port))
		if !useSentinel {
			n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: %s/%d, using password: %v", serverAddr, db, usingPassword)))
		} else {
			n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: Sentinel for name: %s, db: %d, using password: %v", conf.SentinelMasterName, db, usingPassword)))
		}
		pool, _ := poolFactory(serverAddr, getDialOpts(conf)...)
		return pool, nil
	}
	// OK, we should work with cluster.
	n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: cluster addrs: %+v, using password: %v", conf.ClusterAddrs, usingPassword)))
	cluster := &redisc.Cluster{
		DialOptions:  getDialOpts(conf),
		StartupNodes: conf.ClusterAddrs,
		CreatePool:   poolFactory,
	}
	// Initialize cluster mapping.
	if err := cluster.Refresh(); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (s *RedisShard) readTimeout() time.Duration {
	var readTimeout = defaultReadTimeout
	if s.config.ReadTimeout != 0 {
		readTimeout = s.config.ReadTimeout
	}
	return readTimeout
}

// runForever keeps another function running indefinitely.
// The reason this loop is not inside the function itself is
// so that defer can be used to cleanup nicely.
func runForever(fn func()) {
	for {
		fn()
		// Sleep for a while to prevent busy loop when reconnecting to Redis.
		time.Sleep(300 * time.Millisecond)
	}
}

// consistentIndex is an adapted function from https://github.com/dgryski/go-jump
// package by Damian Gryski. It consistently chooses a hash bucket number in the
// range [0, numBuckets) for the given string. numBuckets must be >= 1.
func consistentIndex(s string, numBuckets int) int {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(s))
	key := hash.Sum64()

	var (
		b int64 = -1
		j int64
	)

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int(b)
}
