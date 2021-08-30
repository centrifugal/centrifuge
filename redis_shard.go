package centrifuge

import (
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/timers"

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
	DefaultRedisReadTimeout    = time.Second
	DefaultRedisWriteTimeout   = time.Second
	DefaultRedisConnectTimeout = time.Second
)

const (
	defaultRedisPoolSize = 128
	// redisDataBatchLimit is a max amount of data requests in one batch.
	redisDataBatchLimit = 64
)

type redisConnPool interface {
	Get() redis.Conn
}

type RedisShard struct {
	config           RedisShardConfig
	pool             redisConnPool
	subCh            chan subRequest
	pubCh            chan pubRequest
	dataCh           chan *dataRequest
	useCluster       bool
	scriptsMu        sync.RWMutex
	scripts          []*redis.Script
	scriptsCh        chan struct{}
	reloadPipelineCh chan struct{}
}

func confFromAddress(address string, conf RedisShardConfig) (RedisShardConfig, error) {
	if !strings.HasPrefix(address, "tcp://") && !strings.HasPrefix(address, "redis://") && !strings.HasPrefix(address, "unix://") {
		if host, port, err := net.SplitHostPort(address); err == nil && host != "" && port != "" {
			conf.network = "tcp"
			conf.address = address
			return conf, nil
		}
		return conf, errors.New("malformed connection address")
	}
	u, err := url.Parse(address)
	if err != nil {
		return conf, errors.New("malformed connection address")
	}
	switch u.Scheme {
	case "tcp", "redis":
		conf.network = "tcp"
		conf.address = u.Host
		if u.Path != "" {
			db, err := strconv.Atoi(strings.TrimPrefix(u.Path, "/"))
			if err != nil {
				return conf, fmt.Errorf("can't parse Redis DB number from connection address")
			}
			conf.DB = db
		}
	case "unix":
		conf.network = "unix"
		conf.address = u.Path
	default:
		return conf, errors.New("connection address should have tcp://, redis:// or unix:// scheme")
	}
	if u.User != nil {
		if pass, ok := u.User.Password(); ok {
			conf.Password = pass
		}
	}
	return conf, nil
}

// NewRedisShard initializes new Redis shard.
func NewRedisShard(n *Node, conf RedisShardConfig) (*RedisShard, error) {
	var err error
	if conf.Address != "" {
		conf, err = confFromAddress(conf.Address, conf)
		if err != nil {
			return nil, err
		}
	}
	shard := &RedisShard{
		config:           conf,
		scriptsCh:        make(chan struct{}, 1),
		useCluster:       len(conf.ClusterAddresses) > 0,
		reloadPipelineCh: make(chan struct{}),
	}
	pool, err := newPool(shard, n, conf)
	if err != nil {
		return nil, err
	}
	shard.scripts = []*redis.Script{}
	shard.pool = pool
	shard.subCh = make(chan subRequest)
	shard.pubCh = make(chan pubRequest)
	shard.dataCh = make(chan *dataRequest)
	if !shard.useCluster {
		// Only need data pipeline in non-cluster scenario.
		go func() {
			for {
				err := shard.runDataPipeline()
				if err != nil {
					n.Log(NewLogEntry(LogLevelError, "data pipeline error", map[string]interface{}{"error": err.Error()}))
					time.Sleep(300 * time.Millisecond)
				}
			}
		}()
	}
	return shard, nil
}

// RedisShardConfig contains Redis connection options.
type RedisShardConfig struct {
	// Address is a Redis server connection address.
	// This can be:
	// - host:port
	// - tcp://[[:password]@]host:port[/db][?option1=value1&optionN=valueN]
	// - redis://[[:password]@]host:port[/db][?option1=value1&optionN=valueN]
	// - unix://[[:password]@]path[?option1=value1&optionN=valueN]
	Address string

	// ClusterAddresses is a slice of seed cluster addrs for this shard.
	// Each address should be in form of host:port. If ClusterAddresses set then
	// RedisShardConfig.Address not used.
	ClusterAddresses []string

	// SentinelAddresses is a slice of Sentinel addresses. Each address should
	// be in form of host:port. If set then Redis address will be automatically
	// discovered from Sentinel.
	SentinelAddresses []string
	// SentinelMasterName is a name of Redis instance master Sentinel monitors.
	SentinelMasterName string
	// SentinelPassword is a password for Sentinel. Works with Sentinel >= 5.0.1.
	SentinelPassword string

	// DB is Redis database number. If not set then database 0 used.
	DB int
	// Password is password to use when connecting to Redis database.
	// If zero then password not used.
	Password string
	// Whether to use TLS connection or not.
	UseTLS bool
	// Whether to skip hostname verification as part of TLS handshake.
	TLSSkipVerify bool
	// Connection TLS configuration.
	TLSConfig *tls.Config
	// IdleTimeout is timeout after which idle connections to Redis will be closed.
	// If the value is zero, then idle connections are not closed.
	IdleTimeout time.Duration
	// ReadTimeout is a timeout on read operations. Note that at moment it should be greater
	// than node ping publish interval in order to prevent timing out Pub/Sub connection's
	// Receive call.
	// By default DefaultRedisReadTimeout used.
	ReadTimeout time.Duration
	// WriteTimeout is a timeout on write operations.
	// By default DefaultRedisWriteTimeout used.
	WriteTimeout time.Duration
	// ConnectTimeout is a timeout on connect operation.
	// By default DefaultRedisConnectTimeout used.
	ConnectTimeout time.Duration

	network string
	address string
}

type pubRequest struct {
	channel channelID
	message []byte
	err     chan error
}

func (pr *pubRequest) done(err error) {
	pr.err <- err
}

type dataResponse struct {
	reply interface{}
	err   error
}

type dataRequest struct {
	command    string
	script     *redis.Script
	args       []interface{}
	resp       chan *dataResponse
	clusterKey string
}

func (s *RedisShard) newDataRequest(command string, script *redis.Script, clusterKey channelID, args []interface{}) *dataRequest {
	dr := &dataRequest{command: command, script: script, args: args, resp: make(chan *dataResponse, 1)}
	if s.useCluster {
		dr.setClusterKey(string(clusterKey))
	}
	return dr
}

func (s *RedisShard) string() string {
	return s.config.address
}

func (dr *dataRequest) setClusterKey(key string) {
	dr.clusterKey = key
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

func (s *RedisShard) registerScripts(scripts ...*redis.Script) {
	s.scriptsMu.Lock()
	defer s.scriptsMu.Unlock()
	s.scripts = append(s.scripts, scripts...)
	select {
	case s.scriptsCh <- struct{}{}:
		// Trigger script loading.
	default:
	}
}

// Best effort to process a signal for reloading data pipeline if we know
// that connection should be re-established. If the signal can't be processed
// then pipeline will be automatically reloaded upon first error from Redis.
func (s *RedisShard) reloadPipeline() {
	select {
	case s.reloadPipelineCh <- struct{}{}:
	default:
	}
}

func (s *RedisShard) getDataResponse(r *dataRequest) *dataResponse {
	if s.useCluster {
		reply, err := s.processClusterDataRequest(r)
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

func (s *RedisShard) processClusterDataRequest(dr *dataRequest) (interface{}, error) {
	conn := s.pool.Get()
	defer func() { _ = conn.Close() }()

	var err error

	if dr.clusterKey != "" {
		if c, ok := conn.(*redisc.Conn); ok {
			err := c.Bind(dr.clusterKey)
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

	if dr.script != nil {
		return dr.script.Do(conn, dr.args...)
	}
	return conn.Do(dr.command, dr.args...)
}

func (s *RedisShard) runDataPipeline() error {
	conn := s.pool.Get()
	s.scriptsMu.RLock()
	scripts := make([]*redis.Script, len(s.scripts))
	copy(scripts, s.scripts)
	s.scriptsMu.RUnlock()
	for _, script := range scripts {
		err := script.Load(conn)
		if err != nil {
			// Can not proceed if script has not been loaded.
			_ = conn.Close()
			return fmt.Errorf("error loading Lua script: %w", err)
		}
	}
	_ = conn.Close()

	var drs []*dataRequest

	for {
		select {
		case <-s.reloadPipelineCh:
			return nil
		case <-s.scriptsCh:
			s.scriptsMu.RLock()
			if len(s.scripts) == len(scripts) {
				s.scriptsMu.RUnlock()
				continue
			}
			s.scriptsMu.RUnlock()
			return nil
		case dr := <-s.dataCh:
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
				if drs[i].script != nil {
					_ = drs[i].script.SendHash(conn, drs[i].args...)
				} else {
					_ = conn.Send(drs[i].command, drs[i].args...)
				}
			}

			err := conn.Flush()
			if err != nil {
				for i := range drs {
					drs[i].done(nil, err)
				}
				_ = conn.Close()
				return fmt.Errorf("error flushing data pipeline: %w", err)
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
				return nil
			}
			if noScriptError {
				// Start this func from the beginning and LOAD missing script.
				_ = conn.Close()
				return nil
			}
			_ = conn.Close()
			drs = nil
		}
	}
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

	useSentinel := conf.SentinelMasterName != "" && len(conf.SentinelAddresses) > 0

	var lastMu sync.Mutex
	var lastMaster string

	poolSize := defaultRedisPoolSize
	maxIdle := poolSize

	var sntnl *sentinel.Sentinel
	if useSentinel {
		sntnl = &sentinel.Sentinel{
			Addrs:      conf.SentinelAddresses,
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
					network := s.config.network
					if network == "" {
						// In case of Redis Cluster network can be empty since we don't set it.
						network = "tcp"
					}
					c, err = redis.Dial(network, serverAddr, dialOpts...)
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
	var readTimeout = DefaultRedisReadTimeout
	if conf.ReadTimeout != 0 {
		readTimeout = conf.ReadTimeout
	}
	var writeTimeout = DefaultRedisWriteTimeout
	if conf.WriteTimeout != 0 {
		writeTimeout = conf.WriteTimeout
	}
	var connectTimeout = DefaultRedisConnectTimeout
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
	password := conf.Password
	db := conf.DB

	useSentinel := conf.SentinelMasterName != "" && len(conf.SentinelAddresses) > 0
	usingPassword := password != ""

	poolFactory := makePoolFactory(s, n, conf)

	if !s.useCluster {
		serverAddr := conf.address
		if !useSentinel {
			n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: %s/%d, using password: %v", serverAddr, db, usingPassword)))
		} else {
			n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: Sentinel for name: %s, db: %d, using password: %v", conf.SentinelMasterName, db, usingPassword)))
		}
		pool, _ := poolFactory(serverAddr, getDialOpts(conf)...)
		return pool, nil
	}
	// OK, we should work with cluster.
	n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: cluster addrs: %+v, using password: %v", conf.ClusterAddresses, usingPassword)))
	cluster := &redisc.Cluster{
		DialOptions:  getDialOpts(conf),
		StartupNodes: conf.ClusterAddresses,
		CreatePool:   poolFactory,
	}
	// Initialize cluster mapping.
	if err := cluster.Refresh(); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (s *RedisShard) readTimeout() time.Duration {
	var readTimeout = DefaultRedisReadTimeout
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
