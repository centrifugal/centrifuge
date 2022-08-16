package centrifuge

import (
	"context"
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
	"github.com/centrifugal/centrifuge/internal/util"

	"github.com/go-redis/redis/v9"
)

type (
	// channelID is unique channel identifier in Redis.
	channelID string
)

var errRedisOpTimeout = errors.New("redis: operation timed out")
var errRedisClosed = errors.New("redis: closed")

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

type redisLogger struct{}

func (redisLogger) Printf(ctx context.Context, format string, v ...interface{}) {}

func init() {
	redis.SetLogger(redisLogger{})
}

type RedisShard struct {
	config           RedisShardConfig
	client           redis.UniversalClient
	subCh            chan subRequest
	pubCh            chan pubRequest
	dataCh           chan *dataRequest
	useCluster       bool
	scriptsMu        sync.RWMutex
	scripts          []*redis.Script
	scriptsCh        chan struct{}
	reloadPipelineCh chan struct{}
	closeOnce        sync.Once
	closeCh          chan struct{}
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
		if u.User.Username() != "" {
			conf.User = u.User.Username()
		}
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
		closeCh:          make(chan struct{}),
	}
	client, err := newRedisClient(shard, n, conf)
	if err != nil {
		return nil, err
	}
	shard.scripts = []*redis.Script{}
	shard.client = client
	shard.subCh = make(chan subRequest)
	shard.pubCh = make(chan pubRequest)
	shard.dataCh = make(chan *dataRequest)
	go func() {
		for {
			select {
			case <-shard.closeCh:
				return
			default:
			}
			err := shard.runDataPipeline()
			if err != nil {
				n.Log(NewLogEntry(LogLevelError, "data pipeline error", map[string]interface{}{"error": err.Error()}))
				select {
				case <-shard.closeCh:
					return
				case <-time.After(300 * time.Millisecond):
				}
			}
		}
	}()
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
	// SentinelUser is a user for Sentinel ACL-based auth.
	SentinelUser string
	// SentinelPassword is a password for Sentinel. Works with Sentinel >= 5.0.1.
	SentinelPassword string

	// DB is Redis database number. If not set then database 0 used.
	DB int
	// User for Redis ACL-based auth.
	User string
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
	// By default, DefaultRedisReadTimeout used.
	ReadTimeout time.Duration
	// WriteTimeout is a timeout on write operations.
	// By default, DefaultRedisWriteTimeout used.
	WriteTimeout time.Duration
	// ConnectTimeout is a timeout on connect operation.
	// By default, DefaultRedisConnectTimeout used.
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
	script *redis.Script
	keys   []string
	args   []interface{}
	resp   chan *dataResponse
}

func (s *RedisShard) Close() {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		_ = s.client.Close()
	})
}

func (s *RedisShard) newDataRequest(script *redis.Script, keys []string, args []interface{}) *dataRequest {
	dr := &dataRequest{
		script: script,
		keys:   keys,
		args:   args,
		resp:   make(chan *dataResponse, 1),
	}
	return dr
}

func (s *RedisShard) string() string {
	return s.config.address
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

func (s *RedisShard) getDataResponse(r *dataRequest, closeCh chan struct{}) *dataResponse {
	select {
	case s.dataCh <- r:
	default:
		timer := timers.AcquireTimer(s.readTimeout())
		defer timers.ReleaseTimer(timer)
		select {
		case s.dataCh <- r:
		case <-closeCh:
			return &dataResponse{nil, errRedisClosed}
		case <-timer.C:
			return &dataResponse{nil, errRedisOpTimeout}
		}
	}
	return r.result()
}


func (s *RedisShard) runDataPipeline() error {
	s.scriptsMu.RLock()
	scripts := make([]*redis.Script, len(s.scripts))
	copy(scripts, s.scripts)
	s.scriptsMu.RUnlock()
	for _, script := range scripts {
		cmd := script.Load(context.Background(), s.client)
		if cmd.Err() != nil {
			// Can not proceed if script has not been loaded.
			return fmt.Errorf("error loading Lua script: %w", cmd.Err())
		}
	}

	var drs []*dataRequest
	pipe := s.client.Pipeline()

	for {
		select {
		case <-s.reloadPipelineCh:
			return nil
		case <-s.closeCh:
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
		}
		for _, dr := range drs {
			if dr.script != nil {
				_ = pipe.EvalSha(context.Background(), dr.script.Hash(), dr.keys, dr.args...)
			} else {
				_ = pipe.Do(context.Background(), dr.args...)
			}
		}
		cmds, err := pipe.Exec(context.Background())
		if err == redis.Nil {
			err = nil
		}
		if err != nil {
			for i := range drs {
				drs[i].done(nil, err)
			}
			return fmt.Errorf("error executing data pipeline: %w", err)
		}

		var noScriptError bool
		for i, cmd := range cmds {
			reply, err := cmd.(*redis.Cmd).Result()
			if err == redis.Nil {
				err = nil
			}
			if err != nil {
				// Check for NOSCRIPT error. In normal circumstances this should never happen.
				// The only possible situation is when Redis scripts were flushed. In this case
				// we will return from this func and load publish script from scratch.
				// Redigo does the same check but for single EVALSHA command: see
				// https://github.com/garyburd/redigo/blob/master/redis/script.go#L64
				if e, ok := err.(redis.Error); ok && strings.HasPrefix(e.Error(), "NOSCRIPT ") {
					noScriptError = true
				}
			}
			drs[i].done(reply, err)
		}
		if noScriptError {
			// Start this func from the beginning and LOAD missing script.
			return nil
		}
		drs = nil
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

func getOptions(s *RedisShard, conf RedisShardConfig) *redis.UniversalOptions {
	opt := &redis.UniversalOptions{
		DB:               conf.DB,
		Username:         conf.User,
		Password:         conf.Password,
		SentinelUsername: conf.SentinelUser,
		SentinelPassword: conf.SentinelPassword,
		MasterName:       conf.SentinelMasterName,
	}
	if s.useCluster {
		opt.Addrs = conf.ClusterAddresses
	} else if conf.SentinelMasterName != "" && len(conf.SentinelAddresses) > 0 {
		opt.Addrs = conf.SentinelAddresses
	} else {
		opt.Addrs = []string{conf.address}
	}

	poolSize := defaultRedisPoolSize
	opt.PoolSize = poolSize
	opt.MaxIdleConns = poolSize
	opt.MaxRetryBackoff = 50*time.Millisecond

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
	opt.ReadTimeout = readTimeout
	opt.WriteTimeout = writeTimeout
	opt.DialTimeout = connectTimeout
	opt.ConnMaxIdleTime = conf.IdleTimeout

	if conf.UseTLS {
		if conf.TLSConfig != nil {
			opt.TLSConfig = conf.TLSConfig
		} else {
			opt.TLSConfig = &tls.Config{}
		}
		if conf.TLSSkipVerify {
			opt.TLSConfig.InsecureSkipVerify = true
		}
	}
	return opt
}

func newRedisClient(s *RedisShard, n *Node, conf RedisShardConfig) (redis.UniversalClient, error) {
	password := conf.Password
	db := conf.DB

	useSentinel := conf.SentinelMasterName != "" && len(conf.SentinelAddresses) > 0
	usingPassword := password != ""

	opts := getOptions(s, conf)

	if !s.useCluster {
		serverAddr := conf.address
		if !useSentinel {
			n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: %s/%d, using password: %v", serverAddr, db, usingPassword)))
			return redis.NewClient(opts.Simple()), nil
		} else {
			n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: Sentinel for name: %s, db: %d, using password: %v", conf.SentinelMasterName, db, usingPassword)))
			return redis.NewFailoverClient(opts.Failover()), nil
		}
	}
	// OK, we should work with cluster.
	n.Log(NewLogEntry(LogLevelInfo, fmt.Sprintf("Redis: cluster addrs: %+v, using password: %v", conf.ClusterAddresses, usingPassword)))
	cluster := redis.NewClusterClient(opts.Cluster())
	return cluster, nil
}

func (s *RedisShard) readTimeout() time.Duration {
	var readTimeout = DefaultRedisReadTimeout
	if s.config.ReadTimeout != 0 {
		readTimeout = s.config.ReadTimeout
	}
	return readTimeout
}

// consistentIndex is an adapted function from https://github.com/dgryski/go-jump
// package by Damian Gryski. It consistently chooses a hash bucket number in the
// range [0, numBuckets) for the given string. numBuckets must be >= 1.
func consistentIndex(s string, numBuckets int) int {
	hash := fnv.New64a()
	_, _ = hash.Write(util.StringToBytes(s))
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
