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

	"github.com/redis/rueidis"
)

type (
	// channelID is unique channel identifier in Redis.
	channelID string
)

const (
	defaultRedisIOTimeout      = 4 * time.Second
	defaultRedisConnectTimeout = time.Second
)

type RedisShard struct {
	config        RedisShardConfig
	client        rueidis.Client
	replicaClient rueidis.Client
	closeCh       chan struct{}
	closeOnce     sync.Once
	isCluster     bool
	finalAddress  []string
}

var knownRedisURLPrefixes = []string{
	"redis://",
	"redis+sentinel://",
	"redis+cluster://",
	"unix://",
	"tcp://",
}

func optionsFromAddress(address string, options rueidis.ClientOption) (rueidis.ClientOption, bool, error) {
	hasKnownURLPrefix := false
	for _, prefix := range knownRedisURLPrefixes {
		if strings.HasPrefix(address, prefix) {
			hasKnownURLPrefix = true
			break
		}
	}
	if !hasKnownURLPrefix {
		if host, port, err := net.SplitHostPort(address); err == nil && host != "" && port != "" {
			options.InitAddress = []string{address}
			return options, false, nil
		}
		return options, false, errors.New("malformed connection address, must be Redis URL or host:port")
	}
	u, err := url.Parse(address)
	if err != nil {
		return options, false, fmt.Errorf("malformed connection address, not a valid URL: %w", err)
	}

	var addresses []string

	switch u.Scheme {
	case "tcp", "redis", "redis+sentinel", "redis+cluster":
		addresses = []string{u.Host}
		if u.Path != "" {
			db, err := strconv.Atoi(strings.TrimPrefix(u.Path, "/"))
			if err != nil {
				return options, false, fmt.Errorf("can't parse Redis DB number from connection address: %s is not a number", u.Path)
			}
			options.SelectDB = db
		}
	case "unix":
		addresses = []string{u.Path}
		options.DialFn = func(s string, d *net.Dialer, c *tls.Config) (net.Conn, error) {
			return d.Dial("unix", s)
		}
	}
	if u.User != nil {
		if u.User.Username() != "" {
			options.Username = u.User.Username()
		}
		if pass, ok := u.User.Password(); ok {
			options.Password = pass
		}
	}
	query := u.Query()
	addresses = append(addresses, query["addr"]...)

	if query.Has("connect_timeout") {
		to, err := time.ParseDuration(query.Get("connect_timeout"))
		if err != nil {
			return options, false, fmt.Errorf("invalid connect timeout: %q", query.Get("connect_timeout"))
		}
		options.Dialer.Timeout = to
	}
	if query.Has("io_timeout") {
		to, err := time.ParseDuration(query.Get("io_timeout"))
		if err != nil {
			return options, false, fmt.Errorf("invalid io timeout: %q", query.Get("io_timeout"))
		}
		options.ConnWriteTimeout = to
	}
	if query.Has("tls_enabled") && options.TLSConfig == nil {
		val, err := strconv.ParseBool(query.Get("tls_enabled"))
		if err != nil {
			return options, false, fmt.Errorf("invalid tls_enabled value: %q", query.Get("tls_enabled"))
		}
		if val {
			options.TLSConfig = &tls.Config{}
		}
	}
	if query.Has("force_resp2") {
		val, err := strconv.ParseBool(query.Get("force_resp2"))
		if err != nil {
			return options, false, fmt.Errorf("invalid force_resp2 value: %q", query.Get("force_resp2"))
		}
		options.AlwaysRESP2 = val
	}
	if query.Has("sentinel_master_name") {
		options.Sentinel.MasterSet = query.Get("sentinel_master_name")
	}
	if query.Has("sentinel_user") {
		options.Sentinel.Username = query.Get("sentinel_user")
	}
	if query.Has("sentinel_password") {
		options.Sentinel.Password = query.Get("sentinel_password")
	}
	if query.Has("sentinel_tls_enabled") && options.Sentinel.TLSConfig == nil {
		val, err := strconv.ParseBool(query.Get("sentinel_tls_enabled"))
		if err != nil {
			return options, false, fmt.Errorf("invalid sentinel_tls_enabled value: %q", query.Get("sentinel_tls_enabled"))
		}
		if val {
			options.Sentinel.TLSConfig = &tls.Config{}
		}
	}
	if u.Scheme == "redis+sentinel" && options.Sentinel.MasterSet == "" {
		return options, false, errors.New("sentinel master name must be configured for Redis Sentinel setup")
	}
	options.InitAddress = addresses
	return options, u.Scheme == "redis+cluster", nil
}

// NewRedisShard initializes new Redis shard.
func NewRedisShard(_ *Node, conf RedisShardConfig) (*RedisShard, error) {
	if conf.ConnectTimeout == 0 {
		conf.ConnectTimeout = defaultRedisConnectTimeout
	}
	if conf.IOTimeout == 0 {
		conf.IOTimeout = defaultRedisIOTimeout
	}
	options := rueidis.ClientOption{
		SelectDB:         conf.DB,
		ConnWriteTimeout: conf.IOTimeout,
		TLSConfig:        conf.TLSConfig,
		Username:         conf.User,
		Password:         conf.Password,
		ClientName:       conf.ClientName,
		ShuffleInit:      true,
		DisableCache:     true,
		AlwaysPipelining: true,
		AlwaysRESP2:      conf.ForceRESP2,
		MaxFlushDelay:    100 * time.Microsecond,
		Dialer: net.Dialer{
			Timeout: conf.ConnectTimeout,
		},
	}

	var isCluster bool

	if len(conf.SentinelAddresses) > 0 {
		options.InitAddress = conf.SentinelAddresses
		options.Sentinel = rueidis.SentinelOption{
			TLSConfig:  conf.SentinelTLSConfig,
			MasterSet:  conf.SentinelMasterName,
			Username:   conf.SentinelUser,
			Password:   conf.SentinelPassword,
			ClientName: conf.SentinelClientName,
		}
	} else if len(conf.ClusterAddresses) > 0 {
		isCluster = true
		options.InitAddress = conf.ClusterAddresses
	} else {
		var err error
		options, isCluster, err = optionsFromAddress(conf.Address, options)
		if err != nil {
			return nil, fmt.Errorf("error processing Redis address: %v", err)
		}
	}

	shard := &RedisShard{
		config:       conf,
		isCluster:    isCluster,
		closeCh:      make(chan struct{}),
		finalAddress: options.InitAddress,
	}

	client, err := rueidis.NewClient(options)
	if err != nil {
		return nil, fmt.Errorf("error creating Redis client: %v", err)
	}
	shard.client = client

	if conf.InitReplicaClient {
		options.ReplicaOnly = true
		replicaClient, err := rueidis.NewClient(options)
		if err != nil {
			return nil, fmt.Errorf("error creating Redis replica client: %w", err)
		}
		shard.replicaClient = replicaClient
	}

	return shard, nil
}

// RedisShardConfig contains Redis connection options.
type RedisShardConfig struct {
	// Address is a Redis server connection address. Address can be:
	// - host:port
	// - tcp://[[[user]:password]@]host:port[/db][?option1=value1&optionN=valueN]
	// - redis://[[[user]:password]@]host:port[/db][?option1=value1&optionN=valueN]
	// - unix://[[[user]:password]@]path[?option1=value1&optionN=valueN]
	// It's also possible to use Address with redis+sentinel:// and redis+cluster://
	// schemes when connecting to Redis Sentinel and Redis Cluster respectively.
	// Examples:
	// - redis+sentinel://[[[user]:password]@]host:port?sentinel_master_name=mymaster
	// - redis+cluster://[[[user]:password]@]host:port[?addr=host2:port2&addr=host3:port3]
	// If you need to connect to Redis Cluster then you need to provide ClusterAddresses
	// or must use redis+cluster:// scheme in Address.
	// If you need to connect to Redis Sentinel then you need to provide SentinelAddresses
	// or must use redis+sentinel:// scheme in Address.
	// I.e. Centrifuge requires you to explicitly specify the type of Redis setup you want
	// to connect to.
	Address string
	// ClusterAddresses is a slice of seed cluster addresses to connect to.
	// Each address should be in form of host:port. If ClusterAddresses set then
	// RedisShardConfig.Address not used at all.
	ClusterAddresses []string
	// SentinelAddresses is a slice of Sentinel addresses. Each address should
	// be in form of host:port. If set then Redis address will be automatically
	// discovered from Sentinel. For Sentinel the name of the master instance
	// Sentinel monitors (SentinelMasterName) must be provided. If SentinelAddresses
	// set then RedisShardConfig.Address not used at all.
	SentinelAddresses []string

	// SentinelMasterName is a name of Redis instance master Sentinel monitors.
	SentinelMasterName string
	// SentinelUser is a user for Sentinel ACL-based auth.
	SentinelUser string
	// SentinelPassword is a password for Sentinel. Works with Sentinel >= 5.0.1.
	SentinelPassword string
	// SentinelClientName is a client name for established connections to Sentinel.
	SentinelClientName string
	// SentinelTLSConfig is a TLS configuration for Sentinel connections.
	SentinelTLSConfig *tls.Config

	// DB is Redis database number. If not set then database 0 used.
	// Does not make sense in Redis Cluster case.
	DB int
	// User is a username for Redis ACL-based auth.
	User string
	// Password is password to use when connecting to Redis. If zero then password not used.
	Password string
	// ClientName for established connections with Redis. See https://redis.io/commands/client-setname/
	ClientName string
	// TLSConfig contains connection TLS configuration.
	TLSConfig *tls.Config

	// ConnectTimeout is a timeout on connect operation.
	// By default, 1 second is used.
	ConnectTimeout time.Duration
	// IOTimeout is a timeout on Redis connection operations. This is used as a write deadline
	// for connection, also Redis client we use internally periodically (once in a second) PINGs
	// Redis with this timeout for PING operation to find out stale/broken/blocked connections.
	// By default, 4 seconds is used.
	IOTimeout time.Duration

	// ForceRESP2 if set to true forces using RESP2 protocol for communicating with Redis.
	// By default, Redis client tries to detect supported Redis protocol automatically
	// trying RESP3 first.
	ForceRESP2 bool

	// InitReplicaClient once set to true will initialize replica client for this shard.
	// Replica client can then be used for read-only operations from replica nodes in Redis
	// Cluster or Redis Sentinel setups. Replica client will be initialized with the same
	// options as the main client but with ReplicaOnly option set to true.
	InitReplicaClient bool
}

func (s *RedisShard) Close() {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		s.client.Close()
	})
}

func (s *RedisShard) string() string {
	return strings.Join(s.finalAddress, ",")
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
