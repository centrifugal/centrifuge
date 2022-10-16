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

	"github.com/rueian/rueidis"
)

type (
	// channelID is unique channel identifier in Redis.
	channelID string
)

const (
	DefaultRedisReadTimeout    = 5 * time.Second
	DefaultRedisWriteTimeout   = time.Second
	DefaultRedisConnectTimeout = time.Second
)

type RedisShard struct {
	config           RedisShardConfig
	subChannels      []chan subRequest
	scriptsCh        chan struct{}
	reloadPipelineCh chan struct{}
	client           rueidis.Client
	closeCh          chan struct{}
	closeOnce        sync.Once
	useCluster       bool
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
func NewRedisShard(_ *Node, conf RedisShardConfig) (*RedisShard, error) {
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
		subChannels:      make([]chan subRequest, 0),
		closeCh:          make(chan struct{}),
	}

	options := rueidis.ClientOption{
		SelectDB:         conf.DB,
		ConnWriteTimeout: DefaultRedisWriteTimeout,
		TLSConfig:        conf.TLSConfig,
		Username:         conf.User,
		Password:         conf.Password,
		ClientName:       conf.ClientName,
		ShuffleInit:      true,
		DisableCache:     true,
	}

	if len(conf.SentinelAddresses) > 0 {
		options.InitAddress = conf.SentinelAddresses
		options.Sentinel = rueidis.SentinelOption{
			TLSConfig:  conf.TLSConfig,
			MasterSet:  conf.SentinelMasterName,
			Username:   conf.SentinelUser,
			Password:   conf.SentinelPassword,
			ClientName: conf.SentinelClientName,
		}
	} else if len(conf.ClusterAddresses) > 0 {
		options.InitAddress = conf.ClusterAddresses
	} else {
		options.InitAddress = []string{conf.address}
	}

	options.Dialer = net.Dialer{
		Timeout: DefaultRedisConnectTimeout,
	}

	client, err := rueidis.NewClient(options)
	if err != nil {
		return nil, err
	}

	shard.client = client
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
	// SentinelClientName is a client name for established connections to Sentinel.
	SentinelClientName string

	// DB is Redis database number. If not set then database 0 used. Does not make sense in Redis Cluster case.
	DB int

	// User is a username for Redis ACL-based auth.
	User string
	// Password is password to use when connecting to Redis database.
	// If zero then password not used.
	Password string
	// ClientName for established connections. See https://redis.io/commands/client-setname/
	ClientName string

	// Connection TLS configuration.
	TLSConfig *tls.Config

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

func (s *RedisShard) Close() {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		s.client.Close()
	})
}

func (s *RedisShard) string() string {
	return s.config.address
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
