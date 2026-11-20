package centrifuge

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func TestOptionsFromAddress(t *testing.T) {
	tests := []struct {
		name                string
		address             string
		inputOptions        rueidis.ClientOption
		expectedError       error
		expectedOutput      rueidis.ClientOption
		expectedIsCluster   bool
		expectedIsSentinel  bool
		expectedInitReplica bool
	}{
		{
			name:          "Valid TCP address with host:port",
			address:       "127.0.0.1:6379",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
			},
		},
		{
			name:          "Invalid TCP address, missing port",
			address:       "127.0.0.1",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("malformed connection address, must be Redis URL or host:port"),
		},
		{
			name:          "Malformed URL",
			address:       "tcp://:invalid",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("malformed connection address, not a valid URL: parse \"tcp://:invalid\": invalid port \":invalid\" after host"),
		},
		{
			name:          "Redis URL with DB number",
			address:       "redis://127.0.0.1:6379/2",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				SelectDB:    2,
			},
		},
		{
			name:          "Redis Cluster URL with DB number",
			address:       "redis+cluster://127.0.0.1:6379",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
			},
			expectedIsCluster: true,
		},
		{
			name:          "Redis URL with invalid DB number",
			address:       "redis://127.0.0.1:6379/invalid",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("can't parse Redis DB number from connection address: /invalid is not a number"),
		},
		{
			name:          "Unsupported scheme",
			address:       "http://127.0.0.1:6379",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("malformed connection address, must be Redis URL or host:port"),
		},
		{
			name:          "Redis URL with username and password",
			address:       "redis://user:pass@127.0.0.1:6379",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				Username:    "user",
				Password:    "pass",
			},
		},
		{
			name:          "Redis URL with multiple addresses",
			address:       "redis://@127.0.0.1:7000?addr=127.0.0.1:7001&addr=127.0.0.1:7002",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
			},
		},
		{
			name:          "Redis URL with force_resp2",
			address:       "redis://user:pass@127.0.0.1:6379?force_resp2=true",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				AlwaysRESP2: true,
				Username:    "user",
				Password:    "pass",
			},
		},
		{
			name:          "Redis URL with query parameters",
			address:       "redis://127.0.0.1:6379?connect_timeout=1s&io_timeout=2s&tls_enabled=true",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress:      []string{"127.0.0.1:6379"},
				Dialer:           net.Dialer{Timeout: 1 * time.Second},
				ConnWriteTimeout: 2 * time.Second,
				TLSConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
		},
		{
			name:          "Redis URL with invalid connect_timeout",
			address:       "redis://127.0.0.1:6379?connect_timeout=xs&io_timeout=2s&tls_enabled=true",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("invalid connect timeout: \"xs\""),
		},
		{
			name:          "Redis URL with invalid io_timeout",
			address:       "redis://127.0.0.1:6379?connect_timeout=1s&io_timeout=xs&tls_enabled=true",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("invalid io timeout: \"xs\""),
		},
		{
			name:          "Redis URL with invalid force_resp2",
			address:       "redis://127.0.0.1:6379?force_resp2=xs&io_timeout=1s&tls_enabled=true",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("invalid force_resp2 value: \"xs\""),
		},
		{
			name:          "Redis URL with invalid replica_client_enabled",
			address:       "redis://127.0.0.1:6379?replica_client_enabled=xs",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("invalid replica_client_enabled value: \"xs\""),
		},
		{
			name:         "Redis URL with valid replica_client_enabled",
			address:      "redis+cluster://127.0.0.1:6379?replica_client_enabled=true",
			inputOptions: rueidis.ClientOption{},
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
			},
			expectedError:       nil,
			expectedIsCluster:   true,
			expectedInitReplica: true,
		},
		{
			name:          "Redis URL with Sentinel query parameters",
			address:       "redis+sentinel://127.0.0.1:6379?sentinel_master_name=mymaster&sentinel_user=user&sentinel_password=pass&sentinel_tls_enabled=true",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				Sentinel: rueidis.SentinelOption{
					MasterSet: "mymaster",
					Username:  "user",
					Password:  "pass",
					TLSConfig: &tls.Config{
						MinVersion: tls.VersionTLS12,
					},
				},
			},
			expectedIsSentinel: true,
		},
		{
			name:          "Redis secure URL",
			address:       "rediss://127.0.0.1:6379",
			inputOptions:  rueidis.ClientOption{},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				TLSConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
		},
		{
			name:    "Redis secure URL does not override explicitly set TLS config",
			address: "rediss://127.0.0.1:6379",
			inputOptions: rueidis.ClientOption{
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					MinVersion:         tls.VersionTLS12,
				},
			},
			expectedError: nil,
			expectedOutput: rueidis.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					MinVersion:         tls.VersionTLS12,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := optionsFromAddress(tt.address, tt.inputOptions)
			if tt.expectedError != nil {
				require.Error(t, err)
				require.EqualError(t, err, tt.expectedError.Error())
			} else {
				output, isCluster, isSentinel := opts.ClientOption, opts.IsCluster, opts.IsSentinel
				require.NoError(t, err)
				require.Equal(t, tt.expectedOutput, output)
				require.Equal(t, tt.expectedIsCluster, isCluster)
				require.Equal(t, tt.expectedIsSentinel, isSentinel)
				require.Equal(t, tt.expectedInitReplica, opts.ReplicaClientEnabled)
			}
		})
	}
}

func TestOptionsFromAddressUnix(t *testing.T) {
	opts, err := optionsFromAddress("unix:///tmp/redis.sock", rueidis.ClientOption{})
	require.NoError(t, err)
	require.Equal(t, opts.ClientOption.InitAddress, []string{"/tmp/redis.sock"})
	require.NotNil(t, opts.ClientOption.DialFn)
	_, err = opts.ClientOption.DialFn("", &net.Dialer{}, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})
	require.Error(t, err)
}

// TestRedisShardModeStandalone covers the three branches of Mode (standalone,
// cluster, sentinel).
func TestRedisShardModeStandalone(t *testing.T) {
	t.Parallel()
	s := &RedisShard{}
	require.Equal(t, RedisShardModeStandalone, s.Mode())
	s2 := &RedisShard{isCluster: true}
	require.Equal(t, RedisShardModeCluster, s2.Mode())
	s3 := &RedisShard{isSentinel: true}
	require.Equal(t, RedisShardModeSentinel, s3.Mode())
}

// The parsers below consume data read back from Redis - PUB/SUB payloads,
// history stream values, map state values. Every length in those formats is
// taken off the wire, so a malformed or foreign message must be rejected rather
// than sliced with. They run on the PUB/SUB processing goroutine, where a panic
// is not recovered and takes the process down, and the same message reaches
// every subscribed node.
//
// They live in this file, rather than beside the parsers in
// broker_redis_test.go and map_broker_redis_test.go, because those carry the
// integration build tag: the parsers are pure functions needing no Redis, and
// tests for them must run in the default suite.

func TestExtractPushDataMalformed(t *testing.T) {
	t.Parallel()

	for _, data := range []string{
		"__p__x",           // header shorter than the "p1:" tag
		"__p1__x",          // header one byte short of the tag
		"__p__",            // no payload either
		"__d1:1:e:5:abcde", // delta prev length equal to what remains
		"__d1:1:e:-1:x:1:y",
		"__d1:1:e:1:a:-1:y",
		"__d1:",
		"__d__",
		"__j__",
		"__l__",
		"__",
		"__x__payload",
	} {
		require.NotPanics(t, func() {
			_, _, _, _, _, _ = extractPushData([]byte(data))
		}, "extractPushData(%q)", data)
	}

	// A well formed publication still parses.
	payload, pushType, sp, delta, prevPayload, ok := extractPushData([]byte("__p1:42:epoch__payload"))
	require.True(t, ok)
	require.Equal(t, pubPushType, pushType)
	require.Equal(t, "payload", string(payload))
	require.Equal(t, uint64(42), sp.Offset)
	require.Equal(t, "epoch", sp.Epoch)
	require.False(t, delta)
	require.Nil(t, prevPayload)
}

func TestParseDeltaPushMalformed(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"d1:1:e:5:abcde",  // prev payload length leaves no room for the separator
		"d1:1:e:-1:x:1:y", // negative prev payload length
		"d1:1:e:1:a:-1:y", // negative payload length
		"d1:1:e:1:a:9:y",  // payload length past the end
		"d1:",
		"d1:1:",
		"d1:1:e:",
	} {
		_, err := parseDeltaPush(input)
		require.Error(t, err, "parseDeltaPush(%q)", input)
	}

	parsed, err := parseDeltaPush("d1:7:ep:3:abc:5:hello")
	require.NoError(t, err)
	require.Equal(t, uint64(7), parsed.Offset)
	require.Equal(t, "ep", parsed.Epoch)
	require.Equal(t, "abc", parsed.PrevPayload)
	require.Equal(t, "hello", parsed.Payload)
}

func TestParseMapMessageMalformed(t *testing.T) {
	t.Parallel()

	for _, data := range []string{
		"d:1:e:-1:x:1:y", // negative prev length
		"d:1:e:1:a:-1:y", // negative curr length
		"d:1:e:5:abc",    // prev length past the end
		"d:",
		"d:1:e:1:a:9:y",
	} {
		require.NotPanics(t, func() {
			_, _, _, _, _, _ = parseMessage([]byte(data))
		}, "parseMessage(%q)", data)
	}

	offset, epoch, payload, isDelta, prevPayload, err := parseMessage([]byte("d:7:ep:3:abc:5:hello"))
	require.NoError(t, err)
	require.Equal(t, uint64(7), offset)
	require.Equal(t, "ep", epoch)
	require.Equal(t, "hello", string(payload))
	require.True(t, isDelta)
	require.Equal(t, "abc", string(prevPayload))
}

func FuzzExtractPushData(f *testing.F) {
	for _, seed := range []string{
		"__p1:42:epoch__payload", "__j__info", "__l__info",
		"__d1:7:ep:3:abc:5:hello", "__p__x", "__d1:1:e:-1:x:1:y", "plain", "__",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _, _, _, _ = extractPushData(data)
	})
}

func FuzzParseMapMessage(f *testing.F) {
	for _, seed := range []string{
		"d:7:ep:3:abc:5:hello", "1:epoch:protobuf", "raw", "d:", "d:1:e:-1:x:1:y", "",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _, _, _, _ = parseMessage(data)
		_, _, _, _ = parseStateValue(data)
	})
}

// TestWaitForRedisStartupRetriesUntilConnected: a connect which fails with a
// network error is repeated until it succeeds.
func TestWaitForRedisStartupRetriesUntilConnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		connect := func() (rueidis.Client, error) {
			attempts++
			if attempts < 4 {
				return nil, errors.New("dial tcp: connection refused")
			}
			return nil, nil
		}
		_, err := waitForRedisStartup(time.Now().Add(time.Minute), connect, func(error, time.Duration) {})
		require.NoError(t, err)
		require.Equal(t, 4, attempts)
	})
}

// TestWaitForRedisStartupReturnsLastErrorAfterTimeout: Redis which never
// comes up costs the timeout and no more, and the caller gets the error of
// the last attempt.
func TestWaitForRedisStartupReturnsLastErrorAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		connect := func() (rueidis.Client, error) {
			attempts++
			return nil, fmt.Errorf("attempt %d failed", attempts)
		}
		started := time.Now()
		_, err := waitForRedisStartup(time.Now().Add(30*time.Second), connect, func(error, time.Duration) {})
		require.EqualError(t, err, fmt.Sprintf("attempt %d failed", attempts))
		require.Equal(t, 30*time.Second, time.Since(started))
	})
}

// TestWaitForRedisStartupSharesDeadline: two waits with one deadline, as for
// the primary and the replica client, take the timeout in total.
func TestWaitForRedisStartupSharesDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		deadline := started.Add(30 * time.Second)
		var attempts int
		connectLate := func() (rueidis.Client, error) {
			attempts++
			if attempts < 6 {
				return nil, errors.New("dial tcp: connection refused")
			}
			return nil, nil
		}
		_, err := waitForRedisStartup(deadline, connectLate, func(error, time.Duration) {})
		require.NoError(t, err)
		require.Positive(t, time.Since(started))

		connectNever := func() (rueidis.Client, error) {
			return nil, errors.New("dial tcp: connection refused")
		}
		_, err = waitForRedisStartup(deadline, connectNever, func(error, time.Duration) {})
		require.Error(t, err)
		require.Equal(t, 30*time.Second, time.Since(started))
	})
}

// TestWaitForRedisStartupBacksOffWithJitter: delays grow from the initial one
// to the max one, each with a jitter of up to a half of it.
func TestWaitForRedisStartupBacksOffWithJitter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		connect := func() (rueidis.Client, error) {
			return nil, errors.New("dial tcp: connection refused")
		}
		var delays []time.Duration
		_, err := waitForRedisStartup(time.Now().Add(time.Minute), connect, func(_ error, delay time.Duration) {
			delays = append(delays, delay)
		})
		require.Error(t, err)

		// The last delay is cut to what is left of the timeout.
		delays = delays[:len(delays)-1]
		require.Greater(t, len(delays), 8)
		maxDelay := redisStartupWaitInitialDelay
		for i, delay := range delays {
			require.GreaterOrEqual(t, delay, maxDelay/2, "delay %d", i)
			require.LessOrEqual(t, delay, maxDelay, "delay %d", i)
			maxDelay = min(2*maxDelay, redisStartupWaitMaxDelay)
		}
		require.Equal(t, redisStartupWaitMaxDelay, maxDelay)
	})
}

// TestWaitForRedisStartupDoesNotRetryRedisError: an error Redis replies with,
// other than LOADING, comes from the configuration, waiting does not fix it.
func TestWaitForRedisStartupDoesNotRetryRedisError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		redisErr := (*rueidis.RedisError)(&rueidis.RedisMessage{})
		var attempts int
		connect := func() (rueidis.Client, error) {
			attempts++
			return nil, redisErr
		}
		started := time.Now()
		_, err := waitForRedisStartup(time.Now().Add(time.Minute), connect, func(error, time.Duration) {})
		require.Equal(t, redisErr, err)
		require.Equal(t, 1, attempts)
		require.Zero(t, time.Since(started))
	})
}

// unusedTCPAddr returns an address nothing listens on.
func unusedTCPAddr(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	addr := ln.Addr().String()
	require.NoError(tb, ln.Close())
	return addr
}

func TestNewRedisShardFailsRightAwayWithoutStartupWaitTimeout(t *testing.T) {
	started := time.Now()
	_, err := NewRedisShard(nil, RedisShardConfig{Address: unusedTCPAddr(t)})
	require.Error(t, err)
	require.Less(t, time.Since(started), time.Second)
}

func TestNewRedisShardFailsAfterStartupWaitTimeout(t *testing.T) {
	timeout := 500 * time.Millisecond
	started := time.Now()
	_, err := NewRedisShard(nil, RedisShardConfig{Address: unusedTCPAddr(t), StartupWaitTimeout: timeout})
	require.Error(t, err)
	require.GreaterOrEqual(t, time.Since(started), timeout)
}

func TestNewRedisShardLogsStartupWaitRetries(t *testing.T) {
	var mu sync.Mutex
	var messages []string
	node, err := New(Config{
		LogLevel: LogLevelInfo,
		LogHandler: func(entry LogEntry) {
			mu.Lock()
			defer mu.Unlock()
			messages = append(messages, entry.Message)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	_, err = NewRedisShard(node, RedisShardConfig{Address: unusedTCPAddr(t), StartupWaitTimeout: 300 * time.Millisecond})
	require.Error(t, err)
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, messages, "waiting for Redis on startup")
}

// startLoadingRedis starts a server which speaks enough of the Redis protocol
// for a client to connect, and replies to PING with a LOADING error until
// loaded is closed.
func startLoadingRedis(tb testing.TB, loaded <-chan struct{}) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				r := bufio.NewReader(conn)
				for {
					command, err := readRedisCommand(r)
					if err != nil {
						return
					}
					reply := "+OK\r\n"
					if strings.EqualFold(command[0], "PING") {
						select {
						case <-loaded:
							reply = "+PONG\r\n"
						default:
							reply = "-LOADING Redis is loading the dataset in memory\r\n"
						}
					}
					if _, err := conn.Write([]byte(reply)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// readRedisCommand reads a command a client sent: an array of bulk strings.
func readRedisCommand(r *bufio.Reader) ([]string, error) {
	readLen := func(prefix byte) (int, error) {
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		if line[0] != prefix {
			return 0, fmt.Errorf("unexpected line %q", line)
		}
		return strconv.Atoi(strings.TrimSpace(line[1:]))
	}
	n, err := readLen('*')
	if err != nil {
		return nil, err
	}
	command := make([]string, n)
	for i := range command {
		size, err := readLen('$')
		if err != nil {
			return nil, err
		}
		arg := make([]byte, size+2)
		if _, err := io.ReadFull(r, arg); err != nil {
			return nil, err
		}
		command[i] = string(arg[:size])
	}
	return command, nil
}

// TestNewRedisShardStartupWaitTimeoutWaitsForLoadingRedis: Redis accepts
// connections while it loads a dataset but is of no use yet, so
// StartupWaitTimeout waits for the load to finish too.
func TestNewRedisShardStartupWaitTimeoutWaitsForLoadingRedis(t *testing.T) {
	loaded := make(chan struct{})
	addr := startLoadingRedis(t, loaded)
	loading := 300 * time.Millisecond
	time.AfterFunc(loading, func() { close(loaded) })

	started := time.Now()
	shard, err := NewRedisShard(nil, RedisShardConfig{
		Address:            addr,
		ForceRESP2:         true,
		StartupWaitTimeout: 30 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(shard.Close)
	require.GreaterOrEqual(t, time.Since(started), loading)
	require.NoError(t, shard.client.Do(context.Background(), shard.client.B().Ping().Build()).Error())
}

// TestWaitForRedisStartupAbandonsSlowAttempt: an attempt which is still in
// flight when the deadline passes is not waited for.
func TestWaitForRedisStartupAbandonsSlowAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		connect := func() (rueidis.Client, error) {
			time.Sleep(10 * time.Second)
			return nil, errors.New("dial tcp: i/o timeout")
		}
		started := time.Now()
		_, err := waitForRedisStartup(started.Add(time.Second), connect, func(error, time.Duration) {})
		require.ErrorIs(t, err, errRedisStartupWaitTimeout)
		require.Equal(t, time.Second, time.Since(started))

		// Let the abandoned attempt finish before the bubble ends.
		time.Sleep(10 * time.Second)
	})
}

// TestNewRedisShardStartupWaitTimeoutBoundsSlowAttempt: a server which accepts
// a connection and never replies holds an attempt for ConnectTimeout, yet
// NewRedisShard returns when StartupWaitTimeout passes.
func TestNewRedisShardStartupWaitTimeoutBoundsSlowAttempt(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	started := time.Now()
	_, err = NewRedisShard(nil, RedisShardConfig{
		Address:            ln.Addr().String(),
		ConnectTimeout:     2 * time.Second,
		IOTimeout:          2 * time.Second,
		StartupWaitTimeout: 100 * time.Millisecond,
	})
	require.Error(t, err)
	require.Less(t, time.Since(started), time.Second)
}

// TestNewRedisShardStartupWaitTimeoutWithZeroIOTimeoutInAddress: io_timeout=0s
// in the address means the default IO timeout for the PING of the wait too.
func TestNewRedisShardStartupWaitTimeoutWithZeroIOTimeoutInAddress(t *testing.T) {
	loaded := make(chan struct{})
	close(loaded)
	addr := startLoadingRedis(t, loaded)

	shard, err := NewRedisShard(nil, RedisShardConfig{
		Address:            "redis://" + addr + "?io_timeout=0s&force_resp2=true",
		StartupWaitTimeout: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	shard.Close()
}
