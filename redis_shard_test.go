package centrifuge

import (
	"crypto/tls"
	"errors"
	"net"
	"testing"
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
