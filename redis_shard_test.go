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
				TLSConfig:        &tls.Config{},
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
			name:          "Redis URL with invalid init replica client",
			address:       "redis://127.0.0.1:6379?init_replica_client=xs",
			inputOptions:  rueidis.ClientOption{},
			expectedError: errors.New("invalid init_replica_client value: \"xs\""),
		},
		{
			name:         "Redis URL with valid init replica client",
			address:      "redis+cluster://127.0.0.1:6379?init_replica_client=true",
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
					TLSConfig: &tls.Config{},
				},
			},
			expectedIsSentinel: true,
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
	_, err = opts.ClientOption.DialFn("", &net.Dialer{}, &tls.Config{})
	require.Error(t, err)
}
