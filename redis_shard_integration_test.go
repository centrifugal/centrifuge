//go:build integration

package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewRedisShard(t *testing.T) {
	testCases := []struct {
		name            string
		config          RedisShardConfig
		expectedCluster bool
	}{
		{
			name: "redis standalone",
			config: RedisShardConfig{
				Address: "redis://127.0.0.1:6379",
			},
		},
		{
			name: "redis cluster",
			config: RedisShardConfig{
				Address: "redis://127.0.0.1:7000",
			},
			expectedCluster: true,
		},
		{
			name: "valkey standalone",
			config: RedisShardConfig{
				Address: "redis://127.0.0.1:8379",
			},
		},
		{
			name: "valkey cluster",
			config: RedisShardConfig{
				Address: "redis://127.0.0.1:8000",
			},
			expectedCluster: true,
		},
		{
			name: "dragonfly standalone",
			config: RedisShardConfig{
				Address: "redis://127.0.0.1:9379",
			},
		},
		{
			name: "dragonfly cluster",
			config: RedisShardConfig{
				Address: "redis://127.0.0.1:9000",
			},
			expectedCluster: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewRedisShard(&Node{}, tc.config)
			require.NoError(t, err)
			require.Equal(t, tc.expectedCluster, s.isCluster)
		})
	}
}
