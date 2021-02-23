package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisShard_ConfFromAddress(t *testing.T) {
	conf, err := confFromAddress("127.0.0.1:6379", RedisShardConfig{})
	require.NoError(t, err)
	require.Equal(t, "tcp", conf.network)
	require.Equal(t, "127.0.0.1:6379", conf.address)

	conf, err = confFromAddress("localhost:6379", RedisShardConfig{})
	require.NoError(t, err)
	require.Equal(t, "tcp", conf.network)
	require.Equal(t, "localhost:6379", conf.address)

	_, err = confFromAddress("localhost:", RedisShardConfig{})
	require.Error(t, err)

	conf, err = confFromAddress("tcp://localhost:6379", RedisShardConfig{})
	require.NoError(t, err)
	require.Equal(t, "tcp", conf.network)
	require.Equal(t, "localhost:6379", conf.address)

	conf, err = confFromAddress("tcp://:pass@localhost:6379", RedisShardConfig{})
	require.NoError(t, err)
	require.Equal(t, "tcp", conf.network)
	require.Equal(t, "localhost:6379", conf.address)
	require.Equal(t, 0, conf.DB)
	require.Equal(t, "pass", conf.Password)

	conf, err = confFromAddress("tcp://localhost:6379/9", RedisShardConfig{})
	require.NoError(t, err)
	require.Equal(t, "tcp", conf.network)
	require.Equal(t, "localhost:6379", conf.address)
	require.Equal(t, 9, conf.DB)
	require.Equal(t, "", conf.Password)

	_, err = confFromAddress("tcp://localhost:6379/not_a_number", RedisShardConfig{})
	require.Error(t, err)

	conf, err = confFromAddress("unix://:pass@/var/run/tarantool/my_instance.sock", RedisShardConfig{})
	require.NoError(t, err)
	require.Equal(t, "unix", conf.network)
	require.Equal(t, "/var/run/tarantool/my_instance.sock", conf.address)
	require.Equal(t, 0, conf.DB)
	require.Equal(t, "pass", conf.Password)
}
