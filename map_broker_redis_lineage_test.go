//go:build integration

package centrifuge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A new lineage of the server (a restart or a promotion: the script cache is
// empty) starts a new epoch for a map's stream, so clients reload the state;
// the state itself is what the server has and is kept, bound to the new epoch.
func TestRedisMapBroker_NewLineageKeepsState(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) { c.DetectPotentialLostWrites = true })
		ctx := context.Background()
		ch := randomChannel("lineage_map")

		for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "3"}} {
			_, err := broker.Publish(ctx, ch, kv[0], MapPublishOptions{Data: []byte(kv[1])})
			require.NoError(t, err)
		}
		before, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Equal(t, uint64(3), before.Position.Offset)

		s := broker.shards[0].shard
		resets := counterValue(t, node.metrics.redisPotentialLostWritesEpochResets.WithLabelValues("map", "script_cache"))
		flushScripts(t, s)

		after, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.NotEqual(t, before.Position.Epoch, after.Position.Epoch, "clients must reload: a new epoch")
		require.GreaterOrEqual(t, counterValue(t, node.metrics.redisPotentialLostWritesEpochResets.WithLabelValues("map", "script_cache")), resets+1, "the reset map is counted")
		require.Zero(t, after.Position.Offset, "the change stream starts again")
		state := stateToMap(after.Publications)
		require.Len(t, state, 2, "the state is kept")
		require.Equal(t, []byte("3"), state["a"])
		require.Equal(t, []byte("2"), state["b"])

		// The old position doesn't recover across the new epoch.
		stream, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{Filter: StreamFilter{Limit: -1}})
		require.NoError(t, err)
		require.Equal(t, after.Position.Epoch, stream.Position.Epoch)
		require.Empty(t, stream.Publications, "the old epoch's changes are gone")

		// The next update continues from the kept state in the new epoch.
		res, err := broker.Publish(ctx, ch, "c", MapPublishOptions{Data: []byte("4")})
		require.NoError(t, err)
		require.Equal(t, after.Position.Epoch, res.Position.Epoch)
		require.Equal(t, uint64(1), res.Position.Offset)
		final, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, stateToMap(final.Publications), 3)
	})
}

// A new lineage noticed first by the stream read (a position check) must not
// make the next state read wipe the state as state of a dead epoch.
func TestRedisMapBroker_NewLineageByStreamReadKeepsState(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) { c.DetectPotentialLostWrites = true })
		ctx := context.Background()
		ch := randomChannel("lineage_map_stream")

		for _, k := range []string{"a", "b"} {
			_, err := broker.Publish(ctx, ch, k, MapPublishOptions{Data: []byte(k)})
			require.NoError(t, err)
		}
		before, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{Filter: StreamFilter{Limit: 0}})
		require.NoError(t, err)

		flushScripts(t, broker.shards[0].shard)

		top, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{Filter: StreamFilter{Limit: 0}})
		require.NoError(t, err)
		require.NotEqual(t, before.Position.Epoch, top.Position.Epoch)

		state, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Equal(t, top.Position.Epoch, state.Position.Epoch)
		require.Len(t, stateToMap(state.Publications), 2, "the state is kept")
	})
}
