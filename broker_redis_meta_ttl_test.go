//go:build integration

package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testInitialMetaTTL = 5 * time.Minute

func newMetaTTLTestBroker(t *testing.T) (*Node, *RedisBroker) {
	t.Helper()
	return newMetaTTLTestBrokerWith(t, testInitialMetaTTL)
}

func newMetaTTLTestBrokerWith(t *testing.T, initial time.Duration) (*Node, *RedisBroker) {
	t.Helper()
	node, err := New(Config{LogLevel: LogLevelError, LogHandler: func(LogEntry) {}, HistoryMetaTTLInitial: initial})
	require.NoError(t, err)
	b := newTestRedisBroker(t, node, true, false, 6379)
	node.SetBroker(b)
	t.Cleanup(func() { stopRedisBroker(b) })
	return node, b
}

func metaPTTL(t *testing.T, s *RedisShard, key string) time.Duration {
	t.Helper()
	ms, err := s.client.Do(context.Background(), s.client.B().Pttl().Key(key).Build()).AsInt64()
	require.NoError(t, err)
	if ms < 0 {
		return time.Duration(ms)
	}
	return time.Duration(ms) * time.Millisecond
}

func setPTTL(t *testing.T, s *RedisShard, key string, d time.Duration) {
	t.Helper()
	require.NoError(t, s.client.Do(context.Background(), s.client.B().Pexpire().Key(key).Milliseconds(d.Milliseconds()).Build()).Error())
}

func TestNew_HistoryMetaTTLInitialValidation(t *testing.T) {
	_, err := New(Config{HistoryMetaTTLInitial: time.Minute})
	require.Error(t, err, "below 3 position checks (40s)")
	_, err = New(Config{HistoryMetaTTLInitial: 2 * time.Minute})
	require.NoError(t, err)
}

// A subscription to a channel nobody published to keeps its meta only for the
// short TTL; the first publication switches it to the meta TTL.
func TestRedisBroker_EmptyMetaShortTTL(t *testing.T) {
	node, b := newMetaTTLTestBroker(t)
	s := b.shards[0].shard
	ch := "meta_ttl_" + randString(8)
	key := string(b.historyMetaKey(s, ch))

	_, err := node.streamTop(ch, time.Hour, 0)
	require.NoError(t, err)
	ttl := metaPTTL(t, s, key)
	require.Greater(t, ttl, testInitialMetaTTL-time.Minute)
	require.LessOrEqual(t, ttl, testInitialMetaTTL)

	_, err = node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute, time.Hour))
	require.NoError(t, err)
	require.Greater(t, metaPTTL(t, s, key), 59*time.Minute, "publications: the meta TTL")

	// Reads never shorten a meta with publications.
	_, err = node.streamTop(ch, time.Hour, 0)
	require.NoError(t, err)
	require.Greater(t, metaPTTL(t, s, key), 59*time.Minute)
}

// An empty meta with a long TTL (made before the short TTL existed) is
// shortened by the next read.
func TestRedisBroker_EmptyMetaShortened(t *testing.T) {
	node, b := newMetaTTLTestBroker(t)
	s := b.shards[0].shard
	ch := "meta_ttl_old_" + randString(8)
	key := string(b.historyMetaKey(s, ch))

	_, err := node.streamTop(ch, time.Hour, 0)
	require.NoError(t, err)
	setPTTL(t, s, key, 30*24*time.Hour)

	_, err = node.streamTop(ch, time.Hour, 0)
	require.NoError(t, err)
	require.LessOrEqual(t, metaPTTL(t, s, key), testInitialMetaTTL)
}

// With more than half the TTL left a read writes no EXPIRE; below half it
// extends. With or without the initial TTL.
func TestRedisBroker_MetaExtendedBelowHalf(t *testing.T) {
	for _, initial := range []time.Duration{0, testInitialMetaTTL} {
		node, b := newMetaTTLTestBrokerWith(t, initial)
		s := b.shards[0].shard
		ch := "meta_ttl_half_" + randString(8)
		key := string(b.historyMetaKey(s, ch))

		_, err := node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute, time.Hour))
		require.NoError(t, err)

		setPTTL(t, s, key, 40*time.Minute)
		_, err = node.streamTop(ch, time.Hour, 0)
		require.NoError(t, err)
		require.LessOrEqual(t, metaPTTL(t, s, key), 40*time.Minute, "above half: not extended, initial %s", initial)

		setPTTL(t, s, key, 20*time.Minute)
		_, err = node.streamTop(ch, time.Hour, 0)
		require.NoError(t, err)
		require.Greater(t, metaPTTL(t, s, key), 59*time.Minute, "below half: extended, initial %s", initial)
	}
}

// Without the initial TTL an empty meta gets the meta TTL.
func TestRedisBroker_EmptyMetaWithoutInitial(t *testing.T) {
	node, b := newMetaTTLTestBrokerWith(t, 0)
	s := b.shards[0].shard
	ch := "meta_ttl_noinit_" + randString(8)
	key := string(b.historyMetaKey(s, ch))

	_, err := node.streamTop(ch, time.Hour, 0)
	require.NoError(t, err)
	require.Greater(t, metaPTTL(t, s, key), 59*time.Minute)
}

// A meta TTL below 3 position checks is extended by every read, so a meta
// under live subscribers never expires between checks.
func TestRedisBroker_ShortMetaTTLExtendedEveryRead(t *testing.T) {
	node, b := newMetaTTLTestBroker(t)
	s := b.shards[0].shard
	ch := "meta_ttl_small_" + randString(8)
	key := string(b.historyMetaKey(s, ch))

	_, err := node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute, time.Minute))
	require.NoError(t, err)
	setPTTL(t, s, key, 40*time.Second)
	_, err = node.streamTop(ch, time.Minute, 0)
	require.NoError(t, err)
	require.Greater(t, metaPTTL(t, s, key), 50*time.Second)
}

// An empty meta that expired gives a new epoch: the subscriber held a real
// epoch, so it is told it can't recover.
func TestRedisBroker_EmptyMetaExpiredNewEpoch(t *testing.T) {
	node, b := newMetaTTLTestBroker(t)
	s := b.shards[0].shard
	ch := "meta_ttl_expired_" + randString(8)
	key := string(b.historyMetaKey(s, ch))

	before, err := node.streamTop(ch, time.Hour, 0)
	require.NoError(t, err)
	require.NoError(t, s.client.Do(context.Background(), s.client.B().Del().Key(key).Build()).Error())
	_, err = node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute, time.Hour))
	require.NoError(t, err)
	_, err = node.recoverHistory(ch, before, time.Hour, 0, 0)
	require.ErrorIs(t, err, ErrorUnrecoverablePosition, "a new epoch")
}

// Persistent maps keep metas forever; an empty one gets the short TTL and is
// made permanent again by the first publication.
func TestRedisMapBroker_EmptyMetaShortTTL(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, err := New(Config{
			HistoryMetaTTLInitial: testInitialMetaTTL,
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{Mode: MapModePersistent, StreamSize: 100, StreamTTL: 300 * time.Second}
				},
			},
		})
		require.NoError(t, err)
		broker := mb.make(t, node)
		ctx := context.Background()
		s := broker.shards[0].shard

		for _, read := range []string{"state", "stream"} {
			ch := randomChannel("map_meta_ttl_" + read)
			key := broker.metaKey(s, ch)
			if read == "state" {
				_, err = broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
			} else {
				_, err = broker.ReadStream(ctx, ch, MapReadStreamOptions{Filter: StreamFilter{Limit: 0}})
			}
			require.NoError(t, err)
			ttl := metaPTTL(t, s, key)
			require.Greater(t, ttl, time.Duration(0), read)
			require.LessOrEqual(t, ttl, testInitialMetaTTL, read)

			_, err = broker.Publish(ctx, ch, "a", MapPublishOptions{Data: []byte("1")})
			require.NoError(t, err)
			require.Equal(t, time.Duration(-1), metaPTTL(t, s, key), "%s: permanent again", read)

			_, err = broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
			require.NoError(t, err)
			require.Equal(t, time.Duration(-1), metaPTTL(t, s, key), "%s: reads keep it permanent", read)
		}
	})
}

// A map with publications has its meta extended by every read, as its reads
// extend its state: an expired meta would make the state stale. Only the meta
// of a map without publications is extended below half its TTL.
func TestRedisMapBroker_MetaExtendedWithState(t *testing.T) {
	for _, initial := range []time.Duration{0, testInitialMetaTTL} {
		runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
			node, err := New(Config{
				HistoryMetaTTLInitial: initial,
				Map: MapConfig{
					GetMapChannelOptions: func(channel string) MapChannelOptions {
						return MapChannelOptions{Mode: MapModeRecoverable, StreamSize: 100, StreamTTL: 300 * time.Second, KeyTTL: time.Hour, MetaTTL: time.Hour}
					},
				},
			})
			require.NoError(t, err)
			broker := mb.make(t, node)
			ctx := context.Background()
			s := broker.shards[0].shard

			empty := randomChannel("map_meta_half_empty")
			_, err = broker.ReadState(ctx, empty, MapReadStateOptions{Limit: 100})
			require.NoError(t, err)
			target := time.Hour
			if initial > 0 {
				target = initial
			}
			aboveHalf := target * 4 / 5
			setPTTL(t, s, broker.metaKey(s, empty), aboveHalf)
			_, err = broker.ReadState(ctx, empty, MapReadStateOptions{Limit: 100})
			require.NoError(t, err)
			require.LessOrEqual(t, metaPTTL(t, s, broker.metaKey(s, empty)), aboveHalf, "empty map above half: not extended, initial %s", initial)

			withState := randomChannel("map_meta_half_state")
			_, err = broker.Publish(ctx, withState, "a", MapPublishOptions{Data: []byte("1")})
			require.NoError(t, err)
			setPTTL(t, s, broker.metaKey(s, withState), 40*time.Minute)
			_, err = broker.ReadState(ctx, withState, MapReadStateOptions{Limit: 100})
			require.NoError(t, err)
			require.Greater(t, metaPTTL(t, s, broker.metaKey(s, withState)), 59*time.Minute, "map with state: extended, initial %s", initial)
		})
	}
}
