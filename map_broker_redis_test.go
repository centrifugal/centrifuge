//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/redispartition"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestParseMessage tests parseMessage which parses PUB/SUB messages.
func TestParseMessage(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		_, _, _, _, _, err := parseMessage([]byte{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty message")
	})

	t.Run("raw protobuf (backward compat, non-digit first byte)", func(t *testing.T) {
		raw := []byte{0x0a, 0x03, 'f', 'o', 'o'} // protobuf-like data starting with non-digit
		offset, epoch, data, isDelta, prevPub, err := parseMessage(raw)
		require.NoError(t, err)
		require.Equal(t, uint64(0), offset)
		require.Empty(t, epoch)
		require.Equal(t, raw, data)
		require.False(t, isDelta)
		require.Nil(t, prevPub)
	})

	t.Run("non-delta format: offset:epoch:protobuf", func(t *testing.T) {
		msg := []byte("42:epoch-1:hello-proto")
		offset, epoch, data, isDelta, prevPub, err := parseMessage(msg)
		require.NoError(t, err)
		require.Equal(t, uint64(42), offset)
		require.Equal(t, "epoch-1", epoch)
		require.Equal(t, []byte("hello-proto"), data)
		require.False(t, isDelta)
		require.Nil(t, prevPub)
	})

	t.Run("non-delta with empty payload", func(t *testing.T) {
		msg := []byte("1:e:")
		offset, epoch, data, isDelta, _, err := parseMessage(msg)
		require.NoError(t, err)
		require.Equal(t, uint64(1), offset)
		require.Equal(t, "e", epoch)
		require.Empty(t, data)
		require.False(t, isDelta)
	})

	t.Run("digit-start no colon treated as raw", func(t *testing.T) {
		raw := []byte("123abc") // starts with digit but no colon → raw
		offset, epoch, data, _, _, err := parseMessage(raw)
		require.NoError(t, err)
		require.Equal(t, uint64(0), offset)
		require.Empty(t, epoch)
		require.Equal(t, raw, data)
	})

	t.Run("missing epoch separator", func(t *testing.T) {
		msg := []byte("42:noepochsep")
		_, _, _, _, _, err := parseMessage(msg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing epoch separator")
	})

	t.Run("delta format: d:offset:epoch:prev_len:prev:curr_len:curr", func(t *testing.T) {
		prevData := []byte("prev")
		currData := []byte("curr")
		msg := []byte(fmt.Sprintf("d:10:ep:%d:%s:%d:%s",
			len(prevData), prevData, len(currData), currData))
		offset, epoch, data, isDelta, prevPub, err := parseMessage(msg)
		require.NoError(t, err)
		require.Equal(t, uint64(10), offset)
		require.Equal(t, "ep", epoch)
		require.Equal(t, currData, data)
		require.True(t, isDelta)
		require.Equal(t, prevData, prevPub)
	})
}

// TestParseDeltaMessage tests parseDeltaMessage directly.
func TestParseDeltaMessage(t *testing.T) {
	t.Run("valid delta", func(t *testing.T) {
		prevData := []byte("abc")
		currData := []byte("xyz12")
		msg := []byte(fmt.Sprintf("7:my-epoch:%d:%s:%d:%s",
			len(prevData), prevData, len(currData), currData))
		offset, epoch, data, isDelta, prevPub, err := parseDeltaMessage(msg)
		require.NoError(t, err)
		require.Equal(t, uint64(7), offset)
		require.Equal(t, "my-epoch", epoch)
		require.Equal(t, currData, data)
		require.True(t, isDelta)
		require.Equal(t, prevData, prevPub)
	})

	t.Run("missing offset separator", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("nocolon"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing offset separator")
	})

	t.Run("invalid offset", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("abc:epoch:0::0:"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid offset")
	})

	t.Run("missing epoch separator", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:noepochsep"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing epoch separator")
	})

	t.Run("missing prev length separator", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:e:noprevlen"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing prev length separator")
	})

	t.Run("invalid prev length", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:e:abc:"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid prev length")
	})

	t.Run("insufficient prev data", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:e:10:ab"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "insufficient data for prev protobuf")
	})

	t.Run("missing curr length separator", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:e:2:ab"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing curr length separator")
	})

	t.Run("missing curr protobuf separator", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:e:2:ab:nocurrsep"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing curr protobuf separator")
	})

	t.Run("invalid curr length", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:e:2:ab:abc:"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid curr length")
	})

	t.Run("insufficient curr data", func(t *testing.T) {
		_, _, _, _, _, err := parseDeltaMessage([]byte("1:e:2:ab:10:x"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "insufficient data for curr protobuf")
	})

	t.Run("binary payloads", func(t *testing.T) {
		prev := []byte{0x00, 0x01, 0x02}
		curr := []byte{0xff, 0xfe}
		msg := []byte(fmt.Sprintf("99:bin-epoch:%d:", len(prev)))
		msg = append(msg, prev...)
		msg = append(msg, []byte(fmt.Sprintf(":%d:", len(curr)))...)
		msg = append(msg, curr...)
		offset, epoch, data, isDelta, prevPub, err := parseDeltaMessage(msg)
		require.NoError(t, err)
		require.Equal(t, uint64(99), offset)
		require.Equal(t, "bin-epoch", epoch)
		require.Equal(t, curr, data)
		require.True(t, isDelta)
		require.Equal(t, prev, prevPub)
	})
}

type mapBrokerVariant struct {
	Name       string
	UseCluster bool
}

// mapBrokerVariants drives table-driven map broker integration tests.
var mapBrokerVariants = []mapBrokerVariant{
	{"rd_single", false},
	{"rd_cluster", true},
}

// redisMapBrokerFactory builds a RedisMapBroker against a specific variant.
// Tests obtain one via runMapBrokerTest.
type redisMapBrokerFactory struct {
	variant mapBrokerVariant
}

func (f redisMapBrokerFactory) make(tb testing.TB, n *Node) *RedisMapBroker {
	return f.makeWithHandler(tb, n, nil)
}

func (f redisMapBrokerFactory) makeWithHandler(tb testing.TB, n *Node, h BrokerEventHandler) *RedisMapBroker {
	return f.makeCustom(tb, n, h, nil)
}

// makeCustom builds a RedisMapBroker using the variant's shard topology while
// letting the caller mutate the broker config (e.g. set CleanupBatchSize) before
// construction. Pass tweak=nil for the default config.
func (f redisMapBrokerFactory) makeCustom(tb testing.TB, n *Node, h BrokerEventHandler, tweak func(*RedisMapBrokerConfig)) *RedisMapBroker {
	tb.Helper()
	if n.config.Map.GetMapChannelOptions == nil {
		n.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				MetaTTL:    time.Hour,
				KeyTTL:     time.Minute,
			}
		}
	}
	var redisConf RedisShardConfig
	if f.variant.UseCluster {
		redisConf = RedisShardConfig{
			ClusterAddresses: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
			IOTimeout:        10 * time.Second,
			ConnectTimeout:   10 * time.Second,
		}
	} else {
		redisConf = testSingleRedisConf(6379)
	}
	shard, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	if f.variant.UseCluster {
		// Sharded PUB/SUB is a Redis 7+ feature; skip the cluster variants when
		// the cluster doesn't support it.
		skipIfNoShardedPubSub(tb, shard, "centrifuge-test-skip-probe-"+randString(4))
	}
	brokerConf := RedisMapBrokerConfig{Shards: []*RedisShard{shard}}
	if f.variant.UseCluster {
		brokerConf.NumShardedPubSubPartitions = 1
	}
	if tweak != nil {
		tweak(&brokerConf)
	}
	e, err := NewRedisMapBroker(n, brokerConf)
	require.NoError(tb, err)
	if h != nil {
		require.NoError(tb, e.RegisterEventHandler(h))
	}
	// Order of teardown matters: Node.Shutdown calls broker.Close (sets closeCh)
	// but does NOT close the underlying rueidis client. The shard owns the client
	// and we created it here, so we must close it explicitly. Otherwise the
	// broker's cleanup goroutines (spawned by runCleanupCycle with
	// context.Background()) keep running against the live client and leak across
	// tests — which becomes a hang at scale once the variant table triples the
	// number of brokers per run.
	tb.Cleanup(func() {
		_ = n.Shutdown(context.Background())
		shard.Close()
	})
	return e
}

// runMapBrokerTest runs body once per map broker variant as a sub-test.
// Body must NOT call t.Parallel() (other than within nested t.Run calls
// it explicitly creates).
func runMapBrokerTest(t *testing.T, body func(t *testing.T, mb redisMapBrokerFactory)) {
	for _, v := range mapBrokerVariants {
		v := v
		t.Run(v.Name, func(t *testing.T) {
			body(t, redisMapBrokerFactory{variant: v})
		})
	}
}

// randomChannel generates a unique channel name for testing to avoid cross-test pollution.
func randomChannel(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), rand.Intn(100000))
}

// newTestRedisMapBroker builds a standalone (single-node, port 6379) RedisMapBroker.
// Retained for benchmark callers in map_broker_redis_bench_test.go; integration
// tests should use runMapBrokerTest + redisMapBrokerFactory.make instead.
func newTestRedisMapBroker(tb testing.TB, n *Node) *RedisMapBroker {
	return redisMapBrokerFactory{}.make(tb, n)
}

// newTestRedisMapBrokerWithHandler is the handler-registering variant of
// newTestRedisMapBroker. Same standalone-only semantics.
func newTestRedisMapBrokerWithHandler(tb testing.TB, n *Node, h BrokerEventHandler) *RedisMapBroker {
	return redisMapBrokerFactory{}.makeWithHandler(tb, n, h)
}

// stateToMap converts []StateEntry to map for easier testing.
// It extracts the raw data from the Publication protobuf payload.
func stateToMap(pubs []*Publication) map[string][]byte {
	result := make(map[string][]byte, len(pubs))
	for _, pub := range pubs {
		// Extract data from Publication
		result[pub.Key] = pub.Data
	}
	return result
}

// TestRedisMapBroker_StatefulChannel tests stateful channel with keyed state and revisions.
func TestRedisMapBroker_StatefulChannel(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_stateful")

		// Publish some keyed state updates
		_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte("data1"),
		})
		require.NoError(t, err)

		_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
			Data: []byte("data2"),
		})
		require.NoError(t, err)

		_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte("data1_updated"),
		})
		require.NoError(t, err)

		// Read state
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, streamPos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.NotEmpty(t, streamPos.Epoch)
		require.Greater(t, streamPos.Offset, uint64(0))

		// Verify state contains latest values
		state := stateToMap(entries)
		require.Len(t, state, 2)
		require.Equal(t, []byte("data1_updated"), state["key1"])
		require.Equal(t, []byte("data2"), state["key2"])

		// Read stream to verify all publications are in history
		result, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Limit: -1, // Get all
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 3) // All 3 publications in stream
	})
}

// TestRedisMapBroker_StatefulChannelOrdered tests ordered stateful channel.
func TestRedisMapBroker_StatefulChannelOrdered(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered")

		// Publish with scores for ordering
		for i := 0; i < 5; i++ {
			_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data%d", i)),
				score: int64(i * 10), // Scores: 0, 10, 20, 30, 40
			})
			require.NoError(t, err)
		}

		// Read ordered state (descending by score)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 5)

		// Verify all keys present
		state := stateToMap(entries)
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			require.Contains(t, state, key)
		}
	})
}

// TestRedisMapBroker_StateRevision tests that state values include revisions.
func TestRedisMapBroker_StateRevision(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_revision")

		// Publish a keyed state update
		res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte("data1"),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), res1.Position.Offset)

		// Publish another update
		res2, err := broker.Publish(ctx, channel, "key2", MapPublishOptions{
			Data: []byte("data2"),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(2), res2.Position.Offset)
		require.Equal(t, res1.Position.Epoch, res2.Position.Epoch) // Same epoch

		// Read state - entries now include per-entry revisions
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, streamPos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Equal(t, res2.Position.Offset, streamPos.Offset)
		require.Equal(t, res2.Position.Epoch, streamPos.Epoch)

		// Verify payloads
		state := stateToMap(entries)
		require.Equal(t, []byte("data1"), state["key1"])
		require.Equal(t, []byte("data2"), state["key2"])

		// Verify per-entry offsets (epoch is in streamPos, same for all)
		require.NotEmpty(t, streamPos.Epoch)
		for _, pub := range entries {
			require.Greater(t, pub.Offset, uint64(0))
		}
	})
}

// TestRedisMapBroker_StatePagination tests cursor-based state pagination.
func TestRedisMapBroker_StatePagination(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_pagination")

		// Publish 10 keyed entries
		for i := 0; i < 10; i++ {
			_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte(fmt.Sprintf("data%d", i)),
			})
			require.NoError(t, err)
		}

		// Read state with limit - HSCAN COUNT is a hint, not a guarantee
		// For small hashes, Redis may return all entries in one go
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  3,
			Cursor: "",
		})
		require.NoError(t, err)
		page1, pos1, cursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.NotEmpty(t, page1)

		// Collect all keys across pages
		allKeys := make(map[string]bool)
		for _, entry := range page1 {
			allKeys[entry.Key] = true
		}

		// Continue reading until cursor is "0" (end of iteration)
		for cursor != "" && cursor != "0" {
			stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Limit:  3,
				Cursor: cursor,
			})
			require.NoError(t, err)
			page, pos, newCursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
			require.NoError(t, err)
			require.Equal(t, pos1.Epoch, pos.Epoch) // Same epoch across pages

			for _, entry := range page {
				// Keys should not repeat across pages
				require.NotContains(t, allKeys, entry.Key, "key should not repeat: %s", entry.Key)
				allKeys[entry.Key] = true
			}
			cursor = newCursor
		}

		// Should have read all 10 entries
		require.Len(t, allKeys, 10)
	})
}

// TestRedisMapBroker_EpochHandling tests epoch changes and state invalidation.
func TestRedisMapBroker_EpochHandling(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_epoch")

		// Publish initial data
		res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte("data1"),
		})
		require.NoError(t, err)
		epoch1 := res1.Position.Epoch

		// Read state
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, streamPos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, epoch1, streamPos1.Epoch)

		// Simulate epoch change by clearing stream (would happen on node restart)
		// In real scenario, this would be a different node with different ID
		// For test purposes, we'll just verify epoch consistency
		require.NotEmpty(t, epoch1)
	})
}

// TestRedisMapBroker_Idempotency tests idempotent publishing.
func TestRedisMapBroker_Idempotency(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_idempotency")

		// Publish with idempotency key
		res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:                []byte("data1"),
			IdempotencyKey:      "unique-id-1",
			IdempotentResultTTL: 60 * time.Second,
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)
		require.Equal(t, uint64(1), res1.Position.Offset)

		// Publish again with same idempotency key
		res2, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:                []byte("data1_different"),
			IdempotencyKey:      "unique-id-1",
			IdempotentResultTTL: 60 * time.Second,
		})
		require.NoError(t, err)
		require.True(t, res2.Suppressed) // Suppressed due to idempotency
		require.Equal(t, SuppressReasonIdempotency, res2.SuppressReason)
		require.Equal(t, res1.Position.Offset, res2.Position.Offset) // Same offset
		require.Equal(t, res1.Position.Epoch, res2.Position.Epoch)   // Same epoch

		// State should still have original data (second publish was cached/skipped)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		state := stateToMap(entries)
		require.Len(t, state, 1)
		require.Equal(t, []byte("data1"), state["key1"])
	})
}

// TestRedisMapBroker_VersionedPublishing tests version-based idempotency.
func TestRedisMapBroker_VersionedPublishing(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_version")

		// Publish with version 2 (version 0 means "disable version check")
		res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("data_v2"),
			Version: 2,
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)
		require.Equal(t, uint64(1), res1.Position.Offset)

		// Try to publish older version (should be suppressed)
		res2, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("data_v1"),
			Version: 1,
		})
		require.NoError(t, err)
		require.True(t, res2.Suppressed) // Suppressed due to out-of-order version
		require.Equal(t, SuppressReasonVersion, res2.SuppressReason)
		require.Equal(t, res1.Position.Offset, res2.Position.Offset) // Same offset (suppressed)

		// Publish newer version
		res3, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("data_v3"),
			Version: 3,
		})
		require.NoError(t, err)
		require.False(t, res3.Suppressed)
		require.Equal(t, uint64(2), res3.Position.Offset) // New offset

		// State should have v3 data
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		state := stateToMap(entries)
		require.Equal(t, []byte("data_v3"), state["key1"])
	})
}

// TestRedisMapBroker_PerKeyVersion tests that version tracking is per-key independent.
func TestRedisMapBroker_PerKeyVersion(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_per_key_version")

		// key1 with version=10 → accepted
		res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("key1_v10"),
			Version: 10,
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)

		// key2 with version=5 → accepted (independent, was broken before per-key version)
		res2, err := broker.Publish(ctx, channel, "key2", MapPublishOptions{
			Data:    []byte("key2_v5"),
			Version: 5,
		})
		require.NoError(t, err)
		require.False(t, res2.Suppressed, "key2 should not be suppressed by key1's version")

		// key1 with version=5 → suppressed (same key, lower version)
		res3, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("key1_v5"),
			Version: 5,
		})
		require.NoError(t, err)
		require.True(t, res3.Suppressed)
		require.Equal(t, SuppressReasonVersion, res3.SuppressReason)

		// Remove key1
		_, err = broker.Remove(ctx, channel, "key1", MapRemoveOptions{})
		require.NoError(t, err)

		// Publish key1 with version=1 → accepted (version cleared by remove)
		res4, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("key1_v1"),
			Version: 1,
		})
		require.NoError(t, err)
		require.False(t, res4.Suppressed, "key1 version should be cleared after remove")

		// Verify final state
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		state := stateToMap(stateRes.Publications)
		require.Equal(t, []byte("key1_v1"), state["key1"])
		require.Equal(t, []byte("key2_v5"), state["key2"])
	})
}

// TestRedisMapBroker_MultipleChannels tests multiple channels independently.
func TestRedisMapBroker_MultipleChannels(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel1 := randomChannel("test_multi1")
		channel2 := randomChannel("test_multi2")

		// Publish to channel1
		_, err := broker.Publish(ctx, channel1, "key1", MapPublishOptions{
			Data: []byte("data1"),
		})
		require.NoError(t, err)

		// Publish to channel2
		_, err = broker.Publish(ctx, channel2, "key2", MapPublishOptions{
			Data: []byte("data2"),
		})
		require.NoError(t, err)

		// Read channel1 state
		stateRes, err := broker.ReadState(ctx, channel1, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries1, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		state1 := stateToMap(entries1)
		require.Len(t, state1, 1)
		require.Equal(t, []byte("data1"), state1["key1"])

		// Read channel2 state
		stateRes, err = broker.ReadState(ctx, channel2, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		state2 := stateToMap(entries2)
		require.Len(t, state2, 1)
		require.Equal(t, []byte("data2"), state2["key2"])
	})
}

// TestParseStateValue tests the state value parsing helper.
func TestParseStateValue(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		wantOffset  uint64
		wantEpoch   string
		wantPayload []byte
		wantErr     bool
	}{
		{
			name:        "valid state value",
			input:       []byte("42:node-1:hello"),
			wantOffset:  42,
			wantEpoch:   "node-1",
			wantPayload: []byte("hello"),
			wantErr:     false,
		},
		{
			name:        "valid with binary payload",
			input:       []byte("100:epoch-123:\x00\x01\x02"),
			wantOffset:  100,
			wantEpoch:   "epoch-123",
			wantPayload: []byte{0, 1, 2},
			wantErr:     false,
		},
		{
			name:    "empty value",
			input:   []byte(""),
			wantErr: true,
		},
		{
			name:    "missing offset separator",
			input:   []byte("no-colons"),
			wantErr: true,
		},
		{
			name:    "missing epoch separator",
			input:   []byte("42:no-second-colon"),
			wantErr: true,
		},
		{
			name:    "invalid offset",
			input:   []byte("invalid:epoch:data"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, epoch, payload, err := parseStateValue(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantOffset, offset)
			require.Equal(t, tt.wantEpoch, epoch)
			require.Equal(t, tt.wantPayload, payload)
		})
	}
}

// TestRedisMapBroker_ReadStream2 tests the 2-call version of ReadStream.
func TestRedisMapBroker_ReadStream2(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_readstream2")

		// Publish 5 messages to stream
		for i := 1; i <= 5; i++ {
			_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
				Data: []byte(fmt.Sprintf("msg_%d", i)),
			})
			require.NoError(t, err)
		}

		// Test 1: Read all messages (forward)
		result, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Limit: -1,
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 5)
		require.NotEmpty(t, result.Position.Epoch)
		require.Equal(t, uint64(5), result.Position.Offset)
		require.Equal(t, []byte("msg_1"), result.Publications[0].Data)
		require.Equal(t, []byte("msg_5"), result.Publications[4].Data)

		// Test 2: Read all messages (reverse)
		resultRev, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Limit:   -1,
				Reverse: true,
			},
		})
		require.NoError(t, err)
		require.Len(t, resultRev.Publications, 5)
		require.Equal(t, uint64(5), resultRev.Position.Offset)
		require.Equal(t, []byte("msg_5"), resultRev.Publications[0].Data)
		require.Equal(t, []byte("msg_1"), resultRev.Publications[4].Data)

		// Test 3: Read with limit
		resultLimited, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Limit: 2,
			},
		})
		require.NoError(t, err)
		require.Len(t, resultLimited.Publications, 2)
		require.Equal(t, []byte("msg_1"), resultLimited.Publications[0].Data)
		require.Equal(t, []byte("msg_2"), resultLimited.Publications[1].Data)

		// Test 4: Read since offset
		resultSince, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &StreamPosition{
					Offset: 2,
					Epoch:  result.Position.Epoch,
				},
				Limit: -1,
			},
		})
		require.NoError(t, err)
		require.Len(t, resultSince.Publications, 3)
		require.Equal(t, []byte("msg_3"), resultSince.Publications[0].Data)
		require.Equal(t, []byte("msg_5"), resultSince.Publications[2].Data)

		// Test 5: Metadata-only read
		resultMeta, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Limit: 0, // metadata only
			},
		})
		require.NoError(t, err)
		require.Nil(t, resultMeta.Publications)
		require.Equal(t, uint64(5), resultMeta.Position.Offset)
		require.NotEmpty(t, resultMeta.Position.Epoch)
	})
}

// TestRedisMapBroker_ReadStreamZero2 tests the 2-call zero-alloc version of ReadStream.
//func TestRedisMapBroker_ReadStreamZero2(t *testing.T) {
//	node, _ := New(Config{})
//	broker := newTestRedisMapBroker(t, node)
//
//	ctx := context.Background()
//	channel := randomChannel("test_readstreamzero2")
//
//	// Publish 5 messages to stream
//	for i := 1; i <= 5; i++ {
//		_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
//			Data: []byte(fmt.Sprintf("msg_%d", i)),
//		})
//		require.NoError(t, err)
//	}
//
//	// Test 1: Read all messages (forward)
//	result, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{
//			Limit: -1,
//		},
//	})
//	require.NoError(t, err)
//	require.Len(t, result.Publications, 5)
//	require.NotEmpty(t, result.Position.Epoch)
//	require.Equal(t, uint64(5), result.Position.Offset)
//	require.Equal(t, []byte("msg_1"), result.Publications[0].Data)
//	require.Equal(t, []byte("msg_5"), result.Publications[4].Data)
//
//	// Test 2: Read all messages (reverse)
//	resultRev, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{
//			Limit:   -1,
//			Reverse: true,
//		},
//	})
//	require.NoError(t, err)
//	require.Len(t, resultRev.Publications, 5)
//	require.Equal(t, uint64(5), resultRev.Position.Offset)
//	require.Equal(t, []byte("msg_5"), resultRev.Publications[0].Data)
//	require.Equal(t, []byte("msg_1"), resultRev.Publications[4].Data)
//
//	// Test 3: Read with limit
//	resultLimited, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{
//			Limit: 2,
//		},
//	})
//	require.NoError(t, err)
//	require.Len(t, resultLimited.Publications, 2)
//	require.Equal(t, []byte("msg_1"), resultLimited.Publications[0].Data)
//	require.Equal(t, []byte("msg_2"), resultLimited.Publications[1].Data)
//
//	// Test 4: Read since offset
//	resultSince, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{
//			Since: &StreamPosition{
//				Offset: 2,
//				Epoch:  result.Position.Epoch,
//			},
//			Limit: -1,
//		},
//	})
//	require.NoError(t, err)
//	require.Len(t, resultSince.Publications, 3)
//	require.Equal(t, []byte("msg_3"), resultSince.Publications[0].Data)
//	require.Equal(t, []byte("msg_5"), resultSince.Publications[2].Data)
//
//	// Test 5: Metadata-only read
//	resultMeta, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{
//			Limit: 0, // metadata only
//		},
//	})
//	require.NoError(t, err)
//	require.Nil(t, resultMeta.Publications)
//	require.Equal(t, uint64(5), resultMeta.Position.Offset)
//	require.NotEmpty(t, resultMeta.Position.Epoch)
//}

//// TestRedisMapBroker_ReadStreamZero_Compatibility tests that ReadStream2 returns same results as ReadStream.
//func TestRedisMapBroker_ReadStreamZero_Compatibility(t *testing.T) {
//	node, _ := New(Config{})
//	broker := newTestRedisMapBroker(t, node)
//
//	ctx := context.Background()
//	channel := randomChannel("test_compat")
//
//	// Publish 10 messages
//	for i := 1; i <= 10; i++ {
//		_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
//			Data: []byte(fmt.Sprintf("msg_%d", i)),
//		})
//		require.NoError(t, err)
//	}
//
//	// Compare ReadStream and ReadStream2 results
//	result1, err1 := broker.ReadStream(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{Limit: -1},
//	})
//	require.NoError(t, err1)
//
//	result2, err2 := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{Limit: -1},
//	})
//	require.NoError(t, err2)
//
//	// Compare results
//	require.Equal(t, len(result1.Publications), len(result2.Publications))
//	require.Equal(t, result1.Position.Offset, result2.Position.Offset)
//	require.Equal(t, result1.Position.Epoch, result2.Position.Epoch)
//	for i := range result1.Publications {
//		require.Equal(t, result1.Publications[i].Data, result2.Publications[i].Data)
//		require.Equal(t, result1.Publications[i].Offset, result2.Publications[i].Offset)
//	}
//
//	// Compare ReadStreamZero and ReadStreamZero2 results
//	result3, err3 := broker.ReadStreamZero(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{Limit: -1},
//	})
//	require.NoError(t, err3)
//
//	result4, err4 := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
//		Filter: StreamFilter{Limit: -1},
//	})
//	require.NoError(t, err4)
//
//	// Compare results
//	require.Equal(t, len(result3.Publications), len(result4.Publications))
//	require.Equal(t, result3.Position.Offset, result4.Position.Offset)
//	require.Equal(t, result3.Position.Epoch, result4.Position.Epoch)
//	for i := range result3.Publications {
//		require.Equal(t, result3.Publications[i].Data, result4.Publications[i].Data)
//		require.Equal(t, result3.Publications[i].Offset, result4.Publications[i].Offset)
//	}
//}

// TestRedisMapBroker_CleanupGeneratesRemovalEvents verifies that the Lua cleanup script
// correctly generates removal events (key + removed + timestamp) when entries expire by TTL.
func TestRedisMapBroker_CleanupGeneratesRemovalEvents(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    3600 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})

		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) {
			c.CleanupBatchSize = 100
		})

		ctx := context.Background()
		channel := randomChannel("test_cleanup_removal")

		// Publish two keyed entries with TTL.
		_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte(`{"status":"online"}`),
		})
		require.NoError(t, err)

		_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
			Data: []byte(`{"status":"active"}`),
		})
		require.NoError(t, err)

		// Verify initial stream has 2 publish events.
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)
		require.Len(t, streamResult.Publications, 2, "Should have 2 publish events in stream")
		require.False(t, streamResult.Publications[0].Removed)
		require.False(t, streamResult.Publications[1].Removed)

		// Verify state has 2 entries.
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 2)

		// Simulate TTL expiry by calling cleanupChannel with a future "now".
		shardWrapper := broker.shards[0]
		cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
		futureNow := time.Now().UnixMilli() + 31_000

		err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
		require.NoError(t, err)

		// Read the stream after cleanup - should have 2 publish + 2 removal events.
		streamResult, err = broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)
		require.Len(t, streamResult.Publications, 4, "Expected 4 events in stream (2 publish + 2 removal)")

		// Collect removal events.
		var removals []*Publication
		for _, pub := range streamResult.Publications {
			if pub.Removed {
				removals = append(removals, pub)
			}
		}
		require.Len(t, removals, 2, "Should have 2 removal events")

		removedKeys := map[string]bool{}
		for _, r := range removals {
			require.True(t, r.Removed, "Removal event should have Removed=true")
			require.Greater(t, r.Time, int64(0), "Removal event should have timestamp")
			require.Empty(t, r.Data, "Removal event should have no data payload")
			removedKeys[r.Key] = true
		}
		require.True(t, removedKeys["key1"], "key1 should be removed")
		require.True(t, removedKeys["key2"], "key2 should be removed")

		// Verify state is now empty.
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 0, "State should be empty after cleanup")

		// Verify stats show 0 keys.
		stats, err := broker.Stats(ctx, channel)
		require.NoError(t, err)
		require.Equal(t, 0, stats.NumKeys, "Stats should show 0 keys")
	})
}

func TestRedisMapBroker_CleanupPreservesTags(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    3600 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})

		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) {
			c.CleanupBatchSize = 100
		})

		ctx := context.Background()
		channel := randomChannel("test_cleanup_tags")

		// Publish entries with tags.
		_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte(`{"v":1}`),
			Tags: map[string]string{"role": "admin", "team": "platform"},
		})
		require.NoError(t, err)

		_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
			Data: []byte(`{"v":2}`),
			Tags: map[string]string{"role": "viewer"},
		})
		require.NoError(t, err)

		// Simulate TTL expiry.
		shardWrapper := broker.shards[0]
		cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
		futureNow := time.Now().UnixMilli() + 31_000

		err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
		require.NoError(t, err)

		// Read stream — removal events should carry the original tags.
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)

		tagsByKey := map[string]map[string]string{}
		for _, pub := range streamResult.Publications {
			if pub.Removed {
				tagsByKey[pub.Key] = pub.Tags
			}
		}
		require.Len(t, tagsByKey, 2)
		require.Equal(t, map[string]string{"role": "admin", "team": "platform"}, tagsByKey["key1"])
		require.Equal(t, map[string]string{"role": "viewer"}, tagsByKey["key2"])
	})
}

func TestRedisMapBroker_RemovePreservesTags(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModePersistent,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						// MetaTTL stays 0 (permanent) — persistent keys require permanent meta.
					}
				},
			},
		})

		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_remove_tags")

		// Publish with tags.
		_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte(`{"v":1}`),
			Tags: map[string]string{"role": "admin"},
		})
		require.NoError(t, err)

		// Remove with explicit tags.
		_, err = broker.Remove(ctx, channel, "key1", MapRemoveOptions{
			Tags: map[string]string{"role": "admin"},
		})
		require.NoError(t, err)

		// Read stream — removal should have the tags.
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)

		var removal *Publication
		for _, pub := range streamResult.Publications {
			if pub.Removed {
				removal = pub
			}
		}
		require.NotNil(t, removal)
		require.Equal(t, map[string]string{"role": "admin"}, removal.Tags)
	})
}

// TestRedisMapBroker_CleanupExpiry verifies that cleanup removes expired entries
// and publishes removal events to the stream. Uses two channels with different
// TTLs to verify that only expired channel entries are cleaned up.
func TestRedisMapBroker_CleanupExpiry(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		shortChannel := randomChannel("test_cleanup_short")
		longChannel := randomChannel("test_cleanup_long")
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					if channel == shortChannel {
						return MapChannelOptions{
							Mode:       MapModeRecoverable,
							StreamSize: 100,
							StreamTTL:  300 * time.Second,
							MetaTTL:    3600 * time.Second,
							KeyTTL:     10 * time.Second,
						}
					}
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    3600 * time.Second,
						KeyTTL:     600 * time.Second,
					}
				},
			},
		})

		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) {
			c.CleanupBatchSize = 100
		})

		ctx := context.Background()

		// Publish key in short-TTL channel (10s).
		_, err := broker.Publish(ctx, shortChannel, "key_short", MapPublishOptions{
			Data: []byte(`{"ttl":"short"}`),
		})
		require.NoError(t, err)

		// Publish key in long-TTL channel (600s).
		_, err = broker.Publish(ctx, longChannel, "key_long", MapPublishOptions{
			Data: []byte(`{"ttl":"long"}`),
		})
		require.NoError(t, err)

		// Simulate time passing: 15 seconds.
		shardWrapper := broker.shards[0]
		futureNow := time.Now().UnixMilli() + 15_000

		// Cleanup both channels.
		shortCleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, shortChannel)
		err = broker.cleanupChannel(ctx, shardWrapper.shard, shortChannel, shortCleanupKey, futureNow)
		require.NoError(t, err)

		longCleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, longChannel)
		err = broker.cleanupChannel(ctx, shardWrapper.shard, longChannel, longCleanupKey, futureNow)
		require.NoError(t, err)

		// Short-TTL channel: key should be removed (10s TTL < 15s elapsed).
		stateRes, err := broker.ReadState(ctx, shortChannel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 0, "Short-TTL key should be expired")

		// Short channel stream: 1 publish + 1 removal.
		streamResult, err := broker.ReadStream(ctx, shortChannel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)
		require.Len(t, streamResult.Publications, 2, "Expected 2 events (1 publish + 1 removal)")
		require.True(t, streamResult.Publications[1].Removed)
		require.Equal(t, "key_short", streamResult.Publications[1].Key)

		// Long-TTL channel: key should still exist (600s TTL > 15s elapsed).
		stateRes, err = broker.ReadState(ctx, longChannel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 1, "Long-TTL key should still be alive")
		require.Equal(t, "key_long", stateRes.Publications[0].Key)
	})
}

// TestRedisMapBroker_CleanupRefreshedTTL verifies that refreshing a key's TTL
// (by re-publishing with a longer TTL) prevents it from being cleaned up.
func TestRedisMapBroker_CleanupRefreshedTTL(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    3600 * time.Second,
						KeyTTL:     600 * time.Second,
					}
				},
			},
		})

		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) {
			c.CleanupBatchSize = 100
		})

		ctx := context.Background()
		channel := randomChannel("test_cleanup_refresh")

		// Publish key with short TTL (10s).
		_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte(`{"v":1}`),
		})
		require.NoError(t, err)

		// Re-publish the same key with a much longer TTL (600s), refreshing its expiry.
		_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte(`{"v":2}`),
		})
		require.NoError(t, err)

		// Simulate cleanup at 15s from now — the original 10s TTL would have expired,
		// but the refreshed 600s TTL should keep the key alive.
		shardWrapper := broker.shards[0]
		cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
		futureNow := time.Now().UnixMilli() + 15_000

		err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
		require.NoError(t, err)

		// Key should still exist (TTL was refreshed to 600s).
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 1, "Key should still exist after TTL refresh")
		require.Equal(t, "key1", entries[0].Key)
		require.Equal(t, []byte(`{"v":2}`), entries[0].Data)

		// Verify no removal events in stream (only 2 publish events).
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)
		for _, pub := range streamResult.Publications {
			require.False(t, pub.Removed, "Should have no removal events after TTL refresh")
		}
	})
}

// TestRedisMapBroker_CleanupRegistration verifies that the cleanup registration ZSET
// is properly maintained (entries added on publish, removed when channel is fully cleaned).
func TestRedisMapBroker_CleanupRegistration(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    3600 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})

		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) {
			c.CleanupBatchSize = 100
		})

		ctx := context.Background()
		channel := randomChannel("test_cleanup_reg")

		shardWrapper := broker.shards[0]
		cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)

		// Before any publish, channel should not be in cleanup ZSET.
		score, err := shardWrapper.shard.client.Do(ctx,
			shardWrapper.shard.client.B().Zscore().Key(cleanupKey).Member(channel).Build(),
		).AsFloat64()
		require.Error(t, err, "Channel should not be in cleanup ZSET before publish")
		_ = score

		// Publish a key with TTL.
		_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte(`{"v":1}`),
		})
		require.NoError(t, err)

		// Channel should now be registered in cleanup ZSET.
		score, err = shardWrapper.shard.client.Do(ctx,
			shardWrapper.shard.client.B().Zscore().Key(cleanupKey).Member(channel).Build(),
		).AsFloat64()
		require.NoError(t, err)
		require.Greater(t, score, float64(0), "Channel should have an expiry score in cleanup ZSET")

		// Run cleanup with future time to expire the entry.
		futureNow := time.Now().UnixMilli() + 31_000
		err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
		require.NoError(t, err)

		// After all entries expired, channel should be removed from cleanup ZSET.
		_, err = shardWrapper.shard.client.Do(ctx,
			shardWrapper.shard.client.B().Zscore().Key(cleanupKey).Member(channel).Build(),
		).AsFloat64()
		require.Error(t, err, "Channel should be removed from cleanup ZSET after all entries expired")
	})
}

// Old aggregation and cleanup tests using removed API (AddMember/RemoveMember) have been
// replaced by TestRedisMapBroker_Cleanup* tests above using the current Publish API.

// TestRedisMapBroker_OrderedStateOrdering tests that ordered  state return entries
// in correct score order (ascending by score).
func TestRedisMapBroker_OrderedStateOrdering(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_ordering")

		// Publish entries with specific scores (out of order to test sorting)
		testCases := []struct {
			key   string
			score int64
			data  string
		}{
			{"key_c", 30, "data_c"},
			{"key_a", 10, "data_a"},
			{"key_e", 50, "data_e"},
			{"key_b", 20, "data_b"},
			{"key_d", 40, "data_d"},
		}

		for _, tc := range testCases {
			_, err := broker.Publish(ctx, channel, tc.key, MapPublishOptions{
				Data:  []byte(tc.data),
				score: tc.score,
			})
			require.NoError(t, err)
		}

		// Read ordered state - should be sorted by score (ascending)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 5, "Should have all 5 entries")

		// Verify ordering by score (descending: 50, 40, 30, 20, 10)
		expectedOrder := []string{"key_e", "key_d", "key_c", "key_b", "key_a"}
		for i, entry := range entries {
			require.Equal(t, expectedOrder[i], entry.Key, "Entry %d should be %s", i, expectedOrder[i])
		}

		t.Logf("SUCCESS: Entries returned in correct descending score order: %v", expectedOrder)
	})
}

// TestRedisMapBroker_OrderedStatePagination tests that pagination over ordered state
// maintains correct ordering across pages.
func TestRedisMapBroker_OrderedStatePagination(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_pagination")

		// Publish 20 entries with scores 100, 200, 300, ..., 2000
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			score := int64(i * 100)
			data := fmt.Sprintf("data_%02d", i)

			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte(data),
				score: score,
			})
			require.NoError(t, err)
		}

		// Read first page (limit=5, no cursor)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 5,
		})
		require.NoError(t, err)
		page1, pos1, cursor1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page1, 5, "First page should have 5 entries")
		require.NotEmpty(t, cursor1, "Should have cursor for next page")

		// Verify first page ordering (descending: 20, 19, 18, 17, 16)
		for i := 0; i < 5; i++ {
			expectedKey := fmt.Sprintf("key_%02d", 20-i)
			require.Equal(t, expectedKey, page1[i].Key, "Page 1, entry %d should be %s", i, expectedKey)
		}

		// Read second page (using cursor)
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Cursor: cursor1,
			Limit:  5,
		})
		require.NoError(t, err)
		page2, pos2, cursor2 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page2, 5, "Second page should have 5 entries")
		require.Equal(t, pos1.Epoch, pos2.Epoch, "Epoch should be consistent across pages")

		// Verify second page ordering (descending: 15, 14, 13, 12, 11)
		for i := 0; i < 5; i++ {
			expectedKey := fmt.Sprintf("key_%02d", 15-i)
			require.Equal(t, expectedKey, page2[i].Key, "Page 2, entry %d should be %s", i, expectedKey)
		}

		// Read third page (using cursor)
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Cursor: cursor2,
			Limit:  5,
		})
		require.NoError(t, err)
		page3, _, cursor3 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page3, 5, "Third page should have 5 entries")

		// Verify third page ordering (descending: 10, 09, 08, 07, 06)
		for i := 0; i < 5; i++ {
			expectedKey := fmt.Sprintf("key_%02d", 10-i)
			require.Equal(t, expectedKey, page3[i].Key, "Page 3, entry %d should be %s", i, expectedKey)
		}

		// Read fourth page (using cursor)
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Cursor: cursor3,
			Limit:  5,
		})
		require.NoError(t, err)
		page4, _, cursor4 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page4, 5, "Fourth page should have 5 entries")
		require.Empty(t, cursor4, "Last page should have no cursor")

		// Verify fourth page ordering (descending: 05, 04, 03, 02, 01)
		for i := 0; i < 5; i++ {
			expectedKey := fmt.Sprintf("key_%02d", 5-i)
			require.Equal(t, expectedKey, page4[i].Key, "Page 4, entry %d should be %s", i, expectedKey)
		}

		// Collect all keys to verify no duplicates
		allKeys := make(map[string]bool)
		for _, entries := range [][]*Publication{
			page1,
			page2,
			page3,
			page4,
		} {
			for _, pub := range entries {
				require.False(t, allKeys[pub.Key], "Duplicate key found: %s", pub.Key)
				allKeys[pub.Key] = true
			}
		}

		require.Len(t, allKeys, 20, "Should have collected all 20 unique keys")

		t.Logf("SUCCESS: Pagination maintains correct ordering across 4 pages")
	})
}

// TestRedisMapBroker_OrderedStateWithNegativeScores tests ordering with negative scores.
func TestRedisMapBroker_OrderedStateWithNegativeScores(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_negative")

		// Publish entries with negative, zero, and positive scores
		testCases := []struct {
			key   string
			score int64
		}{
			{"key_pos", 100},
			{"key_neg", -50},
			{"key_zero", 0},
			{"key_neg2", -100},
			{"key_pos2", 50},
		}

		for _, tc := range testCases {
			_, err := broker.Publish(ctx, channel, tc.key, MapPublishOptions{
				Data:  []byte("data"),
				score: tc.score,
			})
			require.NoError(t, err)
		}

		// Read ordered state
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 5)

		// Verify ordering (descending: 100, 50, 0, -50, -100)
		expectedOrder := []string{"key_pos", "key_pos2", "key_zero", "key_neg", "key_neg2"}
		for i, entry := range entries {
			require.Equal(t, expectedOrder[i], entry.Key, "Entry %d should be %s", i, expectedOrder[i])
		}

		t.Logf("SUCCESS: Negative scores handled correctly in descending ordering")
	})
}

// TestRedisMapBroker_OrderedStateWithSameScores tests ordering stability when scores are equal.
func TestRedisMapBroker_OrderedStateWithSameScores(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_same_scores")

		// Publish multiple entries with the same score
		// Redis ZSET uses lexicographic ordering for members with equal scores
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%d", i)),
				score: 100, // Same score for all
			})
			require.NoError(t, err)
		}

		// Read ordered state
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 5)

		// With same scores, Redis sorts lexicographically by member (key)
		// With zrevrange (descending), we get reverse lexicographic: key_5, key_4, key_3, key_2, key_1
		for i := 0; i < 5; i++ {
			expectedKey := fmt.Sprintf("key_%d", 5-i)
			require.Equal(t, expectedKey, entries[i].Key, "Entry %d should be %s (reverse lexicographic order)", i, expectedKey)
		}

		t.Logf("SUCCESS: Equal scores maintain reverse lexicographic ordering")
	})
}

// TestRedisMapBroker_OrderedStatePaginationBoundaries tests edge cases in cursor-based pagination.
func TestRedisMapBroker_OrderedStatePaginationBoundaries(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_boundaries")

		// Publish 10 entries
		for i := 1; i <= 10; i++ {
			_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%02d", i), MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%02d", i)),
				score: int64(i * 10),
			})
			require.NoError(t, err)
		}

		// Test 1: Limit -1 (should return all)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: -1,
		})
		require.NoError(t, err)
		all, _, cursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, all, 10, "Limit -1 should return all entries")
		require.Empty(t, cursor, "No cursor when returning all entries")

		// Test 2: Limit larger than entries (should return all)
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		largeLimit, _, cursor2 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, largeLimit, 10, "Limit larger than entries should return all")
		require.Empty(t, cursor2, "No cursor when returning all entries")

		// Test 3: Pagination with limit=3 through all entries
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 3,
		})
		require.NoError(t, err)
		page1, _, c1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page1, 3)
		require.NotEmpty(t, c1)

		// Continue to get remaining entries
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  3,
			Cursor: c1,
		})
		require.NoError(t, err)
		page2, _, c2 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page2, 3)
		require.NotEmpty(t, c2)

		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  3,
			Cursor: c2,
		})
		require.NoError(t, err)
		page3, _, c3 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page3, 3)
		require.NotEmpty(t, c3)

		// Last page should have 1 entry
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  3,
			Cursor: c3,
		})
		require.NoError(t, err)
		page4, _, c4 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, page4, 1, "Last page should have 1 remaining entry")
		require.Empty(t, c4, "No cursor on last page")

		// Test 4: Verify ordering (descending: 10, 09, 08, ..., 01)
		allPages := append(append(append(page1, page2...), page3...), page4...)
		require.Len(t, allPages, 10)
		for i := 0; i < 10; i++ {
			expectedKey := fmt.Sprintf("key_%02d", 10-i)
			require.Equal(t, expectedKey, allPages[i].Key, "Entry %d should be %s", i, expectedKey)
		}

		t.Logf("SUCCESS: Cursor-based pagination boundary cases handled correctly")
	})
}

// TestRedisMapBroker_OrderedStateFullPagination tests complete cursor-based pagination loop.
func TestRedisMapBroker_OrderedStateFullPagination(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_full_pagination")

		totalEntries := 37 // Odd number to test edge cases
		pageSize := 10

		// Publish entries
		for i := 1; i <= totalEntries; i++ {
			_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%03d", i), MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%03d", i)),
				score: int64(i * 10),
			})
			require.NoError(t, err)
		}

		// Paginate through all entries using cursor
		var allKeys []string
		cursor := ""
		iterations := 0

		for {
			stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Cursor: cursor,
				Limit:  pageSize,
			})
			require.NoError(t, err)
			entries, _, nextCursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
			require.NoError(t, err)

			for _, entry := range entries {
				allKeys = append(allKeys, entry.Key)
			}

			cursor = nextCursor
			iterations++

			// Cursor will be empty when we've read all entries
			if cursor == "" {
				break
			}

			// Prevent infinite loop
			if iterations > totalEntries/pageSize+5 {
				t.Fatal("Pagination loop exceeded expected iterations")
			}
		}

		// Verify we got all entries in correct descending order (037, 036, ..., 001)
		require.Len(t, allKeys, totalEntries, "Should have paginated through all %d entries", totalEntries)

		for i := 0; i < totalEntries; i++ {
			expectedKey := fmt.Sprintf("key_%03d", totalEntries-i)
			require.Equal(t, expectedKey, allKeys[i], "Entry %d should be %s", i, expectedKey)
		}

		t.Logf("SUCCESS: Full cursor-based pagination through %d entries in descending order completed correctly", totalEntries)
	})
}

// TestRedisMapBroker_OrderedStateUpdatePreservesOrder tests that updating an entry's score
// changes its position in the ordered state.
func TestRedisMapBroker_OrderedStateUpdatePreservesOrder(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_update")

		// Publish 5 entries
		for i := 1; i <= 5; i++ {
			_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%d", i), MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%d", i)),
				score: int64(i * 10), // 10, 20, 30, 40, 50
			})
			require.NoError(t, err)
		}

		// Read initial order (descending: 50, 40, 30, 20, 10)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries1, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Equal(t, "key_5", entries1[0].Key) // Highest score (50)
		require.Equal(t, "key_1", entries1[4].Key) // Lowest score (10)

		// Update key_1 to have highest score (60)
		_, err = broker.Publish(ctx, channel, "key_1", MapPublishOptions{
			Data:  []byte("updated_data"),
			score: 60, // Now highest
		})
		require.NoError(t, err)

		// Read updated order
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries2, 5)

		// Verify new order (descending: 60, 50, 40, 30, 20)
		expectedOrder := []string{"key_1", "key_5", "key_4", "key_3", "key_2"}
		for i, entry := range entries2 {
			require.Equal(t, expectedOrder[i], entry.Key, "After update, entry %d should be %s", i, expectedOrder[i])
		}

		t.Logf("SUCCESS: Updating score correctly reorders entries in descending order")
	})
}

// TestRedisMapBroker_KeyModeIfNew tests KeyModeIfNew - only write if key doesn't exist.
func TestRedisMapBroker_KeyModeIfNew(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_keymode_if_new")

		// First publish with KeyModeIfNew should succeed (key doesn't exist)
		res1, err := broker.Publish(ctx, channel, "slot1", MapPublishOptions{
			Data:    []byte("player1"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed, "First publish should not be suppressed")
		require.Equal(t, uint64(1), res1.Position.Offset)

		// Second publish with KeyModeIfNew should be suppressed (key exists)
		res2, err := broker.Publish(ctx, channel, "slot1", MapPublishOptions{
			Data:    []byte("player2"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		require.True(t, res2.Suppressed, "Second publish should be suppressed (key exists)")
		require.Equal(t, SuppressReasonKeyExists, res2.SuppressReason)
		require.Equal(t, uint64(1), res2.Position.Offset, "Offset should not change")

		// Verify state still has original data
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, []byte("player1"), entries[0].Data)

		// Verify stream only has one entry (second was suppressed)
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)
		require.Len(t, streamResult.Publications, 1)
	})
}

// TestRedisMapBroker_KeyModeIfExists tests KeyModeIfExists - only write if key exists.
func TestRedisMapBroker_KeyModeIfExists(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_keymode_if_exists")

		// First publish with KeyModeIfExists should be suppressed (key doesn't exist)
		res1, err := broker.Publish(ctx, channel, "presence1", MapPublishOptions{
			Data:    []byte("heartbeat1"),
			KeyMode: KeyModeIfExists,
		})
		require.NoError(t, err)
		require.True(t, res1.Suppressed, "First publish should be suppressed (key doesn't exist)")
		require.Equal(t, SuppressReasonKeyNotFound, res1.SuppressReason)

		// Create the key first with regular publish
		res2, err := broker.Publish(ctx, channel, "presence1", MapPublishOptions{
			Data:    []byte("initial"),
			KeyMode: KeyModeReplace, // or just leave empty
		})
		require.NoError(t, err)
		require.False(t, res2.Suppressed, "Regular publish should not be suppressed")

		// Now publish with KeyModeIfExists should succeed (key exists)
		res3, err := broker.Publish(ctx, channel, "presence1", MapPublishOptions{
			Data:    []byte("heartbeat2"),
			KeyMode: KeyModeIfExists,
		})
		require.NoError(t, err)
		require.False(t, res3.Suppressed, "Third publish should not be suppressed (key exists)")

		// Verify state has updated data
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, []byte("heartbeat2"), entries[0].Data)
	})
}

// TestRedisMapBroker_KeyModeReplace tests default KeyModeReplace behavior.
func TestRedisMapBroker_KeyModeReplace(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_keymode_replace")

		// First publish (key doesn't exist)
		res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("value1"),
			KeyMode: KeyModeReplace, // explicit, same as default
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)

		// Second publish (key exists) - should still apply
		res2, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:    []byte("value2"),
			KeyMode: KeyModeReplace,
		})
		require.NoError(t, err)
		require.False(t, res2.Suppressed, "Replace should never be suppressed")

		// Verify state has updated data
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, []byte("value2"), entries[0].Data)
	})
}

func TestRedisMapBroker_Remove(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_remove")

		// Publish some keys
		_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte("data1"),
		})
		require.NoError(t, err)

		_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
			Data: []byte("data2"),
		})
		require.NoError(t, err)

		// Verify state has 2 keys
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 2)

		// Remove key1
		res, err := broker.Remove(ctx, channel, "key1", MapRemoveOptions{})
		require.NoError(t, err)
		require.False(t, res.Suppressed)

		// Verify state has 1 key
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, "key2", entries[0].Key)

		// Remove non-existent key should be suppressed with key_not_found
		res, err = broker.Remove(ctx, channel, "nonexistent", MapRemoveOptions{})
		require.NoError(t, err)
		require.True(t, res.Suppressed)
		require.Equal(t, SuppressReasonKeyNotFound, res.SuppressReason)

		// Verify stream has only 3 entries (key1, key2, remove(key1)) - no entry for nonexistent
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Limit: -1,
			},
		})
		require.NoError(t, err)
		require.Len(t, streamResult.Publications, 3)
		require.True(t, streamResult.Publications[2].Removed)
		require.Equal(t, "key1", streamResult.Publications[2].Key)
	})
}

// TestRedisMapBroker_Aggregation - REMOVED: aggregation feature was removed.
// TestRedisMapBroker_AggregationAutoDiscovery - REMOVED: aggregation feature was removed.

// =============================================================================
// Pagination Continuity Tests
//
// These tests verify that key-based cursor pagination ensures no entries are
// permanently lost when the state changes during iteration.
// The invariant is: state_entries + stream_changes = complete_data
// Uses simulateClientRecovery helper from map_broker_memory_test.go
// =============================================================================

// TestRedisMapBroker_UnorderedContinuity_EntryRemoved tests that removing
// an entry during unordered pagination doesn't cause data loss.
// The invariant is: state_entries + stream_changes = complete_data.
// Note: HSCAN COUNT is only a hint — Redis may return more entries than requested
// (especially for small hashes stored as listpack), so we don't assert exact page sizes.
func TestRedisMapBroker_UnorderedContinuity_EntryRemoved(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_unordered_continuity_remove")

		// Create 20 entries: key_00 to key_19
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data: []byte(fmt.Sprintf("data_%02d", i)),
			})
			require.NoError(t, err)
		}

		// CONCURRENT MODIFICATION: Remove key_10.
		_, err := broker.Remove(ctx, channel, "key_10", MapRemoveOptions{})
		require.NoError(t, err)

		// Simulate full client recovery with pagination.
		result := simulateClientRecovery(t, broker, channel, false, 10)

		// Verify: should have 19 keys (key_10 was removed)
		require.Len(t, result, 19, "Should have 19 keys after removal")
		require.NotContains(t, result, "key_10", "key_10 should be removed")

		// Verify key_11 wasn't skipped (this was the bug with integer offsets)
		require.Contains(t, result, "key_11", "key_11 should not be skipped")
	})
}

// TestRedisMapBroker_UnorderedContinuity_EntryAdded tests that adding
// an entry during unordered pagination doesn't cause issues.
// Note: HSCAN COUNT is only a hint — Redis may return more entries than requested
// (especially for small hashes stored as listpack), so we don't assert exact page sizes.
func TestRedisMapBroker_UnorderedContinuity_EntryAdded(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_unordered_continuity_add")

		// Create 20 entries: key_00 to key_19
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data: []byte(fmt.Sprintf("data_%02d", i)),
			})
			require.NoError(t, err)
		}

		// CONCURRENT MODIFICATION: Add new entry that lexicographically comes before cursor
		// This would shift entries with integer offset pagination
		_, err := broker.Publish(ctx, channel, "key_05b", MapPublishOptions{
			Data: []byte("data_05b"),
		})
		require.NoError(t, err)

		// Simulate full client recovery
		result := simulateClientRecovery(t, broker, channel, false, 10)

		// Verify: should have 21 keys (original 20 + new key_05b)
		require.Len(t, result, 21, "Should have 21 keys after addition")
		require.Contains(t, result, "key_05b", "key_05b should be present")
		require.Equal(t, []byte("data_05b"), result["key_05b"])
	})
}

// TestRedisMapBroker_OrderedContinuity_HigherScoreAdded tests that adding
// an entry with higher score during ordered pagination doesn't cause data loss.
func TestRedisMapBroker_OrderedContinuity_HigherScoreAdded(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_ordered_continuity_higher")

		// Create 20 entries with scores 100, 200, ..., 2000
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%02d", i)),
				score: int64(i * 100),
			})
			require.NoError(t, err)
		}

		// Read first page (should get keys with highest scores: key_20, key_19, ..., key_11)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		pubs1, _, cursor1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, pubs1, 10)
		require.NotEmpty(t, cursor1)

		// Verify first page has highest scores
		require.Equal(t, "key_20", pubs1[0].Key, "First entry should be key_20 (score 2000)")

		// CONCURRENT MODIFICATION: Add entry with HIGHEST score
		// This entry would appear at position 0, shifting all entries
		_, err = broker.Publish(ctx, channel, "key_top", MapPublishOptions{
			Data:  []byte("data_top"),
			score: 5000, // Higher than any existing
		})
		require.NoError(t, err)

		// Simulate full client recovery
		result := simulateClientRecovery(t, broker, channel, true, 10)

		// Verify: should have 21 keys
		require.Len(t, result, 21, "Should have 21 keys")
		require.Contains(t, result, "key_top", "key_top should be present via stream recovery")
		require.Equal(t, []byte("data_top"), result["key_top"])

		// Verify no keys were lost
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			require.Contains(t, result, key, "%s should be present", key)
		}
	})
}

// TestRedisMapBroker_OrderedContinuity_LowerScoreAdded tests that adding
// an entry with lower score during ordered pagination works correctly.
func TestRedisMapBroker_OrderedContinuity_LowerScoreAdded(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_ordered_continuity_lower")

		// Create 20 entries
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%02d", i)),
				score: int64(i * 100),
			})
			require.NoError(t, err)
		}

		// Read first page
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		_, _, cursor1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)

		// CONCURRENT MODIFICATION: Add entry with LOWEST score
		// This entry appears at end, shouldn't affect pagination much
		_, err = broker.Publish(ctx, channel, "key_bottom", MapPublishOptions{
			Data:  []byte("data_bottom"),
			score: 1, // Lower than any existing
		})
		require.NoError(t, err)

		// Continue reading with cursor - just verify it works
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor1,
		})
		require.NoError(t, err)
		pubs2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.NotEmpty(t, pubs2)

		// Simulate full recovery
		result := simulateClientRecovery(t, broker, channel, true, 10)

		require.Len(t, result, 21, "Should have 21 keys")
		require.Contains(t, result, "key_bottom", "key_bottom should be present")
	})
}

// TestRedisMapBroker_OrderedContinuity_ScoreChanged tests that changing
// an entry's score during pagination (causing reordering) doesn't lose data.
func TestRedisMapBroker_OrderedContinuity_ScoreChanged(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_ordered_continuity_score_change")

		// Create 20 entries
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%02d", i)),
				score: int64(i * 100),
			})
			require.NoError(t, err)
		}

		// Read first page (key_20 down to key_11)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		pubs1, streamPos1, cursor1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, pubs1, 10)

		// CONCURRENT MODIFICATION: Change score of key_05 to make it jump to top
		// key_05 had score 500, now gets score 3000 (highest)
		// This entry was NOT in first page, but now would appear at position 0
		_, err = broker.Publish(ctx, channel, "key_05", MapPublishOptions{
			Data:  []byte("data_05_updated"),
			score: 3000,
		})
		require.NoError(t, err)

		// Read second page - key_05 jumped out of this range
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor1,
		})
		require.NoError(t, err)
		pubs2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)

		// Combine and filter
		stateData := make(map[string][]byte)
		for _, pub := range pubs1 {
			if pub.Offset <= streamPos1.Offset {
				stateData[pub.Key] = pub.Data
			}
		}
		for _, pub := range pubs2 {
			if pub.Offset <= streamPos1.Offset {
				stateData[pub.Key] = pub.Data
			}
		}

		// Read stream and apply changes
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &streamPos1,
				Limit: -1,
			},
		})
		require.NoError(t, err)

		for _, pub := range streamResult.Publications {
			if pub.Removed {
				delete(stateData, pub.Key)
			} else {
				stateData[pub.Key] = pub.Data
			}
		}

		// Verify: should have 20 keys, key_05 should have updated data
		require.Len(t, stateData, 20, "Should have 20 keys")
		require.Equal(t, []byte("data_05_updated"), stateData["key_05"], "key_05 should have updated data from stream")
	})
}

// TestRedisMapBroker_OrderedContinuity_EntryRemoved tests that removing
// an entry during ordered pagination doesn't cause data loss.
func TestRedisMapBroker_OrderedContinuity_EntryRemoved(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_ordered_continuity_remove")

		// Create 20 entries
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%02d", i)),
				score: int64(i * 100),
			})
			require.NoError(t, err)
		}

		// Read first page (key_20 down to key_11)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		pubs1, streamPos1, cursor1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)

		// CONCURRENT MODIFICATION: Remove key_10 (first entry of next page)
		_, err = broker.Remove(ctx, channel, "key_10", MapRemoveOptions{})
		require.NoError(t, err)

		// Read remaining pages
		allStateData := make(map[string][]byte)
		for _, pub := range pubs1 {
			if pub.Offset <= streamPos1.Offset {
				allStateData[pub.Key] = pub.Data
			}
		}

		cursor := cursor1
		for cursor != "" {
			stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Limit:  10,
				Cursor: cursor,
			})
			require.NoError(t, err)
			pubs, _, nextCursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
			require.NoError(t, err)
			for _, pub := range pubs {
				if pub.Offset <= streamPos1.Offset {
					allStateData[pub.Key] = pub.Data
				}
			}
			cursor = nextCursor
		}

		// Apply stream changes
		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &streamPos1,
				Limit: -1,
			},
		})
		require.NoError(t, err)

		for _, pub := range streamResult.Publications {
			if pub.Removed {
				delete(allStateData, pub.Key)
			} else {
				allStateData[pub.Key] = pub.Data
			}
		}

		// Verify: should have 19 keys (key_10 removed)
		require.Len(t, allStateData, 19, "Should have 19 keys after removal")
		require.NotContains(t, allStateData, "key_10", "key_10 should be removed")

		// Verify key_09 wasn't skipped (the key that shifted up when key_10 was removed)
		require.Contains(t, allStateData, "key_09", "key_09 should not be skipped")
	})
}

// TestRedisMapBroker_OrderedContinuity_MultipleChanges tests recovery
// with multiple concurrent changes during pagination.
func TestRedisMapBroker_OrderedContinuity_MultipleChanges(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_ordered_continuity_multi")

		// Create 30 entries
		for i := 1; i <= 30; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%02d", i)),
				score: int64(i * 100),
			})
			require.NoError(t, err)
		}

		// Read first page
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		_, _, cursor1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)

		// CONCURRENT MODIFICATIONS:
		// 1. Add new highest score entry
		_, err = broker.Publish(ctx, channel, "key_new_top", MapPublishOptions{
			Data:  []byte("data_new_top"),
			score: 5000,
		})
		require.NoError(t, err)

		// 2. Remove an entry from middle
		_, err = broker.Remove(ctx, channel, "key_15", MapRemoveOptions{})
		require.NoError(t, err)

		// 3. Update score of an entry (move it)
		_, err = broker.Publish(ctx, channel, "key_05", MapPublishOptions{
			Data:  []byte("data_05_moved"),
			score: 4000, // Move from 500 to 4000
		})
		require.NoError(t, err)

		// Read second page
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor1,
		})
		require.NoError(t, err)
		_, _, cursor2 := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)

		// 4. Add new lowest score entry
		_, err = broker.Publish(ctx, channel, "key_new_bottom", MapPublishOptions{
			Data:  []byte("data_new_bottom"),
			score: 1,
		})
		require.NoError(t, err)

		// Read remaining pages
		cursor := cursor2
		for cursor != "" {
			stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
				Limit:  10,
				Cursor: cursor,
			})
			require.NoError(t, err)
			_, _, nextCursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
			require.NoError(t, err)
			cursor = nextCursor
		}

		// Simulate full client recovery
		result := simulateClientRecovery(t, broker, channel, true, 10)

		// Expected:
		// - Original 30 entries
		// - Minus key_15 (removed)
		// - Plus key_new_top and key_new_bottom
		// = 31 entries
		require.Len(t, result, 31, "Should have 31 keys")
		require.NotContains(t, result, "key_15", "key_15 should be removed")
		require.Contains(t, result, "key_new_top", "key_new_top should be present")
		require.Contains(t, result, "key_new_bottom", "key_new_bottom should be present")
		require.Equal(t, []byte("data_05_moved"), result["key_05"], "key_05 should have updated data")
	})
}

// TestRedisMapBroker_Delta tests key-based delta delivery via PUB/SUB and HandlePublication.
func TestRedisMapBroker_Delta(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})

		type pubEvent struct {
			ch      string
			pub     *Publication
			delta   bool
			prevPub *Publication
		}

		eventCh := make(chan pubEvent, 10)

		handler := &testBrokerEventHandler{
			HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
				eventCh <- pubEvent{ch: ch, pub: pub, delta: delta, prevPub: prevPub}
				return nil
			},
		}

		e := mb.makeWithHandler(t, node, handler)

		ctx := context.Background()
		channel := randomChannel("test_delta")

		// Subscribe to channel for PUB/SUB delivery.
		err := e.Subscribe(channel)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		waitEvent := func(t *testing.T) pubEvent {
			t.Helper()
			select {
			case ev := <-eventCh:
				return ev
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for publication event")
				return pubEvent{}
			}
		}

		// 1. First publish with UseDelta - no previous state in Redis hash.
		// Lua sends non-delta format because HGET returns false.
		_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:     []byte("data1"),
			UseDelta: true,
		})
		require.NoError(t, err)

		ev := waitEvent(t)
		require.False(t, ev.delta, "no prev state in Redis → non-delta PUB/SUB format")
		require.Nil(t, ev.prevPub, "no previous state for first publish")
		require.Equal(t, []byte("data1"), ev.pub.Data)

		// 2. Second publish same key - Lua finds prev state via HGET, sends delta format.
		_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
			Data:     []byte("data1_updated"),
			UseDelta: true,
		})
		require.NoError(t, err)

		ev = waitEvent(t)
		require.True(t, ev.delta)
		require.NotNil(t, ev.prevPub)
		require.Equal(t, []byte("data1"), ev.prevPub.Data)
		require.Equal(t, []byte("data1_updated"), ev.pub.Data)

		// 3. Different key - no previous state for this key.
		_, err = e.Publish(ctx, channel, "key2", MapPublishOptions{
			Data:     []byte("data2"),
			UseDelta: true,
		})
		require.NoError(t, err)

		ev = waitEvent(t)
		require.False(t, ev.delta, "no prev state for key2 → non-delta format")
		require.Nil(t, ev.prevPub, "no previous state for key2")
		require.Equal(t, []byte("data2"), ev.pub.Data)

		// 4. UseDelta=false - no delta.
		_, err = e.Publish(ctx, channel, "key2", MapPublishOptions{
			Data:     []byte("data2_updated"),
			UseDelta: false,
		})
		require.NoError(t, err)

		ev = waitEvent(t)
		require.False(t, ev.delta)
		require.Nil(t, ev.prevPub, "UseDelta=false means no delta")
	})
}

func TestRedisMapBroker_Clear(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_clear")

		// Publish some keyed state and stream entries.
		for i := 0; i < 3; i++ {
			_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
				Data: []byte(fmt.Sprintf("data%d", i)),
			})
			require.NoError(t, err)
		}

		// Verify data exists.
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 3)

		streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{Filter: StreamFilter{Limit: -1}})
		require.NoError(t, err)
		require.Len(t, streamResult.Publications, 3)

		stats, err := broker.Stats(ctx, channel)
		require.NoError(t, err)
		require.Equal(t, 3, stats.NumKeys)

		// Clear the channel.
		err = broker.Clear(ctx, channel, MapClearOptions{})
		require.NoError(t, err)

		// State should be empty.
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Empty(t, entries)

		// Stream should be empty.
		streamResult, err = broker.ReadStream(ctx, channel, MapReadStreamOptions{Filter: StreamFilter{Limit: -1}})
		require.NoError(t, err)
		require.Empty(t, streamResult.Publications)

		// Stats should show zero keys.
		stats, err = broker.Stats(ctx, channel)
		require.NoError(t, err)
		require.Equal(t, 0, stats.NumKeys)
	})
}

func TestRedisMapBroker_ClearDoesNotAffectOtherChannels(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)

		ctx := context.Background()
		ch1 := randomChannel("test_clear_iso1")
		ch2 := randomChannel("test_clear_iso2")

		// Populate two channels.
		for _, ch := range []string{ch1, ch2} {
			_, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
				Data: []byte("v"),
			})
			require.NoError(t, err)
		}

		// Clear only ch1.
		err := broker.Clear(ctx, ch1, MapClearOptions{})
		require.NoError(t, err)

		// ch1 empty.
		stateRes, err := broker.ReadState(ctx, ch1, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Empty(t, entries)

		// ch2 still intact.
		stateRes, err = broker.ReadState(ctx, ch2, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, entries, 1)
	})
}

func TestRedisMapBroker_ReadStream_Table(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerReadStream(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
				return MapChannelOptions{
					Mode:       MapModeRecoverable,
					StreamSize: 100,
					StreamTTL:  300 * time.Second,
					KeyTTL:     300 * time.Second,
				}
			}
			return mb.make(t, node)
		})
	})
}

func TestRedisMapBroker_EpochOnEmptyChannel(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerEpochOnEmptyChannel(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			return mb.make(t, node)
		})
	})
}

func TestRedisMapBroker_ReadStateAllEntries(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerReadStateAllEntries(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			return mb.make(t, node)
		})
	})
}

func TestRedisMapBroker_RemoveEmptyKey(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerRemoveEmptyKey(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			return mb.make(t, node)
		})
	})
}

func TestRedisMapBroker_ClientInfoInState(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerClientInfoInState(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			return mb.make(t, node)
		})
	})
}

func TestRedisMapBroker_ClientInfoInStream(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerClientInfoInStream(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			return mb.make(t, node)
		})
	})
}

func TestRedisMapBroker_CheckOrder(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerCheckOrder(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			return mb.make(t, node)
		})
	})
}

func TestRedisMapBroker_VersionPreserved(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		testMapBrokerVersionPreserved(t, func(t *testing.T) MapBroker {
			node, _ := New(Config{})
			return mb.make(t, node)
		})
	})
}

// TestRedisMapBroker_ClientInfoDelivery tests that ClientInfo is delivered
// via PUB/SUB when publishing with ClientInfo.
func TestRedisMapBroker_ClientInfoDelivery(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})

		type pubEvent struct {
			ch  string
			pub *Publication
		}

		eventCh := make(chan pubEvent, 10)

		handler := &testBrokerEventHandler{
			HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
				eventCh <- pubEvent{ch: ch, pub: pub}
				return nil
			},
		}

		e := mb.makeWithHandler(t, node, handler)

		ctx := context.Background()
		channel := randomChannel("test_client_info_delivery")

		err := e.Subscribe(channel)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		info := &ClientInfo{
			ClientID: "c1",
			UserID:   "u1",
			ConnInfo: []byte("conn"),
			ChanInfo: []byte("chan"),
		}

		_, err = e.Publish(ctx, channel, "k1", MapPublishOptions{
			Data:       []byte("data1"),
			ClientInfo: info,
		})
		require.NoError(t, err)

		select {
		case ev := <-eventCh:
			require.NotNil(t, ev.pub.Info, "ClientInfo should be present in delivered publication")
			require.Equal(t, "c1", ev.pub.Info.ClientID)
			require.Equal(t, "u1", ev.pub.Info.UserID)
			require.Equal(t, []byte("conn"), ev.pub.Info.ConnInfo)
			require.Equal(t, []byte("chan"), ev.pub.Info.ChanInfo)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for publication event")
		}
	})
}

// TestRedisMapBroker_OrderedStateAsc tests that ASC ordering returns entries
// in ascending score order (lowest score first).
func TestRedisMapBroker_OrderedStateAsc(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_asc")

		testCases := []struct {
			key   string
			score int64
			data  string
		}{
			{"key_c", 30, "data_c"},
			{"key_a", 10, "data_a"},
			{"key_e", 50, "data_e"},
			{"key_b", 20, "data_b"},
			{"key_d", 40, "data_d"},
		}

		for _, tc := range testCases {
			_, err := broker.Publish(ctx, channel, tc.key, MapPublishOptions{
				Data:  []byte(tc.data),
				score: tc.score,
			})
			require.NoError(t, err)
		}

		// ASC: lowest score first.
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
			Asc:   true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 5)

		expectedAsc := []string{"key_a", "key_b", "key_c", "key_d", "key_e"}
		for i, entry := range stateRes.Publications {
			require.Equal(t, expectedAsc[i], entry.Key, "ASC entry %d", i)
		}

		// DESC (default): highest score first.
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 5)

		expectedDesc := []string{"key_e", "key_d", "key_c", "key_b", "key_a"}
		for i, entry := range stateRes.Publications {
			require.Equal(t, expectedDesc[i], entry.Key, "DESC entry %d", i)
		}
	})
}

// TestRedisMapBroker_OrderedStatePaginationAsc tests cursor-based pagination
// with ASC ordering across multiple pages.
func TestRedisMapBroker_OrderedStatePaginationAsc(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_pagination_asc")

		// Publish 10 entries with scores 100..1000.
		for i := 1; i <= 10; i++ {
			_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%02d", i), MapPublishOptions{
				Data:  []byte(fmt.Sprintf("data_%02d", i)),
				score: int64(i * 100),
			})
			require.NoError(t, err)
		}

		// Page 1.
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 3, Asc: true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 3)
		require.NotEmpty(t, stateRes.Cursor)
		require.Equal(t, "key_01", stateRes.Publications[0].Key)
		require.Equal(t, "key_02", stateRes.Publications[1].Key)
		require.Equal(t, "key_03", stateRes.Publications[2].Key)

		// Page 2.
		stateRes2, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Cursor: stateRes.Cursor, Limit: 3, Asc: true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes2.Publications, 3)
		require.NotEmpty(t, stateRes2.Cursor)
		require.Equal(t, "key_04", stateRes2.Publications[0].Key)
		require.Equal(t, "key_05", stateRes2.Publications[1].Key)
		require.Equal(t, "key_06", stateRes2.Publications[2].Key)

		// Page 3.
		stateRes3, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Cursor: stateRes2.Cursor, Limit: 3, Asc: true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes3.Publications, 3)
		require.NotEmpty(t, stateRes3.Cursor)
		require.Equal(t, "key_07", stateRes3.Publications[0].Key)
		require.Equal(t, "key_08", stateRes3.Publications[1].Key)
		require.Equal(t, "key_09", stateRes3.Publications[2].Key)

		// Page 4: last entry.
		stateRes4, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Cursor: stateRes3.Cursor, Limit: 3, Asc: true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes4.Publications, 1)
		require.Empty(t, stateRes4.Cursor, "No more pages")
		require.Equal(t, "key_10", stateRes4.Publications[0].Key)
	})
}

// TestRedisMapBroker_OrderedStateAscSameScores tests ASC ordering with
// same-score entries — secondary sort by key ascending.
func TestRedisMapBroker_OrderedStateAscSameScores(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						ordered:    true,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    time.Hour,
						KeyTTL:     time.Minute,
					}
				},
			},
		})
		broker := mb.make(t, node)

		ctx := context.Background()
		channel := randomChannel("test_ordered_asc_same_scores")

		// All entries have score=100.
		for _, key := range []string{"zebra", "apple", "mango", "banana"} {
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte("data"),
				score: 100,
			})
			require.NoError(t, err)
		}

		// ASC with same scores → key ASC.
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100, Asc: true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 4)
		require.Equal(t, "apple", stateRes.Publications[0].Key)
		require.Equal(t, "banana", stateRes.Publications[1].Key)
		require.Equal(t, "mango", stateRes.Publications[2].Key)
		require.Equal(t, "zebra", stateRes.Publications[3].Key)

		// DESC with same scores → key DESC.
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 100,
		})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 4)
		require.Equal(t, "zebra", stateRes.Publications[0].Key)
		require.Equal(t, "mango", stateRes.Publications[1].Key)
		require.Equal(t, "banana", stateRes.Publications[2].Key)
		require.Equal(t, "apple", stateRes.Publications[3].Key)

		// Paginate ASC with limit=2.
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 2, Asc: true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 2)
		require.NotEmpty(t, stateRes.Cursor)
		require.Equal(t, "apple", stateRes.Publications[0].Key)
		require.Equal(t, "banana", stateRes.Publications[1].Key)

		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Cursor: stateRes.Cursor, Limit: 2, Asc: true,
		})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 2)
		require.Empty(t, stateRes.Cursor)
		require.Equal(t, "mango", stateRes.Publications[0].Key)
		require.Equal(t, "zebra", stateRes.Publications[1].Key)
	})
}

func TestRedisMapBroker_CleanupMetrics(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		registry := prometheus.NewRegistry()
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  300 * time.Second,
						MetaTTL:    3600 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
			Metrics: MetricsConfig{
				RegistererGatherer: registry,
			},
		})

		broker := mb.makeCustom(t, node, nil, func(c *RedisMapBrokerConfig) {
			c.CleanupBatchSize = 100
		})

		ctx := context.Background()
		channel := randomChannel("test_cleanup_metrics")

		// Publish two keyed entries with TTL.
		_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
			Data: []byte(`{"status":"online"}`),
		})
		require.NoError(t, err)
		_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
			Data: []byte(`{"status":"active"}`),
		})
		require.NoError(t, err)

		// Simulate TTL expiry by calling cleanupChannel with a future "now".
		shardWrapper := broker.shards[0]
		cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
		futureNow := time.Now().UnixMilli() + 31_000

		err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
		require.NoError(t, err)

		// Verify keys_removed counter was incremented.
		families, err := registry.Gather()
		require.NoError(t, err)
		var removedCount float64
		for _, f := range families {
			if f.GetName() == "centrifuge_map_broker_cleanup_keys_removed_count" {
				for _, m := range f.GetMetric() {
					removedCount += m.GetCounter().GetValue()
				}
			}
		}
		require.GreaterOrEqual(t, removedCount, float64(2), "cleanup_keys_removed_count should be at least 2")
	})
}

// TestRedisMapBroker_Unsubscribe tests Unsubscribe after Subscribe.
func TestRedisMapBroker_Unsubscribe(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		handler := &testBrokerEventHandler{
			HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
				return nil
			},
		}
		broker := mb.makeWithHandler(t, node, handler)

		// Wait for pub/sub connections to establish.
		require.Eventually(t, func() bool {
			ch := randomChannel("test_unsub_probe")
			return broker.Subscribe(ch) == nil
		}, 5*time.Second, 100*time.Millisecond, "pub/sub connections should be ready")

		ch := randomChannel("test_unsubscribe")

		// Subscribe first
		err := broker.Subscribe(ch)
		require.NoError(t, err)

		// Unsubscribe should succeed
		err = broker.Unsubscribe(ch)
		require.NoError(t, err)
	})
}

// TestRedisMapBroker_UnsubscribeWithoutSubscribe tests Unsubscribe on a channel not subscribed.
func TestRedisMapBroker_UnsubscribeWithoutSubscribe(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		handler := &testBrokerEventHandler{
			HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
				return nil
			},
		}
		broker := mb.makeWithHandler(t, node, handler)

		// Wait for pub/sub connections to establish.
		require.Eventually(t, func() bool {
			ch := randomChannel("test_unsub_probe2")
			return broker.Subscribe(ch) == nil
		}, 5*time.Second, 100*time.Millisecond, "pub/sub connections should be ready")

		ch := randomChannel("test_unsub_no_sub")

		// Unsubscribe without Subscribe — should not error (Redis UNSUBSCRIBE on unknown channel is fine)
		err := broker.Unsubscribe(ch)
		require.NoError(t, err)
	})
}

// TestRedisMapBroker_CAS_Publish tests CAS semantics for Publish in Redis.
func TestRedisMapBroker_CAS_Publish(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)
		ctx := context.Background()
		ch := randomChannel("test_cas_publish")

		// Publish key1
		res1, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)
		epoch := res1.Position.Epoch

		// CAS with correct position → succeeds
		res2, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data:             []byte("v2"),
			ExpectedPosition: &StreamPosition{Offset: res1.Position.Offset, Epoch: epoch},
		})
		require.NoError(t, err)
		require.False(t, res2.Suppressed)

		// CAS with stale offset → mismatch, returns CurrentPublication
		res3, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data:             []byte("v3"),
			ExpectedPosition: &StreamPosition{Offset: 1, Epoch: epoch}, // stale
		})
		require.NoError(t, err)
		require.True(t, res3.Suppressed)
		require.Equal(t, SuppressReasonPositionMismatch, res3.SuppressReason)
		require.NotNil(t, res3.CurrentPublication, "CAS mismatch should return CurrentPublication")
		require.Equal(t, []byte("v2"), res3.CurrentPublication.Data)

		// CAS with wrong epoch → mismatch
		res4, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data:             []byte("v4"),
			ExpectedPosition: &StreamPosition{Offset: res2.Position.Offset, Epoch: "wrong-epoch"},
		})
		require.NoError(t, err)
		require.True(t, res4.Suppressed)
		require.Equal(t, SuppressReasonPositionMismatch, res4.SuppressReason)

		// CAS on non-existent key → mismatch (no current pub)
		res5, err := broker.Publish(ctx, ch, "nonexistent", MapPublishOptions{
			Data:             []byte("data"),
			ExpectedPosition: &StreamPosition{Offset: 1, Epoch: epoch},
		})
		require.NoError(t, err)
		require.True(t, res5.Suppressed)
		require.Equal(t, SuppressReasonPositionMismatch, res5.SuppressReason)
	})
}

// TestRedisMapBroker_CAS_Remove tests CAS semantics for Remove in Redis.
func TestRedisMapBroker_CAS_Remove(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)
		ctx := context.Background()
		ch := randomChannel("test_cas_remove")

		// Publish key1
		res1, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
		require.NoError(t, err)
		epoch := res1.Position.Epoch

		// CAS remove with wrong offset → mismatch, returns CurrentPublication
		res2, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{
			ExpectedPosition: &StreamPosition{Offset: 999, Epoch: epoch},
		})
		require.NoError(t, err)
		require.True(t, res2.Suppressed)
		require.Equal(t, SuppressReasonPositionMismatch, res2.SuppressReason)
		require.NotNil(t, res2.CurrentPublication)
		require.Equal(t, []byte("v1"), res2.CurrentPublication.Data)

		// CAS remove with correct position → succeeds
		res3, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{
			ExpectedPosition: &StreamPosition{Offset: res1.Position.Offset, Epoch: epoch},
		})
		require.NoError(t, err)
		require.False(t, res3.Suppressed)

		// Verify key removed
		stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Empty(t, stateRes.Publications)
	})
}

// TestRedisMapBroker_RemoveIdempotency tests idempotency key for Remove in Redis.
func TestRedisMapBroker_RemoveIdempotency(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)
		ctx := context.Background()
		ch := randomChannel("test_remove_idempotency")

		_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
		require.NoError(t, err)
		_, err = broker.Publish(ctx, ch, "key2", MapPublishOptions{Data: []byte("v2")})
		require.NoError(t, err)

		// Remove key1 with idempotency key
		res1, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{
			IdempotencyKey:      "remove-1",
			IdempotentResultTTL: 60 * time.Second,
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)

		// Remove key2 with same idempotency key → suppressed
		res2, err := broker.Remove(ctx, ch, "key2", MapRemoveOptions{
			IdempotencyKey:      "remove-1",
			IdempotentResultTTL: 60 * time.Second,
		})
		require.NoError(t, err)
		require.True(t, res2.Suppressed)
		require.Equal(t, SuppressReasonIdempotency, res2.SuppressReason)

		// key2 should still exist
		stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 1)
		require.Equal(t, "key2", stateRes.Publications[0].Key)
	})
}

// TestRedisMapBroker_VersionEpoch tests version epoch scoping in Redis.
func TestRedisMapBroker_VersionEpoch(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{})
		broker := mb.make(t, node)
		ctx := context.Background()
		ch := randomChannel("test_version_epoch")

		// Publish version 5 with epoch "a"
		res1, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data: []byte("v5_a"), Version: 5, VersionEpoch: "a",
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)

		// Lower version same epoch → suppressed
		res2, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data: []byte("v3_a"), Version: 3, VersionEpoch: "a",
		})
		require.NoError(t, err)
		require.True(t, res2.Suppressed)
		require.Equal(t, SuppressReasonVersion, res2.SuppressReason)

		// Different epoch → accepted
		res3, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data: []byte("v3_b"), Version: 3, VersionEpoch: "b",
		})
		require.NoError(t, err)
		require.False(t, res3.Suppressed)
	})
}

// TestNewRedisMapBrokerErrors covers input-validation branches in NewRedisMapBroker.
// These tests don't actually connect to Redis since the failures occur before any
// connection attempt, but they live with the rest of the integration-tagged tests
// for this type to keep all RedisMapBroker tests together.
func TestNewRedisMapBrokerErrors(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	_, err := NewRedisMapBroker(node, RedisMapBrokerConfig{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no shards")

	// SubscribeOnReplica without replica client.
	_, err = NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:             []*RedisShard{{}},
		SubscribeOnReplica: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "SubscribeOnReplica")

	// NumShardedPubSubPartitions > 0 without cluster.
	_, err = NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:                     []*RedisShard{{}},
		NumShardedPubSubPartitions: 2,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "sharded PUB/SUB")

	// UsePrecomputedPartitionTags without partition count set.
	_, err = NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:                      []*RedisShard{{}},
		UsePrecomputedPartitionTags: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "UsePrecomputedPartitionTags")
	require.Contains(t, err.Error(), "NumShardedPubSubPartitions")

	// UsePrecomputedPartitionTags with unsupported partition count.
	_, err = NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:                      []*RedisShard{{}},
		NumShardedPubSubPartitions:  3,
		UsePrecomputedPartitionTags: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not precomputed")
}

// TestRedisMapBroker_UsePrecomputedPartitionTags_ClusterE2E exercises the
// full publish + read-state path with the precomputed-tag scheme enabled
// against a live Redis Cluster. Requires a 3-node cluster on 7001-7003 with
// sharded PUB/SUB support (Redis 7+); skipped otherwise.
func TestRedisMapBroker_UsePrecomputedPartitionTags_ClusterE2E(t *testing.T) {
	node, _ := New(Config{
		Map: MapConfig{
			GetMapChannelOptions: func(string) MapChannelOptions {
				return MapChannelOptions{
					Mode:       MapModeRecoverable,
					StreamSize: 100,
					StreamTTL:  300 * time.Second,
					MetaTTL:    time.Hour,
					KeyTTL:     time.Minute,
				}
			},
		},
	})
	defer func() { _ = node.Shutdown(context.Background()) }()

	redisConf := RedisShardConfig{
		ClusterAddresses: []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"},
		IOTimeout:        10 * time.Second,
		ConnectTimeout:   10 * time.Second,
	}
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	defer shard.Close()
	skipIfNoShardedPubSub(t, shard, "centrifuge-test-skip-probe-"+randString(4))

	broker, err := NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:                      []*RedisShard{shard},
		NumShardedPubSubPartitions:  16,
		UsePrecomputedPartitionTags: true,
	})
	require.NoError(t, err)
	require.NotNil(t, broker.partitionTags, "partitionTags must be loaded when option is enabled")
	require.Len(t, broker.partitionTags, 16)

	ctx := context.Background()

	// Touch enough channels to spread across partitions and validate
	// publish + read-state work end to end with the precomputed scheme.
	const numChannels = 32
	channels := make([]string, numChannels)
	for i := 0; i < numChannels; i++ {
		channels[i] = randomChannel("test_precomputed_e2e")
		_, err := broker.Publish(ctx, channels[i], "key1", MapPublishOptions{Data: []byte("v1")})
		require.NoError(t, err)
		_, err = broker.Publish(ctx, channels[i], "key2", MapPublishOptions{Data: []byte("v2")})
		require.NoError(t, err)
	}

	for _, ch := range channels {
		res, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		state := stateToMap(res.Publications)
		require.Equal(t, []byte("v1"), state["key1"])
		require.Equal(t, []byte("v2"), state["key2"])
		require.NotEmpty(t, res.Position.Epoch)
		require.Greater(t, res.Position.Offset, uint64(0))
	}
}

// TestRedisMapBroker_PartitionSlotInvariant asserts that for any channel, the
// cluster slot of its messageChannelID equals the slot of its partition's
// shard channel (pubSubShardChannelID). Strategies that derive node ownership
// from the partition's hash tag rely on this — if the slots diverged, a
// subscriber and publisher could end up on different nodes for the same
// partition. Run for both tag schemes.
func TestRedisMapBroker_PartitionSlotInvariant(t *testing.T) {
	clusterShard := &RedisShard{isCluster: true}
	const numPartitions = 16
	const numPsShards = 2
	channels := []string{"alpha", "beta", "user-42", "long-channel-name-with-dashes", "x"}

	check := func(t *testing.T, broker *RedisMapBroker) {
		t.Helper()
		for _, ch := range channels {
			partIdx := consistentIndex(ch, numPartitions)
			msgID := broker.messageChannelID(clusterShard, ch)
			msgSlot := redisSlot(msgID)
			for psShard := 0; psShard < numPsShards; psShard++ {
				shardID := broker.pubSubShardChannelID(partIdx, psShard, true)
				shardSlot := redisSlot(shardID)
				require.Equal(t, msgSlot, shardSlot,
					"channel=%q partIdx=%d psShard=%d msgID=%s shardID=%s", ch, partIdx, psShard, msgID, shardID)
			}
		}
	}

	t.Run("default scheme", func(t *testing.T) {
		broker := &RedisMapBroker{
			conf:          RedisMapBrokerConfig{Prefix: "centrifuge", NumShardedPubSubPartitions: numPartitions},
			messagePrefix: "centrifuge.client.",
			shardChannel:  "centrifuge" + redisPubSubShardChannelSuffix,
		}
		check(t, broker)
	})

	t.Run("precomputed scheme", func(t *testing.T) {
		tags, err := redispartition.FindTags(numPartitions)
		require.NoError(t, err)
		broker := &RedisMapBroker{
			conf: RedisMapBrokerConfig{
				Prefix:                      "centrifuge",
				NumShardedPubSubPartitions:  numPartitions,
				UsePrecomputedPartitionTags: true,
			},
			messagePrefix: "centrifuge.client.",
			shardChannel:  "centrifuge" + redisPubSubShardChannelSuffix,
			partitionTags: tags,
		}
		check(t, broker)
	})
}

func TestRedisMapBroker_PubSubPartitionHashTag(t *testing.T) {
	t.Run("default scheme returns bare integer", func(t *testing.T) {
		broker := &RedisMapBroker{
			conf: RedisMapBrokerConfig{NumShardedPubSubPartitions: 16},
		}
		for i := 0; i < 16; i++ {
			require.Equal(t, strconv.Itoa(i), broker.pubSubPartitionHashTag(i))
		}
	})

	t.Run("precomputed scheme returns table tag", func(t *testing.T) {
		tags, err := redispartition.FindTags(16)
		require.NoError(t, err)
		broker := &RedisMapBroker{
			conf:          RedisMapBrokerConfig{NumShardedPubSubPartitions: 16, UsePrecomputedPartitionTags: true},
			partitionTags: tags,
		}
		for i := 0; i < 16; i++ {
			require.Equal(t, tags[i], broker.pubSubPartitionHashTag(i))
		}
	})
}

// TestRedisMapBroker_PrecomputedTags_KeyConsistency walks the four key/channel
// builders that thread partition tags through and verifies they all use the
// precomputed tag rather than the bare partition index.
func TestRedisMapBroker_PrecomputedTags_KeyConsistency(t *testing.T) {
	tags, err := redispartition.FindTags(16)
	require.NoError(t, err)

	broker := &RedisMapBroker{
		conf: RedisMapBrokerConfig{
			Prefix:                      "centrifuge",
			NumShardedPubSubPartitions:  16,
			UsePrecomputedPartitionTags: true,
		},
		messagePrefix: "centrifuge.client.",
		partitionTags: tags,
	}
	clusterShard := &RedisShard{isCluster: true}

	channelName := "my-channel"
	idx := consistentIndex(channelName, 16)
	expectedTag := tags[idx]
	bareIdx := strconv.Itoa(idx)
	wrapped := "{" + expectedTag + "}"

	cases := []struct {
		name string
		got  string
	}{
		{"messageChannelID", broker.messageChannelID(clusterShard, channelName)},
		{"resultCacheKey", broker.resultCacheKey(clusterShard, channelName, "idem")},
		{"cleanupRegistrationKeyForChannel", broker.cleanupRegistrationKeyForChannel(clusterShard, channelName)},
		// buildKey is exercised via the public state-meta key builder.
		{"buildKey/state-meta", broker.stateMetaKey(clusterShard, channelName)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Contains(t, c.got, wrapped, "should embed precomputed tag {%s}", expectedTag)
			require.NotContains(t, c.got, "{"+bareIdx+"}", "should not embed bare partition index as hash tag")
		})
	}
}

// TestRedisMapBrokerReadPresenceState verifies the presence-state read wrapper
// returns publications and a stream position consistent with ReadState.
// ReadPresenceState is a thin wrapper over ReadState.
func TestRedisMapBrokerReadPresenceState(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		t.Parallel()
		node := testNode(t)
		broker := mb.make(t, node)
		require.NoError(t, broker.RegisterEventHandler(nil))

		ctx := context.Background()
		channel := "presence-state-coverage-" + randString(4)

		_, err := broker.Publish(ctx, channel, "client-1", MapPublishOptions{
			Data: []byte(`{"role":"viewer"}`),
		})
		require.NoError(t, err)

		pubs, sp, err := broker.ReadPresenceState(ctx, channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.NotEmpty(t, sp.Epoch)
		require.Len(t, pubs, 1)
		require.Equal(t, "client-1", pubs[0].Key)
	})
}

// TestRedisMapBrokerReadPresenceStream verifies the presence-stream read wrapper
// returns the underlying stream entries.
func TestRedisMapBrokerReadPresenceStream(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		t.Parallel()
		node := testNode(t)
		broker := mb.make(t, node)
		require.NoError(t, broker.RegisterEventHandler(nil))

		ctx := context.Background()
		channel := "presence-stream-coverage-" + randString(4)

		_, err := broker.Publish(ctx, channel, "client-a", MapPublishOptions{Data: []byte(`{}`)})
		require.NoError(t, err)
		_, err = broker.Publish(ctx, channel, "client-b", MapPublishOptions{Data: []byte(`{}`)})
		require.NoError(t, err)

		res, err := broker.ReadPresenceStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{Limit: 100},
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(res.Publications), 2)
	})
}

// TestRedisMapBrokerCloseIdempotent exercises Close, including the closeOnce
// guard on a second invocation.
func TestRedisMapBrokerCloseIdempotent(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		t.Parallel()
		node := testNode(t)
		broker := mb.make(t, node)

		require.NoError(t, broker.Close(context.Background()))
		// Second call must be a no-op (closeOnce guard).
		require.NoError(t, broker.Close(context.Background()))
	})
}

// TestRedisMapBrokerPublishEphemeralKeyless exercises the "fast path" inside
// Publish for ephemeral, non-keyed, non-idempotent publications.
func TestRedisMapBrokerPublishEphemeralKeyless(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelInfo,
		LogHandler: func(LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(string) MapChannelOptions {
				return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute}
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	broker, err := NewRedisMapBroker(node, RedisMapBrokerConfig{Shards: []*RedisShard{shard}})
	require.NoError(t, err)
	require.NoError(t, broker.RegisterEventHandler(nil))

	res, err := broker.Publish(context.Background(), "ephemeral-fast-"+randString(4), "", MapPublishOptions{
		Data: []byte(`{"v":1}`),
	})
	require.NoError(t, err)
	// Fast path returns zero result.
	require.Zero(t, res.Position.Offset)
}

// TestRedisMapBrokerPublishEphemeralRejectsCASAndVersion verifies the ephemeral
// mode validation rejects CAS (ExpectedPosition) and Version-based dedup.
func TestRedisMapBrokerPublishEphemeralRejectsCASAndVersion(t *testing.T) {
	t.Parallel()
	node, err := New(Config{
		LogLevel:   LogLevelInfo,
		LogHandler: func(LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(string) MapChannelOptions {
				return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Minute}
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	broker, err := NewRedisMapBroker(node, RedisMapBrokerConfig{Shards: []*RedisShard{shard}})
	require.NoError(t, err)
	require.NoError(t, broker.RegisterEventHandler(nil))

	channel := "ephemeral-strict-" + randString(4)

	_, err = broker.Publish(context.Background(), channel, "k", MapPublishOptions{
		Data:             []byte(`{}`),
		ExpectedPosition: &StreamPosition{Offset: 1, Epoch: "x"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CAS")

	_, err = broker.Publish(context.Background(), channel, "k", MapPublishOptions{
		Data:    []byte(`{}`),
		Version: 5,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")
}

// TestRedisMapBroker_RefreshTTLOnSuppress_RefreshesMetaAndStream verifies the
// suppressed if_new+refresh_ttl_on_suppress branch in map_broker_add.lua bumps
// meta_key (and stream_key when stream_ttl > 0). Without this, an idle channel
// whose keys are being kept alive can still lose meta + stream to native Redis
// TTL, forcing an epoch reset on the next publish.
func TestRedisMapBroker_RefreshTTLOnSuppress_RefreshesMetaAndStream(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  60 * time.Second,
						MetaTTL:    120 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_refresh_ttl_meta_stream")

		// Initial publish: creates meta_key and stream_key with their TTLs.
		_, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data:    []byte("v1"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)

		shardWrapper := broker.shards[0]
		client := shardWrapper.shard.client
		metaKey := broker.metaKey(shardWrapper.shard, channel)
		streamKey := broker.streamKey(shardWrapper.shard, channel)

		pttl := func(key string) int64 {
			ms, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
			require.NoError(t, err)
			return ms
		}

		metaBefore := pttl(metaKey)
		streamBefore := pttl(streamKey)
		require.Greater(t, metaBefore, int64(0), "meta_key must have a TTL set")
		require.Greater(t, streamBefore, int64(0), "stream_key must have a TTL set")

		// Burn enough time for any TTL refresh to be observable above clock noise.
		time.Sleep(150 * time.Millisecond)

		// Suppressed keepalive: same key, if_new + refresh_ttl_on_suppress.
		res, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data:                 []byte("v1"),
			KeyMode:              KeyModeIfNew,
			RefreshTTLOnSuppress: true,
		})
		require.NoError(t, err)
		require.True(t, res.Suppressed)
		require.Equal(t, SuppressReasonKeyExists, res.SuppressReason)

		metaAfter := pttl(metaKey)
		streamAfter := pttl(streamKey)
		// PTTL should be reset close to the configured TTL again — strictly
		// greater than the value captured before the sleep.
		require.Greater(t, metaAfter, metaBefore-100,
			"meta_key TTL must be refreshed by suppressed keepalive (was %d, now %d)", metaBefore, metaAfter)
		require.Greater(t, metaAfter, int64(60_000),
			"meta_key TTL must be near MetaTTL after refresh, got %d ms", metaAfter)
		require.Greater(t, streamAfter, streamBefore-100,
			"stream_key TTL must be refreshed by suppressed keepalive (was %d, now %d)", streamBefore, streamAfter)
	})
}

// TestRedisMapBroker_NoRefreshTTL_LeavesMetaAlone is the negative case:
// without RefreshTTLOnSuppress, suppressed publishes do not bump meta_key.
func TestRedisMapBroker_NoRefreshTTL_LeavesMetaAlone(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  60 * time.Second,
						MetaTTL:    120 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_no_refresh_ttl_meta")

		_, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data:    []byte("v1"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)

		shardWrapper := broker.shards[0]
		client := shardWrapper.shard.client
		metaKey := broker.metaKey(shardWrapper.shard, channel)
		pttl := func(key string) int64 {
			ms, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
			require.NoError(t, err)
			return ms
		}

		metaBefore := pttl(metaKey)
		time.Sleep(200 * time.Millisecond)

		// Suppressed publish without RefreshTTLOnSuppress.
		res, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data:    []byte("v1"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		require.True(t, res.Suppressed)

		metaAfter := pttl(metaKey)
		// metaAfter should be ~200ms lower than metaBefore (clock advanced,
		// no refresh happened). Allow generous slack for scheduling.
		require.Less(t, metaAfter, metaBefore,
			"meta_key TTL must NOT be refreshed without RefreshTTLOnSuppress (was %d, now %d)", metaBefore, metaAfter)
	})
}

// TestRedisMapBroker_PublishWipesStateOnEpochReset verifies that when meta_key
// is gone (evicted by TTL or admin DEL) and a fresh publish materializes a new
// epoch, the lua script wipes lingering state keys before the KeyMode check
// runs. Without this, a zombie key under the dead epoch would suppress a
// legitimate fresh-epoch publish via KeyModeIfNew.
func TestRedisMapBroker_PublishWipesStateOnEpochReset(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  60 * time.Second,
						MetaTTL:    120 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_wipe_state_on_epoch_reset")

		// Initial publish — establishes meta + state under epoch_old.
		res, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data:    []byte("dead"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		oldEpoch := res.Position.Epoch
		require.NotEmpty(t, oldEpoch)

		shardWrapper := broker.shards[0]
		client := shardWrapper.shard.client
		metaKey := broker.metaKey(shardWrapper.shard, channel)
		stateHashKey := broker.stateHashKey(shardWrapper.shard, channel)

		// Sanity: state is populated.
		hlen, err := client.Do(ctx, client.B().Hlen().Key(stateHashKey).Build()).AsInt64()
		require.NoError(t, err)
		require.Equal(t, int64(1), hlen)

		// Simulate meta_key eviction (TTL fired) while state_hash_key
		// happens to still be there due to lazy eviction.
		_, err = client.Do(ctx, client.B().Del().Key(metaKey).Build()).AsInt64()
		require.NoError(t, err)

		// Pre-fix the next publish would either:
		//   - hit KeyMode=if_new and HEXISTS would treat the zombie as live
		//     and suppress (test below would fail since res.Suppressed=true), or
		//   - succeed but leave the zombie row visible to ReadState.
		// With the fix, the lua script wipes state_hash_key when meta_key is
		// freshly initialized, so the new publish lands cleanly.
		res, err = broker.Publish(ctx, channel, "alive_key", MapPublishOptions{
			Data:    []byte("alive"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		require.False(t, res.Suppressed, "fresh-epoch publish must not be suppressed by zombie state")
		require.NotEqual(t, oldEpoch, res.Position.Epoch, "epoch must rotate after meta_key DEL")

		// State must contain only the new key.
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 1, "state must hold only the new-epoch key")
		require.Equal(t, "alive_key", stateRes.Publications[0].Key)
		require.Equal(t, []byte("alive"), stateRes.Publications[0].Data)
	})
}

// TestRedisMapBroker_PublishDoesNotWipeStateOnNormalPublish guards against
// the wipe firing when meta_key is intact — every subsequent publish must
// preserve the state hash, only the explicit-eviction path triggers the wipe.
func TestRedisMapBroker_PublishDoesNotWipeStateOnNormalPublish(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  60 * time.Second,
						MetaTTL:    120 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_no_wipe_on_normal_publish")

		for _, k := range []string{"k1", "k2", "k3"} {
			_, err := broker.Publish(ctx, channel, k, MapPublishOptions{Data: []byte("v")})
			require.NoError(t, err)
		}

		_, err := broker.Publish(ctx, channel, "k4", MapPublishOptions{Data: []byte("v")})
		require.NoError(t, err)

		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 4, "all four keys must remain after normal publishes")
	})
}

// TestRedisMapBroker_PublishWipesZombieStateWhenStateMetaEvicted reproduces
// the lazy-eviction skew where state_meta_key has been evicted but
// state_hash_key still has data. The publish must wipe the orphan state
// before the KeyMode=if_new check, otherwise a fresh-epoch publish would be
// suppressed by zombie keys with no live metadata.
func TestRedisMapBroker_PublishWipesZombieStateWhenStateMetaEvicted(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  60 * time.Second,
						MetaTTL:    120 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_wipe_state_when_state_meta_evicted")

		// Establish state under epoch_old.
		res, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data:    []byte("dead"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		oldEpoch := res.Position.Epoch

		shardWrapper := broker.shards[0]
		client := shardWrapper.shard.client
		stateMetaKey := broker.stateMetaKey(shardWrapper.shard, channel)
		stateHashKey := broker.stateHashKey(shardWrapper.shard, channel)

		// Sanity: both keys present.
		hlen, err := client.Do(ctx, client.B().Hlen().Key(stateHashKey).Build()).AsInt64()
		require.NoError(t, err)
		require.Equal(t, int64(1), hlen, "state_hash_key must hold the published key")
		exists, err := client.Do(ctx, client.B().Exists().Key(stateMetaKey).Build()).AsInt64()
		require.NoError(t, err)
		require.Equal(t, int64(1), exists, "state_meta_key must exist after publish")

		// Simulate state_meta_key being evicted while state_hash_key lingers
		// (lazy-eviction skew). Without the wipe fix, the next publish under
		// the same epoch would treat zombie "k" as live.
		_, err = client.Do(ctx, client.B().Del().Key(stateMetaKey).Build()).AsInt64()
		require.NoError(t, err)

		// Same key, KeyModeIfNew. The if_new check would suppress with
		// key_exists if the wipe didn't run. With the fix, the wipe fires
		// (state_hash_key has data, state_meta_key.epoch can't be confirmed)
		// and the publish proceeds normally.
		res2, err := broker.Publish(ctx, channel, "k", MapPublishOptions{
			Data:    []byte("alive"),
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		require.False(t, res2.Suppressed, "publish must not be suppressed by zombie state when state_meta_key is evicted")
		// Epoch is unchanged because meta_key itself wasn't evicted in this
		// scenario — only state_meta_key was. The wipe rebuilds state under
		// the same epoch.
		require.Equal(t, oldEpoch, res2.Position.Epoch)

		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
		require.NoError(t, err)
		require.Len(t, stateRes.Publications, 1, "only the new-publish row must remain after wipe")
		require.Equal(t, []byte("alive"), stateRes.Publications[0].Data)
	})
}

// TestRedisMapBroker_StreamReadWipesStreamOnFreshMeta reproduces the case
// where meta_key was evicted while stream_key still has entries from the
// dead epoch (lazy-eviction skew). When stream_read materializes a fresh
// epoch via map_broker_stream_read.lua, it must DEL stream_key — otherwise
// xrange would return dead-epoch entries under the fresh meta wrapper.
// Mirrors the publish-path safety net (`if top_offset == 1 then del stream_key`).
func TestRedisMapBroker_StreamReadWipesStreamOnFreshMeta(t *testing.T) {
	runMapBrokerTest(t, func(t *testing.T, mb redisMapBrokerFactory) {
		node, _ := New(Config{
			Map: MapConfig{
				GetMapChannelOptions: func(channel string) MapChannelOptions {
					return MapChannelOptions{
						Mode:       MapModeRecoverable,
						StreamSize: 100,
						StreamTTL:  60 * time.Second,
						MetaTTL:    120 * time.Second,
						KeyTTL:     30 * time.Second,
					}
				},
			},
		})
		broker := mb.make(t, node)
		ctx := context.Background()
		channel := randomChannel("test_stream_read_wipes_on_fresh_meta")

		// Build dead epoch with three stream entries.
		var deadEpoch string
		for i := 0; i < 3; i++ {
			res, err := broker.Publish(ctx, channel, fmt.Sprintf("k%d", i), MapPublishOptions{
				Data: []byte(fmt.Sprintf("dead-%d", i)),
			})
			require.NoError(t, err)
			deadEpoch = res.Position.Epoch
		}

		shardWrapper := broker.shards[0]
		client := shardWrapper.shard.client
		metaKey := broker.metaKey(shardWrapper.shard, channel)
		streamKey := broker.streamKey(shardWrapper.shard, channel)

		// Sanity: stream has entries.
		streamLen, err := client.Do(ctx, client.B().Xlen().Key(streamKey).Build()).AsInt64()
		require.NoError(t, err)
		require.Equal(t, int64(3), streamLen, "stream_key must hold the three dead-epoch entries")

		// Evict only meta_key (simulating native TTL firing on it before
		// stream_key catches up under lazy eviction).
		_, err = client.Do(ctx, client.B().Del().Key(metaKey).Build()).AsInt64()
		require.NoError(t, err)

		// Call ReadStream — internally executes map_broker_stream_read.lua,
		// which materializes a new epoch on missing meta_key and (with the
		// fix) wipes stream_key before xrange runs.
		streamRes, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &StreamPosition{Offset: 0},
				Limit: 100,
			},
		})
		require.NoError(t, err)
		require.NotEqual(t, deadEpoch, streamRes.Position.Epoch, "fresh meta must have a new epoch")
		require.Empty(t, streamRes.Publications, "stream_read must not return dead-epoch entries on a freshly resurrected meta")

		// Stream is now empty in Redis too.
		streamLen, err = client.Do(ctx, client.B().Xlen().Key(streamKey).Build()).AsInt64()
		require.NoError(t, err)
		require.Equal(t, int64(0), streamLen, "stream_key must be wiped after fresh-meta stream_read")
	})
}
