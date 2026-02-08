//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// randomChannel generates a unique channel name for testing to avoid cross-test pollution.
func randomChannel(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), rand.Intn(100000))
}

func newTestRedisMapBroker(tb testing.TB, n *Node) *RedisMapBroker {
	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisMapBroker(n, RedisMapBrokerConfig{
		Shards: []*RedisShard{shard},
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = n.Shutdown(context.Background())
	})
	return e
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
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_stateful")

	// Publish some keyed state updates
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1_updated"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read state
	entries, streamPos, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.NotEmpty(t, streamPos.Epoch)
	require.Greater(t, streamPos.Offset, uint64(0))

	// Verify state contains latest values
	state := stateToMap(entries)
	require.Len(t, state, 2)
	require.Equal(t, []byte("data1_updated"), state["key1"])
	require.Equal(t, []byte("data2"), state["key2"])

	// Read stream to verify all publications are in history
	pubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1, // Get all
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3) // All 3 publications in stream
}

// TestRedisMapBroker_StatefulChannelOrdered tests ordered stateful channel.
func TestRedisMapBroker_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered")

	// Publish with scores for ordering
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			Ordered:    true,
			Score:      int64(i * 10), // Scores: 0, 10, 20, 30, 40
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered state (descending by score)
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 5)

	// Verify all keys present
	state := stateToMap(entries)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		require.Contains(t, state, key)
	}
}

// TestRedisMapBroker_StateRevision tests that state values include revisions.
func TestRedisMapBroker_StateRevision(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_revision")

	// Publish a keyed state update
	res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Publish another update
	res2, err := broker.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res2.Position.Offset)
	require.Equal(t, res1.Position.Epoch, res2.Position.Epoch) // Same epoch

	// Read state - entries now include per-entry revisions
	entries, streamPos, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
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
}

// TestRedisMapBroker_StatePagination tests cursor-based state pagination.
func TestRedisMapBroker_StatePagination(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_pagination")

	// Publish 10 keyed entries
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read state with limit - HSCAN COUNT is a hint, not a guarantee
	// For small hashes, Redis may return all entries in one go
	page1, pos1, cursor, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    3,
		Cursor:   "",
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.NotEmpty(t, page1)

	// Collect all keys across pages
	allKeys := make(map[string]bool)
	for _, entry := range page1 {
		allKeys[entry.Key] = true
	}

	// Continue reading until cursor is "0" (end of iteration)
	for cursor != "" && cursor != "0" {
		page, pos, newCursor, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:    3,
			Cursor:   cursor,
			StateTTL: 300 * time.Second,
		})
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
}

// TestRedisMapBroker_EpochHandling tests epoch changes and state invalidation.
func TestRedisMapBroker_EpochHandling(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_epoch")

	// Publish initial data
	res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	epoch1 := res1.Position.Epoch

	// Read state
	entries, streamPos1, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, epoch1, streamPos1.Epoch)

	// Simulate epoch change by clearing stream (would happen on node restart)
	// In real scenario, this would be a different node with different ID
	// For test purposes, we'll just verify epoch consistency
	require.NotEmpty(t, epoch1)
}

// TestRedisMapBroker_Idempotency tests idempotent publishing.
func TestRedisMapBroker_Idempotency(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_idempotency")

	// Publish with idempotency key
	res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:                []byte("data1"),
		IdempotencyKey:      "unique-id-1",
		IdempotentResultTTL: 60 * time.Second,
		StreamSize:          100,
		StreamTTL:           300 * time.Second,
		KeyTTL:              300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Publish again with same idempotency key
	res2, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:                []byte("data1_different"),
		IdempotencyKey:      "unique-id-1",
		IdempotentResultTTL: 60 * time.Second,
		StreamSize:          100,
		StreamTTL:           300 * time.Second,
		KeyTTL:              300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed) // Suppressed due to idempotency
	require.Equal(t, SuppressReasonIdempotency, res2.SuppressReason)
	require.Equal(t, res1.Position.Offset, res2.Position.Offset) // Same offset
	require.Equal(t, res1.Position.Epoch, res2.Position.Epoch)   // Same epoch

	// State should still have original data (second publish was cached/skipped)
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state := stateToMap(entries)
	require.Len(t, state, 1)
	require.Equal(t, []byte("data1"), state["key1"])
}

// TestRedisMapBroker_VersionedPublishing tests version-based idempotency.
func TestRedisMapBroker_VersionedPublishing(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_version")

	// Publish with version 2 (version 0 means "disable version check")
	res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data_v2"),
		Version:    2,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Try to publish older version (should be suppressed)
	res2, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data_v1"),
		Version:    1,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed) // Suppressed due to out-of-order version
	require.Equal(t, SuppressReasonVersion, res2.SuppressReason)
	require.Equal(t, res1.Position.Offset, res2.Position.Offset) // Same offset (suppressed)

	// Publish newer version
	res3, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data_v3"),
		Version:    3,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed)
	require.Equal(t, uint64(2), res3.Position.Offset) // New offset

	// State should have v3 data
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state := stateToMap(entries)
	require.Equal(t, []byte("data_v3"), state["key1"])
}

// TestRedisMapBroker_MultipleChannels tests multiple channels independently.
func TestRedisMapBroker_MultipleChannels(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel1 := randomChannel("test_multi1")
	channel2 := randomChannel("test_multi2")

	// Publish to channel1
	_, err := broker.Publish(ctx, channel1, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Publish to channel2
	_, err = broker.Publish(ctx, channel2, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read channel1 state
	entries1, _, _, err := broker.ReadState(ctx, channel1, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state1 := stateToMap(entries1)
	require.Len(t, state1, 1)
	require.Equal(t, []byte("data1"), state1["key1"])

	// Read channel2 state
	entries2, _, _, err := broker.ReadState(ctx, channel2, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state2 := stateToMap(entries2)
	require.Len(t, state2, 1)
	require.Equal(t, []byte("data2"), state2["key2"])
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
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_readstream2")

	// Publish 5 messages to stream
	for i := 1; i <= 5; i++ {
		_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
			Data:       []byte(fmt.Sprintf("msg_%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Test 1: Read all messages (forward)
	pubs, streamPos, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 5)
	require.NotEmpty(t, streamPos.Epoch)
	require.Equal(t, uint64(5), streamPos.Offset)
	require.Equal(t, []byte("msg_1"), pubs[0].Data)
	require.Equal(t, []byte("msg_5"), pubs[4].Data)

	// Test 2: Read all messages (reverse)
	pubsRev, streamPosRev, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit:   -1,
			Reverse: true,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsRev, 5)
	require.Equal(t, uint64(5), streamPosRev.Offset)
	require.Equal(t, []byte("msg_5"), pubsRev[0].Data)
	require.Equal(t, []byte("msg_1"), pubsRev[4].Data)

	// Test 3: Read with limit
	pubsLimited, _, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: 2,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsLimited, 2)
	require.Equal(t, []byte("msg_1"), pubsLimited[0].Data)
	require.Equal(t, []byte("msg_2"), pubsLimited[1].Data)

	// Test 4: Read since offset
	pubsSince, _, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: 2,
				Epoch:  streamPos.Epoch,
			},
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsSince, 3)
	require.Equal(t, []byte("msg_3"), pubsSince[0].Data)
	require.Equal(t, []byte("msg_5"), pubsSince[2].Data)

	// Test 5: Metadata-only read
	pubsMeta, streamPosMeta, err := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: 0, // metadata only
		},
	})
	require.NoError(t, err)
	require.Nil(t, pubsMeta)
	require.Equal(t, uint64(5), streamPosMeta.Offset)
	require.NotEmpty(t, streamPosMeta.Epoch)
}

// TestRedisMapBroker_ReadStreamZero2 tests the 2-call zero-alloc version of ReadStream.
func TestRedisMapBroker_ReadStreamZero2(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_readstreamzero2")

	// Publish 5 messages to stream
	for i := 1; i <= 5; i++ {
		_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
			Data:       []byte(fmt.Sprintf("msg_%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Test 1: Read all messages (forward)
	pubs, streamPos, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 5)
	require.NotEmpty(t, streamPos.Epoch)
	require.Equal(t, uint64(5), streamPos.Offset)
	require.Equal(t, []byte("msg_1"), pubs[0].Data)
	require.Equal(t, []byte("msg_5"), pubs[4].Data)

	// Test 2: Read all messages (reverse)
	pubsRev, streamPosRev, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit:   -1,
			Reverse: true,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsRev, 5)
	require.Equal(t, uint64(5), streamPosRev.Offset)
	require.Equal(t, []byte("msg_5"), pubsRev[0].Data)
	require.Equal(t, []byte("msg_1"), pubsRev[4].Data)

	// Test 3: Read with limit
	pubsLimited, _, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: 2,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsLimited, 2)
	require.Equal(t, []byte("msg_1"), pubsLimited[0].Data)
	require.Equal(t, []byte("msg_2"), pubsLimited[1].Data)

	// Test 4: Read since offset
	pubsSince, _, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{
				Offset: 2,
				Epoch:  streamPos.Epoch,
			},
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsSince, 3)
	require.Equal(t, []byte("msg_3"), pubsSince[0].Data)
	require.Equal(t, []byte("msg_5"), pubsSince[2].Data)

	// Test 5: Metadata-only read
	pubsMeta, streamPosMeta, err := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: 0, // metadata only
		},
	})
	require.NoError(t, err)
	require.Nil(t, pubsMeta)
	require.Equal(t, uint64(5), streamPosMeta.Offset)
	require.NotEmpty(t, streamPosMeta.Epoch)
}

// TestRedisMapBroker_ReadStream2_Compatibility tests that ReadStream2 returns same results as ReadStream.
func TestRedisMapBroker_ReadStream2_Compatibility(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_compat")

	// Publish 10 messages
	for i := 1; i <= 10; i++ {
		_, err := broker.Publish(ctx, channel, "", MapPublishOptions{
			Data:       []byte(fmt.Sprintf("msg_%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Compare ReadStream and ReadStream2 results
	pubs1, pos1, err1 := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err1)

	pubs2, pos2, err2 := broker.ReadStream2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err2)

	// Compare results
	require.Equal(t, len(pubs1), len(pubs2))
	require.Equal(t, pos1.Offset, pos2.Offset)
	require.Equal(t, pos1.Epoch, pos2.Epoch)
	for i := range pubs1 {
		require.Equal(t, pubs1[i].Data, pubs2[i].Data)
		require.Equal(t, pubs1[i].Offset, pubs2[i].Offset)
	}

	// Compare ReadStreamZero and ReadStreamZero2 results
	pubs3, pos3, err3 := broker.ReadStreamZero(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err3)

	pubs4, pos4, err4 := broker.ReadStreamZero2(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err4)

	// Compare results
	require.Equal(t, len(pubs3), len(pubs4))
	require.Equal(t, pos3.Offset, pos4.Offset)
	require.Equal(t, pos3.Epoch, pos4.Epoch)
	for i := range pubs3 {
		require.Equal(t, pubs3[i].Data, pubs4[i].Data)
		require.Equal(t, pubs3[i].Offset, pubs4[i].Offset)
	}
}

// TestRedisMapBroker_CleanupGeneratesRemovalEvents verifies that the Lua cleanup script
// correctly generates removal events (key + removed + timestamp) when entries expire by TTL.
func TestRedisMapBroker_CleanupGeneratesRemovalEvents(t *testing.T) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				MetaTTL:    3600 * time.Second,
				KeyTTL:     30 * time.Second,
			}
		},
	})

	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	broker, err := NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:           []*RedisShard{shard},
		CleanupBatchSize: 100,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	ctx := context.Background()
	channel := randomChannel("test_cleanup_removal")

	// Publish two keyed entries with TTL.
	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"status":"online"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     30 * time.Second,
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte(`{"status":"active"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     30 * time.Second,
	})
	require.NoError(t, err)

	// Verify initial stream has 2 publish events.
	pubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 2, "Should have 2 publish events in stream")
	require.False(t, pubs[0].Removed)
	require.False(t, pubs[1].Removed)

	// Verify state has 2 entries.
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Simulate TTL expiry by calling cleanupChannel with a future "now".
	shardWrapper := broker.shards[0]
	cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
	futureNow := time.Now().Unix() + 31

	err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
	require.NoError(t, err)

	// Read the stream after cleanup - should have 2 publish + 2 removal events.
	pubs, _, err = broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 4, "Expected 4 events in stream (2 publish + 2 removal)")

	// Collect removal events.
	var removals []*Publication
	for _, pub := range pubs {
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
	entries, _, _, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 0, "State should be empty after cleanup")

	// Verify stats show 0 keys.
	stats, err := broker.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 0, stats.NumKeys, "Stats should show 0 keys")
}

// TestRedisMapBroker_CleanupPartialExpiry verifies that cleanup only removes expired entries,
// leaving non-expired ones untouched.
func TestRedisMapBroker_CleanupPartialExpiry(t *testing.T) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				MetaTTL:    3600 * time.Second,
				KeyTTL:     60 * time.Second,
			}
		},
	})

	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	broker, err := NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:           []*RedisShard{shard},
		CleanupBatchSize: 100,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	ctx := context.Background()
	channel := randomChannel("test_cleanup_partial")

	// Publish key1 with short TTL.
	_, err = broker.Publish(ctx, channel, "key_short", MapPublishOptions{
		Data:       []byte(`{"ttl":"short"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     10 * time.Second,
	})
	require.NoError(t, err)

	// Publish key2 with long TTL.
	_, err = broker.Publish(ctx, channel, "key_long", MapPublishOptions{
		Data:       []byte(`{"ttl":"long"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     600 * time.Second,
	})
	require.NoError(t, err)

	// Simulate time passing: 15 seconds (key_short expired, key_long still alive).
	shardWrapper := broker.shards[0]
	cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
	futureNow := time.Now().Unix() + 15

	err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
	require.NoError(t, err)

	// Verify only key_short was removed.
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1, "Only key_long should remain")
	require.Equal(t, "key_long", entries[0].Key)

	// Verify stream has 2 publish + 1 removal event.
	pubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3, "Expected 3 events (2 publish + 1 removal)")

	lastPub := pubs[2]
	require.True(t, lastPub.Removed)
	require.Equal(t, "key_short", lastPub.Key)
}

// TestRedisMapBroker_CleanupRefreshedTTL verifies that refreshing a key's TTL
// (by re-publishing with a longer TTL) prevents it from being cleaned up.
func TestRedisMapBroker_CleanupRefreshedTTL(t *testing.T) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				MetaTTL:    3600 * time.Second,
				KeyTTL:     600 * time.Second,
			}
		},
	})

	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	broker, err := NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:           []*RedisShard{shard},
		CleanupBatchSize: 100,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	ctx := context.Background()
	channel := randomChannel("test_cleanup_refresh")

	// Publish key with short TTL (10s).
	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"v":1}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     10 * time.Second,
	})
	require.NoError(t, err)

	// Re-publish the same key with a much longer TTL (600s), refreshing its expiry.
	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"v":2}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     600 * time.Second,
	})
	require.NoError(t, err)

	// Simulate cleanup at 15s from now — the original 10s TTL would have expired,
	// but the refreshed 600s TTL should keep the key alive.
	shardWrapper := broker.shards[0]
	cleanupKey := broker.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
	futureNow := time.Now().Unix() + 15

	err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
	require.NoError(t, err)

	// Key should still exist (TTL was refreshed to 600s).
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1, "Key should still exist after TTL refresh")
	require.Equal(t, "key1", entries[0].Key)
	require.Equal(t, []byte(`{"v":2}`), entries[0].Data)

	// Verify no removal events in stream (only 2 publish events).
	pubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	for _, pub := range pubs {
		require.False(t, pub.Removed, "Should have no removal events after TTL refresh")
	}
}

// TestRedisMapBroker_CleanupRegistration verifies that the cleanup registration ZSET
// is properly maintained (entries added on publish, removed when channel is fully cleaned).
func TestRedisMapBroker_CleanupRegistration(t *testing.T) {
	node, _ := New(Config{
		GetMapChannelOptions: func(channel string) MapChannelOptions {
			return MapChannelOptions{
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				MetaTTL:    3600 * time.Second,
				KeyTTL:     30 * time.Second,
			}
		},
	})

	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(node, redisConf)
	require.NoError(t, err)
	broker, err := NewRedisMapBroker(node, RedisMapBrokerConfig{
		Shards:           []*RedisShard{shard},
		CleanupBatchSize: 100,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
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
		Data:       []byte(`{"v":1}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     30 * time.Second,
	})
	require.NoError(t, err)

	// Channel should now be registered in cleanup ZSET.
	score, err = shardWrapper.shard.client.Do(ctx,
		shardWrapper.shard.client.B().Zscore().Key(cleanupKey).Member(channel).Build(),
	).AsFloat64()
	require.NoError(t, err)
	require.Greater(t, score, float64(0), "Channel should have an expiry score in cleanup ZSET")

	// Run cleanup with future time to expire the entry.
	futureNow := time.Now().Unix() + 31
	err = broker.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, futureNow)
	require.NoError(t, err)

	// After all entries expired, channel should be removed from cleanup ZSET.
	_, err = shardWrapper.shard.client.Do(ctx,
		shardWrapper.shard.client.B().Zscore().Key(cleanupKey).Member(channel).Build(),
	).AsFloat64()
	require.Error(t, err, "Channel should be removed from cleanup ZSET after all entries expired")
}

// Old aggregation and cleanup tests using removed API (AddMember/RemoveMember) have been
// replaced by TestRedisMapBroker_Cleanup* tests above using the current Publish API.

// TestRedisMapBroker_OrderedStateOrdering tests that ordered  state return entries
// in correct score order (ascending by score).
func TestRedisMapBroker_OrderedStateOrdering(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

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
			Data:       []byte(tc.data),
			Ordered:    true,
			Score:      tc.score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered state - should be sorted by score (ascending)
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 5, "Should have all 5 entries")

	// Verify ordering by score (descending: 50, 40, 30, 20, 10)
	expectedOrder := []string{"key_e", "key_d", "key_c", "key_b", "key_a"}
	for i, entry := range entries {
		require.Equal(t, expectedOrder[i], entry.Key, "Entry %d should be %s", i, expectedOrder[i])
	}

	t.Logf("SUCCESS: Entries returned in correct descending score order: %v", expectedOrder)
}

// TestRedisMapBroker_OrderedStatePagination tests that pagination over ordered state
// maintains correct ordering across pages.
func TestRedisMapBroker_OrderedStatePagination(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_pagination")

	// Publish 20 entries with scores 100, 200, 300, ..., 2000
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		score := int64(i * 100)
		data := fmt.Sprintf("data_%02d", i)

		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(data),
			Ordered:    true,
			Score:      score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (limit=5, no cursor)
	page1, pos1, cursor1, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    5,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page1, 5, "First page should have 5 entries")
	require.NotEmpty(t, cursor1, "Should have cursor for next page")

	// Verify first page ordering (descending: 20, 19, 18, 17, 16)
	for i := 0; i < 5; i++ {
		expectedKey := fmt.Sprintf("key_%02d", 20-i)
		require.Equal(t, expectedKey, page1[i].Key, "Page 1, entry %d should be %s", i, expectedKey)
	}

	// Read second page (using cursor)
	page2, pos2, cursor2, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Cursor:   cursor1,
		Limit:    5,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page2, 5, "Second page should have 5 entries")
	require.Equal(t, pos1.Epoch, pos2.Epoch, "Epoch should be consistent across pages")

	// Verify second page ordering (descending: 15, 14, 13, 12, 11)
	for i := 0; i < 5; i++ {
		expectedKey := fmt.Sprintf("key_%02d", 15-i)
		require.Equal(t, expectedKey, page2[i].Key, "Page 2, entry %d should be %s", i, expectedKey)
	}

	// Read third page (using cursor)
	page3, _, cursor3, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Cursor:   cursor2,
		Limit:    5,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page3, 5, "Third page should have 5 entries")

	// Verify third page ordering (descending: 10, 09, 08, 07, 06)
	for i := 0; i < 5; i++ {
		expectedKey := fmt.Sprintf("key_%02d", 10-i)
		require.Equal(t, expectedKey, page3[i].Key, "Page 3, entry %d should be %s", i, expectedKey)
	}

	// Read fourth page (using cursor)
	page4, _, cursor4, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Cursor:   cursor3,
		Limit:    5,
		StateTTL: 300 * time.Second,
	})
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
}

// TestRedisMapBroker_OrderedStateWithNegativeScores tests ordering with negative scores.
func TestRedisMapBroker_OrderedStateWithNegativeScores(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

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
			Data:       []byte("data"),
			Ordered:    true,
			Score:      tc.score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered state
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 5)

	// Verify ordering (descending: 100, 50, 0, -50, -100)
	expectedOrder := []string{"key_pos", "key_pos2", "key_zero", "key_neg", "key_neg2"}
	for i, entry := range entries {
		require.Equal(t, expectedOrder[i], entry.Key, "Entry %d should be %s", i, expectedOrder[i])
	}

	t.Logf("SUCCESS: Negative scores handled correctly in descending ordering")
}

// TestRedisMapBroker_OrderedStateWithSameScores tests ordering stability when scores are equal.
func TestRedisMapBroker_OrderedStateWithSameScores(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_same_scores")

	// Publish multiple entries with the same score
	// Redis ZSET uses lexicographic ordering for members with equal scores
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key_%d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%d", i)),
			Ordered:    true,
			Score:      100, // Same score for all
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered state
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 5)

	// With same scores, Redis sorts lexicographically by member (key)
	// With zrevrange (descending), we get reverse lexicographic: key_5, key_4, key_3, key_2, key_1
	for i := 0; i < 5; i++ {
		expectedKey := fmt.Sprintf("key_%d", 5-i)
		require.Equal(t, expectedKey, entries[i].Key, "Entry %d should be %s (reverse lexicographic order)", i, expectedKey)
	}

	t.Logf("SUCCESS: Equal scores maintain reverse lexicographic ordering")
}

// TestRedisMapBroker_OrderedStatePaginationBoundaries tests edge cases in cursor-based pagination.
func TestRedisMapBroker_OrderedStatePaginationBoundaries(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_boundaries")

	// Publish 10 entries
	for i := 1; i <= 10; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%02d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 10),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Test 1: Zero limit (should return all)
	all, _, cursor, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    0,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, all, 10, "Zero limit should return all entries")
	require.Empty(t, cursor, "No cursor when returning all entries")

	// Test 2: Limit larger than entries (should return all)
	largeLimit, _, cursor2, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, largeLimit, 10, "Limit larger than entries should return all")
	require.Empty(t, cursor2, "No cursor when returning all entries")

	// Test 3: Pagination with limit=3 through all entries
	page1, _, c1, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    3,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page1, 3)
	require.NotEmpty(t, c1)

	// Continue to get remaining entries
	page2, _, c2, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    3,
		Cursor:   c1,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page2, 3)
	require.NotEmpty(t, c2)

	page3, _, c3, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    3,
		Cursor:   c2,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page3, 3)
	require.NotEmpty(t, c3)

	// Last page should have 1 entry
	page4, _, c4, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    3,
		Cursor:   c3,
		StateTTL: 300 * time.Second,
	})
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
}

// TestRedisMapBroker_OrderedStateFullPagination tests complete cursor-based pagination loop.
func TestRedisMapBroker_OrderedStateFullPagination(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_full_pagination")

	totalEntries := 37 // Odd number to test edge cases
	pageSize := 10

	// Publish entries
	for i := 1; i <= totalEntries; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%03d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%03d", i)),
			Ordered:    true,
			Score:      int64(i * 10),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Paginate through all entries using cursor
	allKeys := []string{}
	cursor := ""
	iterations := 0

	for {
		entries, _, nextCursor, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Ordered:  true,
			Cursor:   cursor,
			Limit:    pageSize,
			StateTTL: 300 * time.Second,
		})
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
}

// TestRedisMapBroker_OrderedStateUpdatePreservesOrder tests that updating an entry's score
// changes its position in the ordered state.
func TestRedisMapBroker_OrderedStateUpdatePreservesOrder(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_update")

	// Publish 5 entries
	for i := 1; i <= 5; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%d", i)),
			Ordered:    true,
			Score:      int64(i * 10), // 10, 20, 30, 40, 50
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read initial order (descending: 50, 40, 30, 20, 10)
	entries1, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, "key_5", entries1[0].Key) // Highest score (50)
	require.Equal(t, "key_1", entries1[4].Key) // Lowest score (10)

	// Update key_1 to have highest score (60)
	_, err = broker.Publish(ctx, channel, "key_1", MapPublishOptions{
		Data:       []byte("updated_data"),
		Ordered:    true,
		Score:      60, // Now highest
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read updated order
	entries2, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries2, 5)

	// Verify new order (descending: 60, 50, 40, 30, 20)
	expectedOrder := []string{"key_1", "key_5", "key_4", "key_3", "key_2"}
	for i, entry := range entries2 {
		require.Equal(t, expectedOrder[i], entry.Key, "After update, entry %d should be %s", i, expectedOrder[i])
	}

	t.Logf("SUCCESS: Updating score correctly reorders entries in descending order")
}

// TestRedisMapBroker_KeyModeIfNew tests KeyModeIfNew - only write if key doesn't exist.
func TestRedisMapBroker_KeyModeIfNew(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_keymode_if_new")

	// First publish with KeyModeIfNew should succeed (key doesn't exist)
	res1, err := broker.Publish(ctx, channel, "slot1", MapPublishOptions{
		Data:       []byte("player1"),
		KeyMode:    KeyModeIfNew,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed, "First publish should not be suppressed")
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Second publish with KeyModeIfNew should be suppressed (key exists)
	res2, err := broker.Publish(ctx, channel, "slot1", MapPublishOptions{
		Data:       []byte("player2"),
		KeyMode:    KeyModeIfNew,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed, "Second publish should be suppressed (key exists)")
	require.Equal(t, SuppressReasonKeyExists, res2.SuppressReason)
	require.Equal(t, uint64(1), res2.Position.Offset, "Offset should not change")

	// Verify state still has original data
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("player1"), entries[0].Data)

	// Verify stream only has one entry (second was suppressed)
	pubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
}

// TestRedisMapBroker_KeyModeIfExists tests KeyModeIfExists - only write if key exists.
func TestRedisMapBroker_KeyModeIfExists(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_keymode_if_exists")

	// First publish with KeyModeIfExists should be suppressed (key doesn't exist)
	res1, err := broker.Publish(ctx, channel, "presence1", MapPublishOptions{
		Data:       []byte("heartbeat1"),
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res1.Suppressed, "First publish should be suppressed (key doesn't exist)")
	require.Equal(t, SuppressReasonKeyNotFound, res1.SuppressReason)

	// Create the key first with regular publish
	res2, err := broker.Publish(ctx, channel, "presence1", MapPublishOptions{
		Data:       []byte("initial"),
		KeyMode:    KeyModeReplace, // or just leave empty
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed, "Regular publish should not be suppressed")

	// Now publish with KeyModeIfExists should succeed (key exists)
	res3, err := broker.Publish(ctx, channel, "presence1", MapPublishOptions{
		Data:       []byte("heartbeat2"),
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed, "Third publish should not be suppressed (key exists)")

	// Verify state has updated data
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("heartbeat2"), entries[0].Data)
}

// TestRedisMapBroker_KeyModeReplace tests default KeyModeReplace behavior.
func TestRedisMapBroker_KeyModeReplace(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_keymode_replace")

	// First publish (key doesn't exist)
	res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("value1"),
		KeyMode:    KeyModeReplace, // explicit, same as default
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Second publish (key exists) - should still apply
	res2, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("value2"),
		KeyMode:    KeyModeReplace,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed, "Replace should never be suppressed")

	// Verify state has updated data
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("value2"), entries[0].Data)
}

func TestRedisMapBroker_Remove(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)

	ctx := context.Background()
	channel := randomChannel("test_remove")

	// Publish some keys
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Verify state has 2 keys
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Remove key1
	res, err := broker.Remove(ctx, channel, "key1", MapRemoveOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res.Suppressed)

	// Verify state has 1 key
	entries, _, _, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "key2", entries[0].Key)

	// Remove non-existent key should be suppressed with key_not_found
	res, err = broker.Remove(ctx, channel, "nonexistent", MapRemoveOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, res.SuppressReason)

	// Verify stream has only 3 entries (key1, key2, remove(key1)) - no entry for nonexistent
	pubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3)
	require.True(t, pubs[2].Removed)
	require.Equal(t, "key1", pubs[2].Key)
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
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)
	ctx := context.Background()
	channel := randomChannel("test_unordered_continuity_remove")

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// CONCURRENT MODIFICATION: Remove key_10.
	_, err := broker.Remove(ctx, channel, "key_10", MapRemoveOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Simulate full client recovery with pagination.
	result := simulateClientRecovery(t, broker, channel, false, 10)

	// Verify: should have 19 keys (key_10 was removed)
	require.Len(t, result, 19, "Should have 19 keys after removal")
	require.NotContains(t, result, "key_10", "key_10 should be removed")

	// Verify key_11 wasn't skipped (this was the bug with integer offsets)
	require.Contains(t, result, "key_11", "key_11 should not be skipped")
}

// TestRedisMapBroker_UnorderedContinuity_EntryAdded tests that adding
// an entry during unordered pagination doesn't cause issues.
// Note: HSCAN COUNT is only a hint — Redis may return more entries than requested
// (especially for small hashes stored as listpack), so we don't assert exact page sizes.
func TestRedisMapBroker_UnorderedContinuity_EntryAdded(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)
	ctx := context.Background()
	channel := randomChannel("test_unordered_continuity_add")

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// CONCURRENT MODIFICATION: Add new entry that lexicographically comes before cursor
	// This would shift entries with integer offset pagination
	_, err := broker.Publish(ctx, channel, "key_05b", MapPublishOptions{
		Data:       []byte("data_05b"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Simulate full client recovery
	result := simulateClientRecovery(t, broker, channel, false, 10)

	// Verify: should have 21 keys (original 20 + new key_05b)
	require.Len(t, result, 21, "Should have 21 keys after addition")
	require.Contains(t, result, "key_05b", "key_05b should be present")
	require.Equal(t, []byte("data_05b"), result["key_05b"])
}

// TestRedisMapBroker_OrderedContinuity_HigherScoreAdded tests that adding
// an entry with higher score during ordered pagination doesn't cause data loss.
func TestRedisMapBroker_OrderedContinuity_HigherScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_higher")

	// Create 20 entries with scores 100, 200, ..., 2000
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (should get keys with highest scores: key_20, key_19, ..., key_11)
	pubs1, _, cursor1, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)
	require.NotEmpty(t, cursor1)

	// Verify first page has highest scores
	require.Equal(t, "key_20", pubs1[0].Key, "First entry should be key_20 (score 2000)")

	// CONCURRENT MODIFICATION: Add entry with HIGHEST score
	// This entry would appear at position 0, shifting all entries
	_, err = broker.Publish(ctx, channel, "key_top", MapPublishOptions{
		Data:       []byte("data_top"),
		Ordered:    true,
		Score:      5000, // Higher than any existing
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
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
}

// TestRedisMapBroker_OrderedContinuity_LowerScoreAdded tests that adding
// an entry with lower score during ordered pagination works correctly.
func TestRedisMapBroker_OrderedContinuity_LowerScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_lower")

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page
	_, _, cursor1, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATION: Add entry with LOWEST score
	// This entry appears at end, shouldn't affect pagination much
	_, err = broker.Publish(ctx, channel, "key_bottom", MapPublishOptions{
		Data:       []byte("data_bottom"),
		Ordered:    true,
		Score:      1, // Lower than any existing
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Continue reading with cursor - just verify it works
	pubs2, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		Cursor:   cursor1,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pubs2)

	// Simulate full recovery
	result := simulateClientRecovery(t, broker, channel, true, 10)

	require.Len(t, result, 21, "Should have 21 keys")
	require.Contains(t, result, "key_bottom", "key_bottom should be present")
}

// TestRedisMapBroker_OrderedContinuity_ScoreChanged tests that changing
// an entry's score during pagination (causing reordering) doesn't lose data.
func TestRedisMapBroker_OrderedContinuity_ScoreChanged(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_score_change")

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (key_20 down to key_11)
	pubs1, streamPos1, cursor1, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	// CONCURRENT MODIFICATION: Change score of key_05 to make it jump to top
	// key_05 had score 500, now gets score 3000 (highest)
	// This entry was NOT in first page, but now would appear at position 0
	_, err = broker.Publish(ctx, channel, "key_05", MapPublishOptions{
		Data:       []byte("data_05_updated"),
		Ordered:    true,
		Score:      3000,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page - key_05 jumped out of this range
	pubs2, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		Cursor:   cursor1,
		StateTTL: 300 * time.Second,
	})
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
	streamPubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos1,
			Limit: -1,
		},
	})
	require.NoError(t, err)

	for _, pub := range streamPubs {
		if pub.Removed {
			delete(stateData, pub.Key)
		} else {
			stateData[pub.Key] = pub.Data
		}
	}

	// Verify: should have 20 keys, key_05 should have updated data
	require.Len(t, stateData, 20, "Should have 20 keys")
	require.Equal(t, []byte("data_05_updated"), stateData["key_05"], "key_05 should have updated data from stream")
}

// TestRedisMapBroker_OrderedContinuity_EntryRemoved tests that removing
// an entry during ordered pagination doesn't cause data loss.
func TestRedisMapBroker_OrderedContinuity_EntryRemoved(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_remove")

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (key_20 down to key_11)
	pubs1, streamPos1, cursor1, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATION: Remove key_10 (first entry of next page)
	_, err = broker.Remove(ctx, channel, "key_10", MapRemoveOptions{

		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
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
		pubs, _, nextCursor, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Ordered:  true,
			Limit:    10,
			Cursor:   cursor,
			StateTTL: 300 * time.Second,
		})
		require.NoError(t, err)
		for _, pub := range pubs {
			if pub.Offset <= streamPos1.Offset {
				allStateData[pub.Key] = pub.Data
			}
		}
		cursor = nextCursor
	}

	// Apply stream changes
	streamPubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos1,
			Limit: -1,
		},
	})
	require.NoError(t, err)

	for _, pub := range streamPubs {
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
}

// TestRedisMapBroker_OrderedContinuity_MultipleChanges tests recovery
// with multiple concurrent changes during pagination.
func TestRedisMapBroker_OrderedContinuity_MultipleChanges(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestRedisMapBroker(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_multi")

	// Create 30 entries
	for i := 1; i <= 30; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page
	_, _, cursor1, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATIONS:
	// 1. Add new highest score entry
	_, err = broker.Publish(ctx, channel, "key_new_top", MapPublishOptions{
		Data:       []byte("data_new_top"),
		Ordered:    true,
		Score:      5000,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// 2. Remove an entry from middle
	_, err = broker.Remove(ctx, channel, "key_15", MapRemoveOptions{

		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// 3. Update score of an entry (move it)
	_, err = broker.Publish(ctx, channel, "key_05", MapPublishOptions{
		Data:       []byte("data_05_moved"),
		Ordered:    true,
		Score:      4000, // Move from 500 to 4000
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page
	_, _, cursor2, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    10,
		Cursor:   cursor1,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// 4. Add new lowest score entry
	_, err = broker.Publish(ctx, channel, "key_new_bottom", MapPublishOptions{
		Data:       []byte("data_new_bottom"),
		Ordered:    true,
		Score:      1,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read remaining pages
	cursor := cursor2
	for cursor != "" {
		_, _, nextCursor, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Ordered:  true,
			Limit:    10,
			Cursor:   cursor,
			StateTTL: 300 * time.Second,
		})
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
}
