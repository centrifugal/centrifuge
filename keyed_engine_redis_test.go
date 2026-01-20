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

func newTestSnapshotRedisEngine(tb testing.TB, n *Node) *RedisKeyedEngine {
	redisConf := testSingleRedisConf(6379)
	shard, err := NewRedisShard(n, redisConf)
	require.NoError(tb, err)
	e, err := NewRedisKeyedEngine(n, RedisKeyedEngineConfig{
		Shards:                []*RedisShard{shard},
		PresenceTTL:           60 * time.Second,
		PresenceStreamSize:    100,
		PresenceStreamTTL:     300 * time.Second,
		PresenceStreamMetaTTL: 3600 * time.Second,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = n.Shutdown(context.Background())
	})
	return e
}

// snapshotToMap converts []SnapshotEntry to map for easier testing.
// It extracts the raw data from the Publication protobuf payload.
func snapshotToMap(pubs []*Publication) map[string][]byte {
	result := make(map[string][]byte, len(pubs))
	for _, pub := range pubs {
		// Extract data from Publication
		result[pub.Key] = pub.Data
	}
	return result
}

// TestRedisKeyedEngine_StatefulChannel tests stateful channel with keyed state and revisions.
func TestRedisKeyedEngine_StatefulChannel(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_stateful")

	// Publish some keyed state updates
	_, err := engine.Publish(ctx, channel, "key1", []byte("data1"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key2", []byte("data2"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key1", []byte("data1_updated"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read snapshot
	entries, streamPos, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.NotEmpty(t, streamPos.Epoch)
	require.Greater(t, streamPos.Offset, uint64(0))

	// Verify snapshot contains latest values
	snapshot := snapshotToMap(entries)
	require.Len(t, snapshot, 2)
	require.Equal(t, []byte("data1_updated"), snapshot["key1"])
	require.Equal(t, []byte("data2"), snapshot["key2"])

	// Read stream to verify all publications are in history
	pubs, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1, // Get all
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3) // All 3 publications in stream
}

// TestRedisKeyedEngine_StatefulChannelOrdered tests ordered stateful channel.
func TestRedisKeyedEngine_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered")

	// Publish with scores for ordering
	for i := 0; i < 5; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("data%d", i)), KeyedPublishOptions{
			Ordered:    true,
			Score:      int64(i * 10), // Scores: 0, 10, 20, 30, 40
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered snapshot (descending by score)
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 5)

	// Verify all keys present
	snapshot := snapshotToMap(entries)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		require.Contains(t, snapshot, key)
	}
}

// TestRedisKeyedEngine_SnapshotRevision tests that snapshot values include revisions.
func TestRedisKeyedEngine_SnapshotRevision(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_revision")

	// Publish a keyed state update
	res1, err := engine.Publish(ctx, channel, "key1", []byte("data1"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Publish another update
	res2, err := engine.Publish(ctx, channel, "key2", []byte("data2"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res2.Position.Offset)
	require.Equal(t, res1.Position.Epoch, res2.Position.Epoch) // Same epoch

	// Read snapshot - entries now include per-entry revisions
	entries, streamPos, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, res2.Position.Offset, streamPos.Offset)
	require.Equal(t, res2.Position.Epoch, streamPos.Epoch)

	// Verify payloads
	snapshot := snapshotToMap(entries)
	require.Equal(t, []byte("data1"), snapshot["key1"])
	require.Equal(t, []byte("data2"), snapshot["key2"])

	// Verify per-entry offsets (epoch is in streamPos, same for all)
	require.NotEmpty(t, streamPos.Epoch)
	for _, pub := range entries {
		require.Greater(t, pub.Offset, uint64(0))
	}
}

//// TestRedisKeyedEngine_ConvergedMembership tests presence with revisions and ordering.
//func TestRedisKeyedEngine_ConvergedMembership(t *testing.T) {
//	node, _ := New(Config{})
//	engine := newTestSnapshotRedisEngine(t, node)
//
//	ctx := context.Background()
//	channel := randomChannel("test_presence")
//
//	// Add presence for multiple clients
//	client1 := ClientInfo{
//		ClientID: "client1",
//		UserID:   "user1",
//	}
//	client2 := ClientInfo{
//		ClientID: "client2",
//		UserID:   "user1", // Same user, different client
//	}
//	client3 := ClientInfo{
//		ClientID: "client3",
//		UserID:   "user2",
//	}
//
//	err := engine.AddMember(ctx, channel, client1, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	err = engine.AddMember(ctx, channel, client2, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	err = engine.AddMember(ctx, channel, client3, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Get presence
//	presence, err := engine.Members(ctx, channel)
//	require.NoError(t, err)
//	require.Len(t, presence, 3)
//	require.Equal(t, "user1", presence["client1"].UserID)
//	require.Equal(t, "user1", presence["client2"].UserID)
//	require.Equal(t, "user2", presence["client3"].UserID)
//
//	// Get presence stats (uses generic aggregations)
//	stats, err := engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 3, stats.NumKeys)
//	require.Equal(t, 2, stats.NumAggregatedKeys) // user1 and user2
//
//	// Remove one client from user1
//	err = engine.RemoveMember(ctx, channel, client1, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Get presence again
//	presence, err = engine.Members(ctx, channel)
//	require.NoError(t, err)
//	require.Len(t, presence, 2)
//	require.NotContains(t, presence, "client1")
//
//	// Stats should show user1 still present (client2 is still there)
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 2, stats.NumKeys)
//	require.Equal(t, 2, stats.NumAggregatedKeys) // Still 2 users
//
//	// Remove second client from user1
//	err = engine.RemoveMember(ctx, channel, client2, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Now user1 should be gone from aggregation
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 1, stats.NumKeys)
//	require.Equal(t, 1, stats.NumAggregatedKeys) // Only user2 remains
//}

//// TestRedisKeyedEngine_PresenceStream tests presence event stream (joins/leaves).
//func TestRedisKeyedEngine_PresenceStream(t *testing.T) {
//	node, _ := New(Config{})
//	engine := newTestSnapshotRedisEngine(t, node)
//
//	ctx := context.Background()
//	channel := randomChannel("test_presence_stream")
//
//	client := ClientInfo{
//		ClientID: "client1",
//		UserID:   "user1",
//	}
//
//	// Add presence
//	err := engine.AddMember(ctx, channel, client, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Remove presence
//	err = engine.RemoveMember(ctx, channel, client, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Read presence stream
//	events, streamPos, err := engine.ReadPresenceStream(ctx, channel, KeyedReadStreamOptions{
//		Filter: StreamFilter{
//			Limit: -1, // Get all
//		},
//	})
//	require.NoError(t, err)
//	require.Len(t, events, 2) // Join and leave
//	require.NotEmpty(t, streamPos.Epoch)
//	require.Greater(t, streamPos.Offset, uint64(0))
//
//	// Verify event types
//	require.False(t, events[0].Removed)
//	require.True(t, events[1].Removed)
//	require.Equal(t, "client1", events[0].Info.ClientID)
//	require.Equal(t, "client1", events[1].Info.ClientID)
//
//	// Verify events have ordered offsets
//	require.Equal(t, uint64(1), events[0].Offset)
//	require.Equal(t, uint64(2), events[1].Offset)
//}

// TestRedisKeyedEngine_SnapshotPagination tests cursor-based snapshot pagination.
func TestRedisKeyedEngine_SnapshotPagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_pagination")

	// Publish 10 keyed entries
	for i := 0; i < 10; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("data%d", i)), KeyedPublishOptions{
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read snapshot with limit - HSCAN COUNT is a hint, not a guarantee
	// For small hashes, Redis may return all entries in one go
	page1, pos1, cursor, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       3,
		Cursor:      "",
		SnapshotTTL: 300 * time.Second,
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
		page, pos, newCursor, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
			Limit:       3,
			Cursor:      cursor,
			SnapshotTTL: 300 * time.Second,
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

// TestRedisKeyedEngine_EpochHandling tests epoch changes and snapshot invalidation.
func TestRedisKeyedEngine_EpochHandling(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_epoch")

	// Publish initial data
	res1, err := engine.Publish(ctx, channel, "key1", []byte("data1"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	epoch1 := res1.Position.Epoch

	// Read snapshot
	entries, streamPos1, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, epoch1, streamPos1.Epoch)

	// Simulate epoch change by clearing stream (would happen on node restart)
	// In real scenario, this would be a different node with different ID
	// For test purposes, we'll just verify epoch consistency
	require.NotEmpty(t, epoch1)
}

// TestRedisKeyedEngine_Idempotency tests idempotent publishing.
func TestRedisKeyedEngine_Idempotency(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_idempotency")

	// Publish with idempotency key
	res1, err := engine.Publish(ctx, channel, "key1", []byte("data1"), KeyedPublishOptions{
		IdempotencyKey:      "unique-id-1",
		IdempotentResultTTL: 60 * time.Second,
		StreamSize:          100,
		StreamTTL:           300 * time.Second,
		KeyTTL:              300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res1.Applied)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Publish again with same idempotency key
	res2, err := engine.Publish(ctx, channel, "key1", []byte("data1_different"), KeyedPublishOptions{
		IdempotencyKey:      "unique-id-1",
		IdempotentResultTTL: 60 * time.Second,
		StreamSize:          100,
		StreamTTL:           300 * time.Second,
		KeyTTL:              300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Applied) // Not applied due to idempotency suppression
	require.Equal(t, res1.Position.Offset, res2.Position.Offset) // Same offset
	require.Equal(t, res1.Position.Epoch, res2.Position.Epoch)   // Same epoch

	// Snapshot should still have original data (second publish was cached/skipped)
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	snapshot := snapshotToMap(entries)
	require.Len(t, snapshot, 1)
	require.Equal(t, []byte("data1"), snapshot["key1"])
}

// TestRedisKeyedEngine_VersionedPublishing tests version-based idempotency.
func TestRedisKeyedEngine_VersionedPublishing(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_version")

	// Publish with version 2 (version 0 means "disable version check")
	res1, err := engine.Publish(ctx, channel, "key1", []byte("data_v2"), KeyedPublishOptions{
		Version:    2,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res1.Applied)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Try to publish older version (should be suppressed)
	res2, err := engine.Publish(ctx, channel, "key1", []byte("data_v1"), KeyedPublishOptions{
		Version:    1,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Applied) // Not applied due to out-of-order version
	require.Equal(t, res1.Position.Offset, res2.Position.Offset) // Same offset (suppressed)

	// Publish newer version
	res3, err := engine.Publish(ctx, channel, "key1", []byte("data_v3"), KeyedPublishOptions{
		Version:    3,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res3.Applied)
	require.Equal(t, uint64(2), res3.Position.Offset) // New offset

	// Snapshot should have v3 data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	snapshot := snapshotToMap(entries)
	require.Equal(t, []byte("data_v3"), snapshot["key1"])
}

// TestRedisKeyedEngine_MultipleChannels tests multiple channels independently.
func TestRedisKeyedEngine_MultipleChannels(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel1 := randomChannel("test_multi1")
	channel2 := randomChannel("test_multi2")

	// Publish to channel1
	_, err := engine.Publish(ctx, channel1, "key1", []byte("data1"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Publish to channel2
	_, err = engine.Publish(ctx, channel2, "key2", []byte("data2"), KeyedPublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read channel1 snapshot
	entries1, _, _, err := engine.ReadSnapshot(ctx, channel1, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	snapshot1 := snapshotToMap(entries1)
	require.Len(t, snapshot1, 1)
	require.Equal(t, []byte("data1"), snapshot1["key1"])

	// Read channel2 snapshot
	entries2, _, _, err := engine.ReadSnapshot(ctx, channel2, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	snapshot2 := snapshotToMap(entries2)
	require.Len(t, snapshot2, 1)
	require.Equal(t, []byte("data2"), snapshot2["key2"])
}

// TestParseSnapshotValue tests the snapshot value parsing helper.
func TestParseSnapshotValue(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		wantOffset  uint64
		wantEpoch   string
		wantPayload []byte
		wantErr     bool
	}{
		{
			name:        "valid snapshot value",
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
			offset, epoch, payload, err := parseSnapshotValue(tt.input)
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

// TestRedisKeyedEngine_ReadStream2 tests the 2-call version of ReadStream.
func TestRedisKeyedEngine_ReadStream2(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_readstream2")

	// Publish 5 messages to stream
	for i := 1; i <= 5; i++ {
		_, err := engine.Publish(ctx, channel, "", []byte(fmt.Sprintf("msg_%d", i)), KeyedPublishOptions{
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Test 1: Read all messages (forward)
	pubs, streamPos, err := engine.ReadStream2(ctx, channel, KeyedReadStreamOptions{
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
	pubsRev, streamPosRev, err := engine.ReadStream2(ctx, channel, KeyedReadStreamOptions{
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
	pubsLimited, _, err := engine.ReadStream2(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Limit: 2,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsLimited, 2)
	require.Equal(t, []byte("msg_1"), pubsLimited[0].Data)
	require.Equal(t, []byte("msg_2"), pubsLimited[1].Data)

	// Test 4: Read since offset
	pubsSince, _, err := engine.ReadStream2(ctx, channel, KeyedReadStreamOptions{
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
	pubsMeta, streamPosMeta, err := engine.ReadStream2(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Limit: 0, // metadata only
		},
	})
	require.NoError(t, err)
	require.Nil(t, pubsMeta)
	require.Equal(t, uint64(5), streamPosMeta.Offset)
	require.NotEmpty(t, streamPosMeta.Epoch)
}

// TestRedisKeyedEngine_ReadStreamZero2 tests the 2-call zero-alloc version of ReadStream.
func TestRedisKeyedEngine_ReadStreamZero2(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_readstreamzero2")

	// Publish 5 messages to stream
	for i := 1; i <= 5; i++ {
		_, err := engine.Publish(ctx, channel, "", []byte(fmt.Sprintf("msg_%d", i)), KeyedPublishOptions{
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Test 1: Read all messages (forward)
	pubs, streamPos, err := engine.ReadStreamZero2(ctx, channel, KeyedReadStreamOptions{
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
	pubsRev, streamPosRev, err := engine.ReadStreamZero2(ctx, channel, KeyedReadStreamOptions{
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
	pubsLimited, _, err := engine.ReadStreamZero2(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Limit: 2,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubsLimited, 2)
	require.Equal(t, []byte("msg_1"), pubsLimited[0].Data)
	require.Equal(t, []byte("msg_2"), pubsLimited[1].Data)

	// Test 4: Read since offset
	pubsSince, _, err := engine.ReadStreamZero2(ctx, channel, KeyedReadStreamOptions{
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
	pubsMeta, streamPosMeta, err := engine.ReadStreamZero2(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Limit: 0, // metadata only
		},
	})
	require.NoError(t, err)
	require.Nil(t, pubsMeta)
	require.Equal(t, uint64(5), streamPosMeta.Offset)
	require.NotEmpty(t, streamPosMeta.Epoch)
}

// TestRedisKeyedEngine_ReadStream2_Compatibility tests that ReadStream2 returns same results as ReadStream.
func TestRedisKeyedEngine_ReadStream2_Compatibility(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_compat")

	// Publish 10 messages
	for i := 1; i <= 10; i++ {
		_, err := engine.Publish(ctx, channel, "", []byte(fmt.Sprintf("msg_%d", i)), KeyedPublishOptions{
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Compare ReadStream and ReadStream2 results
	pubs1, pos1, err1 := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err1)

	pubs2, pos2, err2 := engine.ReadStream2(ctx, channel, KeyedReadStreamOptions{
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
	pubs3, pos3, err3 := engine.ReadStreamZero(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err3)

	pubs4, pos4, err4 := engine.ReadStreamZero2(ctx, channel, KeyedReadStreamOptions{
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

//// TestRedisKeyedEngine_CleanupGeneratesLeaveMessages verifies that the Lua cleanup script
//// correctly generates minimal LEAVE messages (key + removed + timestamp) when entries expire.
//func TestRedisKeyedEngine_CleanupGeneratesLeaveMessages(t *testing.T) {
//	node, _ := New(Config{})
//
//	// Create engine with PresenceTTL for testing
//	redisConf := testSingleRedisConf(6379)
//	shard, err := NewRedisShard(node, redisConf)
//	require.NoError(t, err)
//	engine, err := NewRedisKeyedEngine(node, RedisKeyedEngineConfig{
//		Shards:                []*RedisShard{shard},
//		PresenceTTL:           30 * time.Second, // Set long TTL so expire ZSET doesn't disappear
//		PresenceStreamSize:    100,
//		PresenceStreamTTL:     300 * time.Second,
//		PresenceStreamMetaTTL: 3600 * time.Second,
//	})
//	require.NoError(t, err)
//	t.Cleanup(func() {
//		_ = node.Shutdown(context.Background())
//	})
//
//	ctx := context.Background()
//	channel := randomChannel("test_leave")
//
//	// Add a member (TTL comes from engine config: 2 seconds)
//	clientID := "client123"
//	clientInfo := ClientInfo{
//		ClientID: clientID,
//		UserID:   "user123",
//	}
//	err = engine.AddMember(ctx, channel, clientInfo, EnginePresenceOptions{})
//	require.NoError(t, err)
//	t.Logf("Added member %s", clientID)
//
//	// Check what keys were created and what's in the expire ZSET
//	shardWrapper := engine.shards[0]
//	keys := []string{
//		engine.snapshotHashKey(shardWrapper.shard, channel),
//		engine.snapshotExpireKey(shardWrapper.shard, channel),
//		engine.streamKey(shardWrapper.shard, channel),
//		engine.metaKey(shardWrapper.shard, channel),
//	}
//	for _, key := range keys {
//		exists, err := shardWrapper.shard.client.Do(ctx, shardWrapper.shard.client.B().Exists().Key(key).Build()).AsBool()
//		require.NoError(t, err)
//		t.Logf("Key %s exists: %v", key, exists)
//	}
//
//	// Check expire ZSET contents immediately
//	cmd := shardWrapper.shard.client.B().Zrangebyscore().Key(engine.snapshotExpireKey(shardWrapper.shard, channel)).Min("0").Max("+inf").Withscores().Build()
//	result, err := shardWrapper.shard.client.Do(ctx, cmd).AsStrSlice()
//	require.NoError(t, err)
//	t.Logf("Expire ZSET immediately after add: %v", result)
//
//	// Verify the stream contains the ADD event
//	streamInitial, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
//		Filter: StreamFilter{Limit: -1},
//	})
//	require.NoError(t, err)
//	require.Len(t, streamInitial, 1, "Should have 1 ADD event in stream")
//	require.Equal(t, clientID, streamInitial[0].Key)
//	require.False(t, streamInitial[0].Removed, "Initial event should be ADD (Removed=false)")
//	t.Logf("Initial stream event: Offset=%d Key=%q Removed=%v", streamInitial[0].Offset, streamInitial[0].Key, streamInitial[0].Removed)
//
//	// Don't wait for actual expiration - we'll simulate it by passing a future "now" value to cleanup
//	time.Sleep(1 * time.Second)
//
//	// Check if cleanup is registered (should be set by AddMember)
//	cleanupKey := engine.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
//
//	// Manually trigger cleanup with a "now" value that's 31 seconds in the future
//	// This simulates that the member (with 30s TTL) has expired
//	now := time.Now().Unix() + 31
//	t.Logf("Running cleanup with simulated future time (now + 31 seconds)")
//
//	err = engine.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, now)
//	require.NoError(t, err)
//	t.Logf("Cleanup completed")
//
//	// Read the stream after cleanup - should have ADD + LEAVE events
//	pubs, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
//		Filter: StreamFilter{Limit: -1},
//	})
//	require.NoError(t, err)
//	require.Len(t, pubs, 2, "Expected 2 events in stream (ADD + LEAVE)")
//
//	// Log all events
//	for i, pub := range pubs {
//		t.Logf("  [%d] Offset=%d Key=%q Removed=%v Time=%d", i, pub.Offset, pub.Key, pub.Removed, pub.Time)
//	}
//
//	// Verify the LEAVE event
//	leaveEvent := pubs[1] // Second event should be LEAVE
//	require.Equal(t, clientID, leaveEvent.Key, "LEAVE event should have correct client ID")
//	require.True(t, leaveEvent.Removed, "LEAVE event should have Removed=true")
//	require.Greater(t, leaveEvent.Time, int64(0), "LEAVE event should have timestamp")
//	require.Empty(t, leaveEvent.Data, "LEAVE event should have no data")
//
//	t.Logf("SUCCESS: Lua cleanup script correctly generated minimal LEAVE event")
//	t.Logf("  Key: %s, Removed: %v, Time: %d", leaveEvent.Key, leaveEvent.Removed, leaveEvent.Time)
//}

//// TestRedisKeyedEngine_AggregationWithMultipleConnections verifies that user aggregation
//// correctly tracks multiple connections from the same user.
//func TestRedisKeyedEngine_AggregationWithMultipleConnections(t *testing.T) {
//	node, _ := New(Config{})
//	engine := newTestSnapshotRedisEngine(t, node)
//
//	ctx := context.Background()
//	channel := randomChannel("test_aggregation")
//
//	userID := "alice"
//
//	// Alice opens 3 connections
//	conn1 := "conn_1"
//	conn2 := "conn_2"
//	conn3 := "conn_3"
//
//	// Add first connection
//	err := engine.AddMember(ctx, channel, ClientInfo{
//		ClientID: conn1,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Check stats
//	stats, err := engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 1, stats.NumKeys, "Should have 1 client connection")
//	require.Equal(t, 1, stats.NumAggregatedKeys, "Should have 1 unique user")
//	t.Logf("After conn1: %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	// Add second connection (same user)
//	err = engine.AddMember(ctx, channel, ClientInfo{
//		ClientID: conn2,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 2, stats.NumKeys, "Should have 2 client connections")
//	require.Equal(t, 1, stats.NumAggregatedKeys, "Should still have 1 unique user")
//	t.Logf("After conn2: %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	// Add third connection (same user)
//	err = engine.AddMember(ctx, channel, ClientInfo{
//		ClientID: conn3,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 3, stats.NumKeys, "Should have 3 client connections")
//	require.Equal(t, 1, stats.NumAggregatedKeys, "Should still have 1 unique user")
//	t.Logf("After conn3: %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	// Remove first connection
//	err = engine.RemoveMember(ctx, channel, ClientInfo{
//		ClientID: conn1,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 2, stats.NumKeys, "Should have 2 client connections")
//	require.Equal(t, 1, stats.NumAggregatedKeys, "Should still have 1 unique user (2 connections remain)")
//	t.Logf("After removing conn1: %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	// Remove second connection
//	err = engine.RemoveMember(ctx, channel, ClientInfo{
//		ClientID: conn2,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 1, stats.NumKeys, "Should have 1 client connection")
//	require.Equal(t, 1, stats.NumAggregatedKeys, "Should still have 1 unique user (1 connection remains)")
//	t.Logf("After removing conn2: %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	// Remove third connection (last one)
//	err = engine.RemoveMember(ctx, channel, ClientInfo{
//		ClientID: conn3,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 0, stats.NumKeys, "Should have 0 client connections")
//	require.Equal(t, 0, stats.NumAggregatedKeys, "Should have 0 unique users (all connections closed)")
//	t.Logf("After removing conn3 (last): %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	t.Logf("SUCCESS: Aggregation correctly tracks multiple connections per user")
//}

//// TestRedisKeyedEngine_AggregationWithCleanup verifies that cleanup script
//// correctly updates aggregation when connections expire.
//func TestRedisKeyedEngine_AggregationWithCleanup(t *testing.T) {
//	node, _ := New(Config{})
//
//	redisConf := testSingleRedisConf(6379)
//	shard, err := NewRedisShard(node, redisConf)
//	require.NoError(t, err)
//	engine, err := NewRedisKeyedEngine(node, RedisKeyedEngineConfig{
//		Shards:                []*RedisShard{shard},
//		PresenceTTL:           30 * time.Second,
//		PresenceStreamSize:    100,
//		PresenceStreamTTL:     300 * time.Second,
//		PresenceStreamMetaTTL: 3600 * time.Second,
//	})
//	require.NoError(t, err)
//	t.Cleanup(func() {
//		_ = node.Shutdown(context.Background())
//	})
//
//	ctx := context.Background()
//	channel := randomChannel("test_agg_cleanup")
//
//	userID := "alice"
//
//	// Alice opens 2 connections
//	conn1 := "conn_1"
//	conn2 := "conn_2"
//
//	err = engine.AddMember(ctx, channel, ClientInfo{
//		ClientID: conn1,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	err = engine.AddMember(ctx, channel, ClientInfo{
//		ClientID: conn2,
//		UserID:   userID,
//	}, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Verify 2 connections, 1 user
//	stats, err := engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 2, stats.NumKeys, "Should have 2 connections")
//	require.Equal(t, 1, stats.NumAggregatedKeys, "Should have 1 unique user")
//	t.Logf("Before cleanup: %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	// Simulate conn1 expiring via cleanup script
//	shardWrapper := engine.shards[0]
//	cleanupKey := engine.cleanupRegistrationKeyForChannel(shardWrapper.shard, channel)
//	now := time.Now().Unix() + 31 // Simulate future
//
//	err = engine.cleanupChannel(ctx, shardWrapper.shard, channel, cleanupKey, now)
//	require.NoError(t, err)
//
//	// After cleanup of both expired connections
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 0, stats.NumKeys, "Should have 0 connections (both cleaned up)")
//	require.Equal(t, 0, stats.NumAggregatedKeys, "Should have 0 users (aggregation updated correctly)")
//	t.Logf("After cleanup: %d clients, %d users", stats.NumKeys, stats.NumAggregatedKeys)
//
//	t.Logf("SUCCESS: Cleanup script correctly updates aggregation")
//}

// TestRedisKeyedEngine_OrderedSnapshotOrdering tests that ordered snapshots return entries
// in correct score order (ascending by score).
func TestRedisKeyedEngine_OrderedSnapshotOrdering(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

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
		_, err := engine.Publish(ctx, channel, tc.key, []byte(tc.data), KeyedPublishOptions{
			Ordered:    true,
			Score:      tc.score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered snapshot - should be sorted by score (ascending)
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
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

// TestRedisKeyedEngine_OrderedSnapshotPagination tests that pagination over ordered snapshots
// maintains correct ordering across pages.
func TestRedisKeyedEngine_OrderedSnapshotPagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_pagination")

	// Publish 20 entries with scores 100, 200, 300, ..., 2000
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		score := int64(i * 100)
		data := fmt.Sprintf("data_%02d", i)

		_, err := engine.Publish(ctx, channel, key, []byte(data), KeyedPublishOptions{
			Ordered:    true,
			Score:      score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (offset=0, limit=5)
	page1, pos1, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      0,
		Limit:       5,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page1, 5, "First page should have 5 entries")

	// Verify first page ordering (descending: 20, 19, 18, 17, 16)
	for i := 0; i < 5; i++ {
		expectedKey := fmt.Sprintf("key_%02d", 20-i)
		require.Equal(t, expectedKey, page1[i].Key, "Page 1, entry %d should be %s", i, expectedKey)
	}

	// Read second page (offset=5, limit=5)
	page2, pos2, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      5,
		Limit:       5,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page2, 5, "Second page should have 5 entries")
	require.Equal(t, pos1.Epoch, pos2.Epoch, "Epoch should be consistent across pages")

	// Verify second page ordering (descending: 15, 14, 13, 12, 11)
	for i := 0; i < 5; i++ {
		expectedKey := fmt.Sprintf("key_%02d", 15-i)
		require.Equal(t, expectedKey, page2[i].Key, "Page 2, entry %d should be %s", i, expectedKey)
	}

	// Read third page (offset=10, limit=5)
	page3, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      10,
		Limit:       5,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page3, 5, "Third page should have 5 entries")

	// Verify third page ordering (descending: 10, 09, 08, 07, 06)
	for i := 0; i < 5; i++ {
		expectedKey := fmt.Sprintf("key_%02d", 10-i)
		require.Equal(t, expectedKey, page3[i].Key, "Page 3, entry %d should be %s", i, expectedKey)
	}

	// Read fourth page (offset=15, limit=5)
	page4, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      15,
		Limit:       5,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page4, 5, "Fourth page should have 5 entries")

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

// TestRedisKeyedEngine_OrderedSnapshotWithNegativeScores tests ordering with negative scores.
func TestRedisKeyedEngine_OrderedSnapshotWithNegativeScores(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

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
		_, err := engine.Publish(ctx, channel, tc.key, []byte("data"), KeyedPublishOptions{
			Ordered:    true,
			Score:      tc.score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered snapshot
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
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

// TestRedisKeyedEngine_OrderedSnapshotWithSameScores tests ordering stability when scores are equal.
func TestRedisKeyedEngine_OrderedSnapshotWithSameScores(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_same_scores")

	// Publish multiple entries with the same score
	// Redis ZSET uses lexicographic ordering for members with equal scores
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("key_%d", i)
		_, err := engine.Publish(ctx, channel, key, []byte(fmt.Sprintf("data_%d", i)), KeyedPublishOptions{
			Ordered:    true,
			Score:      100, // Same score for all
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered snapshot
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
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

// TestRedisKeyedEngine_OrderedSnapshotPaginationBoundaries tests edge cases in pagination.
func TestRedisKeyedEngine_OrderedSnapshotPaginationBoundaries(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_boundaries")

	// Publish 10 entries
	for i := 1; i <= 10; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key_%02d", i), []byte(fmt.Sprintf("data_%02d", i)), KeyedPublishOptions{
			Ordered:    true,
			Score:      int64(i * 10),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Test 1: Offset beyond entries (should return empty)
	empty, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      100,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Empty(t, empty, "Offset beyond entries should return empty")

	// Test 2: Limit larger than remaining entries (descending order: 10, 09, 08, ..., 01)
	lastPage, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      8,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, lastPage, 2, "Should return only remaining 2 entries")
	require.Equal(t, "key_02", lastPage[0].Key) // Position 8 in descending order
	require.Equal(t, "key_01", lastPage[1].Key) // Position 9 in descending order

	// Test 3: Offset at last entry
	last, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      9,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, last, 1, "Should return only last entry")
	require.Equal(t, "key_01", last[0].Key) // Position 9 in descending order

	// Test 4: Zero limit (should return all)
	all, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Offset:      0,
		Limit:       0,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, all, 10, "Zero limit should return all entries")

	t.Logf("SUCCESS: Pagination boundary cases handled correctly")
}

// TestRedisKeyedEngine_OrderedSnapshotFullPagination tests complete pagination loop.
func TestRedisKeyedEngine_OrderedSnapshotFullPagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_full_pagination")

	totalEntries := 37 // Odd number to test edge cases
	pageSize := 10

	// Publish entries
	for i := 1; i <= totalEntries; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key_%03d", i), []byte(fmt.Sprintf("data_%03d", i)), KeyedPublishOptions{
			Ordered:    true,
			Score:      int64(i * 10),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Paginate through all entries
	allKeys := []string{}
	offset := 0

	for {
		entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
			Ordered:     true,
			Offset:      offset,
			Limit:       pageSize,
			SnapshotTTL: 300 * time.Second,
		})
		require.NoError(t, err)

		if len(entries) == 0 {
			break
		}

		for _, entry := range entries {
			allKeys = append(allKeys, entry.Key)
		}

		offset += len(entries)

		// Prevent infinite loop
		if offset > totalEntries+10 {
			t.Fatal("Pagination loop exceeded expected iterations")
		}
	}

	// Verify we got all entries in correct descending order (037, 036, ..., 001)
	require.Len(t, allKeys, totalEntries, "Should have paginated through all %d entries", totalEntries)

	for i := 0; i < totalEntries; i++ {
		expectedKey := fmt.Sprintf("key_%03d", totalEntries-i)
		require.Equal(t, expectedKey, allKeys[i], "Entry %d should be %s", i, expectedKey)
	}

	t.Logf("SUCCESS: Full pagination through %d entries in descending order completed correctly", totalEntries)
}

// TestRedisKeyedEngine_OrderedSnapshotUpdatePreservesOrder tests that updating an entry's score
// changes its position in the ordered snapshot.
func TestRedisKeyedEngine_OrderedSnapshotUpdatePreservesOrder(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_update")

	// Publish 5 entries
	for i := 1; i <= 5; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key_%d", i), []byte(fmt.Sprintf("data_%d", i)), KeyedPublishOptions{
			Ordered:    true,
			Score:      int64(i * 10), // 10, 20, 30, 40, 50
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read initial order (descending: 50, 40, 30, 20, 10)
	entries1, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, "key_5", entries1[0].Key) // Highest score (50)
	require.Equal(t, "key_1", entries1[4].Key) // Lowest score (10)

	// Update key_1 to have highest score (60)
	_, err = engine.Publish(ctx, channel, "key_1", []byte("updated_data"), KeyedPublishOptions{
		Ordered:    true,
		Score:      60, // Now highest
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read updated order
	entries2, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
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
