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
		Shards:             []*RedisShard{shard},
		PresenceTTL:        60 * time.Second,
		PresenceStreamSize: 100,
		PresenceStreamTTL:  300 * time.Second,
		PresenceMetaTTL:    3600 * time.Second,
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
	_, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1_updated"),
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
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
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
	res1, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Publish another update
	res2, err := engine.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("data2"),
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
//	require.Equal(t, 2, stats.NumAggregationKeys) // user1 and user2
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
//	require.Equal(t, 2, stats.NumAggregationKeys) // Still 2 users
//
//	// Remove second client from user1
//	err = engine.RemoveMember(ctx, channel, client2, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Now user1 should be gone from aggregation
//	stats, err = engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 1, stats.NumKeys)
//	require.Equal(t, 1, stats.NumAggregationKeys) // Only user2 remains
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
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
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
	res1, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
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
	res1, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
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
	res1, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
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
	res3, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data_v3"),
		Version:    3,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed)
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
	_, err := engine.Publish(ctx, channel1, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Publish to channel2
	_, err = engine.Publish(ctx, channel2, "key2", KeyedPublishOptions{
		Data:       []byte("data2"),
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
		_, err := engine.Publish(ctx, channel, "", KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("msg_%d", i)),
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
		_, err := engine.Publish(ctx, channel, "", KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("msg_%d", i)),
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
		_, err := engine.Publish(ctx, channel, "", KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("msg_%d", i)),
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
//		PresenceMetaTTL: 3600 * time.Second,
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
//	require.Equal(t, 1, stats.NumAggregationKeys, "Should have 1 unique user")
//	t.Logf("After conn1: %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
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
//	require.Equal(t, 1, stats.NumAggregationKeys, "Should still have 1 unique user")
//	t.Logf("After conn2: %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
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
//	require.Equal(t, 1, stats.NumAggregationKeys, "Should still have 1 unique user")
//	t.Logf("After conn3: %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
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
//	require.Equal(t, 1, stats.NumAggregationKeys, "Should still have 1 unique user (2 connections remain)")
//	t.Logf("After removing conn1: %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
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
//	require.Equal(t, 1, stats.NumAggregationKeys, "Should still have 1 unique user (1 connection remains)")
//	t.Logf("After removing conn2: %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
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
//	require.Equal(t, 0, stats.NumAggregationKeys, "Should have 0 unique users (all connections closed)")
//	t.Logf("After removing conn3 (last): %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
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
//		PresenceMetaTTL: 3600 * time.Second,
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
//	require.Equal(t, 1, stats.NumAggregationKeys, "Should have 1 unique user")
//	t.Logf("Before cleanup: %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
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
//	require.Equal(t, 0, stats.NumAggregationKeys, "Should have 0 users (aggregation updated correctly)")
//	t.Logf("After cleanup: %d clients, %d users", stats.NumKeys, stats.NumAggregationKeys)
//
//	t.Logf("SUCCESS: Cleanup script correctly updates aggregation")
//}

// TestRedisKeyedEngine_AggregationCleanupOnTTL - REMOVED: aggregation feature was removed.
// TODO: consider adding a simpler cleanup test without aggregation tracking.

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
		_, err := engine.Publish(ctx, channel, tc.key, KeyedPublishOptions{
			Data:       []byte(tc.data),
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

		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
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
	page1, pos1, cursor1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       5,
		SnapshotTTL: 300 * time.Second,
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
	page2, pos2, cursor2, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Cursor:      cursor1,
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

	// Read third page (using cursor)
	page3, _, cursor3, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Cursor:      cursor2,
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

	// Read fourth page (using cursor)
	page4, _, cursor4, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Cursor:      cursor3,
		Limit:       5,
		SnapshotTTL: 300 * time.Second,
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
		_, err := engine.Publish(ctx, channel, tc.key, KeyedPublishOptions{
			Data:       []byte("data"),
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
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%d", i)),
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

// TestRedisKeyedEngine_OrderedSnapshotPaginationBoundaries tests edge cases in cursor-based pagination.
func TestRedisKeyedEngine_OrderedSnapshotPaginationBoundaries(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_boundaries")

	// Publish 10 entries
	for i := 1; i <= 10; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key_%02d", i), KeyedPublishOptions{
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
	all, _, cursor, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       0,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, all, 10, "Zero limit should return all entries")
	require.Empty(t, cursor, "No cursor when returning all entries")

	// Test 2: Limit larger than entries (should return all)
	largeLimit, _, cursor2, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, largeLimit, 10, "Limit larger than entries should return all")
	require.Empty(t, cursor2, "No cursor when returning all entries")

	// Test 3: Pagination with limit=3 through all entries
	page1, _, c1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       3,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page1, 3)
	require.NotEmpty(t, c1)

	// Continue to get remaining entries
	page2, _, c2, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       3,
		Cursor:      c1,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page2, 3)
	require.NotEmpty(t, c2)

	page3, _, c3, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       3,
		Cursor:      c2,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, page3, 3)
	require.NotEmpty(t, c3)

	// Last page should have 1 entry
	page4, _, c4, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       3,
		Cursor:      c3,
		SnapshotTTL: 300 * time.Second,
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

// TestRedisKeyedEngine_OrderedSnapshotFullPagination tests complete cursor-based pagination loop.
func TestRedisKeyedEngine_OrderedSnapshotFullPagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_full_pagination")

	totalEntries := 37 // Odd number to test edge cases
	pageSize := 10

	// Publish entries
	for i := 1; i <= totalEntries; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key_%03d", i), KeyedPublishOptions{
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
		entries, _, nextCursor, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
			Ordered:     true,
			Cursor:      cursor,
			Limit:       pageSize,
			SnapshotTTL: 300 * time.Second,
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

// TestRedisKeyedEngine_OrderedSnapshotUpdatePreservesOrder tests that updating an entry's score
// changes its position in the ordered snapshot.
func TestRedisKeyedEngine_OrderedSnapshotUpdatePreservesOrder(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_ordered_update")

	// Publish 5 entries
	for i := 1; i <= 5; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key_%d", i), KeyedPublishOptions{
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
	entries1, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, "key_5", entries1[0].Key) // Highest score (50)
	require.Equal(t, "key_1", entries1[4].Key) // Lowest score (10)

	// Update key_1 to have highest score (60)
	_, err = engine.Publish(ctx, channel, "key_1", KeyedPublishOptions{
		Data:       []byte("updated_data"),
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

// TestRedisKeyedEngine_KeyModeIfNew tests KeyModeIfNew - only write if key doesn't exist.
func TestRedisKeyedEngine_KeyModeIfNew(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_keymode_if_new")

	// First publish with KeyModeIfNew should succeed (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "slot1", KeyedPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "slot1", KeyedPublishOptions{
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

	// Verify snapshot still has original data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("player1"), entries[0].Data)

	// Verify stream only has one entry (second was suppressed)
	pubs, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
}

// TestRedisKeyedEngine_KeyModeIfExists tests KeyModeIfExists - only write if key exists.
func TestRedisKeyedEngine_KeyModeIfExists(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_keymode_if_exists")

	// First publish with KeyModeIfExists should be suppressed (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "presence1", KeyedPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "presence1", KeyedPublishOptions{
		Data:       []byte("initial"),
		KeyMode:    KeyModeReplace, // or just leave empty
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed, "Regular publish should not be suppressed")

	// Now publish with KeyModeIfExists should succeed (key exists)
	res3, err := engine.Publish(ctx, channel, "presence1", KeyedPublishOptions{
		Data:       []byte("heartbeat2"),
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed, "Third publish should not be suppressed (key exists)")

	// Verify snapshot has updated data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("heartbeat2"), entries[0].Data)
}

// TestRedisKeyedEngine_KeyModeReplace tests default KeyModeReplace behavior.
func TestRedisKeyedEngine_KeyModeReplace(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)

	ctx := context.Background()
	channel := randomChannel("test_keymode_replace")

	// First publish (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("value1"),
		KeyMode:    KeyModeReplace, // explicit, same as default
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Second publish (key exists) - should still apply
	res2, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("value2"),
		KeyMode:    KeyModeReplace,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed, "Replace should never be suppressed")

	// Verify snapshot has updated data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("value2"), entries[0].Data)
}

// TestRedisKeyedEngine_Aggregation - REMOVED: aggregation feature was removed.
// TestRedisKeyedEngine_AggregationAutoDiscovery - REMOVED: aggregation feature was removed.

// =============================================================================
// Pagination Continuity Tests
//
// These tests verify that key-based cursor pagination ensures no entries are
// permanently lost when the snapshot changes during iteration.
// The invariant is: snapshot_entries + stream_changes = complete_data
// Uses simulateClientRecovery helper from keyed_engine_memory_test.go
// =============================================================================

// TestRedisKeyedEngine_UnorderedContinuity_EntryRemoved tests that removing
// an entry during unordered pagination doesn't cause data loss.
func TestRedisKeyedEngine_UnorderedContinuity_EntryRemoved(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)
	ctx := context.Background()
	channel := "test_unordered_continuity_remove"

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (keys key_00 to key_09 lexicographically)
	pubs1, streamPos1, cursor1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)
	require.NotEmpty(t, cursor1)

	// CONCURRENT MODIFICATION: Remove key_10 (first entry of next page)
	// This would cause key_11 to shift into position 10, potentially being skipped
	// with integer offset pagination
	_, err = engine.Unpublish(ctx, channel, "key_10", KeyedUnpublishOptions{
		
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page with cursor
	pubs2, _, cursor2, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:  10,
		Cursor: cursor1,
	})
	require.NoError(t, err)

	// Combine snapshot entries, filter by streamPos1
	snapshotKeys := make(map[string]bool)
	for _, pub := range pubs1 {
		if pub.Offset <= streamPos1.Offset {
			snapshotKeys[pub.Key] = true
		}
	}
	for _, pub := range pubs2 {
		if pub.Offset <= streamPos1.Offset {
			snapshotKeys[pub.Key] = true
		}
	}
	// Continue if more pages
	cursor := cursor2
	for cursor != "" {
		pubs, _, nextCursor, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
			Limit:  10,
			Cursor: cursor,
		})
		require.NoError(t, err)
		for _, pub := range pubs {
			if pub.Offset <= streamPos1.Offset {
				snapshotKeys[pub.Key] = true
			}
		}
		cursor = nextCursor
	}

	// Read stream to get changes since streamPos1
	streamPubs, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos1,
			Limit: -1,
		},
	})
	require.NoError(t, err)

	// Apply stream changes
	for _, pub := range streamPubs {
		if pub.Removed {
			delete(snapshotKeys, pub.Key)
		}
	}

	// Verify: should have 19 keys (key_10 was removed)
	require.Len(t, snapshotKeys, 19, "Should have 19 keys after removal")
	require.NotContains(t, snapshotKeys, "key_10", "key_10 should be removed")

	// Verify key_11 wasn't skipped (this was the bug with integer offsets)
	require.Contains(t, snapshotKeys, "key_11", "key_11 should not be skipped")
}

// TestRedisKeyedEngine_UnorderedContinuity_EntryAdded tests that adding
// an entry during unordered pagination doesn't cause issues.
func TestRedisKeyedEngine_UnorderedContinuity_EntryAdded(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)
	ctx := context.Background()
	channel := "test_unordered_continuity_add"

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page
	pubs1, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	// CONCURRENT MODIFICATION: Add new entry that lexicographically comes before cursor
	// This would shift entries with integer offset pagination
	_, err = engine.Publish(ctx, channel, "key_05b", KeyedPublishOptions{
		Data:       []byte("data_05b"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Simulate full client recovery
	result := simulateClientRecovery(t, engine, channel, false, 10)

	// Verify: should have 21 keys (original 20 + new key_05b)
	require.Len(t, result, 21, "Should have 21 keys after addition")
	require.Contains(t, result, "key_05b", "key_05b should be present")
	require.Equal(t, []byte("data_05b"), result["key_05b"])
}

// TestRedisKeyedEngine_OrderedContinuity_HigherScoreAdded tests that adding
// an entry with higher score during ordered pagination doesn't cause data loss.
func TestRedisKeyedEngine_OrderedContinuity_HigherScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_higher")

	// Create 20 entries with scores 100, 200, ..., 2000
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
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
	pubs1, _, cursor1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)
	require.NotEmpty(t, cursor1)

	// Verify first page has highest scores
	require.Equal(t, "key_20", pubs1[0].Key, "First entry should be key_20 (score 2000)")

	// CONCURRENT MODIFICATION: Add entry with HIGHEST score
	// This entry would appear at position 0, shifting all entries
	_, err = engine.Publish(ctx, channel, "key_top", KeyedPublishOptions{
		Data:       []byte("data_top"),
		Ordered:    true,
		Score:      5000, // Higher than any existing
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Simulate full client recovery
	result := simulateClientRecovery(t, engine, channel, true, 10)

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

// TestRedisKeyedEngine_OrderedContinuity_LowerScoreAdded tests that adding
// an entry with lower score during ordered pagination works correctly.
func TestRedisKeyedEngine_OrderedContinuity_LowerScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_lower")

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
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
	_, _, cursor1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATION: Add entry with LOWEST score
	// This entry appears at end, shouldn't affect pagination much
	_, err = engine.Publish(ctx, channel, "key_bottom", KeyedPublishOptions{
		Data:       []byte("data_bottom"),
		Ordered:    true,
		Score:      1, // Lower than any existing
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Continue reading with cursor - just verify it works
	pubs2, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		Cursor:      cursor1,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pubs2)

	// Simulate full recovery
	result := simulateClientRecovery(t, engine, channel, true, 10)

	require.Len(t, result, 21, "Should have 21 keys")
	require.Contains(t, result, "key_bottom", "key_bottom should be present")
}

// TestRedisKeyedEngine_OrderedContinuity_ScoreChanged tests that changing
// an entry's score during pagination (causing reordering) doesn't lose data.
func TestRedisKeyedEngine_OrderedContinuity_ScoreChanged(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_score_change")

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
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
	pubs1, streamPos1, cursor1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	// CONCURRENT MODIFICATION: Change score of key_05 to make it jump to top
	// key_05 had score 500, now gets score 3000 (highest)
	// This entry was NOT in first page, but now would appear at position 0
	_, err = engine.Publish(ctx, channel, "key_05", KeyedPublishOptions{
		Data:       []byte("data_05_updated"),
		Ordered:    true,
		Score:      3000,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page - key_05 jumped out of this range
	pubs2, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		Cursor:      cursor1,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// Combine and filter
	snapshotData := make(map[string][]byte)
	for _, pub := range pubs1 {
		if pub.Offset <= streamPos1.Offset {
			snapshotData[pub.Key] = pub.Data
		}
	}
	for _, pub := range pubs2 {
		if pub.Offset <= streamPos1.Offset {
			snapshotData[pub.Key] = pub.Data
		}
	}

	// Read stream and apply changes
	streamPubs, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos1,
			Limit: -1,
		},
	})
	require.NoError(t, err)

	for _, pub := range streamPubs {
		if pub.Removed {
			delete(snapshotData, pub.Key)
		} else {
			snapshotData[pub.Key] = pub.Data
		}
	}

	// Verify: should have 20 keys, key_05 should have updated data
	require.Len(t, snapshotData, 20, "Should have 20 keys")
	require.Equal(t, []byte("data_05_updated"), snapshotData["key_05"], "key_05 should have updated data from stream")
}

// TestRedisKeyedEngine_OrderedContinuity_EntryRemoved tests that removing
// an entry during ordered pagination doesn't cause data loss.
func TestRedisKeyedEngine_OrderedContinuity_EntryRemoved(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_remove")

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
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
	pubs1, streamPos1, cursor1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATION: Remove key_10 (first entry of next page)
	_, err = engine.Unpublish(ctx, channel, "key_10", KeyedUnpublishOptions{
		
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read remaining pages
	allSnapshotData := make(map[string][]byte)
	for _, pub := range pubs1 {
		if pub.Offset <= streamPos1.Offset {
			allSnapshotData[pub.Key] = pub.Data
		}
	}

	cursor := cursor1
	for cursor != "" {
		pubs, _, nextCursor, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
			Ordered:     true,
			Limit:       10,
			Cursor:      cursor,
			SnapshotTTL: 300 * time.Second,
		})
		require.NoError(t, err)
		for _, pub := range pubs {
			if pub.Offset <= streamPos1.Offset {
				allSnapshotData[pub.Key] = pub.Data
			}
		}
		cursor = nextCursor
	}

	// Apply stream changes
	streamPubs, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos1,
			Limit: -1,
		},
	})
	require.NoError(t, err)

	for _, pub := range streamPubs {
		if pub.Removed {
			delete(allSnapshotData, pub.Key)
		} else {
			allSnapshotData[pub.Key] = pub.Data
		}
	}

	// Verify: should have 19 keys (key_10 removed)
	require.Len(t, allSnapshotData, 19, "Should have 19 keys after removal")
	require.NotContains(t, allSnapshotData, "key_10", "key_10 should be removed")

	// Verify key_09 wasn't skipped (the key that shifted up when key_10 was removed)
	require.Contains(t, allSnapshotData, "key_09", "key_09 should not be skipped")
}

// TestRedisKeyedEngine_OrderedContinuity_MultipleChanges tests recovery
// with multiple concurrent changes during pagination.
func TestRedisKeyedEngine_OrderedContinuity_MultipleChanges(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestSnapshotRedisEngine(t, node)
	ctx := context.Background()
	channel := randomChannel("test_ordered_continuity_multi")

	// Create 30 entries
	for i := 1; i <= 30; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, KeyedPublishOptions{
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
	_, _, cursor1, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATIONS:
	// 1. Add new highest score entry
	_, err = engine.Publish(ctx, channel, "key_new_top", KeyedPublishOptions{
		Data:       []byte("data_new_top"),
		Ordered:    true,
		Score:      5000,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// 2. Remove an entry from middle
	_, err = engine.Unpublish(ctx, channel, "key_15", KeyedUnpublishOptions{
		
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// 3. Update score of an entry (move it)
	_, err = engine.Publish(ctx, channel, "key_05", KeyedPublishOptions{
		Data:       []byte("data_05_moved"),
		Ordered:    true,
		Score:      4000, // Move from 500 to 4000
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page
	_, _, cursor2, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Ordered:     true,
		Limit:       10,
		Cursor:      cursor1,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// 4. Add new lowest score entry
	_, err = engine.Publish(ctx, channel, "key_new_bottom", KeyedPublishOptions{
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
		_, _, nextCursor, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
			Ordered:     true,
			Limit:       10,
			Cursor:      cursor,
			SnapshotTTL: 300 * time.Second,
		})
		require.NoError(t, err)
		cursor = nextCursor
	}

	// Simulate full client recovery
	result := simulateClientRecovery(t, engine, channel, true, 10)

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
