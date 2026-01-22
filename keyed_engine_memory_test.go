package centrifuge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestMemoryKeyedEngine(tb testing.TB, n *Node) *MemoryKeyedEngine {
	e, err := NewMemoryKeyedEngine(n, MemoryKeyedEngineConfig{})
	require.NoError(tb, err)
	err = e.RegisterBrokerEventHandler(nil)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = n.Shutdown(context.Background())
	})
	return e
}

// snapshotToMapMemory converts []SnapshotEntry to map for easier testing.
func snapshotToMapMemory(pubs []*Publication) map[string][]byte {
	result := make(map[string][]byte, len(pubs))
	for _, pub := range pubs {
		// Extract data from Publication
		result[pub.Key] = pub.Data
	}
	return result
}

// TestMemoryKeyedEngine_StatefulChannel tests stateful channel with keyed state and revisions.
func TestMemoryKeyedEngine_StatefulChannel(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_stateful"

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
	snapshot := snapshotToMapMemory(entries)
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

// TestMemoryKeyedEngine_StatefulChannelOrdered tests ordered stateful channel.
func TestMemoryKeyedEngine_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ordered"

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
	snapshot := snapshotToMapMemory(entries)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		require.Contains(t, snapshot, key)
	}
}

// TestMemoryKeyedEngine_SnapshotRevision tests that snapshot values include revisions.
func TestMemoryKeyedEngine_SnapshotRevision(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_revision"

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
	snapshot := snapshotToMapMemory(entries)
	require.Equal(t, []byte("data1"), snapshot["key1"])
	require.Equal(t, []byte("data2"), snapshot["key2"])

	// Verify per-entry offsets (epoch is in streamPos, same for all)
	require.NotEmpty(t, streamPos.Epoch)
	for _, pub := range entries {
		require.Greater(t, pub.Offset, uint64(0))
	}
}

//
//// TestMemoryKeyedEngine_Membership tests presence with revisions.
//func TestMemoryKeyedEngine_Membership(t *testing.T) {
//	node, _ := New(Config{})
//	engine := newTestMemoryKeyedEngine(t, node)
//
//	ctx := context.Background()
//	channel := "test_presence"
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
//	// Get presence stats
//	stats, err := engine.Stats(ctx, channel)
//	require.NoError(t, err)
//	require.Equal(t, 3, stats.NumKeys)
//
//	// Remove one client
//	err = engine.RemoveMember(ctx, channel, client1, EnginePresenceOptions{})
//	require.NoError(t, err)
//
//	// Get presence again
//	presence, err = engine.Members(ctx, channel)
//	require.NoError(t, err)
//	require.Len(t, presence, 2)
//	require.NotContains(t, presence, "client1")
//}
//
//// TestMemoryKeyedEngine_PresenceStream tests presence event stream (joins/leaves).
//func TestMemoryKeyedEngine_PresenceStream(t *testing.T) {
//	node, _ := New(Config{})
//	engine := newTestMemoryKeyedEngine(t, node)
//
//	ctx := context.Background()
//	channel := "test_presence_stream"
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
//		Filter: HistoryFilter{
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

// TestMemoryKeyedEngine_SnapshotPagination tests cursor-based snapshot pagination.
func TestMemoryKeyedEngine_SnapshotPagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_pagination"

	// Publish 10 keyed entries
	for i := 0; i < 10; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("data%d", i)), KeyedPublishOptions{
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read snapshot with limit
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

	// Continue reading until cursor is empty
	for cursor != "" {
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

// TestMemoryKeyedEngine_EpochHandling tests epoch changes and snapshot invalidation.
func TestMemoryKeyedEngine_EpochHandling(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_epoch"

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

	// Verify epoch consistency
	require.NotEmpty(t, epoch1)
}

// TestMemoryKeyedEngine_Idempotency tests idempotent publishing.
func TestMemoryKeyedEngine_Idempotency(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_idempotency"

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
	snapshot := snapshotToMapMemory(entries)
	require.Len(t, snapshot, 1)
	require.Equal(t, []byte("data1"), snapshot["key1"])
}

// TestMemoryKeyedEngine_VersionedPublishing tests version-based idempotency.
func TestMemoryKeyedEngine_VersionedPublishing(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_version"

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
	snapshot := snapshotToMapMemory(entries)
	require.Equal(t, []byte("data_v3"), snapshot["key1"])
}

// TestMemoryKeyedEngine_MultipleChannels tests multiple channels independently.
func TestMemoryKeyedEngine_MultipleChannels(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel1 := "test_multi1"
	channel2 := "test_multi2"

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
	snapshot1 := snapshotToMapMemory(entries1)
	require.Len(t, snapshot1, 1)
	require.Equal(t, []byte("data1"), snapshot1["key1"])

	// Read channel2 snapshot
	entries2, _, _, err := engine.ReadSnapshot(ctx, channel2, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	snapshot2 := snapshotToMapMemory(entries2)
	require.Len(t, snapshot2, 1)
	require.Equal(t, []byte("data2"), snapshot2["key2"])
}

// TestMemoryKeyedEngine_OrderedSnapshotOrdering tests that ordered snapshots return entries
// in correct score order (descending by score).
func TestMemoryKeyedEngine_OrderedSnapshotOrdering(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ordered_ordering"

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

	// Read ordered snapshot - should be sorted by score (descending)
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

// TestMemoryKeyedEngine_OrderedSnapshotPagination tests that pagination over ordered snapshots
// maintains correct ordering across pages.
func TestMemoryKeyedEngine_OrderedSnapshotPagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ordered_pagination"

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

	t.Logf("SUCCESS: Pagination maintains correct ordering across pages")
}

// TestMemoryKeyedEngine_OrderedSnapshotWithNegativeScores tests ordering with negative scores.
func TestMemoryKeyedEngine_OrderedSnapshotWithNegativeScores(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ordered_negative"

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

// TestMemoryKeyedEngine_OrderedSnapshotUpdatePreservesOrder tests that updating an entry's score
// changes its position in the ordered snapshot.
func TestMemoryKeyedEngine_OrderedSnapshotUpdatePreservesOrder(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ordered_update"

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

// TestMemoryKeyedEngine_Remove tests removing keys from snapshot.
func TestMemoryKeyedEngine_Remove(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_remove"

	// Publish some keys
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

	// Verify snapshot has 2 keys
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Remove key1
	_, err = engine.Unpublish(ctx, channel, "key1", KeyedUnpublishOptions{
		Publish:    true,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Verify snapshot has 1 key
	entries, _, _, err = engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "key2", entries[0].Key)

	// Verify remove was added to stream
	pubs, _, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3) // key1, key2, remove(key1)
	require.True(t, pubs[2].Removed)
	require.Equal(t, "key1", pubs[2].Key)
}

// TestMemoryKeyedEngine_KeyModeIfNew tests KeyModeIfNew - only write if key doesn't exist.
func TestMemoryKeyedEngine_KeyModeIfNew(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_keymode_if_new"

	// First publish with KeyModeIfNew should succeed (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "slot1", []byte("player1"), KeyedPublishOptions{
		KeyMode:    KeyModeIfNew,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res1.Applied, "First publish should be applied")
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Second publish with KeyModeIfNew should be suppressed (key exists)
	res2, err := engine.Publish(ctx, channel, "slot1", []byte("player2"), KeyedPublishOptions{
		KeyMode:    KeyModeIfNew,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Applied, "Second publish should be suppressed (key exists)")
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

// TestMemoryKeyedEngine_KeyModeIfExists tests KeyModeIfExists - only write if key exists.
func TestMemoryKeyedEngine_KeyModeIfExists(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_keymode_if_exists"

	// First publish with KeyModeIfExists should be suppressed (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "presence1", []byte("heartbeat1"), KeyedPublishOptions{
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Applied, "First publish should be suppressed (key doesn't exist)")

	// Create the key first with regular publish
	res2, err := engine.Publish(ctx, channel, "presence1", []byte("initial"), KeyedPublishOptions{
		KeyMode:    KeyModeReplace, // or just leave empty
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Applied, "Regular publish should be applied")

	// Now publish with KeyModeIfExists should succeed (key exists)
	res3, err := engine.Publish(ctx, channel, "presence1", []byte("heartbeat2"), KeyedPublishOptions{
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res3.Applied, "Third publish should be applied (key exists)")

	// Verify snapshot has updated data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("heartbeat2"), entries[0].Data)
}

// TestMemoryKeyedEngine_KeyModeReplace tests default KeyModeReplace behavior.
func TestMemoryKeyedEngine_KeyModeReplace(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_keymode_replace"

	// First publish (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "key1", []byte("value1"), KeyedPublishOptions{
		KeyMode:    KeyModeReplace, // explicit, same as default
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res1.Applied)

	// Second publish (key exists) - should still apply
	res2, err := engine.Publish(ctx, channel, "key1", []byte("value2"), KeyedPublishOptions{
		KeyMode:    KeyModeReplace,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Applied, "Replace should always apply")

	// Verify snapshot has updated data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("value2"), entries[0].Data)
}

// TestMemoryKeyedEngine_Aggregation tests aggregation tracking (e.g., counting unique users).
func TestMemoryKeyedEngine_Aggregation(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)
	ctx := context.Background()
	channel := "test_aggregation"

	// Simulate presence scenario: multiple connections (keys) per user (aggregation value)
	// User "alice" has 2 connections
	_, err := engine.Publish(ctx, channel, "conn1", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "conn2", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	// User "bob" has 1 connection
	_, err = engine.Publish(ctx, channel, "conn3", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "bob",
	})
	require.NoError(t, err)

	// Check stats - should have 3 keys and 2 aggregated keys (unique users)
	stats, err := engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 3, stats.NumKeys)
	require.Equal(t, 2, stats.NumAggregatedKeys)

	// Remove one of alice's connections
	_, err = engine.Unpublish(ctx, channel, "conn1", KeyedUnpublishOptions{
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	// Check stats - should have 2 keys, still 2 unique users
	stats, err = engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 2, stats.NumKeys)
	require.Equal(t, 2, stats.NumAggregatedKeys)

	// Remove alice's last connection
	_, err = engine.Unpublish(ctx, channel, "conn2", KeyedUnpublishOptions{
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	// Check stats - should have 1 key, 1 unique user (bob)
	stats, err = engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 1, stats.NumKeys)
	require.Equal(t, 1, stats.NumAggregatedKeys)

	// Remove bob's connection
	_, err = engine.Unpublish(ctx, channel, "conn3", KeyedUnpublishOptions{
		AggregationKey:   "user_id",
		AggregationValue: "bob",
	})
	require.NoError(t, err)

	// Check stats - should have 0 keys, 0 unique users
	stats, err = engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 0, stats.NumKeys)
	require.Equal(t, 0, stats.NumAggregatedKeys)
}

// TestMemoryKeyedEngine_AggregationAutoDiscovery tests that Unpublish auto-discovers
// aggregation values from stored mapping when not provided in options.
func TestMemoryKeyedEngine_AggregationAutoDiscovery(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)
	ctx := context.Background()
	channel := "test_aggregation_autodiscover"

	// Publish with aggregation
	_, err := engine.Publish(ctx, channel, "conn1", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "conn2", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           300 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	// Verify initial state
	stats, err := engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 2, stats.NumKeys)
	require.Equal(t, 1, stats.NumAggregatedKeys)

	// Unpublish WITHOUT providing aggregation options - should auto-discover
	_, err = engine.Unpublish(ctx, channel, "conn1", KeyedUnpublishOptions{})
	require.NoError(t, err)

	// Should still have 1 unique user (alice still has conn2)
	stats, err = engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 1, stats.NumKeys)
	require.Equal(t, 1, stats.NumAggregatedKeys)

	// Unpublish last connection without aggregation options
	_, err = engine.Unpublish(ctx, channel, "conn2", KeyedUnpublishOptions{})
	require.NoError(t, err)

	// Should have 0 keys and 0 unique users
	stats, err = engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 0, stats.NumKeys)
	require.Equal(t, 0, stats.NumAggregatedKeys)
}

// TestMemoryKeyedEngine_AggregationCleanupOnTTL verifies that key TTL expiration
// correctly updates aggregation counts in the memory engine.
func TestMemoryKeyedEngine_AggregationCleanupOnTTL(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryKeyedEngine(t, node)
	ctx := context.Background()
	channel := "test_aggregation_ttl_cleanup"

	// Publish with very short TTL (1 second)
	_, err := engine.Publish(ctx, channel, "conn1", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           1 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "conn2", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           1 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "alice",
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "conn3", []byte("data"), KeyedPublishOptions{
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
		KeyTTL:           1 * time.Second,
		AggregationKey:   "user_id",
		AggregationValue: "bob",
	})
	require.NoError(t, err)

	// Verify initial state
	stats, err := engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 3, stats.NumKeys)
	require.Equal(t, 2, stats.NumAggregatedKeys)

	// Wait for TTL to expire plus cleanup interval
	time.Sleep(2500 * time.Millisecond)

	// Verify cleanup happened - all keys and aggregations should be removed
	stats, err = engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 0, stats.NumKeys, "All keys should be cleaned up")
	require.Equal(t, 0, stats.NumAggregatedKeys, "All aggregations should be cleaned up")
}
