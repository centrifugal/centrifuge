package centrifuge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestMemoryMapBroker(tb testing.TB, n *Node) *MemoryMapBroker {
	e, err := NewMemoryMapBroker(n, MemoryMapBrokerConfig{})
	require.NoError(tb, err)
	err = e.RegisterBrokerEventHandler(nil)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = n.Shutdown(context.Background())
	})
	return e
}

// stateToMapMemory converts []StateEntry to map for easier testing.
func stateToMapMemory(pubs []*Publication) map[string][]byte {
	result := make(map[string][]byte, len(pubs))
	for _, pub := range pubs {
		// Extract data from Publication
		result[pub.Key] = pub.Data
	}
	return result
}

// TestMemoryMapBroker_StatefulChannel tests stateful channel with keyed state and revisions.
func TestMemoryMapBroker_StatefulChannel(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_stateful"

	// Publish some keyed state updates
	_, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1_updated"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read state
	entries, streamPos, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.NotEmpty(t, streamPos.Epoch)
	require.Greater(t, streamPos.Offset, uint64(0))

	// Verify state contains latest values
	state := stateToMapMemory(entries)
	require.Len(t, state, 2)
	require.Equal(t, []byte("data1_updated"), state["key1"])
	require.Equal(t, []byte("data2"), state["key2"])

	// Read stream to verify all publications are in history
	pubs, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1, // Get all
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3) // All 3 publications in stream
}

// TestMemoryMapBroker_StatefulChannelOrdered tests ordered stateful channel.
func TestMemoryMapBroker_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered"

	// Publish with scores for ordering
	for i := 0; i < 5; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
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
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 5)

	// Verify all keys present
	state := stateToMapMemory(entries)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		require.Contains(t, state, key)
	}
}

// TestMemoryMapBroker_StateRevision tests that state values include revisions.
func TestMemoryMapBroker_StateRevision(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_revision"

	// Publish a keyed state update
	res1, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Publish another update
	res2, err := engine.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), res2.Position.Offset)
	require.Equal(t, res1.Position.Epoch, res2.Position.Epoch) // Same epoch

	// Read state - entries now include per-entry revisions
	entries, streamPos, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, res2.Position.Offset, streamPos.Offset)
	require.Equal(t, res2.Position.Epoch, streamPos.Epoch)

	// Verify payloads
	state := stateToMapMemory(entries)
	require.Equal(t, []byte("data1"), state["key1"])
	require.Equal(t, []byte("data2"), state["key2"])

	// Verify per-entry offsets (epoch is in streamPos, same for all)
	require.NotEmpty(t, streamPos.Epoch)
	for _, pub := range entries {
		require.Greater(t, pub.Offset, uint64(0))
	}
}

//
//// TestMemoryMapBroker_Membership tests presence with revisions.
//func TestMemoryMapBroker_Membership(t *testing.T) {
//	node, _ := New(Config{})
//	engine := newTestMemoryMapBroker(t, node)
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
//// TestMemoryMapBroker_PresenceStream tests presence event stream (joins/leaves).
//func TestMemoryMapBroker_PresenceStream(t *testing.T) {
//	node, _ := New(Config{})
//	engine := newTestMemoryMapBroker(t, node)
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
//	events, streamPos, err := engine.ReadPresenceStream(ctx, channel, MapReadStreamOptions{
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

// TestMemoryMapBroker_StatePagination tests cursor-based state pagination.
func TestMemoryMapBroker_StatePagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_pagination"

	// Publish 10 keyed entries
	for i := 0; i < 10; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read state with limit
	page1, pos1, cursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
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

	// Continue reading until cursor is empty
	for cursor != "" {
		page, pos, newCursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
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

// TestMemoryMapBroker_EpochHandling tests epoch changes and state invalidation.
func TestMemoryMapBroker_EpochHandling(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_epoch"

	// Publish initial data
	res1, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	epoch1 := res1.Position.Epoch

	// Read state
	entries, streamPos1, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, epoch1, streamPos1.Epoch)

	// Verify epoch consistency
	require.NotEmpty(t, epoch1)
}

// TestMemoryMapBroker_EpochMismatchWhenChannelNotExists tests that we return
// ErrorUnrecoverablePosition when client sends an epoch but the channel doesn't exist
// (e.g., after server restart). This is the server restart recovery scenario.
func TestMemoryMapBroker_EpochMismatchWhenChannelNotExists(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_nonexistent_channel"

	// Client tries to read state with an old epoch, but channel doesn't exist
	// (simulates reconnection after server restart)
	_, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Revision: &StreamPosition{
			Epoch:  "old_epoch_from_previous_session",
			Offset: 100,
		},
		Limit: 100,
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)
}

// TestMemoryMapBroker_NoEpochWhenChannelNotExists tests that we return success
// when client doesn't send an epoch and the channel doesn't exist (fresh subscription).
func TestMemoryMapBroker_NoEpochWhenChannelNotExists(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_fresh_channel"

	// Fresh subscription - no epoch provided
	entries, streamPos, cursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Empty(t, entries)            // No entries in non-existent channel
	require.NotEmpty(t, streamPos.Epoch) // Should get a new epoch
	require.Empty(t, cursor)             // No cursor for empty result
}

// TestMemoryMapBroker_Idempotency tests idempotent publishing.
func TestMemoryMapBroker_Idempotency(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_idempotency"

	// Publish with idempotency key
	res1, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state := stateToMapMemory(entries)
	require.Len(t, state, 1)
	require.Equal(t, []byte("data1"), state["key1"])
}

// TestMemoryMapBroker_VersionedPublishing tests version-based idempotency.
func TestMemoryMapBroker_VersionedPublishing(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_version"

	// Publish with version 2 (version 0 means "disable version check")
	res1, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
	res3, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
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
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state := stateToMapMemory(entries)
	require.Equal(t, []byte("data_v3"), state["key1"])
}

// TestMemoryMapBroker_MultipleChannels tests multiple channels independently.
func TestMemoryMapBroker_MultipleChannels(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel1 := "test_multi1"
	channel2 := "test_multi2"

	// Publish to channel1
	_, err := engine.Publish(ctx, channel1, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Publish to channel2
	_, err = engine.Publish(ctx, channel2, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read channel1 state
	entries1, _, _, err := engine.ReadState(ctx, channel1, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state1 := stateToMapMemory(entries1)
	require.Len(t, state1, 1)
	require.Equal(t, []byte("data1"), state1["key1"])

	// Read channel2 state
	entries2, _, _, err := engine.ReadState(ctx, channel2, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state2 := stateToMapMemory(entries2)
	require.Len(t, state2, 1)
	require.Equal(t, []byte("data2"), state2["key2"])
}

// TestMemoryMapBroker_OrderedStateOrdering tests that ordered state return entries
// in correct score order (descending by score).
func TestMemoryMapBroker_OrderedStateOrdering(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

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
		_, err := engine.Publish(ctx, channel, tc.key, MapPublishOptions{
			Data:       []byte(tc.data),
			Ordered:    true,
			Score:      tc.score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered state - should be sorted by score (descending)
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
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

// TestMemoryMapBroker_OrderedStatePagination tests that pagination over ordered state
// maintains correct ordering across pages.
func TestMemoryMapBroker_OrderedStatePagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered_pagination"

	// Publish 20 entries with scores 100, 200, 300, ..., 2000
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		score := int64(i * 100)
		data := fmt.Sprintf("data_%02d", i)

		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
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
	page1, pos1, cursor1, err := engine.ReadState(ctx, channel, MapReadStateOptions{
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
	page2, pos2, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
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

	t.Logf("SUCCESS: Pagination maintains correct ordering across pages")
}

// TestMemoryMapBroker_OrderedStateWithNegativeScores tests ordering with negative scores.
func TestMemoryMapBroker_OrderedStateWithNegativeScores(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

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
		_, err := engine.Publish(ctx, channel, tc.key, MapPublishOptions{
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
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
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

// TestMemoryMapBroker_OrderedStateUpdatePreservesOrder tests that updating an entry's score
// changes its position in the ordered state.
func TestMemoryMapBroker_OrderedStateUpdatePreservesOrder(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered_update"

	// Publish 5 entries
	for i := 1; i <= 5; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key_%d", i), MapPublishOptions{
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
	entries1, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered:  true,
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, "key_5", entries1[0].Key) // Highest score (50)
	require.Equal(t, "key_1", entries1[4].Key) // Lowest score (10)

	// Update key_1 to have highest score (60)
	_, err = engine.Publish(ctx, channel, "key_1", MapPublishOptions{
		Data:       []byte("updated_data"),
		Ordered:    true,
		Score:      60, // Now highest
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Read updated order
	entries2, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
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

// TestMemoryMapBroker_Remove tests removing keys from state.
func TestMemoryMapBroker_Remove(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_remove"

	// Publish some keys
	_, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	// Verify state has 2 keys
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Remove key1
	_, err = engine.Remove(ctx, channel, "key1", MapRemoveOptions{

		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Verify state has 1 key
	entries, _, _, err = engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "key2", entries[0].Key)

	// Verify remove was added to stream
	pubs, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3) // key1, key2, remove(key1)
	require.True(t, pubs[2].Removed)
	require.Equal(t, "key1", pubs[2].Key)
}

// TestMemoryMapBroker_KeyModeIfNew tests KeyModeIfNew - only write if key doesn't exist.
func TestMemoryMapBroker_KeyModeIfNew(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_keymode_if_new"

	// First publish with KeyModeIfNew should succeed (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "slot1", MapPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "slot1", MapPublishOptions{
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
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("player1"), entries[0].Data)

	// Verify stream only has one entry (second was suppressed)
	pubs, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
}

// TestMemoryMapBroker_KeyModeIfExists tests KeyModeIfExists - only write if key exists.
func TestMemoryMapBroker_KeyModeIfExists(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_keymode_if_exists"

	// First publish with KeyModeIfExists should be suppressed (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "presence1", MapPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "presence1", MapPublishOptions{
		Data:       []byte("initial"),
		KeyMode:    KeyModeReplace, // or just leave empty
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed, "Regular publish should not be suppressed")

	// Now publish with KeyModeIfExists should succeed (key exists)
	res3, err := engine.Publish(ctx, channel, "presence1", MapPublishOptions{
		Data:       []byte("heartbeat2"),
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed, "Third publish should not be suppressed (key exists)")

	// Verify state has updated data
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("heartbeat2"), entries[0].Data)
}

// TestMemoryMapBroker_KeyModeReplace tests default KeyModeReplace behavior.
func TestMemoryMapBroker_KeyModeReplace(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_keymode_replace"

	// First publish (key doesn't exist)
	res1, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("value1"),
		KeyMode:    KeyModeReplace, // explicit, same as default
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Second publish (key exists) - should still apply
	res2, err := engine.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("value2"),
		KeyMode:    KeyModeReplace,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed, "Replace should never be suppressed")

	// Verify state has updated data
	entries, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("value2"), entries[0].Data)
}

// =============================================================================
// Pagination Continuity Tests
//
// These tests verify that key-based cursor pagination ensures no entries are
// permanently lost when the state changes during iteration.
// The invariant is: state_entries + stream_changes = complete_data
// =============================================================================

// simulateClientRecovery simulates what a client does:
// 1. Read state pages, capturing streamPos from first page
// 2. Filter out entries with offset > streamPos (added during iteration)
// 3. Read stream from streamPos to get all changes
// 4. Apply stream changes (updates, removals) to get final state
func simulateClientRecovery(
	t *testing.T,
	engine MapBroker,
	channel string,
	ordered bool,
	pageSize int,
) map[string][]byte {
	ctx := context.Background()

	// Step 1: Read all state pages
	allEntries := make(map[string]*Publication)
	var firstStreamPos StreamPosition
	cursor := ""
	pageNum := 0

	for {
		pubs, streamPos, nextCursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Limit:   pageSize,
			Cursor:  cursor,
			Ordered: ordered,
		})
		require.NoError(t, err)

		if pageNum == 0 {
			firstStreamPos = streamPos
		}
		pageNum++

		// Step 2: Filter entries - only accept those with offset <= firstStreamPos
		for _, pub := range pubs {
			if pub.Offset <= firstStreamPos.Offset {
				allEntries[pub.Key] = pub
			}
		}

		if nextCursor == "" {
			break
		}
		cursor = nextCursor
	}

	// Step 3: Read stream from firstStreamPos
	streamPubs, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &firstStreamPos,
			Limit: -1, // Get all
		},
	})
	require.NoError(t, err)

	// Step 4: Apply stream changes
	for _, pub := range streamPubs {
		if pub.Removed {
			delete(allEntries, pub.Key)
		} else {
			allEntries[pub.Key] = pub
		}
	}

	// Convert to simple map for comparison
	result := make(map[string][]byte, len(allEntries))
	for k, pub := range allEntries {
		result[k] = pub.Data
	}
	return result
}

// TestMemoryMapBroker_UnorderedContinuity_EntryRemoved tests that removing
// an entry during unordered pagination doesn't cause data loss.
func TestMemoryMapBroker_UnorderedContinuity_EntryRemoved(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_unordered_continuity_remove"

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (keys key_00 to key_09 lexicographically)
	pubs1, streamPos1, cursor1, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)
	require.NotEmpty(t, cursor1)

	// CONCURRENT MODIFICATION: Remove key_10 (first entry of next page)
	// This would cause key_11 to shift into position 10, potentially being skipped
	// with integer offset pagination
	_, err = engine.Remove(ctx, channel, "key_10", MapRemoveOptions{

		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page with cursor
	pubs2, _, cursor2, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit:  10,
		Cursor: cursor1,
	})
	require.NoError(t, err)

	// Combine state entries, filter by streamPos1
	stateKeys := make(map[string]bool)
	for _, pub := range pubs1 {
		if pub.Offset <= streamPos1.Offset {
			stateKeys[pub.Key] = true
		}
	}
	for _, pub := range pubs2 {
		if pub.Offset <= streamPos1.Offset {
			stateKeys[pub.Key] = true
		}
	}
	// Continue if more pages
	cursor := cursor2
	for cursor != "" {
		pubs, _, nextCursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor,
		})
		require.NoError(t, err)
		for _, pub := range pubs {
			if pub.Offset <= streamPos1.Offset {
				stateKeys[pub.Key] = true
			}
		}
		cursor = nextCursor
	}

	// Read stream to get changes since streamPos1
	streamPubs, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos1,
			Limit: -1,
		},
	})
	require.NoError(t, err)

	// Apply stream changes
	for _, pub := range streamPubs {
		if pub.Removed {
			delete(stateKeys, pub.Key)
		}
	}

	// Verify: should have 19 keys (key_10 was removed)
	require.Len(t, stateKeys, 19, "Should have 19 keys after removal")
	require.NotContains(t, stateKeys, "key_10", "key_10 should be removed")

	// Verify key_11 wasn't skipped (this was the bug with integer offsets)
	require.Contains(t, stateKeys, "key_11", "key_11 should not be skipped")
}

// TestMemoryMapBroker_UnorderedContinuity_EntryAdded tests that adding
// an entry during unordered pagination doesn't cause issues.
func TestMemoryMapBroker_UnorderedContinuity_EntryAdded(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_unordered_continuity_add"

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page
	pubs1, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	// CONCURRENT MODIFICATION: Add new entry that lexicographically comes before cursor
	// This would shift entries with integer offset pagination
	_, err = engine.Publish(ctx, channel, "key_05b", MapPublishOptions{
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

// TestMemoryMapBroker_OrderedContinuity_HigherScoreAdded tests that adding
// an entry with higher score during ordered pagination doesn't cause data loss.
func TestMemoryMapBroker_OrderedContinuity_HigherScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_higher"

	// Create 20 entries with scores 100, 200, ..., 2000
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (should get keys with highest scores: key_20, key_19, ..., key_11)
	pubs1, _, cursor1, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)
	require.NotEmpty(t, cursor1)

	// Verify first page has highest scores
	require.Equal(t, "key_20", pubs1[0].Key, "First entry should be key_20 (score 2000)")

	// CONCURRENT MODIFICATION: Add entry with HIGHEST score
	// This entry would appear at position 0, shifting all entries
	_, err = engine.Publish(ctx, channel, "key_top", MapPublishOptions{
		Data:       []byte("data_top"),
		Ordered:    true,
		Score:      5000, // Higher than any existing
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
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

// TestMemoryMapBroker_OrderedContinuity_LowerScoreAdded tests that adding
// an entry with lower score during ordered pagination works correctly.
func TestMemoryMapBroker_OrderedContinuity_LowerScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_lower"

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page
	_, _, cursor1, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATION: Add entry with LOWEST score
	// This entry appears at end, shouldn't affect pagination much
	_, err = engine.Publish(ctx, channel, "key_bottom", MapPublishOptions{
		Data:       []byte("data_bottom"),
		Ordered:    true,
		Score:      1, // Lower than any existing
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Continue reading with cursor - just verify it works
	pubs2, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
		Cursor:  cursor1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pubs2)

	// Simulate full recovery
	result := simulateClientRecovery(t, engine, channel, true, 10)

	require.Len(t, result, 21, "Should have 21 keys")
	require.Contains(t, result, "key_bottom", "key_bottom should be present")
}

// TestMemoryMapBroker_OrderedContinuity_ScoreChanged tests that changing
// an entry's score during pagination (causing reordering) doesn't lose data.
func TestMemoryMapBroker_OrderedContinuity_ScoreChanged(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_score_change"

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (key_20 down to key_11)
	pubs1, streamPos1, cursor1, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
	})
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	// CONCURRENT MODIFICATION: Change score of key_05 to make it jump to top
	// key_05 had score 500, now gets score 3000 (highest)
	// This entry was NOT in first page, but now would appear at position 0
	_, err = engine.Publish(ctx, channel, "key_05", MapPublishOptions{
		Data:       []byte("data_05_updated"),
		Ordered:    true,
		Score:      3000,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page - key_05 jumped out of this range
	pubs2, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
		Cursor:  cursor1,
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
	streamPubs, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
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

// TestMemoryMapBroker_OrderedContinuity_EntryRemoved tests that removing
// an entry during ordered pagination doesn't cause data loss.
func TestMemoryMapBroker_OrderedContinuity_EntryRemoved(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_remove"

	// Create 20 entries
	for i := 1; i <= 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page (key_20 down to key_11)
	pubs1, streamPos1, cursor1, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATION: Remove key_10 (first entry of next page)
	_, err = engine.Remove(ctx, channel, "key_10", MapRemoveOptions{

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
		pubs, _, nextCursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Ordered: true,
			Limit:   10,
			Cursor:  cursor,
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
	streamPubs, _, err := engine.ReadStream(ctx, channel, MapReadStreamOptions{
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

// TestMemoryMapBroker_OrderedContinuity_MultipleChanges tests recovery
// with multiple concurrent changes during pagination.
func TestMemoryMapBroker_OrderedContinuity_MultipleChanges(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_multi"

	// Create 30 entries
	for i := 1; i <= 30; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Ordered:    true,
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read first page
	_, _, cursor1, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
	})
	require.NoError(t, err)

	// CONCURRENT MODIFICATIONS:
	// 1. Add new highest score entry
	_, err = engine.Publish(ctx, channel, "key_new_top", MapPublishOptions{
		Data:       []byte("data_new_top"),
		Ordered:    true,
		Score:      5000,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// 2. Remove an entry from middle
	_, err = engine.Remove(ctx, channel, "key_15", MapRemoveOptions{

		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// 3. Update score of an entry (move it)
	_, err = engine.Publish(ctx, channel, "key_05", MapPublishOptions{
		Data:       []byte("data_05_moved"),
		Ordered:    true,
		Score:      4000, // Move from 500 to 4000
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read second page
	_, _, cursor2, err := engine.ReadState(ctx, channel, MapReadStateOptions{
		Ordered: true,
		Limit:   10,
		Cursor:  cursor1,
	})
	require.NoError(t, err)

	// 4. Add new lowest score entry
	_, err = engine.Publish(ctx, channel, "key_new_bottom", MapPublishOptions{
		Data:       []byte("data_new_bottom"),
		Ordered:    true,
		Score:      1,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read remaining pages
	cursor := cursor2
	for cursor != "" {
		_, _, nextCursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Ordered: true,
			Limit:   10,
			Cursor:  cursor,
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

// TestMemoryMapBroker_CursorFormat tests that cursor format is correct.
func TestMemoryMapBroker_CursorFormat(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestMemoryMapBroker(t, node)
	ctx := context.Background()

	t.Run("unordered cursor is key-based", func(t *testing.T) {
		channel := "test_cursor_unordered"

		// Create entries
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
				Data:       []byte("data"),
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
			})
			require.NoError(t, err)
		}

		// Read first page
		pubs, _, cursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, pubs, 10)

		// Cursor should be the last key (key_09 for lexicographic order)
		require.Equal(t, "key_09", cursor, "Cursor should be last key of page")

		// Read next page with cursor
		pubs2, _, cursor2, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor,
		})
		require.NoError(t, err)
		require.Len(t, pubs2, 10)

		// First entry of second page should be after cursor
		require.Equal(t, "key_10", pubs2[0].Key, "First entry of page 2 should be key_10")
		// Last entry should be key_19
		require.Equal(t, "key_19", pubs2[9].Key, "Last entry of page 2 should be key_19")
		// No more pages, cursor should be empty
		require.Empty(t, cursor2, "Cursor should be empty when no more pages")
	})

	t.Run("ordered cursor is score:key format", func(t *testing.T) {
		channel := "test_cursor_ordered"

		// Create entries with different scores
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := engine.Publish(ctx, channel, key, MapPublishOptions{
				Data:       []byte("data"),
				Ordered:    true,
				Score:      int64(i * 100),
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
			})
			require.NoError(t, err)
		}

		// Read first page (highest scores first)
		pubs, _, cursor, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Ordered: true,
			Limit:   10,
		})
		require.NoError(t, err)
		require.Len(t, pubs, 10)

		// First entry should be key_20 (highest score 2000)
		require.Equal(t, "key_20", pubs[0].Key)
		// Last entry should be key_11 (score 1100)
		require.Equal(t, "key_11", pubs[9].Key)

		// Cursor format should be "score\x00key"
		require.Contains(t, cursor, "\x00", "Cursor should contain separator")
		parts := make([]string, 0, 2)
		for i := 0; i < len(cursor); i++ {
			if cursor[i] == '\x00' {
				parts = append(parts, cursor[:i], cursor[i+1:])
				break
			}
		}
		require.Len(t, parts, 2)
		require.Equal(t, "1100", parts[0], "Cursor score should be 1100")
		require.Equal(t, "key_11", parts[1], "Cursor key should be key_11")

		// Read next page
		pubs2, _, _, err := engine.ReadState(ctx, channel, MapReadStateOptions{
			Ordered: true,
			Limit:   10,
			Cursor:  cursor,
		})
		require.NoError(t, err)
		require.Len(t, pubs2, 10)

		// First entry of page 2 should be key_10 (score 1000, which is < 1100)
		require.Equal(t, "key_10", pubs2[0].Key, "First entry of page 2 should be key_10")
	})
}
