package centrifuge

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newTestMemoryMapBroker(tb testing.TB, n *Node) *MemoryMapBroker {
	e, err := NewMemoryMapBroker(n, MemoryMapBrokerConfig{})
	require.NoError(tb, err)
	err = e.RegisterEventHandler(nil)
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_stateful"

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
	entries, streamPos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.NotEmpty(t, streamPos.Epoch)
	require.Greater(t, streamPos.Offset, uint64(0))

	// Verify state contains latest values
	state := stateToMapMemory(entries)
	require.Len(t, state, 2)
	require.Equal(t, []byte("data1_updated"), state["key1"])
	require.Equal(t, []byte("data2"), state["key2"])

	// Read stream to verify all publications are in history
	streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1, // Get all
		},
	})
	require.NoError(t, err)
	require.Len(t, streamResult.Publications, 3) // All 3 publications in stream
}

// TestMemoryMapBroker_StatefulChannelOrdered tests ordered stateful channel.
func TestMemoryMapBroker_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered"

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
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_revision"

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
	entries, streamPos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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

// TestMemoryMapBroker_StatePagination tests cursor-based state pagination.
func TestMemoryMapBroker_StatePagination(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_pagination"

	// Publish 10 keyed entries
	for i := 0; i < 10; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Read state with limit
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:  3,
		Cursor: "",
	})
	page1, pos1, cursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.NotEmpty(t, page1)

	// Collect all keys across pages
	allKeys := make(map[string]bool)
	for _, entry := range page1 {
		allKeys[entry.Key] = true
	}

	// Continue reading until cursor is empty
	for cursor != "" {
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  3,
			Cursor: cursor,
		})
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
}

// TestMemoryMapBroker_EpochHandling tests epoch changes and state invalidation.
func TestMemoryMapBroker_EpochHandling(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_epoch"

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
	entries, streamPos1, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_nonexistent_channel"

	// Client tries to read state with an old epoch, but channel doesn't exist
	// (simulates reconnection after server restart)
	_, err := broker.ReadState(ctx, channel, MapReadStateOptions{
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_fresh_channel"

	// Fresh subscription - no epoch provided
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, streamPos, cursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Empty(t, entries)            // No entries in non-existent channel
	require.NotEmpty(t, streamPos.Epoch) // Should get a new epoch
	require.Empty(t, cursor)             // No cursor for empty result
}

// TestMemoryMapBroker_Idempotency tests idempotent publishing.
func TestMemoryMapBroker_Idempotency(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_idempotency"

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
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	state := stateToMapMemory(entries)
	require.Len(t, state, 1)
	require.Equal(t, []byte("data1"), state["key1"])
}

// TestMemoryMapBroker_VersionedPublishing tests version-based idempotency.
func TestMemoryMapBroker_VersionedPublishing(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_version"

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
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	state := stateToMapMemory(entries)
	require.Equal(t, []byte("data_v3"), state["key1"])
}

// TestMemoryMapBroker_PerKeyVersion tests that version tracking is per-key independent.
func TestMemoryMapBroker_PerKeyVersion(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_per_key_version"

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
	state := stateToMapMemory(stateRes.Publications)
	require.Equal(t, []byte("key1_v1"), state["key1"])
	require.Equal(t, []byte("key2_v5"), state["key2"])
}

// TestMemoryMapBroker_MultipleChannels tests multiple channels independently.
func TestMemoryMapBroker_MultipleChannels(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel1 := "test_multi1"
	channel2 := "test_multi2"

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
	entries1, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	state1 := stateToMapMemory(entries1)
	require.Len(t, state1, 1)
	require.Equal(t, []byte("data1"), state1["key1"])

	// Read channel2 state
	stateRes, err = broker.ReadState(ctx, channel2, MapReadStateOptions{
		Limit: 100,
	})
	entries2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	state2 := stateToMapMemory(entries2)
	require.Len(t, state2, 1)
	require.Equal(t, []byte("data2"), state2["key2"])
}

// TestMemoryMapBroker_OrderedStateOrdering tests that ordered state return entries
// in correct score order (descending by score).
func TestMemoryMapBroker_OrderedStateOrdering(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

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
		_, err := broker.Publish(ctx, channel, tc.key, MapPublishOptions{
			Data:  []byte(tc.data),
			score: tc.score,
		})
		require.NoError(t, err)
	}

	// Read ordered state - should be sorted by score (descending)
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered_pagination"

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
	page2, pos2, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

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
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered_update"

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
	entries2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_remove"

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
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Remove key1
	_, err = broker.Remove(ctx, channel, "key1", MapRemoveOptions{})
	require.NoError(t, err)

	// Verify state has 1 key
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "key2", entries[0].Key)

	// Verify remove was added to stream
	streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, streamResult.Publications, 3) // key1, key2, remove(key1)
	require.True(t, streamResult.Publications[2].Removed)
	require.Equal(t, "key1", streamResult.Publications[2].Key)

	// Remove non-existent key should be suppressed with key_not_found
	res, err := broker.Remove(ctx, channel, "nonexistent", MapRemoveOptions{})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, res.SuppressReason)

	// Verify no extra stream entry was added
	streamResult, err = broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, streamResult.Publications, 3) // Still 3 - no entry for nonexistent key
}

func TestMemoryMapBroker_RemovePreservesTags(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_remove_tags"

	// Publish with tags.
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":1}`),
		Tags: map[string]string{"role": "admin", "team": "eng"},
	})
	require.NoError(t, err)

	// Remove without explicit tags — should auto-read from state.
	_, err = broker.Remove(ctx, channel, "key1", MapRemoveOptions{})
	require.NoError(t, err)

	// Read stream — removal should have the original tags.
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
	require.Equal(t, map[string]string{"role": "admin", "team": "eng"}, removal.Tags)
}

func TestMemoryMapBroker_RemoveExplicitTagsOverride(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_remove_explicit_tags"

	// Publish with tags.
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":1}`),
		Tags: map[string]string{"role": "admin"},
	})
	require.NoError(t, err)

	// Remove with explicit tags — should override state tags.
	_, err = broker.Remove(ctx, channel, "key1", MapRemoveOptions{
		Tags: map[string]string{"role": "viewer"},
	})
	require.NoError(t, err)

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
	require.Equal(t, map[string]string{"role": "viewer"}, removal.Tags)
}

func TestMemoryMapBroker_CleanupPreservesTags(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     50 * time.Millisecond,
		}
	}

	handler := &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, useDelta bool, prevPub *Publication) error {
			return nil
		},
	}
	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	ctx := context.Background()
	channel := "test_cleanup_tags"

	// Publish with tags.
	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data: []byte(`{"v":1}`),
		Tags: map[string]string{"role": "admin"},
	})
	require.NoError(t, err)

	// Wait for expiry + trigger cleanup.
	time.Sleep(100 * time.Millisecond)
	var nextCheck int64
	broker.mapHub.expireKeysIteration(&nextCheck)

	// Read stream — removal should have original tags.
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
	require.Equal(t, "key1", removal.Key)
	require.Equal(t, map[string]string{"role": "admin"}, removal.Tags)
}

// TestMemoryMapBroker_KeyModeIfNew tests KeyModeIfNew - only write if key doesn't exist.
func TestMemoryMapBroker_KeyModeIfNew(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_keymode_if_new"

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
}

// TestMemoryMapBroker_KeyModeIfExists tests KeyModeIfExists - only write if key exists.
func TestMemoryMapBroker_KeyModeIfExists(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_keymode_if_exists"

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
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("heartbeat2"), entries[0].Data)
}

// TestMemoryMapBroker_KeyModeReplace tests default KeyModeReplace behavior.
func TestMemoryMapBroker_KeyModeReplace(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_keymode_replace"

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
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	broker MapBroker,
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
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  pageSize,
			Cursor: cursor,
		})
		pubs, streamPos, nextCursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &firstStreamPos,
			Limit: -1, // Get all
		},
	})
	require.NoError(t, err)

	// Step 4: Apply stream changes
	for _, pub := range streamResult.Publications {
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_unordered_continuity_remove"

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data: []byte(fmt.Sprintf("data_%02d", i)),
		})
		require.NoError(t, err)
	}

	// Read first page (keys key_00 to key_09 lexicographically)
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 10,
	})
	pubs1, streamPos1, cursor1 := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 10)
	require.NotEmpty(t, cursor1)

	// CONCURRENT MODIFICATION: Remove key_10 (first entry of next page)
	// This would cause key_11 to shift into position 10, potentially being skipped
	// with integer offset pagination
	_, err = broker.Remove(ctx, channel, "key_10", MapRemoveOptions{})
	require.NoError(t, err)

	// Read second page with cursor
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:  10,
		Cursor: cursor1,
	})
	pubs2, _, cursor2 := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor,
		})
		pubs, _, nextCursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		for _, pub := range pubs {
			if pub.Offset <= streamPos1.Offset {
				stateKeys[pub.Key] = true
			}
		}
		cursor = nextCursor
	}

	// Read stream to get changes since streamPos1
	streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &streamPos1,
			Limit: -1,
		},
	})
	require.NoError(t, err)

	// Apply stream changes
	for _, pub := range streamResult.Publications {
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
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_unordered_continuity_add"

	// Create 20 entries: key_00 to key_19
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data: []byte(fmt.Sprintf("data_%02d", i)),
		})
		require.NoError(t, err)
	}

	// Read first page
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 10,
	})
	pubs1, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, pubs1, 10)

	// CONCURRENT MODIFICATION: Add new entry that lexicographically comes before cursor
	// This would shift entries with integer offset pagination
	_, err = broker.Publish(ctx, channel, "key_05b", MapPublishOptions{
		Data: []byte("data_05b"),
	})
	require.NoError(t, err)

	// Simulate full client recovery
	result := simulateClientRecovery(t, broker, channel, false, 10)

	// Verify: should have 21 keys (original 20 + new key_05b)
	require.Len(t, result, 21, "Should have 21 keys after addition")
	require.Contains(t, result, "key_05b", "key_05b should be present")
	require.Equal(t, []byte("data_05b"), result["key_05b"])
}

// TestMemoryMapBroker_OrderedContinuity_HigherScoreAdded tests that adding
// an entry with higher score during ordered pagination doesn't cause data loss.
func TestMemoryMapBroker_OrderedContinuity_HigherScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_higher"

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
}

// TestMemoryMapBroker_OrderedContinuity_LowerScoreAdded tests that adding
// an entry with lower score during ordered pagination works correctly.
func TestMemoryMapBroker_OrderedContinuity_LowerScoreAdded(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_lower"

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
	pubs2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.NotEmpty(t, pubs2)

	// Simulate full recovery
	result := simulateClientRecovery(t, broker, channel, true, 10)

	require.Len(t, result, 21, "Should have 21 keys")
	require.Contains(t, result, "key_bottom", "key_bottom should be present")
}

// TestMemoryMapBroker_OrderedContinuity_ScoreChanged tests that changing
// an entry's score during pagination (causing reordering) doesn't lose data.
func TestMemoryMapBroker_OrderedContinuity_ScoreChanged(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_score_change"

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
}

// TestMemoryMapBroker_OrderedContinuity_EntryRemoved tests that removing
// an entry during ordered pagination doesn't cause data loss.
func TestMemoryMapBroker_OrderedContinuity_EntryRemoved(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_remove"

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
}

// TestMemoryMapBroker_OrderedContinuity_MultipleChanges tests recovery
// with multiple concurrent changes during pagination.
func TestMemoryMapBroker_OrderedContinuity_MultipleChanges(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	channel := "test_ordered_continuity_multi"

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
}

// TestMemoryMapBroker_CursorFormat tests that cursor format is correct.
func TestMemoryMapBroker_CursorFormat(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()

	t.Run("unordered cursor is key-based", func(t *testing.T) {
		channel := "test_cursor_unordered"

		// Create entries
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data: []byte("data"),
			})
			require.NoError(t, err)
		}

		// Read first page
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		pubs, _, cursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, pubs, 10)

		// Cursor should be the last key (key_09 for lexicographic order)
		require.Equal(t, "key_09", cursor, "Cursor should be last key of page")

		// Read next page with cursor
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor,
		})
		pubs2, _, cursor2 := stateRes.Publications, stateRes.Position, stateRes.Cursor
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

		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModePersistent,
				ordered:    true,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
			}
		}

		// Create entries with different scores
		for i := 1; i <= 20; i++ {
			key := fmt.Sprintf("key_%02d", i)
			_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
				Data:  []byte("data"),
				score: int64(i * 100),
			})
			require.NoError(t, err)
		}

		// Read first page (highest scores first)
		stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit: 10,
		})
		pubs, _, cursor := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
		stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
			Limit:  10,
			Cursor: cursor,
		})
		pubs2, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
		require.NoError(t, err)
		require.Len(t, pubs2, 10)

		// First entry of page 2 should be key_10 (score 1000, which is < 1100)
		require.Equal(t, "key_10", pubs2[0].Key, "First entry of page 2 should be key_10")
	})
}

// TestMemoryMapBroker_Delta tests key-based delta delivery via HandlePublication.
func TestMemoryMapBroker_Delta(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}

	type pubEvent struct {
		ch      string
		pub     *Publication
		delta   bool
		prevPub *Publication
	}

	var mu sync.Mutex
	var events []pubEvent

	handler := &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			mu.Lock()
			events = append(events, pubEvent{ch: ch, pub: pub, delta: delta, prevPub: prevPub})
			mu.Unlock()
			return nil
		},
	}

	e, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = e.RegisterEventHandler(handler)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	ctx := context.Background()
	channel := "test_delta"

	// 1. First publish with UseDelta - no previous state, prevPub should be nil.
	_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:     []byte("data1"),
		UseDelta: true,
	})
	require.NoError(t, err)

	mu.Lock()
	require.Len(t, events, 1)
	require.True(t, events[0].delta)
	require.Nil(t, events[0].prevPub, "no previous state for first publish")
	mu.Unlock()

	// 2. Second publish same key - should get prevPub with first data.
	_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:     []byte("data1_updated"),
		UseDelta: true,
	})
	require.NoError(t, err)

	mu.Lock()
	require.Len(t, events, 2)
	require.True(t, events[1].delta)
	require.NotNil(t, events[1].prevPub)
	require.Equal(t, []byte("data1"), events[1].prevPub.Data)
	mu.Unlock()

	// 3. Different key - no previous state for this key.
	_, err = e.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:     []byte("data2"),
		UseDelta: true,
	})
	require.NoError(t, err)

	mu.Lock()
	require.Len(t, events, 3)
	require.True(t, events[2].delta)
	require.Nil(t, events[2].prevPub, "no previous state for key2")
	mu.Unlock()

	// 4. UseDelta=false - no delta.
	_, err = e.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:     []byte("data2_updated"),
		UseDelta: false,
	})
	require.NoError(t, err)

	mu.Lock()
	require.Len(t, events, 4)
	require.False(t, events[3].delta)
	require.Nil(t, events[3].prevPub, "UseDelta=false means no delta")
	mu.Unlock()
}

func TestMemoryMapBroker_Clear(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_clear"

	// Publish some keyed state and stream entries.
	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("data%d", i)),
		})
		require.NoError(t, err)
	}

	// Verify data exists.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
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
}

func TestMemoryMapBroker_ClearDoesNotAffectOtherChannels(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()

	// Populate two channels.
	for _, ch := range []string{"ch1", "ch2"} {
		_, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data: []byte("v"),
		})
		require.NoError(t, err)
	}

	// Clear only ch1.
	err := broker.Clear(ctx, "ch1", MapClearOptions{})
	require.NoError(t, err)

	// ch1 empty.
	stateRes, err := broker.ReadState(ctx, "ch1", MapReadStateOptions{Limit: 100})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Empty(t, entries)

	// ch2 still intact.
	stateRes, err = broker.ReadState(ctx, "ch2", MapReadStateOptions{Limit: 100})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestMemoryMapBroker_ReadStream_Table(t *testing.T) {
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
		return newTestMemoryMapBroker(t, node)
	})
}

func TestMemoryMapBroker_EpochOnEmptyChannel(t *testing.T) {
	testMapBrokerEpochOnEmptyChannel(t, func(t *testing.T) MapBroker {
		node, _ := New(Config{})
		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			}
		}
		return newTestMemoryMapBroker(t, node)
	})
}

func TestMemoryMapBroker_ReadStateAllEntries(t *testing.T) {
	testMapBrokerReadStateAllEntries(t, func(t *testing.T) MapBroker {
		node, _ := New(Config{})
		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			}
		}
		return newTestMemoryMapBroker(t, node)
	})
}

func TestMemoryMapBroker_RemoveEmptyKey(t *testing.T) {
	testMapBrokerRemoveEmptyKey(t, func(t *testing.T) MapBroker {
		node, _ := New(Config{})
		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			}
		}
		return newTestMemoryMapBroker(t, node)
	})
}

func TestMemoryMapBroker_ClientInfoInState(t *testing.T) {
	testMapBrokerClientInfoInState(t, func(t *testing.T) MapBroker {
		node, _ := New(Config{})
		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			}
		}
		return newTestMemoryMapBroker(t, node)
	})
}

func TestMemoryMapBroker_ClientInfoInStream(t *testing.T) {
	testMapBrokerClientInfoInStream(t, func(t *testing.T) MapBroker {
		node, _ := New(Config{})
		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			}
		}
		return newTestMemoryMapBroker(t, node)
	})
}

func TestMemoryMapBroker_CheckOrder(t *testing.T) {
	testMapBrokerCheckOrder(t, func(t *testing.T) MapBroker {
		node, _ := New(Config{})
		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			}
		}
		return newTestMemoryMapBroker(t, node)
	})
}

func TestMemoryMapBroker_VersionPreserved(t *testing.T) {
	testMapBrokerVersionPreserved(t, func(t *testing.T) MapBroker {
		node, _ := New(Config{})
		node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
			return MapChannelOptions{
				Mode:       MapModeRecoverable,
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
			}
		}
		return newTestMemoryMapBroker(t, node)
	})
}

// TestMemoryMapBroker_ClientInfoDelivery tests that ClientInfo is delivered
// to the event handler when publishing with ClientInfo.
func TestMemoryMapBroker_ClientInfoDelivery(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}

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

	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = broker.RegisterEventHandler(handler)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = node.Shutdown(context.Background())
	})

	ctx := context.Background()
	channel := "test_client_info_delivery"

	info := &ClientInfo{
		ClientID: "c1",
		UserID:   "u1",
		ConnInfo: []byte("conn"),
		ChanInfo: []byte("chan"),
	}

	_, err = broker.Publish(ctx, channel, "k1", MapPublishOptions{
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
}

// TestMemoryMapBroker_OrderedStateAsc tests ascending sort direction.
func TestMemoryMapBroker_OrderedStateAsc(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered_asc"

	testCases := []struct {
		key   string
		score int64
	}{
		{"key_c", 30},
		{"key_a", 10},
		{"key_e", 50},
		{"key_b", 20},
		{"key_d", 40},
	}

	for _, tc := range testCases {
		_, err := broker.Publish(ctx, channel, tc.key, MapPublishOptions{
			Data:  []byte("data"),
			score: tc.score,
		})
		require.NoError(t, err)
	}

	// Read with Asc=true — should be sorted by score ascending.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
		Asc:   true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 5)

	expectedOrder := []string{"key_a", "key_b", "key_c", "key_d", "key_e"}
	for i, entry := range stateRes.Publications {
		require.Equal(t, expectedOrder[i], entry.Key, "ASC entry %d", i)
	}

	// Read with Asc=false (default) — should be descending.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 5)

	expectedDesc := []string{"key_e", "key_d", "key_c", "key_b", "key_a"}
	for i, entry := range stateRes.Publications {
		require.Equal(t, expectedDesc[i], entry.Key, "DESC entry %d", i)
	}
}

// TestMemoryMapBroker_OrderedStatePaginationAsc tests pagination with ascending sort.
func TestMemoryMapBroker_OrderedStatePaginationAsc(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered_pagination_asc"

	// Publish 10 entries with scores 100, 200, ..., 1000.
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("key_%02d", i)
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:  []byte(fmt.Sprintf("data_%02d", i)),
			score: int64(i * 100),
		})
		require.NoError(t, err)
	}

	// Page 1: limit=3, asc.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 3, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 3)
	require.NotEmpty(t, stateRes.Cursor)

	// ASC page 1: keys with scores 100, 200, 300 → key_01, key_02, key_03.
	require.Equal(t, "key_01", stateRes.Publications[0].Key)
	require.Equal(t, "key_02", stateRes.Publications[1].Key)
	require.Equal(t, "key_03", stateRes.Publications[2].Key)

	// Page 2: using cursor.
	stateRes2, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Cursor: stateRes.Cursor, Limit: 3, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes2.Publications, 3)
	require.NotEmpty(t, stateRes2.Cursor)

	// ASC page 2: key_04, key_05, key_06.
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
}

// TestMemoryMapBroker_OrderedStateAscSameScores tests ASC ordering with
// same-score entries — secondary sort by key ascending.
func TestMemoryMapBroker_OrderedStateAscSameScores(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered_asc_same_scores"

	// All entries have score=100, ordering should be by key ASC.
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
}

func TestMemoryMapBroker_CleanupMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	node, err := New(Config{
		Metrics: MetricsConfig{
			RegistererGatherer: registry,
		},
	})
	require.NoError(t, err)
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     50 * time.Millisecond,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	_, err = broker.Publish(ctx, "test_cleanup_metrics", "key1", MapPublishOptions{
		Data: []byte("data1"),
	})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, "test_cleanup_metrics", "key2", MapPublishOptions{
		Data: []byte("data2"),
	})
	require.NoError(t, err)

	// Wait for keys to expire and cleanup to run (1s timer + some margin).
	require.Eventually(t, func() bool {
		families, err := registry.Gather()
		if err != nil {
			return false
		}
		for _, f := range families {
			if f.GetName() == "centrifuge_map_broker_cleanup_keys_removed_count" {
				for _, m := range f.GetMetric() {
					if m.GetCounter().GetValue() >= 2 {
						return true
					}
				}
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "cleanup_keys_removed_count should reach at least 2")

	// Verify lag metric was set (should be >= 0).
	families, err := registry.Gather()
	require.NoError(t, err)
	var foundLag bool
	for _, f := range families {
		if f.GetName() == "centrifuge_map_broker_cleanup_lag_seconds" {
			for _, m := range f.GetMetric() {
				_ = m.GetGauge().GetValue()
				foundLag = true
			}
		}
	}
	require.True(t, foundLag, "cleanup_lag_seconds metric should exist")
}

func TestResolveAndValidateMapChannelOptions(t *testing.T) {
	t.Run("nil resolver", func(t *testing.T) {
		_, err := ResolveAndValidateMapChannelOptions(nil, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "resolver not configured")
	})

	t.Run("mode not set", func(t *testing.T) {
		resolver := func(string) MapChannelOptions { return MapChannelOptions{} }
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "set Mode")
	})

	t.Run("invalid mode", func(t *testing.T) {
		resolver := func(string) MapChannelOptions { return MapChannelOptions{Mode: 99} }
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid Mode")
	})

	t.Run("ephemeral requires KeyTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions { return MapChannelOptions{Mode: MapModeEphemeral} }
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "KeyTTL required")
	})

	t.Run("ephemeral negative KeyTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: -1}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "KeyTTL must be positive")
	})

	t.Run("persistent rejects KeyTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModePersistent, KeyTTL: time.Second}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "KeyTTL must be 0 for persistent")
	})

	t.Run("ephemeral rejects StreamSize", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Second, StreamSize: 10}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "StreamSize requires recoverable")
	})

	t.Run("ephemeral rejects StreamTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Second, StreamTTL: time.Minute}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "StreamTTL requires recoverable")
	})

	t.Run("ephemeral rejects MetaTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: time.Second, MetaTTL: time.Minute}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "MetaTTL requires recoverable")
	})

	t.Run("recoverable negative StreamSize", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeRecoverable, KeyTTL: time.Second, StreamSize: -1}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "StreamSize must be non-negative")
	})

	t.Run("recoverable negative StreamTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeRecoverable, KeyTTL: time.Second, StreamTTL: -1}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "StreamTTL must be non-negative")
	})

	t.Run("recoverable negative MetaTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeRecoverable, KeyTTL: time.Second, MetaTTL: -1}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "MetaTTL must be non-negative")
	})

	t.Run("MetaTTL less than StreamTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{
				Mode: MapModeRecoverable, KeyTTL: time.Second,
				StreamTTL: 10 * time.Minute, MetaTTL: time.Minute,
			}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "MetaTTL must be >= StreamTTL")
	})

	t.Run("MetaTTL with permanent KeyTTL rejected", func(t *testing.T) {
		// Persistent mode has KeyTTL = 0 (permanent). Setting MetaTTL > 0 means
		// the metadata can expire while the keys live forever — invalid.
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{
				Mode:    MapModePersistent,
				MetaTTL: time.Minute,
			}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "MetaTTL must be 0 (permanent) when KeyTTL is 0")
	})

	t.Run("MetaTTL less than KeyTTL", func(t *testing.T) {
		// Metadata must outlive keys: MetaTTL >= KeyTTL.
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{
				Mode:      MapModeRecoverable,
				KeyTTL:    10 * time.Minute,
				StreamTTL: time.Minute, // less than KeyTTL but irrelevant once MetaTTL fires
				MetaTTL:   5 * time.Minute,
			}
		}
		_, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.Error(t, err)
		require.Contains(t, err.Error(), "MetaTTL must be >= KeyTTL")
	})

	t.Run("recoverable auto defaults", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeRecoverable, KeyTTL: time.Second}
		}
		opts, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.NoError(t, err)
		require.Equal(t, 100, opts.StreamSize, "auto StreamSize")
		require.Equal(t, time.Minute, opts.StreamTTL, "auto StreamTTL")
		require.Equal(t, 10*time.Minute, opts.MetaTTL, "auto MetaTTL for recoverable")
	})

	t.Run("persistent auto defaults no MetaTTL", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModePersistent}
		}
		opts, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.NoError(t, err)
		require.Equal(t, 100, opts.StreamSize)
		require.Equal(t, time.Minute, opts.StreamTTL)
		require.Equal(t, time.Duration(0), opts.MetaTTL, "persistent: MetaTTL stays 0")
	})

	t.Run("ephemeral valid", func(t *testing.T) {
		resolver := func(string) MapChannelOptions {
			return MapChannelOptions{Mode: MapModeEphemeral, KeyTTL: 5 * time.Second}
		}
		opts, err := ResolveAndValidateMapChannelOptions(resolver, "ch")
		require.NoError(t, err)
		require.Equal(t, MapModeEphemeral, opts.Mode)
		require.Equal(t, 5*time.Second, opts.KeyTTL)
	})
}

func TestParseOrderedCursor(t *testing.T) {
	t.Run("valid cursor", func(t *testing.T) {
		score, key := parseOrderedCursor("100\x00mykey")
		require.Equal(t, "100", score)
		require.Equal(t, "mykey", key)
	})

	t.Run("empty cursor", func(t *testing.T) {
		score, key := parseOrderedCursor("")
		require.Equal(t, "", score)
		require.Equal(t, "", key)
	})

	t.Run("no separator", func(t *testing.T) {
		score, key := parseOrderedCursor("noseparator")
		require.Equal(t, "", score)
		require.Equal(t, "", key)
	})

	t.Run("roundtrip", func(t *testing.T) {
		cursor := MakeOrderedCursor("42", "testkey")
		score, key := parseOrderedCursor(cursor)
		require.Equal(t, "42", score)
		require.Equal(t, "testkey", key)
	})

	t.Run("negative score", func(t *testing.T) {
		cursor := MakeOrderedCursor("-5", "k")
		score, key := parseOrderedCursor(cursor)
		require.Equal(t, "-5", score)
		require.Equal(t, "k", key)
	})

	t.Run("empty key part", func(t *testing.T) {
		cursor := MakeOrderedCursor("10", "")
		score, key := parseOrderedCursor(cursor)
		require.Equal(t, "10", score)
		require.Equal(t, "", key)
	})
}

func TestMapMode_Methods(t *testing.T) {
	require.True(t, MapModeEphemeral.IsEphemeral())
	require.False(t, MapModeRecoverable.IsEphemeral())
	require.False(t, MapModePersistent.IsEphemeral())

	require.False(t, MapModeEphemeral.HasStream())
	require.True(t, MapModeRecoverable.HasStream())
	require.True(t, MapModePersistent.HasStream())

	require.True(t, MapModeEphemeral.HasExpiry())
	require.True(t, MapModeRecoverable.HasExpiry())
	require.False(t, MapModePersistent.HasExpiry())
}

// TestMemoryMapBroker_CAS_Publish tests Compare-And-Swap semantics for Publish.
func TestMemoryMapBroker_CAS_Publish(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_cas_publish"

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
	require.Equal(t, uint64(2), res2.Position.Offset)

	// CAS with stale offset → mismatch, returns CurrentPublication
	res3, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:             []byte("v3"),
		ExpectedPosition: &StreamPosition{Offset: 1, Epoch: epoch}, // stale offset
	})
	require.NoError(t, err)
	require.True(t, res3.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res3.SuppressReason)
	require.NotNil(t, res3.CurrentPublication)
	require.Equal(t, []byte("v2"), res3.CurrentPublication.Data)
	require.Equal(t, uint64(2), res3.CurrentPublication.Offset)

	// CAS with wrong epoch → mismatch
	res4, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:             []byte("v4"),
		ExpectedPosition: &StreamPosition{Offset: 2, Epoch: "wrong-epoch"},
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
	require.Nil(t, res5.CurrentPublication)

	// Verify state unchanged
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)
	require.Equal(t, []byte("v2"), stateRes.Publications[0].Data)
}

// TestMemoryMapBroker_CAS_Remove tests Compare-And-Swap semantics for Remove.
func TestMemoryMapBroker_CAS_Remove(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_cas_remove"

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

	// CAS remove with wrong epoch → mismatch
	res3, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{
		ExpectedPosition: &StreamPosition{Offset: res1.Position.Offset, Epoch: "bad-epoch"},
	})
	require.NoError(t, err)
	require.True(t, res3.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res3.SuppressReason)

	// CAS remove with correct position → succeeds
	res4, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{
		ExpectedPosition: &StreamPosition{Offset: res1.Position.Offset, Epoch: epoch},
	})
	require.NoError(t, err)
	require.False(t, res4.Suppressed)

	// Verify key removed
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Empty(t, stateRes.Publications)
}

// TestMemoryMapBroker_EphemeralRejectsCAS tests that CAS and Version are rejected in ephemeral mode.
func TestMemoryMapBroker_EphemeralRejectsCAS(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:   MapModeEphemeral,
			KeyTTL: 30 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_ephemeral_cas"

	// CAS publish in ephemeral → error
	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:             []byte("v1"),
		ExpectedPosition: &StreamPosition{Offset: 1, Epoch: "x"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CAS")

	// Version publish in ephemeral → error
	_, err = broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:    []byte("v1"),
		Version: 1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "version")

	// CAS remove in ephemeral → error
	_, err = broker.Remove(ctx, ch, "key1", MapRemoveOptions{
		ExpectedPosition: &StreamPosition{Offset: 1, Epoch: "x"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CAS")
}

// TestMemoryMapBroker_RemoveIdempotency tests idempotency key for Remove operations.
func TestMemoryMapBroker_RemoveIdempotency(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_remove_idempotency"

	// Setup: create key1 and key2
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

	// Verify key2 still exists (second remove was suppressed)
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)
	require.Equal(t, "key2", stateRes.Publications[0].Key)
}

// TestMemoryMapBroker_RemoveCustomIdempotentTTL tests custom IdempotentResultTTL for Remove.
func TestMemoryMapBroker_RemoveCustomIdempotentTTL(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_remove_custom_ttl"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Remove with custom TTL
	res, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{
		IdempotencyKey:      "r1",
		IdempotentResultTTL: 5 * time.Minute,
	})
	require.NoError(t, err)
	require.False(t, res.Suppressed)

	// Repeat → suppressed (cached)
	res2, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{
		IdempotencyKey:      "r1",
		IdempotentResultTTL: 5 * time.Minute,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed)
	require.Equal(t, res.Position, res2.Position)
}

// TestMemoryMapBroker_PublishCustomIdempotentTTL tests custom IdempotentResultTTL for Publish.
func TestMemoryMapBroker_PublishCustomIdempotentTTL(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_publish_custom_ttl"

	res1, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:                []byte("v1"),
		IdempotencyKey:      "p1",
		IdempotentResultTTL: 10 * time.Minute,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	res2, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:                []byte("v2"),
		IdempotencyKey:      "p1",
		IdempotentResultTTL: 10 * time.Minute,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed)
	require.Equal(t, res1.Position, res2.Position)
}

// TestMemoryMapBroker_KeyTTLExpiration tests that keys expire after KeyTTL.
func TestMemoryMapBroker_KeyTTLExpiration(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     50 * time.Millisecond,
		}
	}

	// Register a real event handler so expireKeysIteration creates stream entries.
	handler := &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, useDelta bool, prevPub *Publication) error {
			return nil
		},
	}
	e, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = e.RegisterEventHandler(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	ctx := context.Background()
	ch := "test_key_ttl"

	_, err = e.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)
	_, err = e.Publish(ctx, ch, "key2", MapPublishOptions{Data: []byte("v2")})
	require.NoError(t, err)

	// Keys should exist initially
	stateRes, err := e.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 2)

	// Wait for keys to expire AND removal events to appear in stream.
	// The cleanup runs on a 1s timer: Phase 1 removes keys from state,
	// Phase 2 adds removal events to the stream. Both happen in the same iteration.
	require.Eventually(t, func() bool {
		streamRes, err := e.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		if err != nil {
			return false
		}
		removals := 0
		for _, pub := range streamRes.Publications {
			if pub.Removed {
				removals++
			}
		}
		return removals == 2
	}, 5*time.Second, 100*time.Millisecond, "should have 2 removal events in stream")

	// Verify keys are gone from state
	stateRes, err = e.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Empty(t, stateRes.Publications)
}

// TestMemoryMapBroker_KeyTTLRefresh tests that refreshing a key extends its TTL.
func TestMemoryMapBroker_KeyTTLRefresh(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     200 * time.Millisecond,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_key_ttl_refresh"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Keep refreshing the key by re-publishing every 100ms for 500ms total.
	// If TTL wasn't refreshed, the key would expire at ~200ms.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 4; i++ {
			time.Sleep(100 * time.Millisecond)
			_, _ = broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte(fmt.Sprintf("v%d", i+2))})
		}
	}()
	<-done

	// Key should still exist after 500ms because each publish refreshed the 200ms TTL
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1, "key should still exist after TTL refresh")
}

// TestMemoryMapBroker_StreamTTLExpiration tests that stream entries are cleared after StreamTTL.
func TestMemoryMapBroker_StreamTTLExpiration(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  50 * time.Millisecond,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_stream_ttl"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, ch, "key2", MapPublishOptions{Data: []byte("v2")})
	require.NoError(t, err)

	// Stream should have entries initially
	streamRes, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{Filter: StreamFilter{Limit: -1}})
	require.NoError(t, err)
	require.Len(t, streamRes.Publications, 2)

	// Wait for stream to expire
	require.Eventually(t, func() bool {
		streamRes, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{Filter: StreamFilter{Limit: -1}})
		if err != nil {
			return false
		}
		return len(streamRes.Publications) == 0
	}, 5*time.Second, 100*time.Millisecond, "stream should expire")

	// State should still exist (KeyTTL is long)
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 2)
}

// TestMemoryMapBroker_MetaTTLRemovesChannel tests that channel is removed after MetaTTL.
func TestMemoryMapBroker_MetaTTLRemovesChannel(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  50 * time.Millisecond,
			KeyTTL:     50 * time.Millisecond,
			MetaTTL:    50 * time.Millisecond,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_meta_ttl"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Wait for channel to be removed (MetaTTL expires, cleanup runs on 1s timer).
	// Use Stats (which doesn't refresh MetaTTL, unlike ReadState).
	// After removal, the internal channels map no longer has the channel,
	// so Stats returns NumKeys=0. But it also returns 0 for empty channels.
	// So first verify the key expired, then verify the channel was removed
	// by checking that a subsequent ReadState returns a new epoch.
	require.Eventually(t, func() bool {
		// Check if channel was removed from the internal map.
		broker.mapHub.RLock()
		_, exists := broker.mapHub.channels[ch]
		broker.mapHub.RUnlock()
		return !exists
	}, 10*time.Second, 200*time.Millisecond, "channel should be removed after MetaTTL")

	// After removal, ReadState creates a new channel with a new epoch.
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Empty(t, stateRes.Publications)
	require.NotEmpty(t, stateRes.Position.Epoch)
}

// TestMemoryMapBroker_IdempotencyCacheExpiration tests that idempotency cache entries expire.
func TestMemoryMapBroker_IdempotencyCacheExpiration(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_idempotency_expire"

	// Publish with very short idempotency TTL
	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:                []byte("v1"),
		IdempotencyKey:      "ik1",
		IdempotentResultTTL: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// Immediately → suppressed
	res, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:                []byte("v2"),
		IdempotencyKey:      "ik1",
		IdempotentResultTTL: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)

	// Wait for cache to expire (1s timer + margin)
	require.Eventually(t, func() bool {
		res, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
			Data:                []byte("v3"),
			IdempotencyKey:      "ik1",
			IdempotentResultTTL: 50 * time.Millisecond,
		})
		if err != nil {
			return false
		}
		return !res.Suppressed
	}, 5*time.Second, 200*time.Millisecond, "idempotency cache should expire")
}

// TestMemoryMapBroker_ReadStreamEpochMismatch tests epoch mismatch in ReadStream.
func TestMemoryMapBroker_ReadStreamEpochMismatch(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_stream_epoch_mismatch"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// ReadStream with wrong epoch → ErrorUnrecoverablePosition
	_, err = broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: 0, Epoch: "wrong-epoch"},
			Limit: -1,
		},
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)
}

// TestMemoryMapBroker_ReadStreamReverse tests reverse stream reads.
func TestMemoryMapBroker_ReadStreamReverse(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_stream_reverse"

	for i := 1; i <= 5; i++ {
		_, err := broker.Publish(ctx, ch, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("v%d", i)),
		})
		require.NoError(t, err)
	}

	// Read stream in reverse (no Since)
	streamRes, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 3, Reverse: true},
	})
	require.NoError(t, err)
	require.Len(t, streamRes.Publications, 3)
	// Reverse: latest first → offset 5, 4, 3
	require.Equal(t, uint64(5), streamRes.Publications[0].Offset)
	require.Equal(t, uint64(4), streamRes.Publications[1].Offset)
	require.Equal(t, uint64(3), streamRes.Publications[2].Offset)
}

// TestMemoryMapBroker_ReadStreamSinceMatchingOffset tests Since with current offset.
func TestMemoryMapBroker_ReadStreamSinceMatchingOffset(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_stream_since_matching"

	res, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Since matches current offset → no publications, just position
	streamRes, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &StreamPosition{Offset: res.Position.Offset, Epoch: res.Position.Epoch},
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Empty(t, streamRes.Publications)
	require.Equal(t, res.Position.Offset, streamRes.Position.Offset)
}

// TestMemoryMapBroker_ReadStreamSinceReverse tests Since with reverse read.
func TestMemoryMapBroker_ReadStreamSinceReverse(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_stream_since_reverse"

	for i := 1; i <= 5; i++ {
		_, err := broker.Publish(ctx, ch, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data: []byte(fmt.Sprintf("v%d", i)),
		})
		require.NoError(t, err)
	}

	// Get stream position
	streamRes, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 0},
	})
	require.NoError(t, err)

	// Read reverse from offset 4 (should return items before offset 4)
	streamRes, err = broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{
			Since:   &StreamPosition{Offset: 4, Epoch: streamRes.Position.Epoch},
			Limit:   -1,
			Reverse: true,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, streamRes.Publications)
	// Reverse from 4: offset 3, 2, 1
	for _, pub := range streamRes.Publications {
		require.Less(t, pub.Offset, uint64(4))
	}
}

// TestMemoryMapBroker_ReadStreamNoStream tests ReadStream on a channel without a stream.
func TestMemoryMapBroker_ReadStreamNoStream(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()

	// ReadStream on non-existent channel → creates stream, returns empty
	streamRes, err := broker.ReadStream(ctx, "nonexistent", MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Empty(t, streamRes.Publications)
	require.NotEmpty(t, streamRes.Position.Epoch)
}

// TestMemoryMapBroker_RemoveFromNonExistentChannel tests removing from non-existent channel.
func TestMemoryMapBroker_RemoveFromNonExistentChannel(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()

	// Remove from channel that was never created → key_not_found, empty position
	res, err := broker.Remove(ctx, "nonexistent_ch", "key1", MapRemoveOptions{})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, res.SuppressReason)
	require.Equal(t, StreamPosition{}, res.Position)
}

// TestMemoryMapBroker_RemoveKeyNotFoundWithStream tests that removing a non-existent key
// from an existing channel returns the stream position.
func TestMemoryMapBroker_RemoveKeyNotFoundWithStream(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_remove_key_not_found_stream"

	// Create channel with a key
	pubRes, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Remove non-existent key from existing channel → key_not_found with stream position
	res, err := broker.Remove(ctx, ch, "nonexistent", MapRemoveOptions{})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, res.SuppressReason)
	require.Equal(t, pubRes.Position.Offset, res.Position.Offset)
	require.Equal(t, pubRes.Position.Epoch, res.Position.Epoch)
}

// TestMemoryMapBroker_EphemeralPublishAndExpire tests ephemeral mode publish and auto-expiration.
func TestMemoryMapBroker_EphemeralPublishAndExpire(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:   MapModeEphemeral,
			KeyTTL: 50 * time.Millisecond,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_ephemeral"

	// Publish in ephemeral mode (no stream)
	res, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)
	require.False(t, res.Suppressed)
	require.Equal(t, uint64(0), res.Position.Offset) // no stream

	// Key should exist initially
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)

	// Wait for key to expire
	require.Eventually(t, func() bool {
		stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		if err != nil {
			return false
		}
		return len(stateRes.Publications) == 0
	}, 5*time.Second, 100*time.Millisecond, "ephemeral key should expire")
}

// TestMemoryMapBroker_ReadStateEpochMismatch tests epoch mismatch in ReadState with Revision.
func TestMemoryMapBroker_ReadStateEpochMismatch(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_state_epoch_mismatch"

	// Publish to create channel
	res, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// ReadState with matching epoch → OK
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{
		Limit:    100,
		Revision: &StreamPosition{Offset: res.Position.Offset, Epoch: res.Position.Epoch},
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)

	// ReadState with wrong epoch → ErrorUnrecoverablePosition
	_, err = broker.ReadState(ctx, ch, MapReadStateOptions{
		Limit:    100,
		Revision: &StreamPosition{Offset: res.Position.Offset, Epoch: "wrong-epoch"},
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)
}

// TestMemoryMapBroker_ReadStateKeyFilter tests single key lookup in ReadState.
func TestMemoryMapBroker_ReadStateKeyFilter(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_state_key_filter"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)
	_, err = broker.Publish(ctx, ch, "key2", MapPublishOptions{Data: []byte("v2")})
	require.NoError(t, err)

	// Filter by specific key
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{
		Key:   "key1",
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)
	require.Equal(t, "key1", stateRes.Publications[0].Key)

	// Filter by non-existent key
	stateRes, err = broker.ReadState(ctx, ch, MapReadStateOptions{
		Key:   "nonexistent",
		Limit: 100,
	})
	require.NoError(t, err)
	require.Empty(t, stateRes.Publications)
}

// TestMemoryMapBroker_VersionEpoch tests that version epoch scoping works correctly.
func TestMemoryMapBroker_VersionEpoch(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_version_epoch"

	// Publish version 5 with epoch "a"
	res1, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data: []byte("v5_a"), Version: 5, VersionEpoch: "a",
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Publish version 3 with same epoch → suppressed (lower version)
	res2, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data: []byte("v3_a"), Version: 3, VersionEpoch: "a",
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed)
	require.Equal(t, SuppressReasonVersion, res2.SuppressReason)

	// Publish version 3 with different epoch → accepted (different epoch resets)
	res3, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{
		Data: []byte("v3_b"), Version: 3, VersionEpoch: "b",
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed)
}

// TestMemoryMapBroker_EventHandler tests that event handler receives publications.
func TestMemoryMapBroker_EventHandler(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}

	var mu sync.Mutex
	var received []*Publication
	handler := &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, useDelta bool, prevPub *Publication) error {
			mu.Lock()
			received = append(received, pub)
			mu.Unlock()
			return nil
		},
	}

	e, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	err = e.RegisterEventHandler(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })

	ctx := context.Background()
	ch := "test_event_handler"

	// Publish
	_, err = e.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Remove
	_, err = e.Remove(ctx, ch, "key1", MapRemoveOptions{})
	require.NoError(t, err)

	mu.Lock()
	require.Len(t, received, 2)
	require.Equal(t, []byte("v1"), received[0].Data)
	require.True(t, received[1].Removed)
	mu.Unlock()
}

// TestMemoryMapBroker_EphemeralNoStream tests that ephemeral mode has no stream.
func TestMemoryMapBroker_EphemeralNoStream(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:   MapModeEphemeral,
			KeyTTL: 30 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_ephemeral_no_stream"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Stream should be empty (ephemeral mode has no stream)
	streamRes, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Empty(t, streamRes.Publications)

	// Remove in ephemeral mode
	res, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{})
	require.NoError(t, err)
	require.False(t, res.Suppressed)
}

// TestMemoryMapBroker_PersistentNeverExpires tests that persistent mode keys don't have TTL.
func TestMemoryMapBroker_PersistentNeverExpires(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModePersistent,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_persistent"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Verify key still exists (persistent = no KeyTTL)
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)

	// Use require.Never to verify it never disappears within a short period
	require.Never(t, func() bool {
		stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
		if err != nil {
			return true
		}
		return len(stateRes.Publications) == 0
	}, 500*time.Millisecond, 100*time.Millisecond, "persistent key should never expire")
}

// TestMemoryMapBroker_ParseChKey tests the parseChKey helper.
func TestMemoryMapBroker_ParseChKey(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestMemoryMapBroker(t, node)
	h := broker.mapHub

	// Normal case
	ch, key := h.parseChKey("channel\x00key")
	require.Equal(t, "channel", ch)
	require.Equal(t, "key", key)

	// No separator → empty strings
	ch, key = h.parseChKey("noseparator")
	require.Equal(t, "", ch)
	require.Equal(t, "", key)

	// Separator at start
	ch, key = h.parseChKey("\x00key")
	require.Equal(t, "", ch)
	require.Equal(t, "key", key)

	// Separator at end
	ch, key = h.parseChKey("channel\x00")
	require.Equal(t, "channel", ch)
	require.Equal(t, "", key)

	// Roundtrip with makeChKey
	chKey := h.makeChKey("mychannel", "mykey")
	ch, key = h.parseChKey(chKey)
	require.Equal(t, "mychannel", ch)
	require.Equal(t, "mykey", key)
}

// TestMemoryMapBroker_RemoveEphemeralNoStream tests Remove in ephemeral mode
// where the channel has no stream but does have state.
func TestMemoryMapBroker_RemoveEphemeralNoStream(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:   MapModeEphemeral,
			KeyTTL: 30 * time.Second,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_remove_ephemeral"

	_, err := broker.Publish(ctx, ch, "key1", MapPublishOptions{Data: []byte("v1")})
	require.NoError(t, err)

	// Remove in ephemeral mode (no stream, just state)
	res, err := broker.Remove(ctx, ch, "key1", MapRemoveOptions{})
	require.NoError(t, err)
	require.False(t, res.Suppressed)

	// Verify key is gone
	stateRes, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Empty(t, stateRes.Publications)
}

// TestMemoryMapBroker_SubscribeUnsubscribeNoop tests that Subscribe and Unsubscribe are no-ops.
func TestMemoryMapBroker_SubscribeUnsubscribeNoop(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestMemoryMapBroker(t, node)

	require.NoError(t, broker.Subscribe("ch1"))
	require.NoError(t, broker.Unsubscribe("ch1"))
}

// TestMemoryMapBroker_Close tests that Close stops background goroutines.
func TestMemoryMapBroker_Close(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestMemoryMapBroker(t, node)

	err := broker.Close(context.Background())
	require.NoError(t, err)

	// Double close should be safe
	err = broker.Close(context.Background())
	require.NoError(t, err)
}

// TestMemoryMapBroker_RefreshTTLOnSuppress_RefreshesMetaTTL verifies that a
// suppressed if_new+refresh_ttl_on_suppress publish (the periodic presence
// keepalive path) extends MetaTTL too. Without this, an idle channel with
// active keepalives still gets garbage-collected by removeChannels when
// MetaTTL elapses, forcing an epoch reset on the next publish.
func TestMemoryMapBroker_RefreshTTLOnSuppress_RefreshesMetaTTL(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  300 * time.Millisecond,
			KeyTTL:     300 * time.Millisecond,
			MetaTTL:    300 * time.Millisecond,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_keepalive_meta_ttl"

	// Initial publish creates the channel and records its first epoch.
	res, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
		Data:    []byte("v1"),
		KeyMode: KeyModeIfNew,
	})
	require.NoError(t, err)
	firstEpoch := res.Position.Epoch
	require.NotEmpty(t, firstEpoch)

	// Issue a suppressed keepalive every 100ms for 600ms — comfortably past
	// the 300ms MetaTTL. With the fix the channel survives because each
	// keepalive extends meta's removeAt; without it, removeChannels deletes
	// the channel and the next publish creates a new epoch.
	for i := 0; i < 6; i++ {
		time.Sleep(100 * time.Millisecond)
		res, err = broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:                 []byte("v1"),
			KeyMode:              KeyModeIfNew,
			RefreshTTLOnSuppress: true,
		})
		require.NoError(t, err)
		require.True(t, res.Suppressed, "iteration %d: expected key_exists suppression", i)
	}

	// Channel must still exist with the original epoch.
	broker.mapHub.RLock()
	_, exists := broker.mapHub.channels[ch]
	broker.mapHub.RUnlock()
	require.True(t, exists, "channel must survive while keepalives run")

	// A real publish (non-suppressed) must use the same epoch — confirms no
	// reset happened during the keepalive loop.
	res, err = broker.Publish(ctx, ch, "k2", MapPublishOptions{Data: []byte("v2")})
	require.NoError(t, err)
	require.Equal(t, firstEpoch, res.Position.Epoch,
		"epoch must not flip while keepalives are bumping MetaTTL")
}

// TestMemoryMapBroker_NoRefreshTTL_LetsMetaExpire is the negative case:
// without RefreshTTLOnSuppress, plain suppressed publishes do NOT extend
// MetaTTL. Guards against the fix accidentally bumping meta on every
// suppressed publish (e.g. version-conflict, idempotency hits).
func TestMemoryMapBroker_NoRefreshTTL_LetsMetaExpire(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  100 * time.Millisecond,
			KeyTTL:     100 * time.Millisecond,
			MetaTTL:    100 * time.Millisecond,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()
	ch := "test_no_keepalive_meta_expires"

	_, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
		Data:    []byte("v1"),
		KeyMode: KeyModeIfNew,
	})
	require.NoError(t, err)

	// Without RefreshTTLOnSuppress these calls don't refresh anything.
	for i := 0; i < 3; i++ {
		time.Sleep(50 * time.Millisecond)
		_, _ = broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v1"),
			KeyMode: KeyModeIfNew,
		})
	}

	// Channel should eventually be removed after MetaTTL elapses (the cleanup
	// timer runs on 1s ticks, so allow generous slack).
	require.Eventually(t, func() bool {
		broker.mapHub.RLock()
		_, exists := broker.mapHub.channels[ch]
		broker.mapHub.RUnlock()
		return !exists
	}, 5*time.Second, 100*time.Millisecond,
		"channel must be removed after MetaTTL when keepalives don't refresh it")
}

// newMapBrokerNoOptions builds a node + broker pair where the channel options
// resolver returns an invalid MapChannelOptions (Mode unset) for any channel.
// This forces ResolveAndValidateMapChannelOptions to return an error so we
// can hit the error-return branches in Publish/Remove/ReadState.
func newMapBrokerNoOptions(t *testing.T) (*Node, *MemoryMapBroker) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(channel string) MapChannelOptions {
				// Mode=0 is invalid → resolver returns an error.
				return MapChannelOptions{}
			},
		},
	})
	require.NoError(t, err)
	broker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	require.NoError(t, broker.RegisterEventHandler(nil))
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	return node, broker
}

// TestMapBroker_Publish_InvalidOptions covers the error path from
// ResolveAndValidateMapChannelOptions in Publish.
func TestMapBroker_Publish_InvalidOptions(t *testing.T) {
	_, broker := newMapBrokerNoOptions(t)
	_, err := broker.Publish(context.Background(), "ch", "k", MapPublishOptions{Data: []byte(`{}`)})
	require.Error(t, err)
}

// TestMapBroker_Remove_InvalidOptions covers the error path in Remove.
func TestMapBroker_Remove_InvalidOptions(t *testing.T) {
	_, broker := newMapBrokerNoOptions(t)
	_, err := broker.Remove(context.Background(), "ch", "k", MapRemoveOptions{})
	require.Error(t, err)
}

// TestMapBroker_ReadState_InvalidOptions covers the error path in ReadState.
func TestMapBroker_ReadState_InvalidOptions(t *testing.T) {
	_, broker := newMapBrokerNoOptions(t)
	_, err := broker.ReadState(context.Background(), "ch", MapReadStateOptions{Limit: 10})
	require.Error(t, err)
}

// TestMapBroker_ReadState_LimitZero covers the Limit==0 branch in mapHub.getState
// (returns just stream position, no entries).
func TestMapBroker_ReadState_LimitZero(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{Mode: MapModeRecoverable, StreamSize: 100, StreamTTL: 5 * time.Minute, KeyTTL: 5 * time.Minute}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	_, err := broker.Publish(ctx, "ch", "k1", MapPublishOptions{Data: []byte(`{"v":1}`)})
	require.NoError(t, err)

	res, err := broker.ReadState(ctx, "ch", MapReadStateOptions{Limit: 0})
	require.NoError(t, err)
	require.NotEmpty(t, res.Position.Epoch)
	require.Empty(t, res.Publications)
}

// TestMapBroker_Clear_NonExistentChannel covers the early-return branch in
// mapHub.clear when the channel isn't in the map.
func TestMapBroker_Clear_NonExistentChannel(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{Mode: MapModeRecoverable, StreamSize: 100, StreamTTL: 5 * time.Minute, KeyTTL: 5 * time.Minute}
	}
	broker := newTestMemoryMapBroker(t, node)

	// Clear before any publish — channel doesn't exist in the hub.
	broker.mapHub.clear("never-published")
}

// TestMapBroker_KeyModeIfNew_RefreshTTL_TimerSetters covers the
// nextKeyExpireCheck and nextRemoveCheck update branches inside the
// IfNew + RefreshTTLOnSuppress code path.
func TestMapBroker_KeyModeIfNew_RefreshTTL_TimerSetters(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  10 * time.Minute,
			KeyTTL:     10 * time.Minute,
			MetaTTL:    20 * time.Minute,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	ctx := context.Background()
	_, err := broker.Publish(ctx, "ch", "k", MapPublishOptions{
		Data:    []byte(`{"v":1}`),
		KeyMode: KeyModeIfNew,
	})
	require.NoError(t, err)

	// Force next-check timestamps to be in the far future so the suppress
	// path is the one that ratchets them down to the new earlier values.
	broker.mapHub.Lock()
	broker.mapHub.nextKeyExpireCheck = time.Now().Add(time.Hour).UnixMilli()
	broker.mapHub.nextRemoveCheck = time.Now().Add(time.Hour).UnixMilli()
	broker.mapHub.Unlock()

	// Second publish with IfNew → key already exists → suppressed. With
	// RefreshTTLOnSuppress, the new earlier expireAt should ratchet down
	// nextKeyExpireCheck and nextRemoveCheck.
	res, err := broker.Publish(ctx, "ch", "k", MapPublishOptions{
		Data:                 []byte(`{"v":2}`),
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonKeyExists, res.SuppressReason)
}

// TestMapBroker_Remove_RefreshesRemoveCheck covers the nextRemoveCheck update
// branch in mapHub.remove (when MetaTTL is set and the new removeAt earlier
// than current nextRemoveCheck).
func TestMapBroker_Remove_RefreshesRemoveCheck(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  10 * time.Minute,
			KeyTTL:     10 * time.Minute,
			MetaTTL:    20 * time.Minute,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()

	_, err := broker.Publish(ctx, "ch", "k", MapPublishOptions{Data: []byte(`{"v":1}`)})
	require.NoError(t, err)

	// Force nextRemoveCheck way out so the Remove path ratchets it down.
	broker.mapHub.Lock()
	broker.mapHub.nextRemoveCheck = time.Now().Add(2 * time.Hour).UnixMilli()
	broker.mapHub.Unlock()

	_, err = broker.Remove(ctx, "ch", "k", MapRemoveOptions{})
	require.NoError(t, err)

	broker.mapHub.RLock()
	updated := broker.mapHub.nextRemoveCheck
	broker.mapHub.RUnlock()
	require.Less(t, updated, time.Now().Add(time.Hour).UnixMilli())
}

// TestMapBroker_ExpireKeysIteration_Refreshed covers the path in
// expireKeysIteration where the popped queue entry is stale (storedExpireAt
// has been pushed forward by a refresh).
func TestMapBroker_ExpireKeysIteration_Refreshed(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  10 * time.Minute,
			KeyTTL:     200 * time.Millisecond,
		}
	}
	broker := newTestMemoryMapBroker(t, node)
	ctx := context.Background()

	_, err := broker.Publish(ctx, "ch", "k", MapPublishOptions{Data: []byte(`{"v":1}`)})
	require.NoError(t, err)

	// Refresh the key — pushes a new heap entry with later expireAt so the
	// older popped entry will be stale.
	time.Sleep(50 * time.Millisecond)
	_, err = broker.Publish(ctx, "ch", "k", MapPublishOptions{
		Data:                 []byte(`{"v":2}`),
		KeyMode:              KeyModeIfNew,
		RefreshTTLOnSuppress: true,
	})
	require.NoError(t, err)

	// Force the iteration to run by setting nextKeyExpireCheck into the past.
	broker.mapHub.Lock()
	broker.mapHub.nextKeyExpireCheck = 1
	broker.mapHub.Unlock()

	var nextCheck int64
	broker.mapHub.expireKeysIteration(&nextCheck)
}

// TestMapBroker_GetStream_NoChannel covers the path in getStream where the
// channel doesn't exist and we must allocate a stream-position holder so the
// epoch is stable.
func TestMapBroker_GetStream_NoChannel(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  5 * time.Minute,
			KeyTTL:     5 * time.Minute,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	// Channel does not exist yet — should still return an empty stream result
	// with a freshly-minted epoch.
	res, err := broker.ReadStream(context.Background(), "ch-untouched", MapReadStreamOptions{
		Filter: StreamFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Position.Epoch)
	require.Empty(t, res.Publications)
}

// TestMapBroker_GetStream_ConcurrentChannelCreation exercises the race-recovery
// path in getStream where the channel is created by another goroutine between
// RUnlock and Lock.
func TestMapBroker_GetStream_ConcurrentChannelCreation(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  5 * time.Minute,
			KeyTTL:     5 * time.Minute,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	// Hammer ReadStream + Publish from multiple goroutines on a fresh channel
	// to maximize the chance of hitting the race-recovery branch.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			ch := "race"
			_, _ = broker.ReadStream(context.Background(), ch, MapReadStreamOptions{Filter: StreamFilter{Limit: -1}})
		}(i)
		go func(i int) {
			defer wg.Done()
			ch := "race"
			_, _ = broker.Publish(context.Background(), ch, "k", MapPublishOptions{Data: []byte(`{}`)})
		}(i)
	}
	wg.Wait()
}

// TestMapBroker_ReadState_ConcurrentChannelCreation exercises the race-recovery
// path in getState where another goroutine creates the channel between unlock
// and re-lock.
func TestMapBroker_ReadState_ConcurrentChannelCreation(t *testing.T) {
	node, _ := New(Config{})
	node.config.Map.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Mode:       MapModeRecoverable,
			StreamSize: 100,
			StreamTTL:  5 * time.Minute,
			KeyTTL:     5 * time.Minute,
		}
	}
	broker := newTestMemoryMapBroker(t, node)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = broker.ReadState(context.Background(), "race-state", MapReadStateOptions{Limit: 100})
		}()
		go func() {
			defer wg.Done()
			_, _ = broker.Publish(context.Background(), "race-state", "k", MapPublishOptions{Data: []byte(`{}`)})
		}()
	}
	wg.Wait()
}
