//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getPostgresConnString(tb testing.TB) string {
	connString := os.Getenv("CENTRIFUGE_POSTGRES_URL")
	if connString == "" {
		tb.Skip("CENTRIFUGE_POSTGRES_URL not set, skipping PostgreSQL integration tests")
	}
	return connString
}

// newTestPostgresMapBroker creates a test broker with default outbox mode.
func newTestPostgresMapBroker(tb testing.TB, n *Node) *PostgresMapBroker {
	return newTestPostgresMapBrokerWithOutbox(tb, n)
}

// newTestPostgresMapBrokerWithOutbox creates a test broker with outbox mode (default).
func newTestPostgresMapBrokerWithOutbox(tb testing.TB, n *Node) *PostgresMapBroker {
	connString := getPostgresConnString(tb)

	e, err := NewPostgresMapBroker(n, PostgresMapBrokerConfig{
		ConnString: connString,
		Outbox: OutboxConfig{
			NumShards:    4, // Fewer shards for faster tests
			PollInterval: 10 * time.Millisecond,
			BatchSize:    100,
		},
	})
	require.NoError(tb, err)

	// Clean up tables before test
	ctx := context.Background()
	cleanupTestTables(ctx, e)

	err = e.RegisterEventHandler(nil)
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_ = e.Close(context.Background())
		_ = n.Shutdown(context.Background())
	})
	return e
}

// newTestPostgresMapBrokerWithWAL creates a test broker with WAL mode.
func newTestPostgresMapBrokerWithWAL(tb testing.TB, n *Node) *PostgresMapBroker {
	connString := getPostgresConnString(tb)

	e, err := NewPostgresMapBroker(n, PostgresMapBrokerConfig{
		ConnString: connString,
		WAL: WALConfig{
			Enabled:           true,
			NumShards:         16,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(tb, err)

	// Clean up tables before test
	ctx := context.Background()
	cleanupTestTables(ctx, e)

	tb.Cleanup(func() {
		_ = e.Close(context.Background())
		_ = n.Shutdown(context.Background())
	})
	return e
}

func cleanupTestTables(ctx context.Context, e *PostgresMapBroker) {
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'test_%'")
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'test_%'")
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'test_%'")
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_map_idempotency WHERE channel LIKE 'test_%'")
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_map_outbox WHERE channel LIKE 'test_%'")
}

// stateToMapPostgres converts []Publication to map for easier testing.
func stateToMapPostgres(pubs []*Publication) map[string][]byte {
	result := make(map[string][]byte, len(pubs))
	for _, pub := range pubs {
		result[pub.Key] = pub.Data
	}
	return result
}

// TestPostgresMapBroker_StatefulChannel tests stateful channel with keyed state and revisions.
func TestPostgresMapBroker_StatefulChannel(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_stateful"

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
	state := stateToMapPostgres(entries)
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

// TestPostgresMapBroker_StatefulChannelOrdered tests ordered stateful channel.
func TestPostgresMapBroker_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered"

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
	state := stateToMapPostgres(entries)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		require.Contains(t, state, key)
	}
}

// TestPostgresMapBroker_StateRevision tests that state values include revisions.
func TestPostgresMapBroker_StateRevision(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_revision"

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
	state := stateToMapPostgres(entries)
	require.Equal(t, []byte("data1"), state["key1"])
	require.Equal(t, []byte("data2"), state["key2"])

	// Verify per-entry offsets
	require.NotEmpty(t, streamPos.Epoch)
	for _, pub := range entries {
		require.Greater(t, pub.Offset, uint64(0))
	}
}

// TestPostgresMapBroker_StatePagination tests cursor-based state pagination.
func TestPostgresMapBroker_StatePagination(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_pagination"

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

	// Read state with limit
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

	// Continue reading until cursor is empty
	for cursor != "" {
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

// TestPostgresMapBroker_StreamRecovery tests stream recovery.
func TestPostgresMapBroker_StreamRecovery(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_recovery"

	// Publish some updates
	for i := 1; i <= 5; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Get position after first 2 messages
	sincePos := StreamPosition{Offset: 2}

	// Read stream since position 2
	pubs, streamPos, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &sincePos,
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), streamPos.Offset)
	require.Len(t, pubs, 3) // Should get messages 3, 4, 5
}

// TestPostgresMapBroker_Idempotency tests idempotent publishing.
func TestPostgresMapBroker_Idempotency(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_idempotency"

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
	require.True(t, res2.Suppressed)                                 // Suppressed due to idempotency
	require.Equal(t, SuppressReasonIdempotency, res2.SuppressReason) // The idempotency check returns the original offset
	require.Equal(t, res1.Position.Offset, res2.Position.Offset)     // Same offset

	// State should still have original data
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	state := stateToMapPostgres(entries)
	require.Len(t, state, 1)
	require.Equal(t, []byte("data1"), state["key1"])
}

// TestPostgresMapBroker_KeyMode tests KeyMode (IfNew, IfExists).
func TestPostgresMapBroker_KeyMode(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_keymode"

	// First publish with KeyModeIfNew should succeed
	res1, err := broker.Publish(ctx, channel, "slot1", MapPublishOptions{
		Data:       []byte("player1"),
		KeyMode:    KeyModeIfNew,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)
	require.Equal(t, uint64(1), res1.Position.Offset)

	// Second publish with KeyModeIfNew should be suppressed
	res2, err := broker.Publish(ctx, channel, "slot1", MapPublishOptions{
		Data:       []byte("player2"),
		KeyMode:    KeyModeIfNew,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed)
	require.Equal(t, SuppressReasonKeyExists, res2.SuppressReason)

	// Verify state still has original data
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:    100,
		StateTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("player1"), entries[0].Data)

	// KeyModeIfExists should be suppressed for non-existent key
	res3, err := broker.Publish(ctx, channel, "nonexistent", MapPublishOptions{
		Data:       []byte("data"),
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res3.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, res3.SuppressReason)

	// KeyModeIfExists should succeed for existing key
	res4, err := broker.Publish(ctx, channel, "slot1", MapPublishOptions{
		Data:       []byte("updated"),
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res4.Suppressed)
}

// TestPostgresMapBroker_CAS tests Compare-And-Swap operations.
func TestPostgresMapBroker_CAS(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_cas"

	// Initial publish
	res1, err := broker.Publish(ctx, channel, "item1", MapPublishOptions{
		Data:       []byte(`{"stock": 10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Read current state
	entries, pos, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "item1",
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// CAS update with correct position
	expectedPos := StreamPosition{Offset: entries[0].Offset, Epoch: pos.Epoch}
	res2, err := broker.Publish(ctx, channel, "item1", MapPublishOptions{
		Data:             []byte(`{"stock": 9}`),
		ExpectedPosition: &expectedPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)

	// CAS update with stale position should fail
	res3, err := broker.Publish(ctx, channel, "item1", MapPublishOptions{
		Data:             []byte(`{"stock": 8}`),
		ExpectedPosition: &expectedPos, // Using old position
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res3.Suppressed)
	require.Equal(t, SuppressReasonPositionMismatch, res3.SuppressReason)
	require.NotNil(t, res3.CurrentPublication)
	require.Equal(t, []byte(`{"stock": 9}`), res3.CurrentPublication.Data)
}

// TestPostgresMapBroker_KeyTTL tests key TTL (this is a slower test).
func TestPostgresMapBroker_KeyTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TTL test in short mode")
	}

	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_key_ttl"

	// Publish with short TTL
	_, err := broker.Publish(ctx, channel, "ephemeral", MapPublishOptions{
		Data:       []byte("temporary"),
		KeyTTL:     2 * time.Second,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Verify key exists
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "ephemeral",
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Trigger TTL check
	broker.expireKeys(ctx)

	// Key should be gone
	entries, _, _, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "ephemeral",
	})
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestPostgresMapBroker_Version tests version-based ordering.
func TestPostgresMapBroker_Version(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_version"

	// Publish with version 2
	res1, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data_v2"),
		Version:    2,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Try to publish older version (should be suppressed)
	res2, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data_v1"),
		Version:    1,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed)
	require.Equal(t, SuppressReasonVersion, res2.SuppressReason)

	// Publish newer version
	res3, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data_v3"),
		Version:    3,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed)

	// Verify state has v3 data
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "key1",
	})
	require.NoError(t, err)
	require.Equal(t, []byte("data_v3"), entries[0].Data)
}

// TestPostgresMapBroker_Remove tests removing keys.
func TestPostgresMapBroker_Remove(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_unpublish"

	// Publish some keys
	_, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	_, err = broker.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Verify state has 2 keys
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
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
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "key2", entries[0].Key)

	// Remove non-existent key should be suppressed
	res, err = broker.Remove(ctx, channel, "nonexistent", MapRemoveOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, res.SuppressReason)

	// Verify stream has removal event
	pubs, _, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3) // key1, key2, remove(key1)
	require.True(t, pubs[2].Removed)
	require.Equal(t, "key1", pubs[2].Key)
}

// TestPostgresMapBroker_Stats tests state statistics.
func TestPostgresMapBroker_Stats(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_stats"

	// Initially empty
	stats, err := broker.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 0, stats.NumKeys)

	// Publish some keys
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Should have 5 keys
	stats, err = broker.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 5, stats.NumKeys)
}

// TestPostgresMapBroker_EpochMismatch tests epoch validation.
func TestPostgresMapBroker_EpochMismatch(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_epoch_mismatch"

	// Client tries to read with old epoch (channel doesn't exist yet)
	_, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Revision: &StreamPosition{
			Epoch:  "old_epoch",
			Offset: 100,
		},
		Limit: 100,
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)

	// Create channel
	_, err = broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read actual epoch
	_, pos, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pos.Epoch)

	// Read with wrong epoch should fail
	_, _, _, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Revision: &StreamPosition{
			Epoch:  "wrong_epoch",
			Offset: 1,
		},
		Limit: 100,
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)

	// Read with correct epoch should succeed
	entries, _, _, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Revision: &pos,
		Limit:    100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// TestPostgresMapBroker_WALReader tests logical replication WAL reader (single-node, local delivery).
func TestPostgresMapBroker_WALReader(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())

	// Create PostgreSQL map broker with WAL mode (single-node, local delivery)
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		WAL: WALConfig{
			Enabled:           true,
			NumShards:         16,
			ShardIDs:          nil, // nil means try all shards
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)

	// Clean up tables before test
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'test_wal_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'test_wal_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'test_wal_%'")

	// Use unique channel name to avoid interference from other tests
	channel := fmt.Sprintf("test_wal_channel_%d", time.Now().UnixNano())

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	// Track publications received via broker
	var received []*Publication
	var receivedMu sync.Mutex
	receivedCh := make(chan struct{}, 10)

	// Register event handler to capture publications
	err = broker.RegisterEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			receivedMu.Unlock()
			select {
			case receivedCh <- struct{}{}:
			default:
			}
			return nil
		},
	})
	require.NoError(t, err)

	// Wait for WAL reader to claim at least one shard
	require.Eventually(t, func() bool {
		return len(broker.ClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond, "WAL reader should claim at least one shard")

	// Publish via another connection (simulating external publish)
	// This will go through the WAL reader -> broker -> event handler
	// p_skip_outbox = true since we're testing WAL mode
	directPool := broker.pool
	_, err = directPool.Exec(ctx, `
		SELECT * FROM cf_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, true, 16)
	`, channel, "key1", []byte("data1"))
	require.NoError(t, err)

	// Wait for publication to arrive via WAL reader
	select {
	case <-receivedCh:
		// Got it
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for publication via WAL reader")
	}

	// Verify we received the publication
	receivedMu.Lock()
	require.GreaterOrEqual(t, len(received), 1)
	found := false
	for _, pub := range received {
		if pub.Key == "key1" && string(pub.Data) == "data1" {
			found = true
			break
		}
	}
	receivedMu.Unlock()
	require.True(t, found, "should receive publication with key1 via WAL reader")

	// Test removal publication
	// p_skip_outbox = true since we're testing WAL mode
	_, err = directPool.Exec(ctx, `
		SELECT * FROM cf_map_remove($1, $2, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, true, 16)
	`, channel, "key1")
	require.NoError(t, err)

	// Wait for removal publication
	select {
	case <-receivedCh:
		// Got it
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for removal publication via WAL reader")
	}

	// Verify we received the removal
	receivedMu.Lock()
	foundRemoval := false
	for _, pub := range received {
		if pub.Key == "key1" && pub.Removed {
			foundRemoval = true
			break
		}
	}
	receivedMu.Unlock()
	require.True(t, foundRemoval, "should receive removal publication via WAL reader")
}

// TestPostgresMapBroker_WALReaderOrdering tests that WAL reader preserves publication order.
func TestPostgresMapBroker_WALReaderOrdering(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())

	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		WAL: WALConfig{
			Enabled:           true,
			NumShards:         16,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	_ = ctx

	// Use unique channel name per test run
	channel := fmt.Sprintf("test_wal_order_%d", time.Now().UnixNano())

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	var received []*Publication
	var receivedMu sync.Mutex
	doneCh := make(chan struct{})

	const numMessages = 10

	err = broker.RegisterEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			// Only count messages for our specific channel
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			if len(received) >= numMessages {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			receivedMu.Unlock()
			return nil
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(broker.ClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond)

	// Publish multiple messages in sequence (p_skip_outbox = true for WAL mode)
	for i := 0; i < numMessages; i++ {
		_, err = broker.pool.Exec(ctx, `
			SELECT * FROM cf_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, true, 16)
		`, channel, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("data%d", i)))
		require.NoError(t, err)
	}

	// Wait for all messages
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for all publications")
	}

	// Verify ordering by offset
	receivedMu.Lock()
	defer receivedMu.Unlock()

	require.Len(t, received, numMessages)

	// Offsets should be sequential (1, 2, 3, ...)
	for i, pub := range received {
		require.Equal(t, uint64(i+1), pub.Offset, "offset at index %d should be %d", i, i+1)
	}
}

// TestPostgresMapBroker_WALReaderMetadata tests that WAL reader preserves metadata (tags, score, client info).
func TestPostgresMapBroker_WALReaderMetadata(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())

	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		WAL: WALConfig{
			Enabled:           true,
			NumShards:         16,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	_ = ctx

	// Use unique channel name per test run
	channel := fmt.Sprintf("test_wal_meta_%d", time.Now().UnixNano())

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	var received *Publication
	var receivedMu sync.Mutex
	receivedCh := make(chan struct{}, 1)

	err = broker.RegisterEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			// Only count messages for our specific channel
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = pub
			receivedMu.Unlock()
			select {
			case receivedCh <- struct{}{}:
			default:
			}
			return nil
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(broker.ClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond)

	// Publish with tags and score via SQL (p_skip_outbox = true for WAL mode)
	// cf_map_publish params: channel, key, data, tags, client_id, user_id, conn_info, chan_info,
	//   subscribed_at, key_mode, key_ttl, stream_ttl, stream_size, meta_ttl, expected_offset, score, ...
	_, err = broker.pool.Exec(ctx, `
		SELECT * FROM cf_map_publish(
			$1, $2, $3,
			'{"env": "test", "type": "metadata"}'::jsonb,
			NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, '300 seconds'::interval, NULL, NULL,
			NULL, $4,
			NULL, NULL, NULL, NULL, NULL, NULL, false, true, 16
		)
	`, channel, "metadata_key", []byte("metadata_data"), int64(42))
	require.NoError(t, err)

	select {
	case <-receivedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for publication")
	}

	receivedMu.Lock()
	defer receivedMu.Unlock()

	require.NotNil(t, received)
	require.Equal(t, "metadata_key", received.Key)
	require.Equal(t, []byte("metadata_data"), received.Data)
	require.Equal(t, int64(42), received.Score)
	require.NotNil(t, received.Tags)
	require.Equal(t, "test", received.Tags["env"])
	require.Equal(t, "metadata", received.Tags["type"])
}

// TestPostgresMapBroker_ConcurrentPublishOrdering tests that concurrent publishes maintain per-channel ordering.
func TestPostgresMapBroker_ConcurrentPublishOrdering(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_concurrent_order"

	const numGoroutines = 5
	const publishesPerGoroutine = 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent publishes
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < publishesPerGoroutine; i++ {
				key := fmt.Sprintf("g%d_k%d", goroutineID, i)
				data := []byte(fmt.Sprintf("data_%d_%d", goroutineID, i))
				_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
					Data:       data,
					StreamSize: 1000,
					StreamTTL:  300 * time.Second,
				})
				require.NoError(t, err)
			}
		}(g)
	}

	wg.Wait()

	// Read stream and verify offsets are sequential with no gaps
	pubs, pos, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)

	expectedCount := numGoroutines * publishesPerGoroutine
	require.Len(t, pubs, expectedCount)
	require.Equal(t, uint64(expectedCount), pos.Offset)

	// Verify offsets are 1, 2, 3, ... with no gaps
	for i, pub := range pubs {
		require.Equal(t, uint64(i+1), pub.Offset, "offset at index %d should be %d, got %d", i, i+1, pub.Offset)
	}
}

// TestPostgresMapBroker_WALReaderWithBroker tests WAL reader with Redis broker for multi-node fan-out.
func TestPostgresMapBroker_WALReaderWithBroker(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	// Create node
	node, err := New(Config{})
	require.NoError(t, err)

	// Set up Redis broker
	redisShard, err := NewRedisShard(node, RedisShardConfig{
		Address: "127.0.0.1:6379",
	})
	require.NoError(t, err)

	broker, err := NewRedisBroker(node, RedisBrokerConfig{
		Shards: []*RedisShard{redisShard},
		Prefix: "test_wal_broker_" + fmt.Sprintf("%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)
	node.SetBroker(broker)

	// Use unique channel name per test run
	channel := fmt.Sprintf("test_wal_broker_%d", time.Now().UnixNano())

	// Track publications received via broker
	var received []*Publication
	var receivedMu sync.Mutex
	receivedCh := make(chan struct{}, 10)

	// Register event handler on broker directly (before node.Run which would override it)
	// This captures publications from broker
	err = broker.RegisterBrokerEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			receivedMu.Unlock()
			select {
			case receivedCh <- struct{}{}:
			default:
			}
			return nil
		},
	})
	require.NoError(t, err)

	// Subscribe to channel via broker (simulates what happens when a client subscribes)
	err = broker.Subscribe(channel)
	require.NoError(t, err)

	// Create PostgreSQL map broker with WAL mode and broker for multi-node delivery
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		Broker:     broker,
		WAL: WALConfig{
			Enabled:           true,
			NumShards:         16,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)

	// Clean up tables before test
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_stream WHERE channel LIKE 'test_wal_broker_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_state WHERE channel LIKE 'test_wal_broker_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_map_meta WHERE channel LIKE 'test_wal_broker_%'")

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	// Register nil handler on broker (not used with broker, but required)
	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)

	// Wait for WAL reader to claim at least one shard
	require.Eventually(t, func() bool {
		return len(broker.ClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond, "WAL reader should claim at least one shard")

	// Publish via SQL (simulating external publish, p_skip_outbox = true for WAL mode)
	_, err = broker.pool.Exec(ctx, `
		SELECT * FROM cf_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, $4, NULL, NULL, NULL, NULL, NULL, NULL, false, true, 16)
	`, channel, "test_key", []byte("test_data"), int64(100))
	require.NoError(t, err)

	// Wait for publication to arrive via broker
	select {
	case <-receivedCh:
		// Got it
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for publication via broker")
	}

	// Verify we received the publication with correct keyed fields
	receivedMu.Lock()
	require.GreaterOrEqual(t, len(received), 1)
	found := false
	for _, pub := range received {
		if pub.Key == "test_key" && string(pub.Data) == "test_data" && pub.Score == 100 && pub.Offset > 0 && pub.Epoch != "" {
			found = true
			break
		}
	}
	receivedMu.Unlock()
	require.True(t, found, "expected to receive publication with key=test_key, data=test_data, score=100, offset>0, epoch non-empty")
}

// ============================================================================
// Outbox Mode Tests
// ============================================================================

// TestPostgresMapBroker_OutboxOrdering tests that publications are delivered in channel_offset order.
func TestPostgresMapBroker_OutboxOrdering(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())

	// Create broker with outbox mode (default)
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		Outbox: OutboxConfig{
			NumShards:    4,
			PollInterval: 10 * time.Millisecond,
			BatchSize:    100,
		},
	})
	require.NoError(t, err)

	// Use unique channel name per test run
	channel := fmt.Sprintf("test_outbox_order_%d", time.Now().UnixNano())

	// Clean up tables
	cleanupTestTables(ctx, broker)

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	var received []*Publication
	var receivedMu sync.Mutex
	doneCh := make(chan struct{})

	const numMessages = 10

	err = broker.RegisterEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			if len(received) >= numMessages {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			receivedMu.Unlock()
			return nil
		},
	})
	require.NoError(t, err)

	// Wait for outbox worker to claim at least one shard
	require.Eventually(t, func() bool {
		return len(broker.OutboxClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond, "Outbox worker should claim at least one shard")

	// Publish multiple messages in sequence
	for i := 0; i < numMessages; i++ {
		_, err = broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:      []byte(fmt.Sprintf("data%d", i)),
			StreamTTL: 300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Wait for all messages
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for all publications")
	}

	// Verify ordering by offset
	receivedMu.Lock()
	defer receivedMu.Unlock()

	require.Len(t, received, numMessages)

	// Offsets should be sequential (1, 2, 3, ...)
	for i, pub := range received {
		require.Equal(t, uint64(i+1), pub.Offset, "offset at index %d should be %d", i, i+1)
	}
}

// TestPostgresMapBroker_OutboxDeliveryGuarantee tests that no messages are lost after worker restart.
func TestPostgresMapBroker_OutboxDeliveryGuarantee(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	// Use unique channel name per test run
	channel := fmt.Sprintf("test_outbox_guarantee_%d", time.Now().UnixNano())

	// First, publish some messages without starting workers
	node1, err := New(Config{})
	require.NoError(t, err)

	broker1, err := NewPostgresMapBroker(node1, PostgresMapBrokerConfig{
		ConnString: connString,
		Outbox: OutboxConfig{
			NumShards:    4,
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	cleanupTestTables(ctx, broker1)

	// Publish messages directly via SQL (bypassing workers)
	for i := 0; i < 5; i++ {
		_, err = broker1.pool.Exec(ctx, `
			SELECT * FROM cf_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, false, 4)
		`, channel, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("data%d", i)))
		require.NoError(t, err)
	}

	// Verify messages are in outbox
	var count int
	err = broker1.pool.QueryRow(ctx, "SELECT COUNT(*) FROM cf_map_outbox WHERE channel = $1", channel).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 5, count, "5 messages should be in outbox")

	_ = broker1.Close(ctx)
	_ = node1.Shutdown(ctx)

	// Now start a new broker with workers - they should pick up the pending messages
	node2, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node2.Run())

	broker2, err := NewPostgresMapBroker(node2, PostgresMapBrokerConfig{
		ConnString: connString,
		Outbox: OutboxConfig{
			NumShards:    4,
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = broker2.Close(ctx)
		_ = node2.Shutdown(ctx)
	})

	var received []*Publication
	var receivedMu sync.Mutex
	doneCh := make(chan struct{})

	err = broker2.RegisterEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			if len(received) >= 5 {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			receivedMu.Unlock()
			return nil
		},
	})
	require.NoError(t, err)

	// Wait for all messages to be delivered
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		receivedMu.Lock()
		t.Fatalf("timeout waiting for publications, received %d", len(received))
		receivedMu.Unlock()
	}

	// Verify all messages delivered
	receivedMu.Lock()
	require.Len(t, received, 5)
	receivedMu.Unlock()

	// Verify outbox is empty
	err = broker2.pool.QueryRow(ctx, "SELECT COUNT(*) FROM cf_map_outbox WHERE channel = $1", channel).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "outbox should be empty after processing")
}

// TestPostgresMapBroker_OutboxMarkProcessed tests mark-processed mode.
func TestPostgresMapBroker_OutboxMarkProcessed(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())

	// Create broker with mark-processed mode
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		Outbox: OutboxConfig{
			NumShards:     4,
			PollInterval:  10 * time.Millisecond,
			BatchSize:     100,
			MarkProcessed: true,
		},
	})
	require.NoError(t, err)

	channel := fmt.Sprintf("test_outbox_mark_%d", time.Now().UnixNano())
	cleanupTestTables(ctx, broker)

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	var received []*Publication
	var receivedMu sync.Mutex
	doneCh := make(chan struct{})

	err = broker.RegisterEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			if len(received) >= 3 {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			receivedMu.Unlock()
			return nil
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(broker.OutboxClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond)

	// Publish messages
	for i := 0; i < 3; i++ {
		_, err = broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:      []byte(fmt.Sprintf("data%d", i)),
			StreamTTL: 300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Wait for delivery
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for publications")
	}

	// In mark-processed mode, rows should still exist but have processed_at set
	var totalCount, processedCount int
	err = broker.pool.QueryRow(ctx, "SELECT COUNT(*) FROM cf_map_outbox WHERE channel = $1", channel).Scan(&totalCount)
	require.NoError(t, err)
	err = broker.pool.QueryRow(ctx, "SELECT COUNT(*) FROM cf_map_outbox WHERE channel = $1 AND processed_at IS NOT NULL", channel).Scan(&processedCount)
	require.NoError(t, err)

	require.Equal(t, 3, totalCount, "rows should still exist")
	require.Equal(t, 3, processedCount, "all rows should be marked as processed")
}

// TestPostgresMapBroker_OutboxConcurrentPublish tests concurrent publishes maintain ordering.
func TestPostgresMapBroker_OutboxConcurrentPublish(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())

	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		Outbox: OutboxConfig{
			NumShards:    4,
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	channel := fmt.Sprintf("test_outbox_concurrent_%d", time.Now().UnixNano())
	cleanupTestTables(ctx, broker)

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	const numGoroutines = 5
	const publishesPerGoroutine = 10
	totalPublishes := numGoroutines * publishesPerGoroutine

	var received []*Publication
	var receivedMu sync.Mutex
	doneCh := make(chan struct{})

	err = broker.RegisterEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			if len(received) >= totalPublishes {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			receivedMu.Unlock()
			return nil
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(broker.OutboxClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond)

	// Concurrent publishes
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < publishesPerGoroutine; i++ {
				key := fmt.Sprintf("g%d_k%d", goroutineID, i)
				data := []byte(fmt.Sprintf("data_%d_%d", goroutineID, i))
				_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
					Data:      data,
					StreamTTL: 300 * time.Second,
				})
				require.NoError(t, err)
			}
		}(g)
	}

	wg.Wait()

	// Wait for all messages
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		receivedMu.Lock()
		t.Fatalf("timeout waiting for publications, received %d/%d", len(received), totalPublishes)
		receivedMu.Unlock()
	}

	receivedMu.Lock()
	defer receivedMu.Unlock()

	require.Len(t, received, totalPublishes)

	// Collect offsets and verify no gaps
	offsets := make(map[uint64]bool)
	for _, pub := range received {
		offsets[pub.Offset] = true
	}

	for i := 1; i <= totalPublishes; i++ {
		require.True(t, offsets[uint64(i)], "offset %d should exist", i)
	}
}

// TestPostgresMapBroker_OutboxWithBroker tests multi-node delivery via broker.
func TestPostgresMapBroker_OutboxWithBroker(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	// Create node
	node, err := New(Config{})
	require.NoError(t, err)

	// Set up Redis broker
	redisShard, err := NewRedisShard(node, RedisShardConfig{
		Address: "127.0.0.1:6379",
	})
	require.NoError(t, err)

	broker, err := NewRedisBroker(node, RedisBrokerConfig{
		Shards: []*RedisShard{redisShard},
		Prefix: "test_outbox_broker_" + fmt.Sprintf("%d", time.Now().UnixNano()),
	})
	require.NoError(t, err)
	node.SetBroker(broker)

	channel := fmt.Sprintf("test_outbox_broker_%d", time.Now().UnixNano())

	var received []*Publication
	var receivedMu sync.Mutex
	receivedCh := make(chan struct{}, 10)

	// Register event handler on broker
	err = broker.RegisterBrokerEventHandler(&testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
			if ch != channel {
				return nil
			}
			receivedMu.Lock()
			received = append(received, pub)
			receivedMu.Unlock()
			select {
			case receivedCh <- struct{}{}:
			default:
			}
			return nil
		},
	})
	require.NoError(t, err)

	// Subscribe to channel via broker
	err = broker.Subscribe(channel)
	require.NoError(t, err)

	// Create PostgreSQL map broker with broker for multi-node delivery
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		Broker:     broker,
		Outbox: OutboxConfig{
			NumShards:    4,
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	cleanupTestTables(ctx, broker)

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(broker.OutboxClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond, "Outbox worker should claim at least one shard")

	// Publish via broker
	_, err = broker.Publish(ctx, channel, "test_key", MapPublishOptions{
		Data:      []byte("test_data"),
		Score:     100,
		StreamTTL: 300 * time.Second,
	})
	require.NoError(t, err)

	// Wait for publication to arrive via broker
	select {
	case <-receivedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for publication via broker")
	}

	// Verify we received the publication with correct keyed fields
	receivedMu.Lock()
	require.GreaterOrEqual(t, len(received), 1)
	found := false
	for _, pub := range received {
		if pub.Key == "test_key" && string(pub.Data) == "test_data" && pub.Score == 100 && pub.Offset > 0 && pub.Epoch != "" {
			found = true
			break
		}
	}
	receivedMu.Unlock()
	require.True(t, found, "expected to receive publication with key=test_key, data=test_data, score=100")
}
