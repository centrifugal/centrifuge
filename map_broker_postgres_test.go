//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
		NumShards:  4, // Fewer shards for faster tests
		BinaryData: true,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
			BatchSize:    100,
		},
	})
	require.NoError(tb, err)

	ctx := context.Background()
	require.NoError(tb, e.EnsureSchema(ctx))

	// Clean up tables before test
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
		BinaryData: true,
		WAL: WALConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(tb, err)

	ctx := context.Background()
	require.NoError(tb, e.EnsureSchema(ctx))
	dropStaleReplicationSlots(ctx, e.pool)

	// Clean up tables before test
	cleanupTestTables(ctx, e)

	tb.Cleanup(func() {
		_ = e.Close(context.Background())
		_ = n.Shutdown(context.Background())
	})
	return e
}

// dropStaleReplicationSlots drops all inactive cf_map replication slots.
// Stale slots from previous test runs can be far behind the current WAL
// position, causing WAL reader tests to timeout replaying old WAL data.
func dropStaleReplicationSlots(ctx context.Context, pool *pgxpool.Pool) {
	rows, _ := pool.Query(ctx,
		`SELECT slot_name FROM pg_replication_slots WHERE (slot_name LIKE 'cf_map_%' OR slot_name LIKE 'cf_binary_map_%') AND NOT active`)
	if rows != nil {
		var names []string
		for rows.Next() {
			var n string
			_ = rows.Scan(&n)
			names = append(names, n)
		}
		rows.Close()
		for _, n := range names {
			_, _ = pool.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", n))
		}
	}
}

func cleanupTestTables(ctx context.Context, e *PostgresMapBroker) {
	_, _ = e.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE channel LIKE 'test_%%'", e.names.stream))
	_, _ = e.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE channel LIKE 'test_%%'", e.names.state))
	_, _ = e.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE channel LIKE 'test_%%'", e.names.meta))
	_, _ = e.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE channel LIKE 'test_%%'", e.names.idempotency))
	_, _ = e.pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET last_processed_id = 0", e.names.outboxCursor))
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   100,
		MetaTTL: 300 * time.Second,
	})
	entries, streamPos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.NotEmpty(t, streamPos.Epoch)
	require.Greater(t, streamPos.Offset, uint64(0))

	// Verify state contains latest values
	state := stateToMapPostgres(entries)
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

// TestPostgresMapBroker_StatefulChannelOrdered tests ordered stateful channel.
func TestPostgresMapBroker_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	node.config.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_ordered"

	// Publish with scores for ordering
	for i := 0; i < 5; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			Score:      int64(i * 10), // Scores: 0, 10, 20, 30, 40
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Read ordered state (descending by score)
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   100,
		MetaTTL: 300 * time.Second,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   100,
		MetaTTL: 300 * time.Second,
	})
	entries, streamPos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   3,
		Cursor:  "",
		MetaTTL: 300 * time.Second,
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
			Limit:   3,
			Cursor:  cursor,
			MetaTTL: 300 * time.Second,
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
	streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Since: &sincePos,
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), streamResult.Position.Offset)
	require.Len(t, streamResult.Publications, 3) // Should get messages 3, 4, 5
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   100,
		MetaTTL: 300 * time.Second,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   100,
		MetaTTL: 300 * time.Second,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "item1",
	})
	entries, pos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "ephemeral",
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Trigger TTL check
	broker.expireKeys(ctx)

	// Key should be gone
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "ephemeral",
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Key: "key1",
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
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
	streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Len(t, streamResult.Publications, 3) // key1, key2, remove(key1)
	require.True(t, streamResult.Publications[2].Removed)
	require.Equal(t, "key1", streamResult.Publications[2].Key)
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
	_, err := broker.ReadState(ctx, channel, MapReadStateOptions{
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
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100,
	})
	_, pos, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.NotEmpty(t, pos.Epoch)

	// Read with wrong epoch should fail
	_, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Revision: &StreamPosition{
			Epoch:  "wrong_epoch",
			Offset: 1,
		},
		Limit: 100,
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)

	// Read with correct epoch should succeed
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Revision: &pos,
		Limit:    100,
	})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
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
		BinaryData: true,
		WAL: WALConfig{
			Enabled:           true,
			ShardIDs:          nil, // nil means try all shards
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	require.NoError(t, broker.EnsureSchema(ctx))
	dropStaleReplicationSlots(ctx, broker.pool)

	// Clean up tables before test
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_binary_map_stream WHERE channel LIKE 'test_wal_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_binary_map_state WHERE channel LIKE 'test_wal_%'")
	_, _ = broker.pool.Exec(ctx, "DELETE FROM cf_binary_map_meta WHERE channel LIKE 'test_wal_%'")

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
	directPool := broker.pool
	_, err = directPool.Exec(ctx, `
		SELECT * FROM cf_binary_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, false, 16)
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
	_, err = directPool.Exec(ctx, `
		SELECT * FROM cf_binary_map_remove($1, $2, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, 16)
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
		BinaryData: true,
		WAL: WALConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	require.NoError(t, broker.EnsureSchema(ctx))
	dropStaleReplicationSlots(ctx, broker.pool)

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

	// Publish multiple messages in sequence
	for i := 0; i < numMessages; i++ {
		_, err = broker.pool.Exec(ctx, `
			SELECT * FROM cf_binary_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, false, 16)
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
		BinaryData: true,
		WAL: WALConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	require.NoError(t, broker.EnsureSchema(ctx))
	dropStaleReplicationSlots(ctx, broker.pool)

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

	// Publish with tags and score via SQL
	// cf_binary_map_publish params: channel, key, data, tags, client_id, user_id, conn_info, chan_info,
	//   subscribed_at, key_mode, key_ttl, stream_ttl, stream_size, meta_ttl, expected_offset, score, ...
	_, err = broker.pool.Exec(ctx, `
		SELECT * FROM cf_binary_map_publish(
			$1, $2, $3,
			'{"env": "test", "type": "metadata"}'::jsonb,
			NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, '300 seconds'::interval, NULL, NULL,
			NULL, $4,
			NULL, NULL, NULL, NULL, NULL, NULL, false, false, 16
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
	streamResult, err := broker.ReadStream(ctx, channel, MapReadStreamOptions{
		Filter: StreamFilter{
			Limit: -1,
		},
	})
	require.NoError(t, err)

	expectedCount := numGoroutines * publishesPerGoroutine
	require.Len(t, streamResult.Publications, expectedCount)
	require.Equal(t, uint64(expectedCount), streamResult.Position.Offset)

	// Verify offsets are 1, 2, 3, ... with no gaps
	for i, pub := range streamResult.Publications {
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

	// Create PostgreSQL map broker with WAL mode and broker for multi-node delivery
	pgBroker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		BinaryData: true,
		Broker:     broker,
		WAL: WALConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second,
		},
	})
	require.NoError(t, err)
	require.NoError(t, pgBroker.EnsureSchema(ctx))
	dropStaleReplicationSlots(ctx, pgBroker.pool)

	// Clean up tables before test
	_, _ = pgBroker.pool.Exec(ctx, "DELETE FROM cf_binary_map_stream WHERE channel LIKE 'test_wal_broker_%'")
	_, _ = pgBroker.pool.Exec(ctx, "DELETE FROM cf_binary_map_state WHERE channel LIKE 'test_wal_broker_%'")
	_, _ = pgBroker.pool.Exec(ctx, "DELETE FROM cf_binary_map_meta WHERE channel LIKE 'test_wal_broker_%'")

	t.Cleanup(func() {
		_ = pgBroker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	// Register handler on pgBroker — this passes it through to
	// broker.RegisterBrokerEventHandler(h), starting pub/sub with the handler.
	err = pgBroker.RegisterEventHandler(&testBrokerEventHandler{
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

	// Wait for WAL reader to claim at least one shard
	require.Eventually(t, func() bool {
		return len(pgBroker.ClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond, "WAL reader should claim at least one shard")

	// Publish via SQL (simulating external publish)
	_, err = pgBroker.pool.Exec(ctx, `
		SELECT * FROM cf_binary_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, $4, NULL, NULL, NULL, NULL, NULL, NULL, false, false, 16)
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
		BinaryData: true,
		NumShards:  4,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
			BatchSize:    100,
		},
	})
	require.NoError(t, err)
	require.NoError(t, broker.EnsureSchema(ctx))

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
		BinaryData: true,
		NumShards:  4,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	require.NoError(t, broker1.EnsureSchema(ctx))

	cleanupTestTables(ctx, broker1)

	// Publish messages directly via SQL (bypassing workers)
	for i := 0; i < 5; i++ {
		_, err = broker1.pool.Exec(ctx, `
			SELECT * FROM cf_binary_map_publish($1, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '300 seconds'::interval, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false, false, 4)
		`, channel, fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("data%d", i)))
		require.NoError(t, err)
	}

	// Verify messages are in stream
	var count int
	err = broker1.pool.QueryRow(ctx, "SELECT COUNT(*) FROM cf_binary_map_stream WHERE channel = $1", channel).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 5, count, "5 messages should be in stream")

	_ = broker1.Close(ctx)
	_ = node1.Shutdown(ctx)

	// Now start a new broker with workers - they should pick up the pending messages
	node2, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node2.Run())

	broker2, err := NewPostgresMapBroker(node2, PostgresMapBrokerConfig{
		ConnString: connString,
		BinaryData: true,
		NumShards:  4,
		Outbox: OutboxConfig{
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

	// Verify cursor has advanced for the shard that received messages.
	require.Eventually(t, func() bool {
		var maxCursor int64
		_ = broker2.pool.QueryRow(ctx, "SELECT COALESCE(MAX(last_processed_id), 0) FROM cf_binary_map_outbox_cursor").Scan(&maxCursor)
		return maxCursor > 0
	}, 5*time.Second, 50*time.Millisecond, "cursor should have advanced after processing")
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
		BinaryData: true,
		NumShards:  4,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	require.NoError(t, broker.EnsureSchema(ctx))

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

	// Create PostgreSQL map broker with broker for multi-node delivery
	pgBroker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		BinaryData: true,
		Broker:     broker,
		NumShards:  4,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	require.NoError(t, pgBroker.EnsureSchema(ctx))

	cleanupTestTables(ctx, pgBroker)

	t.Cleanup(func() {
		_ = pgBroker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	// Register handler on pgBroker — this passes it through to
	// broker.RegisterBrokerEventHandler(h), starting pub/sub with the handler.
	err = pgBroker.RegisterEventHandler(&testBrokerEventHandler{
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

	require.Eventually(t, func() bool {
		return len(pgBroker.OutboxClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond, "Outbox worker should claim at least one shard")

	// Publish via broker
	_, err = pgBroker.Publish(ctx, channel, "test_key", MapPublishOptions{
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

// TestPostgresMapBroker_Delta_Outbox tests key-based delta delivery via outbox workers.
func TestPostgresMapBroker_Delta_Outbox(t *testing.T) {
	connString := getPostgresConnString(t)
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

	e, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		BinaryData: true,
		NumShards:  4,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
			BatchSize:    100,
		},
	})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, e.EnsureSchema(ctx))
	cleanupTestTables(ctx, e)

	err = e.RegisterEventHandler(handler)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = e.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	require.Eventually(t, func() bool {
		return len(e.OutboxClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond, "Outbox worker should claim at least one shard")

	channel := fmt.Sprintf("test_delta_%d", time.Now().UnixNano())

	waitEvent := func(t *testing.T) pubEvent {
		t.Helper()
		select {
		case ev := <-eventCh:
			return ev
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for publication event")
			return pubEvent{}
		}
	}

	// 1. First publish with UseDelta - no previous state.
	_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1"),
		UseDelta:   true,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	ev := waitEvent(t)
	require.False(t, ev.delta, "no previous data means useDelta is false in outbox delivery")
	require.Nil(t, ev.prevPub, "no previous state for first publish")
	require.Equal(t, []byte("data1"), ev.pub.Data)

	// 2. Second publish same key - should get prevPub with first data.
	_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1_updated"),
		UseDelta:   true,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	ev = waitEvent(t)
	require.True(t, ev.delta)
	require.NotNil(t, ev.prevPub)
	require.Equal(t, []byte("data1"), ev.prevPub.Data)
	require.Equal(t, []byte("data1_updated"), ev.pub.Data)

	// 3. Different key - no previous state for this key.
	_, err = e.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2"),
		UseDelta:   true,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	ev = waitEvent(t)
	require.False(t, ev.delta, "no previous data for key2")
	require.Nil(t, ev.prevPub, "no previous state for key2")
	require.Equal(t, []byte("data2"), ev.pub.Data)

	// 4. StreamData present - no delta.
	_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1_full"),
		StreamData: []byte("data1_stream"),
		UseDelta:   true,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	ev = waitEvent(t)
	require.False(t, ev.delta, "StreamData disables delta")
	require.Nil(t, ev.prevPub, "StreamData disables key-based delta")

	// 5. UseDelta=false - no delta.
	_, err = e.Publish(ctx, channel, "key2", MapPublishOptions{
		Data:       []byte("data2_updated"),
		UseDelta:   false,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	ev = waitEvent(t)
	require.False(t, ev.delta)
	require.Nil(t, ev.prevPub, "UseDelta=false means no delta")

	// 6. Third publish to key1 after StreamData update - prevPub should have data1_full.
	_, err = e.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte("data1_v3"),
		UseDelta:   true,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)

	ev = waitEvent(t)
	require.True(t, ev.delta)
	require.NotNil(t, ev.prevPub)
	require.Equal(t, []byte("data1_full"), ev.prevPub.Data, "prevPub should have state data, not stream data")
}

func TestPostgresMapBroker_Clear(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := "test_clear"

	// Publish some keyed state and stream entries.
	for i := 0; i < 3; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key%d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Verify data exists.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100, MetaTTL: 300 * time.Second})
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
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100, MetaTTL: 300 * time.Second})
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

// TestPostgresMapBroker_CursorBasedDelivery tests that cursor-based outbox delivery works correctly.
func TestPostgresMapBroker_CursorBasedDelivery(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, err := New(Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())

	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		BinaryData: true,
		NumShards:  4,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
			BatchSize:    100,
		},
	})
	require.NoError(t, err)
	require.NoError(t, broker.EnsureSchema(ctx))

	channel := fmt.Sprintf("test_cursor_%d", time.Now().UnixNano())
	cleanupTestTables(ctx, broker)

	t.Cleanup(func() {
		_ = broker.Close(context.Background())
		_ = node.Shutdown(context.Background())
	})

	var received []*Publication
	var receivedMu sync.Mutex
	doneCh := make(chan struct{})

	const numMessages = 5

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

	require.Eventually(t, func() bool {
		return len(broker.OutboxClaimedShards()) > 0
	}, 10*time.Second, 100*time.Millisecond)

	// Publish messages
	for i := 0; i < numMessages; i++ {
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

	receivedMu.Lock()
	require.Len(t, received, numMessages)
	receivedMu.Unlock()

	// Verify cursor has advanced for the shard that received messages.
	require.Eventually(t, func() bool {
		var maxCursor int64
		_ = broker.pool.QueryRow(ctx, "SELECT COALESCE(MAX(last_processed_id), 0) FROM cf_binary_map_outbox_cursor").Scan(&maxCursor)
		return maxCursor > 0
	}, 5*time.Second, 50*time.Millisecond, "cursor should have advanced")

	// Stream entries should still exist (not deleted by cursor advancement)
	var streamCount int
	err = broker.pool.QueryRow(ctx, "SELECT COUNT(*) FROM cf_binary_map_stream WHERE channel = $1", channel).Scan(&streamCount)
	require.NoError(t, err)
	require.Equal(t, numMessages, streamCount, "stream entries should persist after delivery")
}

func TestPostgresMapBroker_ClearDoesNotAffectOtherChannels(t *testing.T) {
	node, _ := New(Config{})
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()

	// Populate two channels.
	for _, ch := range []string{"test_clear_iso_ch1", "test_clear_iso_ch2"} {
		_, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:       []byte("v"),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Clear only ch1.
	err := broker.Clear(ctx, "test_clear_iso_ch1", MapClearOptions{})
	require.NoError(t, err)

	// ch1 empty.
	stateRes, err := broker.ReadState(ctx, "test_clear_iso_ch1", MapReadStateOptions{Limit: 100, MetaTTL: 300 * time.Second})
	entries, _, _ := stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Empty(t, entries)

	// ch2 still intact.
	stateRes, err = broker.ReadState(ctx, "test_clear_iso_ch2", MapReadStateOptions{Limit: 100, MetaTTL: 300 * time.Second})
	entries, _, _ = stateRes.Publications, stateRes.Position, stateRes.Cursor
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

// ============================================================================
// EnsureSchema Tests
// ============================================================================

// dropAllSchemaObjects drops all centrifuge map objects (both JSONB and binary variants)
// in reverse dependency order.
func dropAllSchemaObjects(ctx context.Context, pool *pgxpool.Pool) {
	// Drop publications first (they depend on the table).
	rows, _ := pool.Query(ctx, `SELECT pubname FROM pg_publication WHERE pubname LIKE 'cf_map_stream_%' OR pubname LIKE 'cf_binary_map_stream_%'`)
	if rows != nil {
		var names []string
		for rows.Next() {
			var n string
			_ = rows.Scan(&n)
			names = append(names, n)
		}
		rows.Close()
		for _, n := range names {
			_, _ = pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", n))
		}
	}

	for _, prefix := range []string{"cf_map_", "cf_binary_map_"} {
		// Drop functions (they depend on tables).
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %spublish CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %spublish_strict CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %sremove CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %sremove_strict CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %sexpire_keys CASCADE", prefix))

		// Drop tables.
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %sidempotency CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %soutbox_cursor CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %sstream CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %sstate CASCADE", prefix))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %smeta CASCADE", prefix))
	}
}

// verifySchemaComplete checks that all expected tables, indexes, functions, and column types exist.
// prefix is "cf_map_" or "cf_binary_map_".
func verifySchemaComplete(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prefix string, expectJSONB bool) {
	t.Helper()

	// Check tables exist.
	for _, suffix := range []string{"stream", "outbox_cursor", "state", "meta", "idempotency"} {
		table := prefix + suffix
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, table).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists, "table %s should exist", table)
	}

	// Check indexes exist.
	for _, suffix := range []string{
		"stream_channel_offset_idx",
		"stream_channel_id_idx",
		"stream_expires_idx",
		"stream_shard_cursor_idx",
		"state_ordered_idx",
		"state_expires_idx",
		"meta_expires_idx",
		"idempotency_expires_idx",
	} {
		idx := prefix + suffix
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)`, idx).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists, "index %s should exist", idx)
	}

	// Check functions exist.
	for _, suffix := range []string{"publish", "publish_strict", "remove", "remove_strict", "expire_keys"} {
		fn := prefix + suffix
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1)`, fn).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists, "function %s should exist", fn)
	}

	// Check data column types.
	expectedType := "jsonb"
	if !expectJSONB {
		expectedType = "bytea"
	}

	dataColumns := []struct {
		table  string
		column string
	}{
		{prefix + "stream", "data"},
		{prefix + "stream", "previous_data"},
		{prefix + "stream", "conn_info"},
		{prefix + "stream", "chan_info"},
		{prefix + "state", "data"},
		{prefix + "state", "conn_info"},
		{prefix + "state", "chan_info"},
	}
	for _, dc := range dataColumns {
		var dataType string
		err := pool.QueryRow(ctx,
			`SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2`,
			dc.table, dc.column).Scan(&dataType)
		require.NoError(t, err, "column %s.%s should exist", dc.table, dc.column)
		require.Equal(t, expectedType, dataType, "column %s.%s should be %s", dc.table, dc.column, expectedType)
	}

	// tags should always be JSONB regardless of BinaryData setting.
	for _, suffix := range []string{"stream", "state"} {
		table := prefix + suffix
		var dataType string
		err := pool.QueryRow(ctx,
			`SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = 'tags'`,
			table).Scan(&dataType)
		require.NoError(t, err)
		require.Equal(t, "jsonb", dataType, "%s.tags should always be jsonb", table)
	}
}

// TestPostgresMapBroker_EnsureSchema_Fresh tests creating schema from scratch.
func TestPostgresMapBroker_EnsureSchema_Fresh(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = broker.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	// Drop everything first.
	dropAllSchemaObjects(ctx, broker.pool)

	// Create from scratch.
	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	// Verify all objects exist with JSONB (default).
	verifySchemaComplete(t, ctx, broker.pool, "cf_map_", true)
}

// TestPostgresMapBroker_EnsureSchema_Idempotent tests calling EnsureSchema twice.
func TestPostgresMapBroker_EnsureSchema_Idempotent(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = broker.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	dropAllSchemaObjects(ctx, broker.pool)

	// First call.
	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	// Second call — should succeed without errors.
	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	verifySchemaComplete(t, ctx, broker.pool, "cf_map_", true)
}

// TestPostgresMapBroker_EnsureSchema_PartialState tests that EnsureSchema handles partial schema.
func TestPostgresMapBroker_EnsureSchema_PartialState(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = broker.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	// Drop everything.
	dropAllSchemaObjects(ctx, broker.pool)

	// Create only some tables manually.
	_, err = broker.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS cf_map_meta (
		channel TEXT PRIMARY KEY, top_offset BIGINT NOT NULL DEFAULT 0,
		epoch TEXT NOT NULL DEFAULT '', version BIGINT DEFAULT 0,
		version_epoch TEXT, created_at TIMESTAMPTZ DEFAULT NOW(),
		updated_at TIMESTAMPTZ DEFAULT NOW(), expires_at TIMESTAMPTZ
	)`)
	require.NoError(t, err)

	// EnsureSchema should create the rest.
	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	verifySchemaComplete(t, ctx, broker.pool, "cf_map_", true)
}

// TestPostgresMapBroker_EnsureSchema_WALPublications tests that publications are created in WAL mode.
func TestPostgresMapBroker_EnsureSchema_WALPublications(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
		WAL: WALConfig{
			Enabled: true,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = broker.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	// Drop everything (including existing publications).
	dropAllSchemaObjects(ctx, broker.pool)

	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	// Verify shard publications exist.
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("cf_map_stream_shard_%d", i)
		var exists bool
		err := broker.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)`, name).Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists, "publication %s should exist", name)
	}

	// Verify "all" publication.
	var exists bool
	err = broker.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = 'cf_map_stream_all')`,
	).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "publication cf_map_stream_all should exist")
}

// TestPostgresMapBroker_EnsureSchema_NumShardsMismatch tests error on shard count mismatch.
func TestPostgresMapBroker_EnsureSchema_NumShardsMismatch(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})

	// First, create schema with 4 shards.
	broker4, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
		WAL:        WALConfig{Enabled: true},
	})
	require.NoError(t, err)

	dropAllSchemaObjects(ctx, broker4.pool)

	err = broker4.EnsureSchema(ctx)
	require.NoError(t, err)
	_ = broker4.Close(ctx)

	// Now try with 8 shards — should error.
	broker8, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  8,
		WAL:        WALConfig{Enabled: true},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = broker8.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	err = broker8.EnsureSchema(ctx)
	require.Error(t, err)

	var schemaErr *SchemaError
	require.ErrorAs(t, err, &schemaErr)
	require.Equal(t, "publication", schemaErr.Object.Type)
	require.Equal(t, "verify", schemaErr.Op)
	require.Contains(t, schemaErr.Err.Error(), "found 4 shard publications but NumShards=8")
}

// TestPostgresMapBroker_EnsureSchema_OutboxNoPublications tests that outbox mode skips publications.
func TestPostgresMapBroker_EnsureSchema_OutboxNoPublications(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
		// WAL.Enabled is false (default) — outbox mode.
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = broker.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	dropAllSchemaObjects(ctx, broker.pool)

	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	// No publications should be created in outbox mode.
	var count int
	err = broker.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM pg_publication WHERE pubname LIKE 'cf_map_stream_%'`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "outbox mode should not create publications")

	// Tables/functions should still exist.
	verifySchemaComplete(t, ctx, broker.pool, "cf_map_", true)
}

// TestPostgresMapBroker_EnsureSchema_BinaryData tests BYTEA columns when BinaryData=true.
func TestPostgresMapBroker_EnsureSchema_BinaryData(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
		BinaryData: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = broker.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	dropAllSchemaObjects(ctx, broker.pool)

	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	// Verify data columns are BYTEA.
	verifySchemaComplete(t, ctx, broker.pool, "cf_binary_map_", false)
}

// TestPostgresMapBroker_EnsureSchema_FunctionalAfterSetup tests that the broker works after EnsureSchema.
func TestPostgresMapBroker_EnsureSchema_FunctionalAfterSetup(t *testing.T) {
	connString := getPostgresConnString(t)
	ctx := context.Background()

	node, _ := New(Config{})
	broker, err := NewPostgresMapBroker(node, PostgresMapBrokerConfig{
		ConnString: connString,
		NumShards:  4,
		Outbox: OutboxConfig{
			PollInterval: 10 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	// Drop everything and recreate from scratch.
	dropAllSchemaObjects(ctx, broker.pool)

	err = broker.EnsureSchema(ctx)
	require.NoError(t, err)

	err = broker.RegisterEventHandler(nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = broker.Close(ctx)
		_ = node.Shutdown(ctx)
	})

	channel := fmt.Sprintf("test_ensure_func_%d", time.Now().UnixNano())

	// Publish.
	res, err := broker.Publish(ctx, channel, "key1", MapPublishOptions{
		Data:       []byte(`{"hello":"world"}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res.Suppressed)
	require.Equal(t, uint64(1), res.Position.Offset)

	// ReadState.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 1)
	require.Equal(t, "key1", stateRes.Publications[0].Key)
	// JSONB normalizes whitespace, so compare via JSONEq.
	require.JSONEq(t, `{"hello":"world"}`, string(stateRes.Publications[0].Data))

	// Remove.
	removeRes, err := broker.Remove(ctx, channel, "key1", MapRemoveOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, removeRes.Suppressed)

	// Verify removed.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{Limit: 100})
	require.NoError(t, err)
	require.Empty(t, stateRes.Publications)
}

// TestPostgresMapBroker_OrderedStateAsc tests that ASC ordering returns entries
// in ascending score order (lowest score first).
func TestPostgresMapBroker_OrderedStateAsc(t *testing.T) {
	node, _ := New(Config{})
	node.config.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := fmt.Sprintf("test_ordered_asc_%d", time.Now().UnixNano())

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
			Score:      tc.score,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// ASC: lowest score first.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   100,
		MetaTTL: 300 * time.Second,
		Asc:     true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 5)

	expectedAsc := []string{"key_a", "key_b", "key_c", "key_d", "key_e"}
	for i, entry := range stateRes.Publications {
		require.Equal(t, expectedAsc[i], entry.Key, "ASC entry %d", i)
	}

	// DESC (default): highest score first.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit:   100,
		MetaTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 5)

	expectedDesc := []string{"key_e", "key_d", "key_c", "key_b", "key_a"}
	for i, entry := range stateRes.Publications {
		require.Equal(t, expectedDesc[i], entry.Key, "DESC entry %d", i)
	}
}

// TestPostgresMapBroker_OrderedStatePaginationAsc tests cursor-based pagination
// with ASC ordering across multiple pages.
func TestPostgresMapBroker_OrderedStatePaginationAsc(t *testing.T) {
	node, _ := New(Config{})
	node.config.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := fmt.Sprintf("test_ordered_pagination_asc_%d", time.Now().UnixNano())

	// Publish 10 entries with scores 100..1000.
	for i := 1; i <= 10; i++ {
		_, err := broker.Publish(ctx, channel, fmt.Sprintf("key_%02d", i), MapPublishOptions{
			Data:       []byte(fmt.Sprintf("data_%02d", i)),
			Score:      int64(i * 100),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Page 1.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 3, MetaTTL: 300 * time.Second, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 3)
	require.NotEmpty(t, stateRes.Cursor)
	require.Equal(t, "key_01", stateRes.Publications[0].Key)
	require.Equal(t, "key_02", stateRes.Publications[1].Key)
	require.Equal(t, "key_03", stateRes.Publications[2].Key)

	// Page 2.
	stateRes2, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Cursor: stateRes.Cursor, Limit: 3, MetaTTL: 300 * time.Second, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes2.Publications, 3)
	require.NotEmpty(t, stateRes2.Cursor)
	require.Equal(t, "key_04", stateRes2.Publications[0].Key)
	require.Equal(t, "key_05", stateRes2.Publications[1].Key)
	require.Equal(t, "key_06", stateRes2.Publications[2].Key)

	// Page 3.
	stateRes3, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Cursor: stateRes2.Cursor, Limit: 3, MetaTTL: 300 * time.Second, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes3.Publications, 3)
	require.NotEmpty(t, stateRes3.Cursor)
	require.Equal(t, "key_07", stateRes3.Publications[0].Key)
	require.Equal(t, "key_08", stateRes3.Publications[1].Key)
	require.Equal(t, "key_09", stateRes3.Publications[2].Key)

	// Page 4: last entry.
	stateRes4, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Cursor: stateRes3.Cursor, Limit: 3, MetaTTL: 300 * time.Second, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes4.Publications, 1)
	require.Empty(t, stateRes4.Cursor, "No more pages")
	require.Equal(t, "key_10", stateRes4.Publications[0].Key)
}

// TestPostgresMapBroker_OrderedStateAscSameScores tests ASC ordering with
// same-score entries — secondary sort by key ascending.
func TestPostgresMapBroker_OrderedStateAscSameScores(t *testing.T) {
	node, _ := New(Config{})
	node.config.GetMapChannelOptions = func(channel string) MapChannelOptions {
		return MapChannelOptions{
			Ordered:    true,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		}
	}
	broker := newTestPostgresMapBroker(t, node)

	ctx := context.Background()
	channel := fmt.Sprintf("test_ordered_asc_same_scores_%d", time.Now().UnixNano())

	// All entries have score=100.
	for _, key := range []string{"zebra", "apple", "mango", "banana"} {
		_, err := broker.Publish(ctx, channel, key, MapPublishOptions{
			Data:       []byte("data"),
			Score:      100,
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
			KeyTTL:     300 * time.Second,
		})
		require.NoError(t, err)
	}

	// ASC with same scores → key ASC.
	stateRes, err := broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100, MetaTTL: 300 * time.Second, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 4)
	require.Equal(t, "apple", stateRes.Publications[0].Key)
	require.Equal(t, "banana", stateRes.Publications[1].Key)
	require.Equal(t, "mango", stateRes.Publications[2].Key)
	require.Equal(t, "zebra", stateRes.Publications[3].Key)

	// DESC with same scores → key DESC.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 100, MetaTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 4)
	require.Equal(t, "zebra", stateRes.Publications[0].Key)
	require.Equal(t, "mango", stateRes.Publications[1].Key)
	require.Equal(t, "banana", stateRes.Publications[2].Key)
	require.Equal(t, "apple", stateRes.Publications[3].Key)

	// Paginate ASC with limit=2.
	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Limit: 2, MetaTTL: 300 * time.Second, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 2)
	require.NotEmpty(t, stateRes.Cursor)
	require.Equal(t, "apple", stateRes.Publications[0].Key)
	require.Equal(t, "banana", stateRes.Publications[1].Key)

	stateRes, err = broker.ReadState(ctx, channel, MapReadStateOptions{
		Cursor: stateRes.Cursor, Limit: 2, MetaTTL: 300 * time.Second, Asc: true,
	})
	require.NoError(t, err)
	require.Len(t, stateRes.Publications, 2)
	require.Empty(t, stateRes.Cursor)
	require.Equal(t, "mango", stateRes.Publications[0].Key)
	require.Equal(t, "zebra", stateRes.Publications[1].Key)
}
