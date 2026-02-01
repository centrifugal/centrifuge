//go:build integration
// +build integration

package centrifuge

import (
	"context"
	"fmt"
	"os"
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

func newTestPostgresKeyedEngine(tb testing.TB, n *Node) *PostgresKeyedEngine {
	connString := getPostgresConnString(tb)

	e, err := NewPostgresKeyedEngine(n, PostgresKeyedEngineConfig{
		ConnString:   connString,
		PollInterval: 50 * time.Millisecond,
	})
	require.NoError(tb, err)

	err = e.RegisterBrokerEventHandler(nil)
	require.NoError(tb, err)

	// Clean up tables before test
	ctx := context.Background()
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_keyed_stream WHERE channel LIKE 'test_%'")
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_keyed_snapshot WHERE channel LIKE 'test_%'")
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_keyed_stream_meta WHERE channel LIKE 'test_%'")
	_, _ = e.pool.Exec(ctx, "DELETE FROM cf_keyed_idempotency WHERE channel LIKE 'test_%'")

	tb.Cleanup(func() {
		_ = e.Close(context.Background())
		_ = n.Shutdown(context.Background())
	})
	return e
}

// snapshotToMapPostgres converts []Publication to map for easier testing.
func snapshotToMapPostgres(pubs []*Publication) map[string][]byte {
	result := make(map[string][]byte, len(pubs))
	for _, pub := range pubs {
		result[pub.Key] = pub.Data
	}
	return result
}

// TestPostgresKeyedEngine_StatefulChannel tests stateful channel with keyed state and revisions.
func TestPostgresKeyedEngine_StatefulChannel(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_stateful"

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
	snapshot := snapshotToMapPostgres(entries)
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

// TestPostgresKeyedEngine_StatefulChannelOrdered tests ordered stateful channel.
func TestPostgresKeyedEngine_StatefulChannelOrdered(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_ordered"

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
	snapshot := snapshotToMapPostgres(entries)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		require.Contains(t, snapshot, key)
	}
}

// TestPostgresKeyedEngine_SnapshotRevision tests that snapshot values include revisions.
func TestPostgresKeyedEngine_SnapshotRevision(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_revision"

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
	snapshot := snapshotToMapPostgres(entries)
	require.Equal(t, []byte("data1"), snapshot["key1"])
	require.Equal(t, []byte("data2"), snapshot["key2"])

	// Verify per-entry offsets
	require.NotEmpty(t, streamPos.Epoch)
	for _, pub := range entries {
		require.Greater(t, pub.Offset, uint64(0))
	}
}

// TestPostgresKeyedEngine_SnapshotPagination tests cursor-based snapshot pagination.
func TestPostgresKeyedEngine_SnapshotPagination(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_pagination"

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

// TestPostgresKeyedEngine_StreamRecovery tests stream recovery.
func TestPostgresKeyedEngine_StreamRecovery(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_recovery"

	// Publish some updates
	for i := 1; i <= 5; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Get position after first 2 messages
	sincePos := StreamPosition{Offset: 2}

	// Read stream since position 2
	pubs, streamPos, err := engine.ReadStream(ctx, channel, KeyedReadStreamOptions{
		Filter: StreamFilter{
			Since: &sincePos,
			Limit: -1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), streamPos.Offset)
	require.Len(t, pubs, 3) // Should get messages 3, 4, 5
}

// TestPostgresKeyedEngine_Idempotency tests idempotent publishing.
func TestPostgresKeyedEngine_Idempotency(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_idempotency"

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
	require.True(t, res2.Suppressed)                                 // Suppressed due to idempotency
	require.Equal(t, SuppressReasonIdempotency, res2.SuppressReason) // The idempotency check returns the original offset
	require.Equal(t, res1.Position.Offset, res2.Position.Offset)     // Same offset

	// Snapshot should still have original data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	snapshot := snapshotToMapPostgres(entries)
	require.Len(t, snapshot, 1)
	require.Equal(t, []byte("data1"), snapshot["key1"])
}

// TestPostgresKeyedEngine_KeyMode tests KeyMode (IfNew, IfExists).
func TestPostgresKeyedEngine_KeyMode(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_keymode"

	// First publish with KeyModeIfNew should succeed
	res1, err := engine.Publish(ctx, channel, "slot1", KeyedPublishOptions{
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
	res2, err := engine.Publish(ctx, channel, "slot1", KeyedPublishOptions{
		Data:       []byte("player2"),
		KeyMode:    KeyModeIfNew,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed)
	require.Equal(t, SuppressReasonKeyExists, res2.SuppressReason)

	// Verify snapshot still has original data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit:       100,
		SnapshotTTL: 300 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("player1"), entries[0].Data)

	// KeyModeIfExists should be suppressed for non-existent key
	res3, err := engine.Publish(ctx, channel, "nonexistent", KeyedPublishOptions{
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
	res4, err := engine.Publish(ctx, channel, "slot1", KeyedPublishOptions{
		Data:       []byte("updated"),
		KeyMode:    KeyModeIfExists,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
		KeyTTL:     300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res4.Suppressed)
}

// TestPostgresKeyedEngine_CAS tests Compare-And-Swap operations.
func TestPostgresKeyedEngine_CAS(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_cas"

	// Initial publish
	res1, err := engine.Publish(ctx, channel, "item1", KeyedPublishOptions{
		Data:       []byte(`{"stock": 10}`),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Read current state
	entries, pos, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Key: "item1",
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// CAS update with correct position
	expectedPos := StreamPosition{Offset: entries[0].Offset, Epoch: pos.Epoch}
	res2, err := engine.Publish(ctx, channel, "item1", KeyedPublishOptions{
		Data:             []byte(`{"stock": 9}`),
		ExpectedPosition: &expectedPos,
		StreamSize:       100,
		StreamTTL:        300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res2.Suppressed)

	// CAS update with stale position should fail
	res3, err := engine.Publish(ctx, channel, "item1", KeyedPublishOptions{
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

// TestPostgresKeyedEngine_KeyTTL tests key TTL (this is a slower test).
func TestPostgresKeyedEngine_KeyTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TTL test in short mode")
	}

	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_key_ttl"

	// Publish with short TTL
	_, err := engine.Publish(ctx, channel, "ephemeral", KeyedPublishOptions{
		Data:       []byte("temporary"),
		KeyTTL:     2 * time.Second,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Verify key exists
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Key: "ephemeral",
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// Wait for TTL to expire
	time.Sleep(3 * time.Second)

	// Trigger TTL check
	engine.expireKeys(ctx)

	// Key should be gone
	entries, _, _, err = engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Key: "ephemeral",
	})
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestPostgresKeyedEngine_Version tests version-based ordering.
func TestPostgresKeyedEngine_Version(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_version"

	// Publish with version 2
	res1, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data_v2"),
		Version:    2,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res1.Suppressed)

	// Try to publish older version (should be suppressed)
	res2, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data_v1"),
		Version:    1,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res2.Suppressed)
	require.Equal(t, SuppressReasonVersion, res2.SuppressReason)

	// Publish newer version
	res3, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data_v3"),
		Version:    3,
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res3.Suppressed)

	// Verify snapshot has v3 data
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Key: "key1",
	})
	require.NoError(t, err)
	require.Equal(t, []byte("data_v3"), entries[0].Data)
}

// TestPostgresKeyedEngine_Unpublish tests removing keys.
func TestPostgresKeyedEngine_Unpublish(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_unpublish"

	// Publish some keys
	_, err := engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	_, err = engine.Publish(ctx, channel, "key2", KeyedPublishOptions{
		Data:       []byte("data2"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Verify snapshot has 2 keys
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Remove key1
	res, err := engine.Unpublish(ctx, channel, "key1", KeyedUnpublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.False(t, res.Suppressed)

	// Verify snapshot has 1 key
	entries, _, _, err = engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "key2", entries[0].Key)

	// Remove non-existent key should be suppressed
	res, err = engine.Unpublish(ctx, channel, "nonexistent", KeyedUnpublishOptions{
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, res.Suppressed)
	require.Equal(t, SuppressReasonKeyNotFound, res.SuppressReason)

	// Verify stream has removal event
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

// TestPostgresKeyedEngine_Stats tests snapshot statistics.
func TestPostgresKeyedEngine_Stats(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_stats"

	// Initially empty
	stats, err := engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 0, stats.NumKeys)

	// Publish some keys
	for i := 0; i < 5; i++ {
		_, err := engine.Publish(ctx, channel, fmt.Sprintf("key%d", i), KeyedPublishOptions{
			Data:       []byte(fmt.Sprintf("data%d", i)),
			StreamSize: 100,
			StreamTTL:  300 * time.Second,
		})
		require.NoError(t, err)
	}

	// Should have 5 keys
	stats, err = engine.Stats(ctx, channel)
	require.NoError(t, err)
	require.Equal(t, 5, stats.NumKeys)
}

// TestPostgresKeyedEngine_EpochMismatch tests epoch validation.
func TestPostgresKeyedEngine_EpochMismatch(t *testing.T) {
	node, _ := New(Config{})
	engine := newTestPostgresKeyedEngine(t, node)

	ctx := context.Background()
	channel := "test_epoch_mismatch"

	// Client tries to read with old epoch (channel doesn't exist yet)
	_, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Revision: &StreamPosition{
			Epoch:  "old_epoch",
			Offset: 100,
		},
		Limit: 100,
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)

	// Create channel
	_, err = engine.Publish(ctx, channel, "key1", KeyedPublishOptions{
		Data:       []byte("data1"),
		StreamSize: 100,
		StreamTTL:  300 * time.Second,
	})
	require.NoError(t, err)

	// Read actual epoch
	_, pos, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Limit: 100,
	})
	require.NoError(t, err)
	require.NotEmpty(t, pos.Epoch)

	// Read with wrong epoch should fail
	_, _, _, err = engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Revision: &StreamPosition{
			Epoch:  "wrong_epoch",
			Offset: 1,
		},
		Limit: 100,
	})
	require.ErrorIs(t, err, ErrorUnrecoverablePosition)

	// Read with correct epoch should succeed
	entries, _, _, err := engine.ReadSnapshot(ctx, channel, KeyedReadSnapshotOptions{
		Revision: &pos,
		Limit:    100,
	})
	require.NoError(t, err)
	require.Len(t, entries, 1)
}
