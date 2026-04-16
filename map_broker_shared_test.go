package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testMapBrokerEpochOnEmptyChannel verifies that ReadState and ReadStream
// create and return a non-empty epoch for channels that don't exist yet.
// This is important because clients need a stable epoch during subscription
// setup — without it, the epoch could change between ReadState and ReadStream.
func testMapBrokerEpochOnEmptyChannel(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	t.Run("ReadState_creates_epoch_on_empty_channel", func(t *testing.T) {
		broker := factory(t)
		result, err := broker.ReadState(ctx, "nonexistent_state_ch", MapReadStateOptions{
			Limit: -1,
		})
		require.NoError(t, err)
		require.Empty(t, result.Publications)
		require.NotEmpty(t, result.Position.Epoch, "ReadState should create epoch for empty channel")
		require.Equal(t, uint64(0), result.Position.Offset)
	})

	t.Run("ReadStream_creates_epoch_on_empty_channel", func(t *testing.T) {
		broker := factory(t)
		result, err := broker.ReadStream(ctx, "nonexistent_stream_ch", MapReadStreamOptions{
			Filter: StreamFilter{Limit: 0},
		})
		require.NoError(t, err)
		require.Empty(t, result.Publications)
		require.NotEmpty(t, result.Position.Epoch, "ReadStream should create epoch for empty channel")
		require.Equal(t, uint64(0), result.Position.Offset)
	})

	t.Run("ReadState_and_ReadStream_return_same_epoch", func(t *testing.T) {
		broker := factory(t)
		ch := "epoch_consistency_ch"

		stateResult, err := broker.ReadState(ctx, ch, MapReadStateOptions{
			Limit: -1,
		})
		require.NoError(t, err)

		streamResult, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{Limit: 0},
		})
		require.NoError(t, err)

		require.Equal(t, stateResult.Position.Epoch, streamResult.Position.Epoch,
			"ReadState and ReadStream should return the same epoch for a channel")
	})

	t.Run("epoch_stable_across_multiple_reads", func(t *testing.T) {
		broker := factory(t)
		ch := "epoch_stable_ch"

		result1, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: -1})
		require.NoError(t, err)

		result2, err := broker.ReadState(ctx, ch, MapReadStateOptions{Limit: -1})
		require.NoError(t, err)

		require.Equal(t, result1.Position.Epoch, result2.Position.Epoch,
			"epoch should be stable across multiple reads")
	})
}

// testMapBrokerReadStateAllEntries verifies that ReadState with Limit=-1
// returns all entries in the state.
func testMapBrokerReadStateAllEntries(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	t.Run("Limit_minus_one_returns_all_entries", func(t *testing.T) {
		broker := factory(t)
		ch := "limit_all_ch"

		// Publish multiple entries.
		for i := 0; i < 25; i++ {
			_, err := broker.Publish(ctx, ch, "key"+string(rune('a'+i)), MapPublishOptions{
				Data: []byte("data"),
			})
			require.NoError(t, err)
		}

		// ReadState with Limit=-1 should return all 25 entries.
		result, err := broker.ReadState(ctx, ch, MapReadStateOptions{
			Limit: -1,
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 25)
		require.Empty(t, result.Cursor, "no cursor expected when all entries returned")
	})

	t.Run("Limit_minus_one_no_cursor", func(t *testing.T) {
		broker := factory(t)
		ch := "limit_no_cursor_ch"

		// Publish a few entries.
		for i := 0; i < 5; i++ {
			_, err := broker.Publish(ctx, ch, "k"+string(rune('0'+i)), MapPublishOptions{
				Data: []byte("data"),
			})
			require.NoError(t, err)
		}

		// ReadState with Limit=-1 should return all with no cursor.
		result, err := broker.ReadState(ctx, ch, MapReadStateOptions{
			Limit: -1,
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 5)
		require.Empty(t, result.Cursor)
	})
}

// testMapBrokerRemoveEmptyKey verifies that Remove with empty key succeeds at broker level.
// Empty key validation is done at the Node.MapRemove level.
func testMapBrokerRemoveEmptyKey(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	t.Run("Remove_empty_key_no_error_at_broker_level", func(t *testing.T) {
		broker := factory(t)
		_, err := broker.Remove(ctx, "some_ch", "", MapRemoveOptions{})
		require.NoError(t, err)
	})
}

// testMapBrokerClientInfoInState verifies that ClientInfo is preserved in state
// when reading back via ReadState (both with Key filter and paginated).
func testMapBrokerClientInfoInState(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	info := &ClientInfo{
		ClientID: "c1",
		UserID:   "u1",
		ConnInfo: []byte("conn"),
		ChanInfo: []byte("chan"),
	}

	t.Run("ReadState_with_key_filter", func(t *testing.T) {
		broker := factory(t)
		ch := "client_info_state_key_ch"

		_, err := broker.Publish(ctx, ch, "k1", MapPublishOptions{
			Data:       []byte("data1"),
			ClientInfo: info,
		})
		require.NoError(t, err)

		result, err := broker.ReadState(ctx, ch, MapReadStateOptions{
			Key: "k1",
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 1)
		pub := result.Publications[0]
		require.NotNil(t, pub.Info, "ClientInfo should be present in state")
		require.Equal(t, "c1", pub.Info.ClientID)
		require.Equal(t, "u1", pub.Info.UserID)
		require.Equal(t, []byte("conn"), pub.Info.ConnInfo)
		require.Equal(t, []byte("chan"), pub.Info.ChanInfo)
	})

	t.Run("ReadState_paginated", func(t *testing.T) {
		broker := factory(t)
		ch := "client_info_state_pag_ch"

		_, err := broker.Publish(ctx, ch, "k1", MapPublishOptions{
			Data:       []byte("data1"),
			ClientInfo: info,
		})
		require.NoError(t, err)

		result, err := broker.ReadState(ctx, ch, MapReadStateOptions{
			Limit: -1,
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 1)
		pub := result.Publications[0]
		require.NotNil(t, pub.Info, "ClientInfo should be present in paginated state")
		require.Equal(t, "c1", pub.Info.ClientID)
		require.Equal(t, "u1", pub.Info.UserID)
		require.Equal(t, []byte("conn"), pub.Info.ConnInfo)
		require.Equal(t, []byte("chan"), pub.Info.ChanInfo)
	})
}

// testMapBrokerClientInfoInStream verifies that ClientInfo is preserved in stream
// when reading back via ReadStream.
func testMapBrokerClientInfoInStream(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	info := &ClientInfo{
		ClientID: "c1",
		UserID:   "u1",
		ConnInfo: []byte("conn"),
		ChanInfo: []byte("chan"),
	}

	t.Run("ReadStream_contains_client_info", func(t *testing.T) {
		broker := factory(t)
		ch := "client_info_stream_ch_" + strconv.FormatInt(time.Now().UnixNano(), 36)

		_, err := broker.Publish(ctx, ch, "k1", MapPublishOptions{
			Data:       []byte("data1"),
			ClientInfo: info,
		})
		require.NoError(t, err)

		result, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 1)
		pub := result.Publications[0]
		require.NotNil(t, pub.Info, "ClientInfo should be present in stream")
		require.Equal(t, "c1", pub.Info.ClientID)
		require.Equal(t, "u1", pub.Info.UserID)
		require.Equal(t, []byte("conn"), pub.Info.ConnInfo)
		require.Equal(t, []byte("chan"), pub.Info.ChanInfo)
	})
}

// testMapBrokerCheckOrder verifies the canonical order of suppression checks
// across brokers: Idempotency → Version → KeyMode → CAS. When multiple
// conditions would suppress a publish, the first failing check (in this order)
// is the one whose reason is reported.
func testMapBrokerCheckOrder(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	t.Run("Version_runs_before_KeyMode", func(t *testing.T) {
		broker := factory(t)
		ch := "order_v_before_km_" + strconv.FormatInt(time.Now().UnixNano(), 36)

		// Seed key with version=10.
		_, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v1"),
			Version: 10,
		})
		require.NoError(t, err)

		// Publish with stale version=5 AND KeyModeIfNew. Both would suppress —
		// version (5 <= 10) and key_exists. Canonical order returns "version".
		res, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v2"),
			Version: 5,
			KeyMode: KeyModeIfNew,
		})
		require.NoError(t, err)
		require.True(t, res.Suppressed)
		require.Equal(t, SuppressReasonVersion, res.SuppressReason,
			"Version check must run before KeyMode")
	})

	t.Run("KeyMode_runs_before_CAS", func(t *testing.T) {
		broker := factory(t)
		ch := "order_km_before_cas_" + strconv.FormatInt(time.Now().UnixNano(), 36)

		// Seed key. After this, key exists at offset 1.
		res1, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data: []byte("v1"),
		})
		require.NoError(t, err)
		require.False(t, res1.Suppressed)

		// Publish with KeyModeIfNew (would suppress with key_exists) AND a wrong
		// ExpectedPosition (would suppress with position_mismatch). Canonical order
		// returns "key_exists".
		res, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v2"),
			KeyMode: KeyModeIfNew,
			ExpectedPosition: &StreamPosition{
				Offset: 999,
				Epoch:  res1.Position.Epoch,
			},
		})
		require.NoError(t, err)
		require.True(t, res.Suppressed)
		require.Equal(t, SuppressReasonKeyExists, res.SuppressReason,
			"KeyMode check must run before CAS")
	})

	t.Run("Version_runs_before_CAS", func(t *testing.T) {
		broker := factory(t)
		ch := "order_v_before_cas_" + strconv.FormatInt(time.Now().UnixNano(), 36)

		// Seed key with version=10.
		res1, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v1"),
			Version: 10,
		})
		require.NoError(t, err)

		// Stale version + wrong CAS offset. Both fail; "version" wins.
		res, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v2"),
			Version: 5,
			ExpectedPosition: &StreamPosition{
				Offset: 999,
				Epoch:  res1.Position.Epoch,
			},
		})
		require.NoError(t, err)
		require.True(t, res.Suppressed)
		require.Equal(t, SuppressReasonVersion, res.SuppressReason,
			"Version check must run before CAS")
	})
}

// testMapBrokerVersionPreserved verifies that publishing a key without a
// version does NOT reset the stored version. This matters for late-arriving
// older-versioned writes from a concurrent producer — without this, an
// unversioned publish would erase the dedup protection.
func testMapBrokerVersionPreserved(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	t.Run("unversioned_publish_keeps_stored_version", func(t *testing.T) {
		broker := factory(t)
		ch := "version_preserved_" + strconv.FormatInt(time.Now().UnixNano(), 36)

		// Publish with version=10.
		_, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v10"),
			Version: 10,
		})
		require.NoError(t, err)

		// Publish without a version (Version == 0). Should succeed but must
		// NOT reset the stored version.
		_, err = broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data: []byte("unversioned"),
		})
		require.NoError(t, err)

		// Now a stale-version publish must still be suppressed because the
		// stored version is preserved at 10.
		res, err := broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v5_should_be_dropped"),
			Version: 5,
		})
		require.NoError(t, err)
		require.True(t, res.Suppressed,
			"stale version should still be suppressed after unversioned publish")
		require.Equal(t, SuppressReasonVersion, res.SuppressReason)

		// Newer version still wins.
		res, err = broker.Publish(ctx, ch, "k", MapPublishOptions{
			Data:    []byte("v11"),
			Version: 11,
		})
		require.NoError(t, err)
		require.False(t, res.Suppressed,
			"newer version should still be accepted after unversioned publish")
	})
}
