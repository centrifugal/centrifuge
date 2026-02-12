package centrifuge

import (
	"context"
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
				Data:       []byte("data"),
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
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
				Data:       []byte("data"),
				StreamSize: 100,
				StreamTTL:  300 * time.Second,
				KeyTTL:     300 * time.Second,
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

// testMapBrokerRemoveEmptyKey verifies that Remove with empty key returns an error.
func testMapBrokerRemoveEmptyKey(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	t.Run("Remove_empty_key_returns_error", func(t *testing.T) {
		broker := factory(t)
		_, err := broker.Remove(ctx, "some_ch", "", MapRemoveOptions{})
		require.Error(t, err)
	})
}
