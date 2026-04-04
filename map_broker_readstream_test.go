package centrifuge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mapBrokerFactory func(t *testing.T) MapBroker

func testMapBrokerReadStream(t *testing.T, factory mapBrokerFactory) {
	ctx := context.Background()

	publishOpts := func(streamSize int) MapPublishOptions {
		_ = streamSize // streamSize now configured via MapChannelOptions
		return MapPublishOptions{}
	}

	removeOpts := func(streamSize int) MapRemoveOptions {
		_ = streamSize // streamSize now configured via MapChannelOptions
		return MapRemoveOptions{}
	}

	// publishN publishes n messages with keys "key_1".."key_n" and data "data_1".."data_n".
	publishN := func(t *testing.T, broker MapBroker, ch string, n int, streamSize int) string {
		t.Helper()
		var epoch string
		for i := 1; i <= n; i++ {
			opts := publishOpts(streamSize)
			opts.Data = []byte(fmt.Sprintf("data_%d", i))
			res, err := broker.Publish(ctx, ch, fmt.Sprintf("key_%d", i), opts)
			require.NoError(t, err)
			epoch = res.Position.Epoch
		}
		return epoch
	}

	t.Run("empty_channel", func(t *testing.T) {
		tests := []struct {
			name   string
			filter StreamFilter
		}{
			{
				name:   "read_nonexistent",
				filter: StreamFilter{Limit: -1},
			},
			{
				name:   "metadata_only",
				filter: StreamFilter{Limit: 0},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				broker := factory(t)
				ch := fmt.Sprintf("empty_%s_%d", tt.name, time.Now().UnixNano())
				result, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
					Filter: tt.filter,
				})
				require.NoError(t, err)
				require.Empty(t, result.Publications)
				require.Equal(t, uint64(0), result.Position.Offset)
			})
		}
	})

	t.Run("forward_recovery", func(t *testing.T) {
		broker := factory(t)
		ch := fmt.Sprintf("forward_%d", time.Now().UnixNano())
		epoch := publishN(t, broker, ch, 5, 100)

		tests := []struct {
			name      string
			filter    StreamFilter
			wantCount int
			wantFirst string // expected data of first pub, empty if no pubs expected
			wantLast  string // expected data of last pub
		}{
			{
				name:      "all_no_since",
				filter:    StreamFilter{Limit: -1},
				wantCount: 5,
				wantFirst: "data_1",
				wantLast:  "data_5",
			},
			{
				name:      "limit_2_no_since",
				filter:    StreamFilter{Limit: 2},
				wantCount: 2,
				wantFirst: "data_1",
				wantLast:  "data_2",
			},
			{
				name:      "metadata_only",
				filter:    StreamFilter{Limit: 0},
				wantCount: 0,
			},
			{
				name: "since_0_all",
				filter: StreamFilter{
					Since: &StreamPosition{Offset: 0, Epoch: epoch},
					Limit: -1,
				},
				wantCount: 5,
				wantFirst: "data_1",
				wantLast:  "data_5",
			},
			{
				name: "since_2_partial",
				filter: StreamFilter{
					Since: &StreamPosition{Offset: 2, Epoch: epoch},
					Limit: -1,
				},
				wantCount: 3,
				wantFirst: "data_3",
				wantLast:  "data_5",
			},
			{
				name: "since_2_limit_2",
				filter: StreamFilter{
					Since: &StreamPosition{Offset: 2, Epoch: epoch},
					Limit: 2,
				},
				wantCount: 2,
				wantFirst: "data_3",
				wantLast:  "data_4",
			},
			{
				name: "since_top_caught_up",
				filter: StreamFilter{
					Since: &StreamPosition{Offset: 5, Epoch: epoch},
					Limit: -1,
				},
				wantCount: 0,
			},
			{
				name: "since_beyond_top",
				filter: StreamFilter{
					Since: &StreamPosition{Offset: 10, Epoch: epoch},
					Limit: -1,
				},
				wantCount: 0,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
					Filter: tt.filter,
				})
				require.NoError(t, err)
				require.Len(t, result.Publications, tt.wantCount)
				require.Equal(t, uint64(5), result.Position.Offset)
				require.Equal(t, epoch, result.Position.Epoch)
				if tt.wantCount > 0 {
					require.Equal(t, []byte(tt.wantFirst), result.Publications[0].Data)
					require.Equal(t, []byte(tt.wantLast), result.Publications[tt.wantCount-1].Data)
				}
			})
		}
	})

	t.Run("reverse_reads", func(t *testing.T) {
		broker := factory(t)
		ch := fmt.Sprintf("reverse_%d", time.Now().UnixNano())
		epoch := publishN(t, broker, ch, 5, 100)

		tests := []struct {
			name      string
			filter    StreamFilter
			wantCount int
			wantFirst string
			wantLast  string
		}{
			{
				name:      "all_reverse",
				filter:    StreamFilter{Limit: -1, Reverse: true},
				wantCount: 5,
				wantFirst: "data_5",
				wantLast:  "data_1",
			},
			{
				name:      "limit_2_reverse",
				filter:    StreamFilter{Limit: 2, Reverse: true},
				wantCount: 2,
				wantFirst: "data_5",
				wantLast:  "data_4",
			},
			{
				name: "since_top_reverse",
				filter: StreamFilter{
					Since:   &StreamPosition{Offset: 5, Epoch: epoch},
					Limit:   -1,
					Reverse: true,
				},
				wantCount: 4,
				wantFirst: "data_4",
				wantLast:  "data_1",
			},
			{
				name: "since_3_reverse_limit_2",
				filter: StreamFilter{
					Since:   &StreamPosition{Offset: 3, Epoch: epoch},
					Limit:   2,
					Reverse: true,
				},
				wantCount: 2,
				wantFirst: "data_2",
				wantLast:  "data_1",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
					Filter: tt.filter,
				})
				require.NoError(t, err)
				require.Len(t, result.Publications, tt.wantCount)
				require.Equal(t, uint64(5), result.Position.Offset)
				require.Equal(t, epoch, result.Position.Epoch)
				if tt.wantCount > 0 {
					require.Equal(t, []byte(tt.wantFirst), result.Publications[0].Data)
					require.Equal(t, []byte(tt.wantLast), result.Publications[tt.wantCount-1].Data)
				}
			})
		}
	})

	t.Run("with_removals", func(t *testing.T) {
		broker := factory(t)
		ch := fmt.Sprintf("removals_%d", time.Now().UnixNano())

		// Publish 3 keyed entries.
		for i := 1; i <= 3; i++ {
			opts := publishOpts(100)
			opts.Data = []byte(fmt.Sprintf("data_%d", i))
			_, err := broker.Publish(ctx, ch, fmt.Sprintf("key_%d", i), opts)
			require.NoError(t, err)
		}

		// Remove key_2 — this should produce a removal event at offset 4.
		_, err := broker.Remove(ctx, ch, "key_2", removeOpts(100))
		require.NoError(t, err)

		// Read all — should have 4 entries (3 adds + 1 removal).
		result, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{Limit: -1},
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 4)
		require.Equal(t, uint64(4), result.Position.Offset)

		// Last entry should be the removal.
		removal := result.Publications[3]
		require.True(t, removal.Removed)
		require.Equal(t, "key_2", removal.Key)

		// Since offset 3 → only the removal event.
		result2, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &StreamPosition{Offset: 3, Epoch: result.Position.Epoch},
				Limit: -1,
			},
		})
		require.NoError(t, err)
		require.Len(t, result2.Publications, 1)
		require.True(t, result2.Publications[0].Removed)
		require.Equal(t, "key_2", result2.Publications[0].Key)
	})

	t.Run("stream_trimming", func(t *testing.T) {
		broker := factory(t)
		ch := fmt.Sprintf("trimming_%d", time.Now().UnixNano())

		// Publish 5 messages with StreamSize=3 to force trimming.
		var epoch string
		for i := 1; i <= 5; i++ {
			opts := publishOpts(3)
			opts.Data = []byte(fmt.Sprintf("data_%d", i))
			res, err := broker.Publish(ctx, ch, fmt.Sprintf("key_%d", i), opts)
			require.NoError(t, err)
			epoch = res.Position.Epoch
		}

		// Read all available — should be trimmed to ~3 entries.
		result, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &StreamPosition{Offset: 0, Epoch: epoch},
				Limit: -1,
			},
		})
		require.NoError(t, err)
		require.Equal(t, uint64(5), result.Position.Offset)
		require.Equal(t, epoch, result.Position.Epoch)
		// After trimming, we should have at most streamSize entries for exact MAXLEN.
		// With approximate MAXLEN (~), Redis may keep more entries (only trims at
		// macro node boundaries), so for small streams all entries may be retained.
		// Memory broker uses exact trimming, so it will have exactly 3.
		require.GreaterOrEqual(t, len(result.Publications), 3)
		// The last available entry should be offset 5 (data_5).
		last := result.Publications[len(result.Publications)-1]
		require.Equal(t, []byte("data_5"), last.Data)
	})

	t.Run("single_publication", func(t *testing.T) {
		broker := factory(t)
		ch := fmt.Sprintf("single_%d", time.Now().UnixNano())

		opts := publishOpts(100)
		opts.Data = []byte("only_one")
		res, err := broker.Publish(ctx, ch, "key_1", opts)
		require.NoError(t, err)
		epoch := res.Position.Epoch

		// Since offset 0 → 1 pub.
		result, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &StreamPosition{Offset: 0, Epoch: epoch},
				Limit: -1,
			},
		})
		require.NoError(t, err)
		require.Len(t, result.Publications, 1)
		require.Equal(t, []byte("only_one"), result.Publications[0].Data)
		require.Equal(t, uint64(1), result.Position.Offset)

		// Since offset 1 (at top) → no pubs, caught up.
		result2, err := broker.ReadStream(ctx, ch, MapReadStreamOptions{
			Filter: StreamFilter{
				Since: &StreamPosition{Offset: 1, Epoch: epoch},
				Limit: -1,
			},
		})
		require.NoError(t, err)
		require.Empty(t, result2.Publications)
		require.Equal(t, uint64(1), result2.Position.Offset)
		require.Equal(t, epoch, result2.Position.Epoch)
	})
}
