package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestHistoryUnrecoverablePositionKeepsResult pins a subtle contract of
// Node.history: it returns a POPULATED HistoryResult together with
// ErrorUnrecoverablePosition, and recovery depends on the StreamPosition in it.
// The single flight branch must not swallow the result when it returns an error.
//
// Both modes are asserted because single flight routes through a different code
// path (historySingleFlight) that unwraps the result from singleflight.Group.
func TestHistoryUnrecoverablePositionKeepsResult(t *testing.T) {
	t.Parallel()
	for _, singleFlight := range []bool{false, true} {
		name := "singleflight=false"
		if singleFlight {
			name = "singleflight=true"
		}
		t.Run(name, func(t *testing.T) {
			node := defaultNodeNoHandlers()
			node.config.UseSingleFlight = singleFlight
			defer func() { _ = node.Shutdown(context.Background()) }()

			_, err := node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
			require.NoError(t, err)

			// Epoch mismatch makes the position unrecoverable.
			res, err := node.History("test", WithLimit(1), WithSince(&StreamPosition{
				Offset: 0,
				Epoch:  "definitely-not-the-current-epoch",
			}))
			require.ErrorIs(t, err, ErrorUnrecoverablePosition)
			require.NotEmpty(t, res.StreamPosition.Epoch,
				"StreamPosition must survive alongside ErrorUnrecoverablePosition")
			require.Equal(t, uint64(1), res.StreamPosition.Offset)
		})
	}
}

// TestStreamTopMatchesHistory verifies streamTop, which no longer goes through
// Node.History's variadic options, still returns the same stream position as
// History does — in both single flight modes.
func TestStreamTopMatchesHistory(t *testing.T) {
	t.Parallel()
	for _, singleFlight := range []bool{false, true} {
		name := "singleflight=false"
		if singleFlight {
			name = "singleflight=true"
		}
		t.Run(name, func(t *testing.T) {
			node := defaultNodeNoHandlers()
			node.config.UseSingleFlight = singleFlight
			defer func() { _ = node.Shutdown(context.Background()) }()

			for i := 0; i < 3; i++ {
				_, err := node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
				require.NoError(t, err)
			}

			viaHistory, err := node.History("test", WithHistoryMetaTTL(time.Minute))
			require.NoError(t, err)

			top, err := node.streamTop("test", time.Minute)
			require.NoError(t, err)

			require.Equal(t, viaHistory.StreamPosition, top,
				"streamTop must agree with History on the stream position")
			require.Equal(t, uint64(3), top.Offset)
		})
	}
}

// TestStreamTopIsAllocationFree guards the periodic position check path: it runs
// for every positioned channel of every connection on every check, so it must
// not allocate. Node.History's variadic HistoryOption closures force
// HistoryOptions to escape, which is why streamTop builds them by value.
func TestStreamTopIsAllocationFree(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	_, err := node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
	require.NoError(t, err)

	// Warm up any lazily initialized state (metrics label combinations etc).
	for i := 0; i < 100; i++ {
		_, err := node.streamTop("test", time.Minute)
		require.NoError(t, err)
	}

	allocs := testing.AllocsPerRun(200, func() {
		_, _ = node.streamTop("test", time.Minute)
	})
	require.Zero(t, allocs, "streamTop must not allocate, got %v allocs/op", allocs)
}
