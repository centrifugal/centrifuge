package centrifuge

import (
	"runtime"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestChannelWriter_SizeFlushDoesNotLeakGoroutines guards the per-channel batch
// writer's timer lifecycle. When a batch reaches MaxSize before its MaxDelay
// timer fires, Add stops the timer — but the waitTimer goroutine is blocked on
// <-tm.C, which a stopped timer never delivers, so it must not be left hanging.
func TestChannelWriter_SizeFlushDoesNotLeakGoroutines(t *testing.T) {
	w := newChannelWriter(func([]queue.Item) error { return nil })
	config := ChannelBatchConfig{MaxSize: 4, MaxDelay: time.Second}

	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()

	const rounds = 200
	for r := 0; r < rounds; r++ {
		// The 4th item reaches MaxSize and flushes, cancelling the timer that the
		// 1st item started.
		for i := 0; i < 4; i++ {
			w.Add(queue.Item{FrameType: protocol.FrameTypePushPublication, Data: []byte("x")}, config)
		}
	}

	time.Sleep(150 * time.Millisecond) // let any exiting goroutines exit
	leaked := runtime.NumGoroutine() - base
	require.Less(t, leaked, rounds/4,
		"leaked ~%d goroutines over %d size-triggered flushes — waitTimer stays blocked on <-tm.C after the timer is stopped",
		leaked, rounds)
}
