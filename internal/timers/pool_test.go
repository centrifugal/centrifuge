package timers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcquireTimer(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		// Empty pool.
		timer := AcquireTimer(0 * time.Second)
		require.NotNil(t, timer)

		// Stop timer.
		stopped := timer.Stop()
		require.True(t, stopped)
		timerPool.Put(timer)

		// Pool with timer.
		pooledTimer := AcquireTimer(0 * time.Second)
		require.Equal(t, timer, pooledTimer)
	})

	t.Run("Panic", func(t *testing.T) {
		t.Parallel()

		defer func() {
			r := recover()
			require.NotNil(t, r)
		}()

		timer := time.NewTimer(0 * time.Second)
		timerPool.Put(timer)
		AcquireTimer(0 * time.Second)
	})
}

func TestReleaseTimer(t *testing.T) {
	// Release stopped timer.
	timer := time.NewTimer(0 * time.Second)
	select {
	case <-timer.C:
	}
	ReleaseTimer(timer)
	pooledTimer := timerPool.Get()
	require.Nil(t, pooledTimer)

	// Release active timer.
	timer = time.NewTimer(0 * time.Second)
	ReleaseTimer(timer)
	pooledTimer = timerPool.Get()
	require.Equal(t, timer, pooledTimer)
}
