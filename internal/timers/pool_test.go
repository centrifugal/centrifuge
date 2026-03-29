package timers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcquireReleaseTimer(t *testing.T) {
	tm := AcquireTimer(time.Hour)
	require.NotNil(t, tm)
	ReleaseTimer(tm)

	// Acquire again — should come from pool.
	tm2 := AcquireTimer(50 * time.Millisecond)
	require.NotNil(t, tm2)
	// Let it fire.
	<-tm2.C
	ReleaseTimer(tm2)
}

func TestAcquireTimerFresh(t *testing.T) {
	// First acquire creates a new timer (pool is empty).
	tm := AcquireTimer(time.Hour)
	require.NotNil(t, tm)
	ReleaseTimer(tm)
}
