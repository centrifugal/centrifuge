package timers

import (
	"sync"
	"time"
)

var timerPool sync.Pool

// AcquireTimer from pool.
func AcquireTimer(d time.Duration) *time.Timer {
	v := timerPool.Get()
	if v == nil {
		return time.NewTimer(d)
	}
	tm := v.(*time.Timer)
	if tm.Reset(d) {
		// Timer was still active, create a new one instead
		// This should not happen in normal operation, but handle it gracefully
		return time.NewTimer(d)
	}
	return tm
}

// ReleaseTimer to pool.
func ReleaseTimer(tm *time.Timer) {
	if !tm.Stop() {
		// Collect possibly added time from the channel
		// If timer has been stopped and nobody collected its value.
		select {
		case <-tm.C:
		default:
		}
	}
	timerPool.Put(tm)
}
