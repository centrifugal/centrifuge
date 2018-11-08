package centrifuge

import (
	"sync"
	"time"
)

var timerPool sync.Pool

func acquireTimer(d time.Duration) *time.Timer {
	v := timerPool.Get()
	if v == nil {
		return time.NewTimer(d)
	}

	tm := v.(*time.Timer)
	if tm.Reset(d) {
		panic("Received an active timer from the pool!")
	}
	return tm
}

func releaseTimer(tm *time.Timer) {
	if !tm.Stop() {
		// Timer is already stopped and possibly filled or will be filled with time in timer.C.
		// We could not guarantee that timer.C will not be filled even after timer.Stop().
		//
		// It is a known "bug" in golang:
		// See https://groups.google.com/forum/#!topic/golang-nuts/-8O3AknKpwk
		//
		// The tip from manual to read from timer.C possibly blocks caller if caller has already done <-timer.C.
		// Non-blocking read from timer.C with select does not help either because send is done concurrently
		// from another goroutine.
		return
	}
	timerPool.Put(tm)
}
