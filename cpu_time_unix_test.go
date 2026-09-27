//go:build unix

package centrifuge

import (
	"syscall"
	"time"
)

// processCPUTime returns the user and system CPU time used by the process.
func processCPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
