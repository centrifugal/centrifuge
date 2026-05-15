package centrifuge

import "time"

// init shrinks the ping/pong defaults to test-friendly values so tests that
// pay the "wait past the default disconnect window" tax (e.g. verifying that
// PingPongConfig{-1, 0} actually disables pings rather than silently falling
// back to 25s) finish in milliseconds instead of tens of seconds.
//
// Production builds use the literals defined in config.go (25s / 10s). This
// override applies only to the test binary because _test.go files are not
// included in non-test builds.
func init() {
	defaultPingInterval = 1000 * time.Millisecond
	defaultPongTimeout = 500 * time.Millisecond
}
