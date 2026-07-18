package centrifuge

import (
	"sync"
	"sync/atomic"
	"time"
)

// testSharedTimerScheduler is a minimal TimerScheduler for tests. It runs
// scheduled callbacks on a small fixed pool of goroutines shared across all
// connections — the property that matters for exercising onTimerOp's offload
// path: a callback that blocks (e.g. a slow RefreshHandler) holds up other
// callbacks queued on the same worker, so without offloading it would delay
// unrelated connections' pings.
//
// It is intentionally simple (one time.AfterFunc per timer, round-robin worker
// dispatch) rather than a real batching timer wheel — that lives in Centrifugo
// PRO and is not part of the OSS tree.
type testSharedTimerScheduler struct {
	workers  []chan func()
	next     atomic.Uint64
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newTestSharedTimerScheduler(numWorkers int) *testSharedTimerScheduler {
	if numWorkers < 1 {
		numWorkers = 1
	}
	s := &testSharedTimerScheduler{
		workers: make([]chan func(), numWorkers),
		stopCh:  make(chan struct{}),
	}
	for i := range s.workers {
		s.workers[i] = make(chan func(), 4096)
		s.wg.Add(1)
		go func(ch chan func()) {
			defer s.wg.Done()
			for {
				select {
				case <-s.stopCh:
					return
				case fn := <-ch:
					fn()
				}
			}
		}(s.workers[i])
	}
	return s
}

func (s *testSharedTimerScheduler) ScheduleTimer(d time.Duration, cb func()) TimerCanceler {
	w := s.workers[int((s.next.Add(1)-1)%uint64(len(s.workers)))]
	c := &testTimerCanceler{}
	c.timer = time.AfterFunc(d, func() {
		if c.canceled.Load() {
			return
		}
		select {
		case w <- cb:
		case <-s.stopCh:
		}
	})
	return c
}

func (s *testSharedTimerScheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

type testTimerCanceler struct {
	timer    *time.Timer
	canceled atomic.Bool
}

func (c *testTimerCanceler) Cancel() {
	c.canceled.Store(true)
	if c.timer != nil {
		c.timer.Stop()
	}
}

// countingTransport is a minimal Transport that counts writes and reports a
// configurable ping/pong config. Used by the presence/runtime stress tests.
type countingTransport struct {
	writes       atomic.Int64
	pingInterval time.Duration
	pongTimeout  time.Duration
}

func (t *countingTransport) Name() string           { return "counting" }
func (t *countingTransport) AcceptProtocol() string { return "" }
func (t *countingTransport) Protocol() ProtocolType { return ProtocolTypeJSON }
func (t *countingTransport) ProtocolVersion() ProtocolVersion {
	return ProtocolVersion2
}
func (t *countingTransport) Unidirectional() bool      { return false }
func (t *countingTransport) Emulation() bool           { return false }
func (t *countingTransport) DisabledPushFlags() uint64 { return 0 }
func (t *countingTransport) PingPongConfig() PingPongConfig {
	return PingPongConfig{PingInterval: t.pingInterval, PongTimeout: t.pongTimeout}
}
func (t *countingTransport) Write(_ []byte) error {
	t.writes.Add(1)
	return nil
}
func (t *countingTransport) WriteMany(m ...[]byte) error {
	t.writes.Add(int64(len(m)))
	return nil
}
func (t *countingTransport) Close(_ Disconnect) error { return nil }

// noopPresenceManager is a PresenceManager whose operations do nothing, used by
// presence-tick benchmarks that measure tick overhead independent of a backend.
type noopPresenceManager struct{}

func (m *noopPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	return nil, nil
}
func (m *noopPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}
func (m *noopPresenceManager) AddPresence(_ string, _ string, _ *ClientInfo) error { return nil }
func (m *noopPresenceManager) RemovePresence(_ string, _ string, _ string) error   { return nil }
