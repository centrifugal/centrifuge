package centrifuge

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// trackingPresenceManager records live presence entries so leaks (an entry
// re-added by a presence tick after close removed it) can be detected, and can
// simulate PresenceManager latency.
type trackingPresenceManager struct {
	mu      sync.Mutex
	entries map[string]map[string]struct{} // channel -> clientID
	rtt     time.Duration
	adds    atomic.Int64
	removes atomic.Int64
}

func newTrackingPresenceManager(rtt time.Duration) *trackingPresenceManager {
	return &trackingPresenceManager{
		entries: map[string]map[string]struct{}{},
		rtt:     rtt,
	}
}

func (m *trackingPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	return nil, nil
}
func (m *trackingPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}
func (m *trackingPresenceManager) AddPresence(ch string, clientID string, _ *ClientInfo) error {
	if m.rtt > 0 {
		time.Sleep(m.rtt)
	}
	m.adds.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries[ch] == nil {
		m.entries[ch] = map[string]struct{}{}
	}
	m.entries[ch][clientID] = struct{}{}
	return nil
}
func (m *trackingPresenceManager) RemovePresence(ch string, clientID string, _ string) error {
	m.removes.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.entries[ch]; ok {
		delete(s, clientID)
		if len(s) == 0 {
			delete(m.entries, ch)
		}
	}
	return nil
}
func (m *trackingPresenceManager) liveEntries() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.entries {
		n += len(s)
	}
	return n
}

type stressOpts struct {
	useWheel    bool
	rtt         time.Duration
	numWorkers  int
	numChannels int
	duration    time.Duration
	concurrency int
	positioning bool
	// explicitUnsub races an explicit unsubscribe() against an in-flight presence
	// tick. Unlike close(), unsubscribe() does not take presenceMu, so a tick
	// that already passed its c.channels[ch] check can re-add presence after
	// unsubscribe removed it. That resurrection race is PRE-EXISTING on master
	// (verified by A/B) and is not what these tests are guarding, so it is off by
	// default. See TestPresenceStress_KnownUnsubscribeRace.
	explicitUnsub bool
}

// runPresenceStress hammers connect/subscribe/unsubscribe/disconnect against a
// slow PresenceManager while presence ticks run, exercising the presenceInFlight
// guard and the closing flag under contention.
func runPresenceStress(t *testing.T, o stressOpts) {
	pm := newTrackingPresenceManager(o.rtt)
	cfg := Config{
		LogLevel:           LogLevelError,
		LogHandler:         func(entry LogEntry) {},
		ClientChannelLimit: 1000,
		// Deliberately far shorter than the time a tick takes with o.rtt, so
		// ticks overlap and the in-flight guard is exercised hard.
		ClientPresenceUpdateInterval:    5 * time.Millisecond,
		clientPresenceUpdateConcurrency: o.concurrency,
		clientPositionCheckConcurrency:  o.concurrency,
	}
	if o.positioning {
		// Force the position-check path to actually run on every tick, so the
		// concurrent dutyPosition workers are exercised under churn.
		cfg.ClientChannelPositionCheckDelay = time.Millisecond
	}
	var wheel *ShardedTimerWheel
	if o.useWheel {
		wheel = NewShardedTimerWheel(4, 256, 10*time.Millisecond, 4)
		cfg.ClientTimerScheduler = wheel
	}
	node, err := New(cfg)
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true, EnablePositioning: o.positioning}}, nil)
		})
	})
	require.NoError(t, node.Run())
	defer func() {
		_ = node.Shutdown(context.Background())
		if wheel != nil {
			wheel.Stop()
		}
	}()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var cycles atomic.Int64

	for w := 0; w < o.numWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithCancel(context.Background())
				tr := &countingTransport{pingInterval: 20 * time.Millisecond, pongTimeout: -1}
				newCtx := SetCredentials(ctx, &Credentials{UserID: "u" + strconv.Itoa(w)})
				client, err := newClient(newCtx, node, tr)
				if err != nil {
					cancel()
					return
				}
				rw := testReplyWriterWrapper()
				if err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw); err != nil {
					cancel()
					return
				}
				client.triggerConnect()
				client.scheduleOnConnectTimers()
				for i := 0; i < o.numChannels; i++ {
					srw := testReplyWriterWrapper()
					_ = client.handleSubscribe(&protocol.SubscribeRequest{
						Channel: "ch" + strconv.Itoa(w) + "_" + strconv.Itoa(i),
					}, &protocol.Command{Id: 1}, time.Now(), srw.rw)
				}
				// Live long enough for several presence ticks to start.
				time.Sleep(time.Duration(rnd.Intn(20)+5) * time.Millisecond)
				if o.explicitUnsub && rnd.Intn(2) == 0 && o.numChannels > 0 {
					_ = client.unsubscribe("ch"+strconv.Itoa(w)+"_0", unsubscribeClient, nil)
				}
				_ = client.close(DisconnectForceNoReconnect)
				cancel()
				cycles.Add(1)
			}
		}(w)
	}

	time.Sleep(o.duration)
	close(stop)
	wg.Wait()

	// All clients closed: presence entries must not be left behind. A leak here
	// means a presence tick re-added an entry after close removed it.
	require.Eventually(t, func() bool {
		return pm.liveEntries() == 0
	}, 5*time.Second, 20*time.Millisecond,
		"presence entries leaked after all clients closed: %d live (adds=%d removes=%d)",
		pm.liveEntries(), pm.adds.Load(), pm.removes.Load())

	require.Zero(t, node.hub.NumClients(), "clients left in hub after close")
	require.Greater(t, cycles.Load(), int64(0), "stress did no work")
	t.Logf("wheel=%v rtt=%v: %d connect/subscribe/close cycles, presence adds=%d removes=%d, no leaks",
		o.useWheel, o.rtt, cycles.Load(), pm.adds.Load(), pm.removes.Load())
}

func TestPresenceStress_Runtime(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_Wheel(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: 2 * time.Millisecond,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

// Zero-latency variant maximizes tick throughput and lock churn.
func TestPresenceStress_Runtime_FastPM(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 0,
		numWorkers: 32, numChannels: 4, duration: 3 * time.Second,
	})
}

// TestPresenceStress_UnsubscribeRace covers the presence resurrection race: an
// explicit unsubscribe() does not take presenceMu, so a presence tick already
// past its c.channels[ch] existence check can call AddPresence after unsubscribe
// removed the entry, leaving it to linger until PresenceTTL (60s by default).
//
// This race pre-dates the presence tick changes — A/B against master leaked
// 78/55 entries here — and is fixed by the post-add re-check in
// updateChannelPresence (see removeRacedPresence).
func TestPresenceStress_UnsubscribeRace(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_UnsubscribeRace_Wheel(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: 2 * time.Millisecond, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

// Concurrent presence updates: several updates per connection are in flight at
// once, so the unsubscribe race, the closing bail and the compensation all run
// against parallel workers touching the same snapshot.
func TestPresenceStress_Concurrent(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond, concurrency: 8,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_Concurrent_UnsubscribeRace(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: false, rtt: 2 * time.Millisecond, concurrency: 8, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

func TestPresenceStress_Concurrent_Wheel(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: 2 * time.Millisecond, concurrency: 8, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}

// TestPresenceStress_KitchenSink turns on everything at once: the timer wheel
// (offloaded ticks), presence + position concurrency, and an explicit
// unsubscribe race, under churn. It is the broadest -race exercise of the tick
// machinery: presence workers, position workers, the closing bail, and
// compensation all run concurrently against connect/subscribe/unsubscribe/close.
func TestPresenceStress_KitchenSink(t *testing.T) {
	runPresenceStress(t, stressOpts{
		useWheel: true, rtt: time.Millisecond, concurrency: 8,
		positioning: true, explicitUnsub: true,
		numWorkers: 16, numChannels: 8, duration: 3 * time.Second,
	})
}
