package centrifuge

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// slowPresenceManager simulates a Redis PresenceManager with a fixed RTT.
type slowPresenceManager struct {
	rtt   time.Duration
	calls atomic.Int64
}

func (m *slowPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	return nil, nil
}
func (m *slowPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}
func (m *slowPresenceManager) AddPresence(_ string, _ string, _ *ClientInfo) error {
	m.calls.Add(1)
	time.Sleep(m.rtt)
	return nil
}
func (m *slowPresenceManager) RemovePresence(_ string, _ string, _ string) error { return nil }

func newSlowPresenceNode(t *testing.T, pm PresenceManager, presenceIvl time.Duration) *Node {
	node, err := New(Config{
		LogLevel:                     LogLevelError,
		LogHandler:                   func(entry LogEntry) {},
		ClientChannelLimit:           1000,
		ClientPresenceUpdateInterval: presenceIvl,
	})
	require.NoError(t, err)
	node.SetPresenceManager(pm)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
		})
	})
	require.NoError(t, node.Run())
	return node
}

// TestPresenceLoop_DelaysOwnPing proves the serial per-channel presence loop
// delays the connection's own ping, because ping and presence share one timer
// and updatePresence only re-arms it after every Redis round trip has drained.
func TestPresenceLoop_DelaysOwnPing(t *testing.T) {
	const numChannels = 200
	const rtt = 2 * time.Millisecond
	const pingInterval = 300 * time.Millisecond

	pm := &slowPresenceManager{rtt: rtt}
	node := newSlowPresenceNode(t, pm, 400*time.Millisecond)
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	transport.setProtocolType(ProtocolTypeJSON)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setPing(pingInterval, 0)
	sink := make(chan []byte, 4096)
	transport.setSink(sink)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newTestConnectedClientWithTransport(t, ctx, node, transport, "user")
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(t, client, "ch"+strconv.Itoa(i))
	}

	var pingTimes []time.Time
	done := time.After(3 * time.Second)
loop:
	for {
		select {
		case data := <-sink:
			if string(data) == "{}\n" || string(data) == "{}" {
				pingTimes = append(pingTimes, time.Now())
			}
		case <-done:
			break loop
		}
	}

	require.Greater(t, len(pingTimes), 2, "expected several pings")
	var maxGap time.Duration
	for i := 1; i < len(pingTimes); i++ {
		if gap := pingTimes[i].Sub(pingTimes[i-1]); gap > maxGap {
			maxGap = gap
		}
	}
	t.Logf("ping interval %v, %d channels @ %v RTT, max observed ping gap: %v (%.2fx)",
		pingInterval, numChannels, rtt, maxGap, float64(maxGap)/float64(pingInterval))

	require.Less(t, maxGap, time.Duration(float64(pingInterval)*1.5),
		"ping was delayed by the serial presence loop")
}

// TestPresenceLoop_BlocksClose proves close() waits for the whole presence loop
// because both take presenceMu, delaying connection cleanup by N*RTT.
func TestPresenceLoop_BlocksClose(t *testing.T) {
	const numChannels = 200
	const rtt = 2 * time.Millisecond

	pm := &slowPresenceManager{rtt: rtt}
	node := newSlowPresenceNode(t, pm, 25*time.Second)
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestConnectedClientV2(t, node, "user")
	for i := 0; i < numChannels; i++ {
		subscribeClientV2(t, client, "ch"+strconv.Itoa(i))
	}

	closeBlocked := make(chan time.Duration, 1)
	go func() {
		time.Sleep(20 * time.Millisecond) // let the presence loop get going
		s := time.Now()
		_ = client.close(DisconnectForceNoReconnect)
		closeBlocked <- time.Since(s)
	}()
	loopStart := time.Now()
	client.updatePresence()
	loopDuration := time.Since(loopStart)
	blocked := <-closeBlocked

	t.Logf("presence loop took %v; close() blocked for %v", loopDuration, blocked)
	require.Less(t, blocked, 50*time.Millisecond,
		"close() was blocked waiting for the presence loop to drain")
}
