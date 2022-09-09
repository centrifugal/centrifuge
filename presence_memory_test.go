package centrifuge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func testMemoryPresenceManager(t testing.TB) *MemoryPresenceManager {
	n, _ := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
	})
	require.NoError(t, n.Run())
	m, _ := NewMemoryPresenceManager(n, MemoryPresenceManagerConfig{})
	return m
}

func TestNewMemoryPresenceManager_RemovePresence(t *testing.T) {
	m := testMemoryPresenceManager(t)
	defer func() { _ = m.node.Shutdown(context.Background()) }()

	require.NotEqual(t, nil, m.presenceHub)
	require.NoError(t, m.AddPresence("channel", "uid", &ClientInfo{}))
	p, err := m.Presence("channel")
	require.NoError(t, err)
	require.Equal(t, 1, len(p))
	require.NoError(t, m.RemovePresence("channel", "uid"))
	p, err = m.Presence("channel")
	require.NoError(t, err)
	require.Equal(t, 0, len(p))
}

func TestMemoryPresenceHub(t *testing.T) {
	h := newPresenceHub()
	require.Equal(t, 0, len(h.presence))

	testCh1 := "channel1"
	testCh2 := "channel2"
	uid := "uid"

	info := &ClientInfo{
		UserID:   "user",
		ClientID: "client",
	}

	_ = h.add(testCh1, uid, info)
	require.Equal(t, 1, len(h.presence))

	_ = h.add(testCh2, uid, info)
	require.Equal(t, 2, len(h.presence))

	stats, err := h.getStats(testCh1)
	require.NoError(t, err)
	require.Equal(t, 1, stats.NumClients)
	require.Equal(t, 1, stats.NumUsers)

	// stats for unknown channel must not fail.
	stats, err = h.getStats("unknown_channel")
	require.NoError(t, err)
	require.Equal(t, 0, stats.NumClients)
	require.Equal(t, 0, stats.NumUsers)

	// remove non existing client ID must not fail.
	err = h.remove(testCh1, "unknown_client_id")
	require.NoError(t, err)

	// valid remove.
	err = h.remove(testCh1, uid)
	require.NoError(t, err)

	// remove non existing channel must not fail.
	err = h.remove(testCh1, uid)
	require.NoError(t, err)

	require.Equal(t, 1, len(h.presence))
	p, err := h.get(testCh1)
	require.NoError(t, err)
	require.Equal(t, 0, len(p))

	p, err = h.get(testCh2)
	require.NoError(t, err)
	require.Equal(t, 1, len(p))
}

func BenchmarkMemoryAddPresence_OneChannel(b *testing.B) {
	e := testMemoryPresenceManager(b)
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := e.AddPresence("channel", "uid", &ClientInfo{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryAddPresence_OneChannel_Parallel(b *testing.B) {
	e := testMemoryPresenceManager(b)
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := e.AddPresence("channel", "uid", &ClientInfo{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMemoryPresence_OneChannel(b *testing.B) {
	e := testMemoryPresenceManager(b)
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	_ = e.AddPresence("channel", "uid", &ClientInfo{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.Presence("channel")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryPresence_OneChannel_Parallel(b *testing.B) {
	e := testMemoryPresenceManager(b)
	defer func() { _ = e.node.Shutdown(context.Background()) }()

	_ = e.AddPresence("channel", "uid", &ClientInfo{})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := e.Presence("channel")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
