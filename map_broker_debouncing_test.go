package centrifuge

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockMapBroker struct {
	mu         sync.Mutex
	publishes  []mockPublish
	removes    []mockRemove
	publishErr error
	removeErr  error
}

type mockPublish struct {
	ch   string
	key  string
	opts MapPublishOptions
}

type mockRemove struct {
	ch   string
	key  string
	opts MapRemoveOptions
}

func (m *mockMapBroker) RegisterEventHandler(BrokerEventHandler) error { return nil }
func (m *mockMapBroker) Subscribe(string) error                        { return nil }
func (m *mockMapBroker) Unsubscribe(string) error                      { return nil }
func (m *mockMapBroker) ReadStream(context.Context, string, MapReadStreamOptions) (MapStreamResult, error) {
	return MapStreamResult{}, nil
}
func (m *mockMapBroker) ReadState(context.Context, string, MapReadStateOptions) (MapStateResult, error) {
	return MapStateResult{}, nil
}
func (m *mockMapBroker) Stats(context.Context, string) (MapStats, error) {
	return MapStats{}, nil
}
func (m *mockMapBroker) Clear(context.Context, string, MapClearOptions) error { return nil }

func (m *mockMapBroker) Publish(_ context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishes = append(m.publishes, mockPublish{ch: ch, key: key, opts: opts})
	return MapPublishResult{}, m.publishErr
}

func (m *mockMapBroker) Remove(_ context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removes = append(m.removes, mockRemove{ch: ch, key: key, opts: opts})
	return MapPublishResult{}, m.removeErr
}

func (m *mockMapBroker) getPublishes() []mockPublish {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]mockPublish, len(m.publishes))
	copy(cp, m.publishes)
	return cp
}

func (m *mockMapBroker) getRemoves() []mockRemove {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]mockRemove, len(m.removes))
	copy(cp, m.removes)
	return cp
}

func TestDebouncingMapBrokerPassthrough(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 0 },
	})
	defer b.Close()

	ctx := context.Background()
	_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte("hello")})
	require.NoError(t, err)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
	require.Equal(t, "ch1", pubs[0].ch)
	require.Equal(t, "key1", pubs[0].key)
	require.Equal(t, []byte("hello"), pubs[0].opts.Data)
}

func TestDebouncingMapBrokerPassthroughNilDebounce(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{})
	defer b.Close()

	ctx := context.Background()
	_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte("hello")})
	require.NoError(t, err)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
}

func TestDebouncingMapBrokerCoalesce(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 50 * time.Millisecond },
	})
	defer b.Close()

	ctx := context.Background()

	// Rapid publishes — only the last should be forwarded.
	for i := 0; i < 10; i++ {
		_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte{byte(i)}})
		require.NoError(t, err)
	}

	// No publishes forwarded yet.
	require.Empty(t, mock.getPublishes())

	// Wait for debounce to fire.
	time.Sleep(100 * time.Millisecond)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
	require.Equal(t, []byte{9}, pubs[0].opts.Data) // last wins
}

func TestDebouncingMapBrokerContinuousPublish(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 50 * time.Millisecond },
	})
	defer b.Close()

	ctx := context.Background()

	// Simulate continuous cursor updates every 10ms for 200ms.
	// With throttle behavior (50ms interval), we expect ~4 flushes.
	for i := 0; i < 20; i++ {
		_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte{byte(i)}})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for final flush.
	time.Sleep(100 * time.Millisecond)

	pubs := mock.getPublishes()
	// Must have multiple flushes — not starved to 0 or 1.
	require.GreaterOrEqual(t, len(pubs), 2, "continuous publishing must produce periodic flushes")
}

func TestDebouncingMapBrokerRemoveCancelsPending(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 50 * time.Millisecond },
	})
	defer b.Close()

	ctx := context.Background()

	// Publish with debounce.
	_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte("data")})
	require.NoError(t, err)

	// Remove cancels the pending publish.
	_, err = b.Remove(ctx, "ch1", "key1", MapRemoveOptions{})
	require.NoError(t, err)

	// Wait past debounce window.
	time.Sleep(100 * time.Millisecond)

	// No publish should have been forwarded — only the remove.
	require.Empty(t, mock.getPublishes())
	require.Len(t, mock.getRemoves(), 1)
}

func TestDebouncingMapBrokerFlushThenRemove(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 10 * time.Millisecond },
	})
	defer b.Close()

	ctx := context.Background()

	_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte("data")})
	require.NoError(t, err)

	// Wait for flush.
	time.Sleep(50 * time.Millisecond)

	// Now remove — flush already fired.
	_, err = b.Remove(ctx, "ch1", "key1", MapRemoveOptions{})
	require.NoError(t, err)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
	require.Equal(t, []byte("data"), pubs[0].opts.Data)

	removes := mock.getRemoves()
	require.Len(t, removes, 1)
}

func TestDebouncingMapBrokerMultipleKeys(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 50 * time.Millisecond },
	})
	defer b.Close()

	ctx := context.Background()

	_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte("a")})
	require.NoError(t, err)
	_, err = b.Publish(ctx, "ch1", "key2", MapPublishOptions{Data: []byte("b")})
	require.NoError(t, err)

	// Update key1 — should coalesce.
	_, err = b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte("a2")})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 2)

	pubByKey := map[string][]byte{}
	for _, p := range pubs {
		pubByKey[p.key] = p.opts.Data
	}
	require.Equal(t, []byte("a2"), pubByKey["key1"])
	require.Equal(t, []byte("b"), pubByKey["key2"])
}

func TestDebouncingMapBrokerClose(t *testing.T) {
	mock := &mockMapBroker{}
	b := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 1 * time.Second },
	})

	ctx := context.Background()

	_, err := b.Publish(ctx, "ch1", "key1", MapPublishOptions{Data: []byte("data")})
	require.NoError(t, err)
	_, err = b.Publish(ctx, "ch1", "key2", MapPublishOptions{Data: []byte("data2")})
	require.NoError(t, err)

	// Close should cancel all pending timers.
	b.Close()

	time.Sleep(100 * time.Millisecond)

	// Nothing should have been forwarded.
	require.Empty(t, mock.getPublishes())
}

func BenchmarkDebouncingMapBrokerPassthrough(b *testing.B) {
	mock := &mockMapBroker{}
	broker := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 0 },
	})
	defer broker.Close()

	ctx := context.Background()
	opts := MapPublishOptions{Data: []byte("data")}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = broker.Publish(ctx, "ch1", "key1", opts)
	}
}

func BenchmarkDebouncingMapBrokerDebounce(b *testing.B) {
	mock := &mockMapBroker{}
	broker := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 1 * time.Hour }, // never fires during bench
	})
	defer broker.Close()

	ctx := context.Background()
	opts := MapPublishOptions{Data: []byte("data")}

	// Seed entry so subsequent calls hit the Reset path.
	_, _ = broker.Publish(ctx, "ch1", "key1", opts)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = broker.Publish(ctx, "ch1", "key1", opts)
	}
}

func BenchmarkDebouncingMapBrokerDebounceParallel(b *testing.B) {
	mock := &mockMapBroker{}
	broker := NewDebouncingMapBroker(mock, DebouncingMapBrokerConfig{
		Debounce: func(string) time.Duration { return 1 * time.Hour },
	})
	defer broker.Close()

	ctx := context.Background()
	opts := MapPublishOptions{Data: []byte("data")}

	var keyCounter atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own key to minimize contention.
		myKey := "key" + time.Now().String() + string(rune(keyCounter.Add(1)))
		_, _ = broker.Publish(ctx, "ch1", myKey, opts) // seed
		for pb.Next() {
			_, _ = broker.Publish(ctx, "ch1", myKey, opts)
		}
	})
}
