package centrifuge

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mockBroker struct {
	mu         sync.Mutex
	publishes  []mockBrokerPublish
	publishErr error
}

type mockBrokerPublish struct {
	ch   string
	data []byte
	opts PublishOptions
}

func (m *mockBroker) RegisterBrokerEventHandler(BrokerEventHandler) error { return nil }
func (m *mockBroker) Subscribe(string) error                              { return nil }
func (m *mockBroker) Unsubscribe(string) error                            { return nil }
func (m *mockBroker) PublishJoin(string, *ClientInfo) error               { return nil }
func (m *mockBroker) PublishLeave(string, *ClientInfo) error              { return nil }
func (m *mockBroker) History(string, HistoryOptions) ([]*Publication, StreamPosition, error) {
	return nil, StreamPosition{}, nil
}
func (m *mockBroker) RemoveHistory(string) error { return nil }

func (m *mockBroker) Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishes = append(m.publishes, mockBrokerPublish{ch: ch, data: data, opts: opts})
	return PublishResult{}, m.publishErr
}

func (m *mockBroker) getPublishes() []mockBrokerPublish {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]mockBrokerPublish, len(m.publishes))
	copy(cp, m.publishes)
	return cp
}

func TestDebouncingBrokerPassthrough(t *testing.T) {
	mock := &mockBroker{}
	b := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 0 },
	})
	defer b.Close()

	_, err := b.Publish("ch1", []byte("hello"), PublishOptions{Key: "key1"})
	require.NoError(t, err)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
	require.Equal(t, "ch1", pubs[0].ch)
	require.Equal(t, []byte("hello"), pubs[0].data)
	require.Equal(t, "key1", pubs[0].opts.Key)
}

func TestDebouncingBrokerPassthroughNilDebounce(t *testing.T) {
	mock := &mockBroker{}
	b := NewDebouncingBroker(mock, DebouncingBrokerConfig{})
	defer b.Close()

	_, err := b.Publish("ch1", []byte("hello"), PublishOptions{Key: "key1"})
	require.NoError(t, err)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
}

func TestDebouncingBrokerCoalesce(t *testing.T) {
	mock := &mockBroker{}
	b := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 50 * time.Millisecond },
	})
	defer b.Close()

	for i := 0; i < 10; i++ {
		_, err := b.Publish("ch1", []byte{byte(i)}, PublishOptions{Key: "key1"})
		require.NoError(t, err)
	}

	require.Empty(t, mock.getPublishes())

	time.Sleep(100 * time.Millisecond)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
	require.Equal(t, []byte{9}, pubs[0].data)
}

func TestDebouncingBrokerRemovedCancelsPending(t *testing.T) {
	mock := &mockBroker{}
	b := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 50 * time.Millisecond },
	})
	defer b.Close()

	// Publish with debounce.
	_, err := b.Publish("ch1", []byte("data"), PublishOptions{Key: "key1"})
	require.NoError(t, err)

	// Publish with Removed=true cancels the pending publish.
	_, err = b.Publish("ch1", nil, PublishOptions{Key: "key1", Removed: true})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Only the removal should have been forwarded.
	pubs := mock.getPublishes()
	require.Len(t, pubs, 1)
	require.True(t, pubs[0].opts.Removed)
}

func TestDebouncingBrokerFlushThenRemoved(t *testing.T) {
	mock := &mockBroker{}
	b := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 10 * time.Millisecond },
	})
	defer b.Close()

	_, err := b.Publish("ch1", []byte("data"), PublishOptions{Key: "key1"})
	require.NoError(t, err)

	// Wait for flush.
	time.Sleep(50 * time.Millisecond)

	// Now remove — flush already fired.
	_, err = b.Publish("ch1", nil, PublishOptions{Key: "key1", Removed: true})
	require.NoError(t, err)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 2)
	require.Equal(t, []byte("data"), pubs[0].data)
	require.True(t, pubs[1].opts.Removed)
}

func TestDebouncingBrokerMultipleKeys(t *testing.T) {
	mock := &mockBroker{}
	b := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 50 * time.Millisecond },
	})
	defer b.Close()

	_, err := b.Publish("ch1", []byte("a"), PublishOptions{Key: "key1"})
	require.NoError(t, err)
	_, err = b.Publish("ch1", []byte("b"), PublishOptions{Key: "key2"})
	require.NoError(t, err)

	// Update key1 — should coalesce.
	_, err = b.Publish("ch1", []byte("a2"), PublishOptions{Key: "key1"})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	pubs := mock.getPublishes()
	require.Len(t, pubs, 2)

	pubByKey := map[string][]byte{}
	for _, p := range pubs {
		pubByKey[p.opts.Key] = p.data
	}
	require.Equal(t, []byte("a2"), pubByKey["key1"])
	require.Equal(t, []byte("b"), pubByKey["key2"])
}

func TestDebouncingBrokerClose(t *testing.T) {
	mock := &mockBroker{}
	b := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 1 * time.Second },
	})

	_, err := b.Publish("ch1", []byte("data"), PublishOptions{Key: "key1"})
	require.NoError(t, err)
	_, err = b.Publish("ch1", []byte("data2"), PublishOptions{Key: "key2"})
	require.NoError(t, err)

	b.Close()

	time.Sleep(100 * time.Millisecond)

	require.Empty(t, mock.getPublishes())
}

func BenchmarkDebouncingBrokerPassthrough(b *testing.B) {
	mock := &mockBroker{}
	broker := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 0 },
	})
	defer broker.Close()

	data := []byte("data")
	opts := PublishOptions{Key: "key1"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = broker.Publish("ch1", data, opts)
	}
}

func BenchmarkDebouncingBrokerDebounce(b *testing.B) {
	mock := &mockBroker{}
	broker := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 1 * time.Hour },
	})
	defer broker.Close()

	data := []byte("data")
	opts := PublishOptions{Key: "key1"}

	_, _ = broker.Publish("ch1", data, opts) // seed

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = broker.Publish("ch1", data, opts)
	}
}

func BenchmarkDebouncingBrokerDebounceParallel(b *testing.B) {
	mock := &mockBroker{}
	broker := NewDebouncingBroker(mock, DebouncingBrokerConfig{
		Debounce: func(string) time.Duration { return 1 * time.Hour },
	})
	defer broker.Close()

	data := []byte("data")

	var keyCounter atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		myKey := "key" + time.Now().String() + string(rune(keyCounter.Add(1)))
		opts := PublishOptions{Key: myKey}
		_, _ = broker.Publish("ch1", data, opts) // seed
		for pb.Next() {
			_, _ = broker.Publish("ch1", data, opts)
		}
	})
}
