package centrifuge

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsistentIndex(t *testing.T) {
	// Test basic distribution
	numBuckets := 5
	buckets := make(map[int]int)
	channels := []string{"ch1", "ch2", "ch3", "ch4", "ch5", "ch6", "ch7", "ch8", "ch9", "ch10"}

	for _, ch := range channels {
		idx := consistentIndex(ch, numBuckets)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, numBuckets)
		buckets[idx]++
	}

	// Should have at least 2 different buckets used
	require.GreaterOrEqual(t, len(buckets), 2)
}

func TestConsistentIndexStability(t *testing.T) {
	// Same channel should always map to same bucket
	numBuckets := 10
	channel := "test:channel:123"

	firstIdx := consistentIndex(channel, numBuckets)
	for i := 0; i < 100; i++ {
		idx := consistentIndex(channel, numBuckets)
		require.Equal(t, firstIdx, idx, "consistent hash should be stable")
	}
}

func TestConsistentIndexAddBucket(t *testing.T) {
	// When adding a bucket, most keys should stay in place
	oldBuckets := 10
	newBuckets := 11
	numChannels := 1000
	moved := 0

	for i := 0; i < numChannels; i++ {
		ch := "channel:" + string(rune(i))
		oldIdx := consistentIndex(ch, oldBuckets)
		newIdx := consistentIndex(ch, newBuckets)
		if oldIdx != newIdx {
			moved++
		}
	}

	// Jump consistent hash should only move ~1/(n+1) keys
	// With 10->11 buckets, expect ~9% to move (allowing some variance)
	movePercent := float64(moved) / float64(numChannels)
	require.Less(t, movePercent, 0.20, "too many keys moved when adding bucket")
}

func TestDefaultShardFunc(t *testing.T) {
	require.NotNil(t, DefaultShardFunc)
	idx := DefaultShardFunc("test", 5)
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, 5)
}

// mockBroker implements Broker interface for testing
type mockBroker struct {
	mu               sync.Mutex
	name             string
	publishCalls     []string
	subscribeCalls   []string
	unsubscribeCalls []string
	historyCalls     []string
	joinCalls        []string
	leaveCalls       []string
	removeHistoryCalls []string
	eventHandler     BrokerEventHandler
	closeCalled      bool
}

func newMockBroker(name string) *mockBroker {
	return &mockBroker{name: name}
}

func (m *mockBroker) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventHandler = h
	return nil
}

func (m *mockBroker) Subscribe(ch string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeCalls = append(m.subscribeCalls, ch)
	return nil
}

func (m *mockBroker) Unsubscribe(ch string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribeCalls = append(m.unsubscribeCalls, ch)
	return nil
}

func (m *mockBroker) Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishCalls = append(m.publishCalls, ch)
	return StreamPosition{Offset: 1, Epoch: "test"}, false, nil
}

func (m *mockBroker) PublishJoin(ch string, info *ClientInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.joinCalls = append(m.joinCalls, ch)
	return nil
}

func (m *mockBroker) PublishLeave(ch string, info *ClientInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaveCalls = append(m.leaveCalls, ch)
	return nil
}

func (m *mockBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.historyCalls = append(m.historyCalls, ch)
	return nil, StreamPosition{}, nil
}

func (m *mockBroker) RemoveHistory(ch string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeHistoryCalls = append(m.removeHistoryCalls, ch)
	return nil
}

func (m *mockBroker) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalled = true
	return nil
}

func (m *mockBroker) getPublishCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.publishCalls...)
}

func (m *mockBroker) getSubscribeCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.subscribeCalls...)
}

func TestShardedBrokerCreation(t *testing.T) {
	// No shards - error
	_, err := NewShardedBroker(ShardedBrokerConfig{Shards: nil})
	require.Error(t, err)

	_, err = NewShardedBroker(ShardedBrokerConfig{Shards: []Broker{}})
	require.Error(t, err)

	// Single shard - works
	b1 := newMockBroker("shard1")
	sb, err := NewShardedBroker(ShardedBrokerConfig{Shards: []Broker{b1}})
	require.NoError(t, err)
	require.NotNil(t, sb)
}

func TestShardedBrokerSharding(t *testing.T) {
	b1 := newMockBroker("shard1")
	b2 := newMockBroker("shard2")
	b3 := newMockBroker("shard3")

	sb, err := NewShardedBroker(ShardedBrokerConfig{
		Shards: []Broker{b1, b2, b3},
	})
	require.NoError(t, err)

	// Publish to various channels
	channels := []string{"ch1", "ch2", "ch3", "ch4", "ch5", "ch6"}
	for _, ch := range channels {
		_, _, err := sb.Publish(ch, []byte("data"), PublishOptions{})
		require.NoError(t, err)
	}

	// Verify operations were distributed across shards
	total := len(b1.getPublishCalls()) + len(b2.getPublishCalls()) + len(b3.getPublishCalls())
	require.Equal(t, len(channels), total)

	// At least 2 shards should have received calls
	used := 0
	if len(b1.getPublishCalls()) > 0 {
		used++
	}
	if len(b2.getPublishCalls()) > 0 {
		used++
	}
	if len(b3.getPublishCalls()) > 0 {
		used++
	}
	require.GreaterOrEqual(t, used, 2, "operations should be distributed across shards")
}

func TestShardedBrokerConsistentRouting(t *testing.T) {
	b1 := newMockBroker("shard1")
	b2 := newMockBroker("shard2")

	sb, err := NewShardedBroker(ShardedBrokerConfig{
		Shards: []Broker{b1, b2},
	})
	require.NoError(t, err)

	channel := "test:channel"

	// Multiple operations on same channel should go to same shard
	for i := 0; i < 5; i++ {
		_, _, err := sb.Publish(channel, []byte("data"), PublishOptions{})
		require.NoError(t, err)
	}

	// All 5 calls should be on one shard only
	b1Calls := len(b1.getPublishCalls())
	b2Calls := len(b2.getPublishCalls())

	require.True(t, (b1Calls == 5 && b2Calls == 0) || (b1Calls == 0 && b2Calls == 5),
		"all operations on same channel should go to same shard")
}

func TestShardedBrokerEventHandler(t *testing.T) {
	b1 := newMockBroker("shard1")
	b2 := newMockBroker("shard2")

	sb, err := NewShardedBroker(ShardedBrokerConfig{
		Shards: []Broker{b1, b2},
	})
	require.NoError(t, err)

	handler := &shardingTestEventHandler{}
	err = sb.RegisterBrokerEventHandler(handler)
	require.NoError(t, err)

	// Both shards should have the handler registered
	require.NotNil(t, b1.eventHandler)
	require.NotNil(t, b2.eventHandler)
}

func TestShardedBrokerClose(t *testing.T) {
	b1 := newMockBroker("shard1")
	b2 := newMockBroker("shard2")

	sb, err := NewShardedBroker(ShardedBrokerConfig{
		Shards: []Broker{b1, b2},
	})
	require.NoError(t, err)

	err = sb.Close(context.Background())
	require.NoError(t, err)

	// Both shards should be closed
	require.True(t, b1.closeCalled)
	require.True(t, b2.closeCalled)
}

func TestShardedBrokerAllMethods(t *testing.T) {
	b1 := newMockBroker("shard1")

	sb, err := NewShardedBroker(ShardedBrokerConfig{
		Shards: []Broker{b1},
	})
	require.NoError(t, err)

	ch := "test:channel"

	err = sb.Subscribe(ch)
	require.NoError(t, err)
	require.Equal(t, []string{ch}, b1.getSubscribeCalls())

	err = sb.Unsubscribe(ch)
	require.NoError(t, err)

	_, _, err = sb.Publish(ch, []byte("data"), PublishOptions{})
	require.NoError(t, err)

	err = sb.PublishJoin(ch, &ClientInfo{})
	require.NoError(t, err)

	err = sb.PublishLeave(ch, &ClientInfo{})
	require.NoError(t, err)

	_, _, err = sb.History(ch, HistoryOptions{})
	require.NoError(t, err)

	err = sb.RemoveHistory(ch)
	require.NoError(t, err)
}

func TestShardedBrokerCustomShardFunc(t *testing.T) {
	b1 := newMockBroker("shard1")
	b2 := newMockBroker("shard2")

	// Custom shard func that always returns 1
	customShardFunc := func(ch string, n int) int {
		return 1
	}

	sb, err := NewShardedBroker(ShardedBrokerConfig{
		Shards:    []Broker{b1, b2},
		ShardFunc: customShardFunc,
	})
	require.NoError(t, err)

	// All operations should go to shard 1 (b2)
	_, _, err = sb.Publish("ch1", []byte("data"), PublishOptions{})
	require.NoError(t, err)
	_, _, err = sb.Publish("ch2", []byte("data"), PublishOptions{})
	require.NoError(t, err)

	require.Equal(t, 0, len(b1.getPublishCalls()))
	require.Equal(t, 2, len(b2.getPublishCalls()))
}

// shardingTestEventHandler is a simple event handler for testing
type shardingTestEventHandler struct{}

func (h *shardingTestEventHandler) HandlePublication(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
	return nil
}

func (h *shardingTestEventHandler) HandleJoin(ch string, info *ClientInfo) error {
	return nil
}

func (h *shardingTestEventHandler) HandleLeave(ch string, info *ClientInfo) error {
	return nil
}

// mockPresenceManager implements PresenceManager interface for testing
type mockPresenceManager struct {
	mu              sync.Mutex
	name            string
	presenceCalls   []string
	addCalls        []string
	removeCalls     []string
	statsCalls      []string
	closeCalled     bool
}

func newMockPresenceManager(name string) *mockPresenceManager {
	return &mockPresenceManager{name: name}
}

func (m *mockPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.presenceCalls = append(m.presenceCalls, ch)
	return nil, nil
}

func (m *mockPresenceManager) PresenceStats(ch string) (PresenceStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statsCalls = append(m.statsCalls, ch)
	return PresenceStats{}, nil
}

func (m *mockPresenceManager) AddPresence(ch string, clientID string, info *ClientInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addCalls = append(m.addCalls, ch)
	return nil
}

func (m *mockPresenceManager) RemovePresence(ch string, clientID string, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeCalls = append(m.removeCalls, ch)
	return nil
}

func (m *mockPresenceManager) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalled = true
	return nil
}

func (m *mockPresenceManager) getPresenceCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.presenceCalls...)
}

func (m *mockPresenceManager) getAddCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.addCalls...)
}

func TestShardedPresenceManagerCreation(t *testing.T) {
	// No shards - error
	_, err := NewShardedPresenceManager(ShardedPresenceManagerConfig{Shards: nil})
	require.Error(t, err)

	// Single shard - works
	pm1 := newMockPresenceManager("shard1")
	spm, err := NewShardedPresenceManager(ShardedPresenceManagerConfig{Shards: []PresenceManager{pm1}})
	require.NoError(t, err)
	require.NotNil(t, spm)
}

func TestShardedPresenceManagerSharding(t *testing.T) {
	pm1 := newMockPresenceManager("shard1")
	pm2 := newMockPresenceManager("shard2")

	spm, err := NewShardedPresenceManager(ShardedPresenceManagerConfig{
		Shards: []PresenceManager{pm1, pm2},
	})
	require.NoError(t, err)

	// Operations on same channel should go to same shard
	ch := "test:channel"
	for i := 0; i < 3; i++ {
		_, err := spm.Presence(ch)
		require.NoError(t, err)
	}

	// All calls on one shard
	pm1Calls := len(pm1.getPresenceCalls())
	pm2Calls := len(pm2.getPresenceCalls())
	require.True(t, (pm1Calls == 3 && pm2Calls == 0) || (pm1Calls == 0 && pm2Calls == 3))
}

func TestShardedPresenceManagerAllMethods(t *testing.T) {
	pm1 := newMockPresenceManager("shard1")

	spm, err := NewShardedPresenceManager(ShardedPresenceManagerConfig{
		Shards: []PresenceManager{pm1},
	})
	require.NoError(t, err)

	ch := "test:channel"

	_, err = spm.Presence(ch)
	require.NoError(t, err)

	_, err = spm.PresenceStats(ch)
	require.NoError(t, err)

	err = spm.AddPresence(ch, "client1", &ClientInfo{})
	require.NoError(t, err)

	err = spm.RemovePresence(ch, "client1", "user1")
	require.NoError(t, err)
}

func TestShardedPresenceManagerClose(t *testing.T) {
	pm1 := newMockPresenceManager("shard1")
	pm2 := newMockPresenceManager("shard2")

	spm, err := NewShardedPresenceManager(ShardedPresenceManagerConfig{
		Shards: []PresenceManager{pm1, pm2},
	})
	require.NoError(t, err)

	err = spm.Close(context.Background())
	require.NoError(t, err)

	require.True(t, pm1.closeCalled)
	require.True(t, pm2.closeCalled)
}

// mockMapEngine implements MapEngine interface for testing
type mockMapEngine struct {
	mu              sync.Mutex
	name            string
	subscribeCalls  []string
	publishCalls    []string
	removeCalls     []string
	readStateCalls  []string
	readStreamCalls []string
	statsCalls      []string
	clearCalls      []string
	eventHandler    BrokerEventHandler
	closeCalled     bool
}

func newMockMapEngine(name string) *mockMapEngine {
	return &mockMapEngine{name: name}
}

func (m *mockMapEngine) Subscribe(ch string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribeCalls = append(m.subscribeCalls, ch)
	return nil
}

func (m *mockMapEngine) Unsubscribe(ch string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

func (m *mockMapEngine) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishCalls = append(m.publishCalls, ch)
	return MapPublishResult{}, nil
}

func (m *mockMapEngine) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeCalls = append(m.removeCalls, ch)
	return MapPublishResult{}, nil
}

func (m *mockMapEngine) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) ([]*Publication, StreamPosition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readStreamCalls = append(m.readStreamCalls, ch)
	return nil, StreamPosition{}, nil
}

func (m *mockMapEngine) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readStateCalls = append(m.readStateCalls, ch)
	return nil, StreamPosition{}, "", nil
}

func (m *mockMapEngine) Stats(ctx context.Context, ch string) (MapStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statsCalls = append(m.statsCalls, ch)
	return MapStats{}, nil
}

func (m *mockMapEngine) Clear(ctx context.Context, ch string, opts MapClearOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clearCalls = append(m.clearCalls, ch)
	return nil
}

func (m *mockMapEngine) RegisterEventHandler(h BrokerEventHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eventHandler = h
	return nil
}

func (m *mockMapEngine) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalled = true
	return nil
}

func (m *mockMapEngine) getPublishCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.publishCalls...)
}

func (m *mockMapEngine) getSubscribeCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.subscribeCalls...)
}

func TestShardedMapEngineCreation(t *testing.T) {
	// No shards - error
	_, err := NewShardedMapEngine(ShardedMapEngineConfig{Shards: nil})
	require.Error(t, err)

	// Single shard - works
	me1 := newMockMapEngine("shard1")
	sme, err := NewShardedMapEngine(ShardedMapEngineConfig{Shards: []MapEngine{me1}})
	require.NoError(t, err)
	require.NotNil(t, sme)
}

func TestShardedMapEngineSharding(t *testing.T) {
	me1 := newMockMapEngine("shard1")
	me2 := newMockMapEngine("shard2")

	sme, err := NewShardedMapEngine(ShardedMapEngineConfig{
		Shards: []MapEngine{me1, me2},
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Operations on same channel should go to same shard
	ch := "test:channel"
	for i := 0; i < 3; i++ {
		_, err := sme.Publish(ctx, ch, "key", MapPublishOptions{})
		require.NoError(t, err)
	}

	me1Calls := len(me1.getPublishCalls())
	me2Calls := len(me2.getPublishCalls())
	require.True(t, (me1Calls == 3 && me2Calls == 0) || (me1Calls == 0 && me2Calls == 3))
}

func TestShardedMapEngineEventHandler(t *testing.T) {
	me1 := newMockMapEngine("shard1")
	me2 := newMockMapEngine("shard2")

	sme, err := NewShardedMapEngine(ShardedMapEngineConfig{
		Shards: []MapEngine{me1, me2},
	})
	require.NoError(t, err)

	handler := &shardingTestEventHandler{}
	err = sme.RegisterEventHandler(handler)
	require.NoError(t, err)

	// Both shards should have the handler registered
	require.NotNil(t, me1.eventHandler)
	require.NotNil(t, me2.eventHandler)
}

func TestShardedMapEngineAllMethods(t *testing.T) {
	me1 := newMockMapEngine("shard1")

	sme, err := NewShardedMapEngine(ShardedMapEngineConfig{
		Shards: []MapEngine{me1},
	})
	require.NoError(t, err)

	ctx := context.Background()
	ch := "test:channel"

	err = sme.Subscribe(ch)
	require.NoError(t, err)

	err = sme.Unsubscribe(ch)
	require.NoError(t, err)

	_, err = sme.Publish(ctx, ch, "key", MapPublishOptions{})
	require.NoError(t, err)

	_, err = sme.Remove(ctx, ch, "key", MapRemoveOptions{})
	require.NoError(t, err)

	_, _, err = sme.ReadStream(ctx, ch, MapReadStreamOptions{})
	require.NoError(t, err)

	_, _, _, err = sme.ReadState(ctx, ch, MapReadStateOptions{})
	require.NoError(t, err)

	_, err = sme.Stats(ctx, ch)
	require.NoError(t, err)

	err = sme.Clear(ctx, ch, MapClearOptions{})
	require.NoError(t, err)
}

func TestShardedMapEngineClose(t *testing.T) {
	me1 := newMockMapEngine("shard1")
	me2 := newMockMapEngine("shard2")

	sme, err := NewShardedMapEngine(ShardedMapEngineConfig{
		Shards: []MapEngine{me1, me2},
	})
	require.NoError(t, err)

	err = sme.Close(context.Background())
	require.NoError(t, err)

	require.True(t, me1.closeCalled)
	require.True(t, me2.closeCalled)
}

// Test integration with real Memory implementations
func TestShardedBrokerWithMemoryBrokers(t *testing.T) {
	n1, _ := New(Config{LogHandler: func(entry LogEntry) {}})
	n2, _ := New(Config{LogHandler: func(entry LogEntry) {}})
	defer func() {
		_ = n1.Shutdown(context.Background())
		_ = n2.Shutdown(context.Background())
	}()

	b1, _ := NewMemoryBroker(n1, MemoryBrokerConfig{})
	b2, _ := NewMemoryBroker(n2, MemoryBrokerConfig{})

	sb, err := NewShardedBroker(ShardedBrokerConfig{
		Shards: []Broker{b1, b2},
	})
	require.NoError(t, err)

	// Register handler
	handler := &shardingTestEventHandler{}
	err = sb.RegisterBrokerEventHandler(handler)
	require.NoError(t, err)

	// Publish with history
	sp, _, err := sb.Publish("ch1", []byte("data"), PublishOptions{
		HistorySize: 10,
		HistoryTTL:  time.Minute,
	})
	require.NoError(t, err)
	require.NotZero(t, sp.Offset)

	// Get history
	pubs, _, err := sb.History("ch1", HistoryOptions{Filter: HistoryFilter{Limit: 10}})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
}

func TestShardedPresenceManagerWithMemoryManagers(t *testing.T) {
	n1, _ := New(Config{LogHandler: func(entry LogEntry) {}})
	n2, _ := New(Config{LogHandler: func(entry LogEntry) {}})
	defer func() {
		_ = n1.Shutdown(context.Background())
		_ = n2.Shutdown(context.Background())
	}()

	pm1, _ := NewMemoryPresenceManager(n1, MemoryPresenceManagerConfig{})
	pm2, _ := NewMemoryPresenceManager(n2, MemoryPresenceManagerConfig{})

	spm, err := NewShardedPresenceManager(ShardedPresenceManagerConfig{
		Shards: []PresenceManager{pm1, pm2},
	})
	require.NoError(t, err)

	ch := "test:channel"

	// Add presence
	err = spm.AddPresence(ch, "client1", &ClientInfo{ClientID: "client1", UserID: "user1"})
	require.NoError(t, err)

	// Get presence
	presence, err := spm.Presence(ch)
	require.NoError(t, err)
	require.Len(t, presence, 1)
	require.Equal(t, "client1", presence["client1"].ClientID)

	// Get stats
	stats, err := spm.PresenceStats(ch)
	require.NoError(t, err)
	require.Equal(t, 1, stats.NumClients)
	require.Equal(t, 1, stats.NumUsers)

	// Remove presence
	err = spm.RemovePresence(ch, "client1", "user1")
	require.NoError(t, err)

	// Verify removed
	presence, err = spm.Presence(ch)
	require.NoError(t, err)
	require.Len(t, presence, 0)
}

func TestShardedMapEngineWithMemoryEngines(t *testing.T) {
	n1, _ := New(Config{LogHandler: func(entry LogEntry) {}})
	n2, _ := New(Config{LogHandler: func(entry LogEntry) {}})
	defer func() {
		_ = n1.Shutdown(context.Background())
		_ = n2.Shutdown(context.Background())
	}()

	me1, _ := NewMemoryMapEngine(n1, MemoryMapEngineConfig{})
	me2, _ := NewMemoryMapEngine(n2, MemoryMapEngineConfig{})

	sme, err := NewShardedMapEngine(ShardedMapEngineConfig{
		Shards: []MapEngine{me1, me2},
	})
	require.NoError(t, err)

	// Register handler
	handler := &shardingTestEventHandler{}
	err = sme.RegisterEventHandler(handler)
	require.NoError(t, err)

	ctx := context.Background()
	ch := "test:channel"

	// Publish
	result, err := sme.Publish(ctx, ch, "key1", MapPublishOptions{
		Data:       []byte(`{"value": 1}`),
		StreamSize: 10,
		StreamTTL:  time.Minute,
		MetaTTL:    time.Minute,
	})
	require.NoError(t, err)
	require.False(t, result.Suppressed)

	// Read state
	pubs, _, _, err := sme.ReadState(ctx, ch, MapReadStateOptions{})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, "key1", pubs[0].Key)

	// Read stream
	streamPubs, _, err := sme.ReadStream(ctx, ch, MapReadStreamOptions{
		Filter: StreamFilter{Limit: 10},
	})
	require.NoError(t, err)
	require.Len(t, streamPubs, 1)

	// Stats
	stats, err := sme.Stats(ctx, ch)
	require.NoError(t, err)
	require.Equal(t, 1, stats.NumKeys)
}
