package centrifuge

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper function to create a channelMedium with options.
func setupChannelMedium(t testing.TB, options ChannelMediumOptions, node nodeSubset) *channelMedium {
	t.Helper()
	channel := "testChannel"
	cache, err := newChannelMedium(channel, node, options)
	if err != nil {
		require.NoError(t, err)
	}
	return cache
}

type mockNode struct {
	handlePublicationFunc func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error
	streamTopFunc         func(ch string, historyMetaTTL time.Duration) (StreamPosition, error)
	mapStreamTopFunc      func(ch string) (StreamPosition, error)
}

func (m *mockNode) handlePublication(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
	if m.handlePublicationFunc != nil {
		return m.handlePublicationFunc(channel, sp, pub, prevPub, localPrevPub)
	}
	return nil
}

func (m *mockNode) streamTop(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
	if m.streamTopFunc != nil {
		return m.streamTopFunc(ch, historyMetaTTL)
	}
	return StreamPosition{}, nil
}

func (m *mockNode) mapStreamTop(ch string) (StreamPosition, error) {
	if m.mapStreamTopFunc != nil {
		return m.mapStreamTopFunc(ch)
	}
	return StreamPosition{}, nil
}

func TestChannelMediumHandlePublication(t *testing.T) {
	t.Parallel()
	var testCases = []struct {
		numPublications int
		options         ChannelMediumOptions
	}{
		{
			numPublications: 10,
			options: ChannelMediumOptions{
				enableQueue:           false,
				KeepLatestPublication: false,
			},
		},
		{
			numPublications: 10,
			options: ChannelMediumOptions{
				enableQueue:           true,
				KeepLatestPublication: false,
			},
		},
		{
			numPublications: 1,
			options: ChannelMediumOptions{
				enableQueue:           true,
				KeepLatestPublication: false,
				broadcastDelay:        10 * time.Millisecond,
			},
		},
		{
			numPublications: 1,
			options: ChannelMediumOptions{
				enableQueue:           true,
				KeepLatestPublication: true,
				broadcastDelay:        10 * time.Millisecond,
			},
		},
	}

	for i, tt := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			numPublications := tt.numPublications

			doneCh := make(chan struct{}, numPublications)

			cache := setupChannelMedium(t, tt.options, &mockNode{
				handlePublicationFunc: func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
					doneCh <- struct{}{}
					return nil
				},
			})

			pub := &Publication{Data: []byte("test data")}
			sp := StreamPosition{Offset: 1}

			for i := 0; i < numPublications; i++ {
				cache.broadcastPublication(pub, sp, false, nil)
			}

			for i := 0; i < numPublications; i++ {
				select {
				case <-doneCh:
				case <-time.After(5 * time.Second):
					require.Fail(t, "handlePublicationFunc was not called")
				}
			}
		})
	}
}

func TestChannelMediumInsufficientState(t *testing.T) {
	t.Parallel()
	options := ChannelMediumOptions{
		enableQueue:           true,
		KeepLatestPublication: true,
	}
	doneCh := make(chan struct{})
	medium := setupChannelMedium(t, options, &mockNode{
		handlePublicationFunc: func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
			require.Equal(t, uint64(math.MaxUint64), pub.Offset)
			require.Equal(t, uint64(math.MaxUint64), sp.Offset)
			close(doneCh)
			return nil
		},
	})

	// Simulate the behavior when the state is marked as insufficient
	medium.broadcastInsufficientState()

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "handlePublicationFunc was not called")
	}
}

func TestChannelMediumPositionSync(t *testing.T) {
	t.Parallel()
	options := ChannelMediumOptions{
		SharedPositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	medium := setupChannelMedium(t, options, &mockNode{
		streamTopFunc: func(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
			closeOnce.Do(func() {
				close(doneCh)
			})
			return StreamPosition{}, nil
		},
	})
	medium.nowFn = func() time.Time { return time.Now().Add(time.Hour) }
	medium.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "historyFunc was not called")
	}
}

func TestChannelMediumPositionSyncRetry(t *testing.T) {
	t.Parallel()
	options := ChannelMediumOptions{
		SharedPositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	numCalls := 0
	medium := setupChannelMedium(t, options, &mockNode{
		streamTopFunc: func(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
			if numCalls == 0 {
				numCalls++
				return StreamPosition{}, errors.New("boom")
			}
			closeOnce.Do(func() {
				close(doneCh)
			})
			return StreamPosition{}, nil
		},
	})
	medium.nowFn = func() time.Time { return time.Now().Add(time.Hour) }
	medium.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "streamTopLatestPubFunc was not called")
	}
}

func TestChannelMediumPositionSyncMap(t *testing.T) {
	t.Parallel()
	// Verify that SharedPositionSync for map channels calls mapStreamTop (not streamTop).
	options := ChannelMediumOptions{
		SharedPositionSync: true,
	}
	mapStreamTopCalled := make(chan struct{})
	var closeOnce sync.Once
	medium := setupChannelMedium(t, options, &mockNode{
		streamTopFunc: func(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
			require.Fail(t, "streamTop should not be called for map channel")
			return StreamPosition{}, nil
		},
		mapStreamTopFunc: func(ch string) (StreamPosition, error) {
			closeOnce.Do(func() {
				close(mapStreamTopCalled)
			})
			return StreamPosition{Offset: 5, Epoch: "abc"}, nil
		},
	})
	medium.isMap = true
	medium.nowFn = func() time.Time { return time.Now().Add(time.Hour) }
	// Position matches — should return true.
	valid := medium.CheckPosition(time.Second, StreamPosition{Offset: 5, Epoch: "abc"}, time.Second)
	require.True(t, valid)
	select {
	case <-mapStreamTopCalled:
	case <-time.After(5 * time.Second):
		require.Fail(t, "mapStreamTopFunc was not called")
	}
}

func TestChannelMediumPositionSyncMapMismatch(t *testing.T) {
	t.Parallel()
	// Verify that SharedPositionSync for map channels detects position mismatch.
	options := ChannelMediumOptions{
		SharedPositionSync: true,
		enableQueue:        true,
	}
	insufficientCh := make(chan struct{})
	var closeOnce sync.Once
	medium := setupChannelMedium(t, options, &mockNode{
		handlePublicationFunc: func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
			if pub.Offset == math.MaxUint64 {
				closeOnce.Do(func() {
					close(insufficientCh)
				})
			}
			return nil
		},
		mapStreamTopFunc: func(ch string) (StreamPosition, error) {
			return StreamPosition{Offset: 10, Epoch: "xyz"}, nil
		},
	})
	medium.isMap = true
	medium.nowFn = func() time.Time { return time.Now().Add(time.Hour) }
	// Client has stale position — should return false.
	valid := medium.CheckPosition(time.Second, StreamPosition{Offset: 5, Epoch: "xyz"}, time.Second)
	require.False(t, valid)
	select {
	case <-insufficientCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "insufficient state was not broadcast")
	}
}

func TestChannelMediumNoLocalPrevPubForMapSubs(t *testing.T) {
	t.Parallel()
	// Test that channel medium does NOT provide localPrevPub for map publications
	// (pub.Key != ""). Map keys are independent streams — a single latestPublication
	// can't serve as a correct delta base across different keys. Only non-map
	// publications get medium-level localPrevPub tracking.
	type broadcastEvent struct {
		pub          *Publication
		localPrevPub *Publication
	}

	var mu sync.Mutex
	var events []broadcastEvent

	options := ChannelMediumOptions{
		KeepLatestPublication: true,
	}
	medium := setupChannelMedium(t, options, &mockNode{
		handlePublicationFunc: func(channel string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
			mu.Lock()
			events = append(events, broadcastEvent{pub: pub, localPrevPub: localPrevPub})
			mu.Unlock()
			return nil
		},
	})

	// Map publications always get nil localPrevPub.
	pubA1 := &Publication{Data: []byte("a1"), Key: "a"}
	medium.broadcastPublication(pubA1, StreamPosition{Offset: 1}, true, nil)
	pubA2 := &Publication{Data: []byte("a2"), Key: "a"}
	medium.broadcastPublication(pubA2, StreamPosition{Offset: 2}, true, nil)
	pubB1 := &Publication{Data: []byte("b1"), Key: "b"}
	medium.broadcastPublication(pubB1, StreamPosition{Offset: 3}, true, nil)

	mu.Lock()
	require.Len(t, events, 3)
	require.Nil(t, events[0].localPrevPub, "map pub must not get localPrevPub")
	require.Nil(t, events[1].localPrevPub, "map pub same key must not get localPrevPub")
	require.Nil(t, events[2].localPrevPub, "map pub different key must not get localPrevPub")
	mu.Unlock()

	// Map publications must not pollute latestPublication used by non-map pubs.
	pubNoKey1 := &Publication{Data: []byte("nokey1")}
	medium.broadcastPublication(pubNoKey1, StreamPosition{Offset: 4}, true, nil)
	mu.Lock()
	require.Len(t, events, 4)
	require.Nil(t, events[3].localPrevPub, "first non-map pub should have nil localPrevPub")
	mu.Unlock()

	pubNoKey2 := &Publication{Data: []byte("nokey2")}
	medium.broadcastPublication(pubNoKey2, StreamPosition{Offset: 5}, true, nil)
	mu.Lock()
	require.Len(t, events, 5)
	require.NotNil(t, events[4].localPrevPub)
	require.Equal(t, []byte("nokey1"), events[4].localPrevPub.Data)
	mu.Unlock()
}

// TestPublicationQueueCloseAndClosed covers the queue lifecycle helpers used by
// channel medium teardown.
func TestPublicationQueueCloseAndClosed(t *testing.T) {
	t.Parallel()
	q := newPublicationQueue(2)
	require.False(t, q.Closed())
	require.True(t, q.Add(queuedPublication{}))
	q.Close()
	require.True(t, q.Closed())
	// Adding after Close must report failure and not panic.
	require.False(t, q.Add(queuedPublication{}))
	// Wait must report false on a closed queue without blocking.
	require.False(t, q.Wait())
}

// TestChannelMediumClose verifies the medium close helper signals waiters via closeCh.
func TestChannelMediumClose(t *testing.T) {
	t.Parallel()
	medium, err := newChannelMedium("test-close", &mockNode{}, ChannelMediumOptions{})
	require.NoError(t, err)
	medium.close()
	select {
	case <-medium.closeCh:
	default:
		t.Fatal("close did not signal closeCh")
	}
}

// TestNewChannelMediumBroadcastDelayWithoutQueueRejected ensures the constructor
// rejects an invalid options combination (broadcastDelay > 0 requires queue).
func TestNewChannelMediumBroadcastDelayWithoutQueueRejected(t *testing.T) {
	t.Parallel()
	_, err := newChannelMedium("ch", &mockNode{}, ChannelMediumOptions{
		broadcastDelay: 10 * time.Millisecond,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "broadcast delay")
}

// TestChannelMediumQueuedBroadcast verifies the queued broadcast path:
// publications enqueued via broadcastPublication are delivered through the
// medium's writer goroutine to the underlying node's handlePublication.
func TestChannelMediumQueuedBroadcast(t *testing.T) {
	t.Parallel()
	delivered := make(chan *Publication, 1)
	node := &mockNode{
		handlePublicationFunc: func(_ string, _ StreamPosition, pub, _, _ *Publication) error {
			delivered <- pub
			return nil
		},
	}
	medium, err := newChannelMedium("ch", node, ChannelMediumOptions{
		enableQueue: true,
	})
	require.NoError(t, err)

	pub := &Publication{Data: []byte(`{"value":"queued"}`), Offset: 1}
	medium.broadcastPublication(pub, StreamPosition{Offset: 1, Epoch: "e"}, false, nil)

	select {
	case got := <-delivered:
		require.Equal(t, pub.Data, got.Data)
	case <-time.After(time.Second):
		t.Fatal("publication not delivered through medium queue")
	}
	medium.close()
}

// TestChannelMediumCheckPositionAndInsufficientState exercises the position-check
// flow including the insufficient-state broadcast path. The mock node returns a
// stream top that does NOT match the client's position, forcing
// broadcastInsufficientState to fire.
func TestChannelMediumCheckPositionAndInsufficientState(t *testing.T) {
	t.Parallel()
	delivered := make(chan *Publication, 4)
	node := &mockNode{
		streamTopFunc: func(string, time.Duration) (StreamPosition, error) {
			return StreamPosition{Offset: 99, Epoch: "e1"}, nil
		},
		mapStreamTopFunc: func(string) (StreamPosition, error) {
			return StreamPosition{Offset: 99, Epoch: "e1"}, nil
		},
		handlePublicationFunc: func(_ string, _ StreamPosition, pub, _, _ *Publication) error {
			delivered <- pub
			return nil
		},
	}

	medium, err := newChannelMedium("pos-check", node, ChannelMediumOptions{
		SharedPositionSync: true,
	})
	require.NoError(t, err)
	defer medium.close()

	// Mismatch: client is at offset 5, stream top at 99. Position is invalid,
	// medium must broadcast insufficient state.
	clientPos := StreamPosition{Offset: 5, Epoch: "e1"}
	valid := medium.CheckPosition(time.Minute, clientPos, 0)
	require.False(t, valid)

	// One publication delivered with the magic insufficient-state sentinel offset.
	select {
	case got := <-delivered:
		require.Equal(t, uint64(0xFFFFFFFFFFFFFFFF), got.Offset)
	case <-time.After(time.Second):
		t.Fatal("insufficient state broadcast not delivered")
	}

	// Calling again immediately should skip the check (delay not elapsed).
	valid = medium.CheckPosition(time.Minute, clientPos, time.Hour)
	require.True(t, valid)
}

// TestChannelMediumOptionsIsMediumEnabled exercises isMediumEnabled across the
// enable conditions to keep the table-driven check covered.
func TestChannelMediumOptionsIsMediumEnabled(t *testing.T) {
	t.Parallel()
	require.False(t, (ChannelMediumOptions{}).isMediumEnabled())
	require.True(t, (ChannelMediumOptions{SharedPositionSync: true}).isMediumEnabled())
	require.True(t, (ChannelMediumOptions{KeepLatestPublication: true}).isMediumEnabled())
	require.True(t, (ChannelMediumOptions{enableQueue: true}).isMediumEnabled())
	require.True(t, (ChannelMediumOptions{broadcastDelay: time.Millisecond}).isMediumEnabled())
}

// TestChannelMediumWithBroadcastDelayCoalesces verifies the broadcastDelay path:
// multiple publications enqueued within a delay window get coalesced — only the
// last publication is broadcast.
func TestChannelMediumWithBroadcastDelayCoalesces(t *testing.T) {
	t.Parallel()
	delivered := make(chan *Publication, 8)
	node := &mockNode{
		handlePublicationFunc: func(_ string, _ StreamPosition, pub, _, _ *Publication) error {
			delivered <- pub
			return nil
		},
	}
	medium, err := newChannelMedium("delay", node, ChannelMediumOptions{
		enableQueue:    true,
		broadcastDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	for i := uint64(1); i <= 5; i++ {
		medium.broadcastPublication(
			&Publication{Data: []byte(`x`), Offset: i},
			StreamPosition{Offset: i, Epoch: "e"},
			false, nil,
		)
	}

	// Wait for first delivery — the 50ms broadcastDelay can stretch under -race
	// load, so use Eventually rather than a fixed inner timeout that flakes.
	var received []*Publication
	require.Eventually(t, func() bool {
		select {
		case p := <-delivered:
			received = append(received, p)
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "expected at least one coalesced delivery")

	// Drain any follow-ups arriving shortly after the first delivery. Use a
	// long enough quiet window that all coalesced publications have a chance
	// to arrive even under -race contention.
	drainTimeout := time.After(500 * time.Millisecond)
drain:
	for {
		select {
		case p := <-delivered:
			received = append(received, p)
		case <-drainTimeout:
			break drain
		}
	}

	// Coalescing: at minimum we should receive far fewer than 5 publications.
	require.Less(t, len(received), 5)
	// The last delivered publication must be the highest offset queued.
	require.Equal(t, uint64(5), received[len(received)-1].Offset)
	medium.close()
}

// TestChannelMediumKeepLatestPublicationDelta covers the
// KeepLatestPublication+delta path inside broadcast: when a delta-flagged pub
// arrives without a key, the medium uses its stored latestPublication as
// localPrevPub on the next broadcast.
func TestChannelMediumKeepLatestPublicationDelta(t *testing.T) {
	t.Parallel()
	delivered := make(chan struct{}, 4)
	var captured struct {
		first  *Publication
		second *Publication
	}
	idx := 0
	node := &mockNode{
		handlePublicationFunc: func(_ string, _ StreamPosition, pub, _, localPrev *Publication) error {
			idx++
			switch idx {
			case 1:
				captured.first = pub
			case 2:
				captured.second = localPrev
			}
			delivered <- struct{}{}
			return nil
		},
	}
	medium, err := newChannelMedium("keep-latest", node, ChannelMediumOptions{
		KeepLatestPublication: true,
	})
	require.NoError(t, err)
	defer medium.close()

	medium.broadcastPublication(
		&Publication{Data: []byte("first")},
		StreamPosition{}, true, nil,
	)
	<-delivered
	medium.broadcastPublication(
		&Publication{Data: []byte("second")},
		StreamPosition{}, true, nil,
	)
	<-delivered

	require.NotNil(t, captured.first)
	require.NotNil(t, captured.second, "second broadcast should see prior pub as localPrev")
	require.Equal(t, []byte("first"), captured.second.Data)
}
