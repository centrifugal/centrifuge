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

func TestChannelMediumHandlePublication(t *testing.T) {
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
	originalGetter := channelMediumTimeNow
	channelMediumTimeNow = func() time.Time {
		return time.Now().Add(time.Hour)
	}
	medium.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	channelMediumTimeNow = originalGetter
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "historyFunc was not called")
	}
}

func TestChannelMediumPositionSyncRetry(t *testing.T) {
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
	originalGetter := channelMediumTimeNow
	channelMediumTimeNow = func() time.Time {
		return time.Now().Add(time.Hour)
	}
	medium.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	channelMediumTimeNow = originalGetter
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "streamTopLatestPubFunc was not called")
	}
}

func TestChannelMediumPerKeyLatestPublication(t *testing.T) {
	// Test that channel medium tracks latest publication per-key for map subscriptions,
	// and provides correct localPrevPub for delta computation.
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

	// Publish key "a" first time — localPrevPub should be nil.
	pubA1 := &Publication{Data: []byte("a1"), Key: "a"}
	medium.broadcastPublication(pubA1, StreamPosition{Offset: 1}, true, nil)
	mu.Lock()
	require.Len(t, events, 1)
	require.Nil(t, events[0].localPrevPub)
	mu.Unlock()

	// Publish key "b" first time — localPrevPub should be nil.
	pubB1 := &Publication{Data: []byte("b1"), Key: "b"}
	medium.broadcastPublication(pubB1, StreamPosition{Offset: 2}, true, nil)
	mu.Lock()
	require.Len(t, events, 2)
	require.Nil(t, events[1].localPrevPub)
	mu.Unlock()

	// Publish key "a" second time — localPrevPub should be pubA1.
	pubA2 := &Publication{Data: []byte("a2"), Key: "a"}
	medium.broadcastPublication(pubA2, StreamPosition{Offset: 3}, true, nil)
	mu.Lock()
	require.Len(t, events, 3)
	require.NotNil(t, events[2].localPrevPub)
	require.Equal(t, []byte("a1"), events[2].localPrevPub.Data)
	require.Equal(t, "a", events[2].localPrevPub.Key)
	mu.Unlock()

	// Publish key "b" second time — localPrevPub should be pubB1 (not pubA2).
	pubB2 := &Publication{Data: []byte("b2"), Key: "b"}
	medium.broadcastPublication(pubB2, StreamPosition{Offset: 4}, true, nil)
	mu.Lock()
	require.Len(t, events, 4)
	require.NotNil(t, events[3].localPrevPub)
	require.Equal(t, []byte("b1"), events[3].localPrevPub.Data)
	require.Equal(t, "b", events[3].localPrevPub.Key)
	mu.Unlock()

	// Remove key "a" — should clean up tracking.
	pubARemove := &Publication{Key: "a", Removed: true}
	medium.broadcastPublication(pubARemove, StreamPosition{Offset: 5}, true, nil)
	mu.Lock()
	require.Len(t, events, 5)
	mu.Unlock()

	// Publish key "a" again after removal — localPrevPub should be nil.
	pubA3 := &Publication{Data: []byte("a3"), Key: "a"}
	medium.broadcastPublication(pubA3, StreamPosition{Offset: 6}, true, nil)
	mu.Lock()
	require.Len(t, events, 6)
	require.Nil(t, events[5].localPrevPub, "after removal, localPrevPub should be nil")
	mu.Unlock()

	// Non-map publication (no Key) should use latestPublication (single value), not per-key.
	pubNoKey1 := &Publication{Data: []byte("nokey1")}
	medium.broadcastPublication(pubNoKey1, StreamPosition{Offset: 7}, true, nil)
	mu.Lock()
	require.Len(t, events, 7)
	require.Nil(t, events[6].localPrevPub, "first non-key pub should have nil localPrevPub")
	mu.Unlock()

	pubNoKey2 := &Publication{Data: []byte("nokey2")}
	medium.broadcastPublication(pubNoKey2, StreamPosition{Offset: 8}, true, nil)
	mu.Lock()
	require.Len(t, events, 8)
	require.NotNil(t, events[7].localPrevPub)
	require.Equal(t, []byte("nokey1"), events[7].localPrevPub.Data)
	mu.Unlock()
}
