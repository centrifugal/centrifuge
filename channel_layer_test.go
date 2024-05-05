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

// Helper function to create a channelLayer with options.
func setupChannelCache(t testing.TB, options ChannelLayerOptions, node node) *channelLayer {
	t.Helper()
	channel := "testChannel"
	cache, err := newChannelInterlayer(channel, node, options)
	if err != nil {
		require.NoError(t, err)
	}
	return cache
}

type mockNode struct {
	// Store function outputs and any state needed for testing
	handlePublicationFunc  func(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication, bypassOffset bool) error
	streamTopLatestPubFunc func(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error)
}

func (m *mockNode) handlePublication(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication, bypassOffset bool) error {
	if m.handlePublicationFunc != nil {
		return m.handlePublicationFunc(channel, pub, sp, delta, prevPublication, bypassOffset)
	}
	return nil
}

func (m *mockNode) streamTopLatestPub(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error) {
	if m.streamTopLatestPubFunc != nil {
		return m.streamTopLatestPubFunc(ch, historyMetaTTL)
	}
	return nil, StreamPosition{}, nil
}

func TestChannelCacheInitialization(t *testing.T) {
	options := ChannelLayerOptions{
		EnableQueue:           true,
		KeepLatestPublication: true,
		BroadcastDelay:        10 * time.Millisecond,
		PositionSyncInterval:  1 * time.Second,
	}
	cache := setupChannelCache(t, options, &mockNode{})

	require.NotNil(t, cache)
	require.NotNil(t, cache.messages)
	require.Equal(t, int64(0), cache.initialized.Load())
	cache.InitState(&Publication{}, StreamPosition{1, "epoch"})
	require.Equal(t, int64(1), cache.initialized.Load())
}

func TestChannelCacheHandlePublication(t *testing.T) {
	optionSet := []ChannelLayerOptions{
		{
			EnableQueue:           false,
			KeepLatestPublication: false,
		},
		{
			EnableQueue:           true,
			KeepLatestPublication: false,
		},
		{
			EnableQueue:           true,
			KeepLatestPublication: false,
			BroadcastDelay:        10 * time.Millisecond,
		},
		{
			EnableQueue:           true,
			KeepLatestPublication: true,
			BroadcastDelay:        10 * time.Millisecond,
		},
	}

	for i, options := range optionSet {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			doneCh := make(chan struct{})

			cache := setupChannelCache(t, options, &mockNode{
				handlePublicationFunc: func(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication, bypassOffset bool) error {
					close(doneCh)
					return nil
				},
			})
			cache.InitState(&Publication{}, StreamPosition{})

			pub := &Publication{Data: []byte("test data")}
			sp := StreamPosition{Offset: 1}

			cache.processPublication(pub, sp, false, nil)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				require.Fail(t, "handlePublicationFunc was not called")
			}
		})
	}
}

func TestChannelCacheInsufficientState(t *testing.T) {
	options := ChannelLayerOptions{
		EnableQueue:           true,
		KeepLatestPublication: true,
	}
	doneCh := make(chan struct{})
	cache := setupChannelCache(t, options, &mockNode{
		handlePublicationFunc: func(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication, bypassOffset bool) error {
			require.Equal(t, uint64(math.MaxUint64), pub.Offset)
			require.Equal(t, uint64(math.MaxUint64), sp.Offset)
			require.False(t, bypassOffset)
			close(doneCh)
			return nil
		},
	})
	cache.InitState(&Publication{}, StreamPosition{})

	// Simulate the behavior when the state is marked as insufficient
	cache.processInsufficientState(StreamPosition{Offset: 2}, &Publication{})

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "handlePublicationFunc was not called")
	}
}

func TestChannelCachePositionSync(t *testing.T) {
	options := ChannelLayerOptions{
		PositionSyncInterval: 10 * time.Millisecond,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	cache := setupChannelCache(t, options, &mockNode{
		streamTopLatestPubFunc: func(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error) {
			closeOnce.Do(func() {
				close(doneCh)
			})
			return nil, StreamPosition{}, nil
		},
	})
	cache.InitState(&Publication{}, StreamPosition{})

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "historyFunc was not called")
	}
}

func TestChannelCachePositionSyncRetry(t *testing.T) {
	options := ChannelLayerOptions{
		PositionSyncInterval: 10 * time.Millisecond,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	numCalls := 0
	cache := setupChannelCache(t, options, &mockNode{
		streamTopLatestPubFunc: func(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error) {
			if numCalls == 0 {
				numCalls++
				return nil, StreamPosition{}, errors.New("boom")
			}
			closeOnce.Do(func() {
				close(doneCh)
			})
			return nil, StreamPosition{}, nil
		},
	})
	cache.InitState(&Publication{}, StreamPosition{})

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "historyFunc was not called")
	}
}
