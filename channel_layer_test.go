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
func setupChannelLayer(t testing.TB, options ChannelLayerOptions, node node) *channelLayer {
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
	handlePublicationFunc  func(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication) error
	streamTopLatestPubFunc func(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error)
}

func (m *mockNode) handlePublication(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication) error {
	if m.handlePublicationFunc != nil {
		return m.handlePublicationFunc(channel, pub, sp, delta, prevPublication)
	}
	return nil
}

func (m *mockNode) streamTopLatestPub(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error) {
	if m.streamTopLatestPubFunc != nil {
		return m.streamTopLatestPubFunc(ch, historyMetaTTL)
	}
	return nil, StreamPosition{}, nil
}

func TestChannelLayerHandlePublication(t *testing.T) {
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

			cache := setupChannelLayer(t, options, &mockNode{
				handlePublicationFunc: func(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication) error {
					close(doneCh)
					return nil
				},
			})

			pub := &Publication{Data: []byte("test data")}
			sp := StreamPosition{Offset: 1}

			cache.broadcastPublication(pub, sp, false, nil)

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				require.Fail(t, "handlePublicationFunc was not called")
			}
		})
	}
}

func TestChannelLayerInsufficientState(t *testing.T) {
	options := ChannelLayerOptions{
		EnableQueue:           true,
		KeepLatestPublication: true,
	}
	doneCh := make(chan struct{})
	cache := setupChannelLayer(t, options, &mockNode{
		handlePublicationFunc: func(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication) error {
			require.Equal(t, uint64(math.MaxUint64), pub.Offset)
			require.Equal(t, uint64(math.MaxUint64), sp.Offset)
			close(doneCh)
			return nil
		},
	})

	// Simulate the behavior when the state is marked as insufficient
	cache.broadcastInsufficientState(StreamPosition{Offset: 2}, &Publication{})

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "handlePublicationFunc was not called")
	}
}

func TestChannelLayerPositionSync(t *testing.T) {
	options := ChannelLayerOptions{
		EnablePositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	layer := setupChannelLayer(t, options, &mockNode{
		streamTopLatestPubFunc: func(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error) {
			closeOnce.Do(func() {
				close(doneCh)
			})
			return nil, StreamPosition{}, nil
		},
	})
	originalGetter := channelLayerTimeNow
	channelLayerTimeNow = func() time.Time {
		return time.Now().Add(time.Hour)
	}
	layer.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	channelLayerTimeNow = originalGetter
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "historyFunc was not called")
	}
}

func TestChannelLayerPositionSyncRetry(t *testing.T) {
	options := ChannelLayerOptions{
		EnablePositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	numCalls := 0
	layer := setupChannelLayer(t, options, &mockNode{
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
	originalGetter := channelLayerTimeNow
	channelLayerTimeNow = func() time.Time {
		return time.Now().Add(time.Hour)
	}
	layer.CheckPosition(time.Second, StreamPosition{Offset: 1, Epoch: "test"}, time.Second)
	channelLayerTimeNow = originalGetter
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "streamTopLatestPubFunc was not called")
	}
}
