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
func setupChannelMedium(t testing.TB, options ChannelMediumOptions, node node) *channelMedium {
	t.Helper()
	channel := "testChannel"
	cache, err := newChannelMedium(channel, node, options)
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

func TestChannelMediumHandlePublication(t *testing.T) {
	optionSet := []ChannelMediumOptions{
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

			cache := setupChannelMedium(t, options, &mockNode{
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

func TestChannelMediumInsufficientState(t *testing.T) {
	options := ChannelMediumOptions{
		EnableQueue:           true,
		KeepLatestPublication: true,
	}
	doneCh := make(chan struct{})
	medium := setupChannelMedium(t, options, &mockNode{
		handlePublicationFunc: func(channel string, pub *Publication, sp StreamPosition, delta bool, prevPublication *Publication) error {
			require.Equal(t, uint64(math.MaxUint64), pub.Offset)
			require.Equal(t, uint64(math.MaxUint64), sp.Offset)
			close(doneCh)
			return nil
		},
	})

	// Simulate the behavior when the state is marked as insufficient
	medium.broadcastInsufficientState(StreamPosition{Offset: 2}, &Publication{})

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		require.Fail(t, "handlePublicationFunc was not called")
	}
}

func TestChannelMediumPositionSync(t *testing.T) {
	options := ChannelMediumOptions{
		EnablePositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	medium := setupChannelMedium(t, options, &mockNode{
		streamTopLatestPubFunc: func(ch string, historyMetaTTL time.Duration) (*Publication, StreamPosition, error) {
			closeOnce.Do(func() {
				close(doneCh)
			})
			return nil, StreamPosition{}, nil
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
		EnablePositionSync: true,
	}
	doneCh := make(chan struct{})
	var closeOnce sync.Once
	numCalls := 0
	medium := setupChannelMedium(t, options, &mockNode{
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