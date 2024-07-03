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
