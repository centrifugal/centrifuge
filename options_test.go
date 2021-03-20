package centrifuge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithHistory(t *testing.T) {
	opt := WithHistory(10, time.Second)
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, 10, opts.HistorySize)
	require.Equal(t, time.Second, opts.HistoryTTL)
}

func TestSubscribeOptions(t *testing.T) {
	subscribeOpts := []SubscribeOption{
		WithExpireAt(1),
		WithPresence(true),
		WithJoinLeave(true),
		WithPosition(true),
		WithRecover(true),
		WithChannelInfo([]byte(`test`)),
	}
	opts := &SubscribeOptions{}
	for _, opt := range subscribeOpts {
		opt(opts)
	}
	require.Equal(t, int64(1), opts.ExpireAt)
	require.True(t, opts.Presence)
	require.True(t, opts.JoinLeave)
	require.True(t, opts.Position)
	require.True(t, opts.Recover)
	require.Equal(t, []byte(`test`), opts.ChannelInfo)
}

func TestWithDisconnect(t *testing.T) {
	opt := WithDisconnect(DisconnectConnectionLimit)
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, DisconnectConnectionLimit, opts.Disconnect)
}

func TestWithClientWhitelist(t *testing.T) {
	opt := WithClientWhitelist([]string{"client"})
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, []string{"client"}, opts.ClientWhitelist)
}

func TestWithLimit(t *testing.T) {
	opt := WithLimit(NoLimit)
	opts := &HistoryOptions{}
	opt(opts)
	require.Equal(t, NoLimit, opts.Limit)
}

func TestWithSubscribeClient(t *testing.T) {
	opt := WithSubscribeClient("client")
	opts := &SubscribeOptions{}
	opt(opts)
	require.Equal(t, "client", opts.clientID)
}

func TestWithSubscribeData(t *testing.T) {
	opt := WithSubscribeData([]byte("test"))
	opts := &SubscribeOptions{}
	opt(opts)
	require.Equal(t, []byte("test"), opts.Data)
}

func TestWithUnsubscribeClient(t *testing.T) {
	opt := WithUnsubscribeClient("client")
	opts := &UnsubscribeOptions{}
	opt(opts)
	require.Equal(t, "client", opts.clientID)
}
