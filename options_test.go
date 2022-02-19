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

func TestWithMeta(t *testing.T) {
	opt := WithTags(map[string]string{"test": "value"})
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, "value", opts.Tags["test"])
}

func TestSubscribeOptions(t *testing.T) {
	subscribeOpts := []SubscribeOption{
		WithExpireAt(1),
		WithPresence(true),
		WithJoinLeave(true),
		WithPosition(true),
		WithRecover(true),
		WithChannelInfo([]byte(`test`)),
		WithSubscribeSession("session"),
		WithSubscribeClient("test"),
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
	require.Equal(t, "test", opts.clientID)
	require.Equal(t, "session", opts.sessionID)
}

func TestWithDisconnect(t *testing.T) {
	opt := WithDisconnect(DisconnectConnectionLimit)
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, DisconnectConnectionLimit, opts.Disconnect)
}

func TestWithClientWhitelist(t *testing.T) {
	opt := WithDisconnectClientWhitelist([]string{"client"})
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

func TestWithUnsubscribeSession(t *testing.T) {
	opt := WithUnsubscribeSession("session")
	opts := &UnsubscribeOptions{}
	opt(opts)
	require.Equal(t, "session", opts.sessionID)
}

func TestWithDisconnectClient(t *testing.T) {
	opt := WithDisconnectClient("client")
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, "client", opts.clientID)
}

func TestWithDisconnectSession(t *testing.T) {
	opt := WithDisconnectSession("session")
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, "session", opts.sessionID)
}

func TestRefreshOptions(t *testing.T) {
	refreshOpts := []RefreshOption{
		WithRefreshClient("client"),
		WithRefreshExpireAt(12),
		WithRefreshExpired(true),
		WithRefreshInfo([]byte(`test`)),
		WithRefreshSession("session"),
	}
	opts := &RefreshOptions{}
	for _, opt := range refreshOpts {
		opt(opts)
	}
	require.Equal(t, int64(12), opts.ExpireAt)
	require.True(t, opts.Expired)
	require.Equal(t, "client", opts.clientID)
	require.Equal(t, []byte(`test`), opts.Info)
	require.Equal(t, "session", opts.sessionID)
}
