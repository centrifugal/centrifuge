package centrifuge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithHistory(t *testing.T) {
	t.Parallel()
	opt := WithHistory(10, time.Second)
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, 10, opts.HistorySize)
	require.Equal(t, time.Second, opts.HistoryTTL)
}

func TestWithIdempotencyKey(t *testing.T) {
	t.Parallel()
	opt := WithIdempotencyKey("ik")
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, "ik", opts.IdempotencyKey)
}

func TestWithDelta(t *testing.T) {
	t.Parallel()
	opt := WithDelta(true)
	opts := &PublishOptions{}
	opt(opts)
	require.True(t, opts.UseDelta)
}

func TestWithIdempotentResultTTL(t *testing.T) {
	t.Parallel()
	opt := WithIdempotentResultTTL(time.Minute)
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, time.Minute, opts.IdempotentResultTTL)
}

func TestWithMeta(t *testing.T) {
	t.Parallel()
	opt := WithTags(map[string]string{"test": "value"})
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, "value", opts.Tags["test"])
}

func TestWithVersion(t *testing.T) {
	t.Parallel()
	opt := WithVersion(2, "xxx")
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, uint64(2), opts.Version)
	require.Equal(t, "xxx", opts.VersionEpoch)
}

func TestSubscribeOptions(t *testing.T) {
	t.Parallel()
	subscribeOpts := []SubscribeOption{
		WithExpireAt(1),
		WithEmitPresence(true),
		WithEmitJoinLeave(true),
		WithPushJoinLeave(true),
		WithPositioning(true),
		WithRecovery(true),
		WithChannelInfo([]byte(`test`)),
		WithSubscribeSession("session"),
		WithSubscribeClient("test"),
		WithSubscribeSource(4),
		WithRecoveryMode(RecoveryModeCache),
		WithSubscribeHistoryMetaTTL(24 * time.Hour),
	}
	opts := &SubscribeOptions{}
	for _, opt := range subscribeOpts {
		opt(opts)
	}
	require.Equal(t, int64(1), opts.ExpireAt)
	require.True(t, opts.EmitPresence)
	require.True(t, opts.EmitJoinLeave)
	require.True(t, opts.PushJoinLeave)
	require.True(t, opts.EnablePositioning)
	require.True(t, opts.EnableRecovery)
	require.Equal(t, []byte(`test`), opts.ChannelInfo)
	require.Equal(t, "test", opts.clientID)
	require.Equal(t, "session", opts.sessionID)
	require.Equal(t, uint8(4), opts.Source)
	require.Equal(t, RecoveryModeCache, opts.RecoveryMode)
	require.Equal(t, 24*time.Hour, opts.HistoryMetaTTL)
}

func TestWithDisconnect(t *testing.T) {
	t.Parallel()
	opt := WithCustomDisconnect(DisconnectConnectionLimit)
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, DisconnectConnectionLimit.Code, opts.Disconnect.Code)
}

func TestWithClientWhitelist(t *testing.T) {
	t.Parallel()
	opt := WithDisconnectClientWhitelist([]string{"client"})
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, []string{"client"}, opts.ClientWhitelist)
}

func TestWithLimit(t *testing.T) {
	t.Parallel()
	opt := WithLimit(NoLimit)
	opts := &HistoryOptions{}
	opt(opts)
	require.Equal(t, NoLimit, opts.Filter.Limit)
}

func TestWithSubscribeClient(t *testing.T) {
	t.Parallel()
	opt := WithSubscribeClient("client")
	opts := &SubscribeOptions{}
	opt(opts)
	require.Equal(t, "client", opts.clientID)
}

func TestWithSubscribeData(t *testing.T) {
	t.Parallel()
	opt := WithSubscribeData([]byte("test"))
	opts := &SubscribeOptions{}
	opt(opts)
	require.Equal(t, []byte("test"), opts.Data)
}

func TestWithUnsubscribeClient(t *testing.T) {
	t.Parallel()
	opt := WithUnsubscribeClient("client")
	opts := &UnsubscribeOptions{}
	opt(opts)
	require.Equal(t, "client", opts.clientID)
}

func TestWithUnsubscribeSession(t *testing.T) {
	t.Parallel()
	opt := WithUnsubscribeSession("session")
	opts := &UnsubscribeOptions{}
	opt(opts)
	require.Equal(t, "session", opts.sessionID)
}

func TestWithCustomUnsubscribe(t *testing.T) {
	t.Parallel()
	opt := WithCustomUnsubscribe(Unsubscribe{
		Code:   2200,
		Reason: "x",
	})
	opts := &UnsubscribeOptions{}
	opt(opts)
	require.Equal(t, uint32(2200), opts.unsubscribe.Code)
	require.Equal(t, "x", opts.unsubscribe.Reason)
}

func TestWithDisconnectClient(t *testing.T) {
	t.Parallel()
	opt := WithDisconnectClient("client")
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, "client", opts.clientID)
}

func TestWithDisconnectSession(t *testing.T) {
	t.Parallel()
	opt := WithDisconnectSession("session")
	opts := &DisconnectOptions{}
	opt(opts)
	require.Equal(t, "session", opts.sessionID)
}

func TestWithKey(t *testing.T) {
	t.Parallel()
	opt := WithKey("mykey")
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, "mykey", opts.Key)
}

func TestWithClientInfo(t *testing.T) {
	t.Parallel()
	info := &ClientInfo{UserID: "user1"}
	opt := WithClientInfo(info)
	opts := &PublishOptions{}
	opt(opts)
	require.Equal(t, "user1", opts.ClientInfo.UserID)
}

func TestSubscriptionType_String(t *testing.T) {
	t.Parallel()
	require.Equal(t, "stream", SubscriptionTypeStream.String())
	require.Equal(t, "map", SubscriptionTypeMap.String())
	require.Equal(t, "map_clients", SubscriptionTypeMapClients.String())
	require.Equal(t, "map_users", SubscriptionTypeMapUsers.String())
	require.Equal(t, "shared_poll", SubscriptionTypeSharedPoll.String())
	require.Equal(t, "unknown", SubscriptionType(99).String())
}

func TestSubscriptionType_IsMapPresence(t *testing.T) {
	t.Parallel()
	require.False(t, SubscriptionTypeStream.IsMapPresence())
	require.False(t, SubscriptionTypeMap.IsMapPresence())
	require.True(t, SubscriptionTypeMapClients.IsMapPresence())
	require.True(t, SubscriptionTypeMapUsers.IsMapPresence())
	require.False(t, SubscriptionTypeSharedPoll.IsMapPresence())
}

func TestWithRecoverSince(t *testing.T) {
	t.Parallel()
	opt := WithRecoverSince(&StreamPosition{Offset: 10, Epoch: "abc"})
	opts := &SubscribeOptions{}
	opt(opts)
	require.Equal(t, uint64(10), opts.RecoverSince.Offset)
	require.Equal(t, "abc", opts.RecoverSince.Epoch)
}

func TestWithHistoryFilter(t *testing.T) {
	t.Parallel()
	opt := WithHistoryFilter(HistoryFilter{Limit: 5, Reverse: true})
	opts := &HistoryOptions{}
	opt(opts)
	require.Equal(t, 5, opts.Filter.Limit)
	require.True(t, opts.Filter.Reverse)
}

func TestWithSince(t *testing.T) {
	t.Parallel()
	opt := WithSince(&StreamPosition{Offset: 7, Epoch: "xyz"})
	opts := &HistoryOptions{}
	opt(opts)
	require.Equal(t, uint64(7), opts.Filter.Since.Offset)
	require.Equal(t, "xyz", opts.Filter.Since.Epoch)
}

func TestWithReverse(t *testing.T) {
	t.Parallel()
	opt := WithReverse(true)
	opts := &HistoryOptions{}
	opt(opts)
	require.True(t, opts.Filter.Reverse)
}

func TestWithHistoryMetaTTL(t *testing.T) {
	t.Parallel()
	opt := WithHistoryMetaTTL(time.Hour)
	opts := &HistoryOptions{}
	opt(opts)
	require.Equal(t, time.Hour, opts.MetaTTL)
}

func TestRefreshOptions(t *testing.T) {
	t.Parallel()
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
