package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// A server tags filter with a nil node, as a null in a JSON proxy response
// decodes to.
func invalidServerTagsFilter() *FilterNode {
	return &FilterNode{Op: "and", Nodes: []*FilterNode{nil}}
}

// A subscription with an invalid server tags filter is refused, so publications
// are never matched against it.
func TestSubscribeInvalidServerTagsFilterRefused(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{ServerTagsFilter: invalidServerTagsFilter()}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	rw := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw)
	if err == nil {
		require.Len(t, rw.replies, 1)
		require.NotNil(t, rw.replies[0].Error)
		require.Equal(t, ErrorInternal.Code, rw.replies[0].Error.Code)
	} else {
		require.Equal(t, ErrorInternal, err)
	}
	require.NotContains(t, client.Channels(), "ch")

	require.NotPanics(t, func() {
		_, err := node.Publish("ch", []byte(`{}`), WithTags(map[string]string{"team": "eng"}))
		require.NoError(t, err)
	})
}

// A map subscription with an invalid server tags filter is refused.
func TestMapSubscribeInvalidServerTagsFilterRefused(t *testing.T) {
	t.Parallel()
	node, _ := newTestNodeWithMapBroker(t)
	setTestMapChannelOptionsConverging(node)
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				Type:             SubscriptionTypeMap,
				ServerTagsFilter: invalidServerTagsFilter(),
			}}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	protoErr := subscribeMapClientExpectError(t, client, &protocol.SubscribeRequest{
		Channel: "map_ch",
		Type:    int32(SubscriptionTypeMap),
		Phase:   MapPhaseState,
		Limit:   100,
	})
	require.Equal(t, ErrorInternal.Code, protoErr.Code)
}

// A sub refresh returning an invalid server tags filter fails: the
// subscription keeps its previous filter and publications are still delivered
// without a panic.
func TestSubRefreshInvalidServerTagsFilterFails(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{ClientSideRefresh: true, Credentials: &Credentials{UserID: "user1"}}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options:           SubscribeOptions{ExpireAt: time.Now().Unix() + 60},
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{
				ExpireAt:         time.Now().Unix() + 60,
				ServerTagsFilter: invalidServerTagsFilter(),
			}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Len(t, rw.replies, 1)
	require.Nil(t, rw.replies[0].Error)

	refreshRW := testReplyWriterWrapper()
	require.NoError(t, client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "ch", Token: "new_token",
	}, &protocol.Command{}, time.Now(), refreshRW.rw))
	require.Len(t, refreshRW.replies, 1)
	require.NotNil(t, refreshRW.replies[0].Error)
	require.Equal(t, ErrorInternal.Code, refreshRW.replies[0].Error.Code)

	client.mu.RLock()
	chCtx := client.channels["ch"]
	client.mu.RUnlock()
	require.False(t, channelHasFlag(chCtx.flags, flagServerTagsFilter))
	require.NotPanics(t, func() {
		_, err := node.Publish("ch", []byte(`{}`), WithTags(map[string]string{"team": "eng"}))
		require.NoError(t, err)
	})
}

// A server-side sub refresh returning an invalid server tags filter counts as
// failed, as a refresh error does.
func TestServerSideSubRefreshInvalidServerTagsFilterFails(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "user1"}}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{ExpireAt: time.Now().Unix() + 60}}, nil)
		})
		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{
				ExpireAt:         time.Now().Unix() + 7200,
				ServerTagsFilter: invalidServerTagsFilter(),
			}, nil)
		})
	})
	client := newTestConnectedClientV2(t, node, "user1")
	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(&protocol.SubscribeRequest{Channel: "ch"}, &protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.Len(t, rw.replies, 1)

	client.mu.RLock()
	chCtx := client.channels["ch"]
	client.mu.RUnlock()
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time { return time.Now().Add(time.Hour) }
	node.mu.Unlock()
	refreshed := make(chan bool, 1)
	client.checkSubscriptionExpiration("ch", chCtx, 0, func(ok bool) { refreshed <- ok })
	require.False(t, <-refreshed)
}
