package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// TestClientSubscribeRecovery_ServerTagsFilterApplied guards against a filter
// bypass on recovery. A regular (non-map) subscription with a ServerTagsFilter
// must, on recovery, receive only publications matching that server filter —
// exactly as the live broadcast path filters them. isStreamRecovered previously
// applied only the client tags filter, so server-filtered publications were
// recovered and delivered on reconnect.
func TestClientSubscribeRecovery_ServerTagsFilterApplied(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery:    true,
				EnablePositioning: true,
				ServerTagsFilter:  &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
			}}, nil)
		})
	})

	const ch = "recovery_server_filter"
	// Publish three tagged pubs with history; the "sales" one must be filtered out.
	_, err := node.Publish(ch, []byte(`{"n":1}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "eng"}))
	require.NoError(t, err)
	_, err = node.Publish(ch, []byte(`{"n":2}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "sales"}))
	require.NoError(t, err)
	_, err = node.Publish(ch, []byte(`{"n":3}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "eng"}))
	require.NoError(t, err)

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(
		&protocol.SubscribeRequest{Channel: ch, Recover: true},
		&protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.NotEmpty(t, rw.replies)
	res := rw.replies[0].Subscribe
	require.NotNil(t, res)
	require.True(t, res.Recovered, "should have recovered")

	// Only the team=eng publications (offsets 1 and 3) should be delivered; the
	// team=sales one (offset 2) is filtered by the server tags filter.
	require.Len(t, res.Publications, 2, "server tags filter not applied on recovery")
	for _, p := range res.Publications {
		require.Equal(t, "eng", p.Tags["team"],
			"recovered a publication that the server tags filter should have excluded")
	}
}

// TestClientSubscribeCacheRecovery_ServerTagsFilterApplied guards the same
// filter bypass on the RecoveryModeCache path. Cache recovery delivers the
// newest publication, but recoverCache applied only the client tags filter, so
// a server-filtered latest publication was delivered on recovery. With the
// filter applied, cache recovery must fall back to the newest publication that
// passes the server filter.
func TestClientSubscribeCacheRecovery_ServerTagsFilterApplied(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	node.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{
				EnableRecovery:    true,
				EnablePositioning: true,
				RecoveryMode:      RecoveryModeCache,
				ServerTagsFilter:  &FilterNode{Key: "team", Cmp: "eq", Val: "eng"},
			}}, nil)
		})
	})

	const ch = "cache_recovery_server_filter"
	// The newest publication (offset 2) is team=sales and must be excluded; cache
	// recovery must fall back to the newest team=eng publication (offset 1).
	_, err := node.Publish(ch, []byte(`{"n":1}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "eng"}))
	require.NoError(t, err)
	_, err = node.Publish(ch, []byte(`{"n":2}`), WithHistory(10, time.Minute), WithTags(map[string]string{"team": "sales"}))
	require.NoError(t, err)

	client := newTestClientV2(t, node, "u")
	connectClientV2(t, client)

	rw := testReplyWriterWrapper()
	require.NoError(t, client.handleSubscribe(
		&protocol.SubscribeRequest{Channel: ch, Recover: true},
		&protocol.Command{Id: 1}, time.Now(), rw.rw))
	require.NotEmpty(t, rw.replies)
	res := rw.replies[0].Subscribe
	require.NotNil(t, res)

	for _, p := range res.Publications {
		require.Equal(t, "eng", p.Tags["team"],
			"cache-recovered a publication that the server tags filter should have excluded")
	}
}
