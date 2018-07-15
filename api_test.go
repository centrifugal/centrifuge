package centrifuge

import (
	"context"
	"testing"

	"github.com/centrifugal/centrifuge/internal/proto/apiproto"

	"github.com/stretchr/testify/assert"
)

func TestPublishAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.Publish(context.Background(), &apiproto.PublishRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)

	resp = api.Publish(context.Background(), &apiproto.PublishRequest{Channel: "test"})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)

	resp = api.Publish(context.Background(), &apiproto.PublishRequest{Channel: "test", Data: []byte("test")})
	assert.Nil(t, resp.Error)

	resp = api.Publish(context.Background(), &apiproto.PublishRequest{Channel: "test:test", Data: []byte("test")})
	assert.Equal(t, apiproto.ErrorNamespaceNotFound, resp.Error)
}

func TestBroadcastAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.Broadcast(context.Background(), &apiproto.BroadcastRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)

	resp = api.Broadcast(context.Background(), &apiproto.BroadcastRequest{Channels: []string{"test"}})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)

	resp = api.Broadcast(context.Background(), &apiproto.BroadcastRequest{Channels: []string{"test"}, Data: []byte("test")})
	assert.Nil(t, resp.Error)

	resp = api.Broadcast(context.Background(), &apiproto.BroadcastRequest{Channels: []string{"test:test"}, Data: []byte("test")})
	assert.Equal(t, apiproto.ErrorNamespaceNotFound, resp.Error)

	resp = api.Broadcast(context.Background(), &apiproto.BroadcastRequest{Channels: []string{"test", "test:test"}, Data: []byte("test")})
	assert.Equal(t, apiproto.ErrorNamespaceNotFound, resp.Error)
}

func TestHistoryAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.History(context.Background(), &apiproto.HistoryRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)
	resp = api.History(context.Background(), &apiproto.HistoryRequest{Channel: "test"})
	assert.Equal(t, apiproto.ErrorNotAvailable, resp.Error)

	config := node.Config()
	config.HistorySize = 1
	config.HistoryLifetime = 1
	node.Reload(config)

	resp = api.History(context.Background(), &apiproto.HistoryRequest{Channel: "test"})
	assert.Nil(t, resp.Error)
}

func TestHistoryRemoveAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.HistoryRemove(context.Background(), &apiproto.HistoryRemoveRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)
	resp = api.HistoryRemove(context.Background(), &apiproto.HistoryRemoveRequest{Channel: "test"})
	assert.Equal(t, apiproto.ErrorNotAvailable, resp.Error)

	config := node.Config()
	config.HistorySize = 1
	config.HistoryLifetime = 1
	node.Reload(config)

	resp = api.HistoryRemove(context.Background(), &apiproto.HistoryRemoveRequest{Channel: "test"})
	assert.Nil(t, resp.Error)
}

func TestPresenceAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.Presence(context.Background(), &apiproto.PresenceRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)
	resp = api.Presence(context.Background(), &apiproto.PresenceRequest{Channel: "test"})

	assert.Equal(t, apiproto.ErrorNotAvailable, resp.Error)

	config := node.Config()
	config.Presence = true
	node.Reload(config)

	resp = api.Presence(context.Background(), &apiproto.PresenceRequest{Channel: "test"})
	assert.Nil(t, resp.Error)
}

func TestPresenceStatsAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.PresenceStats(context.Background(), &apiproto.PresenceStatsRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)
	resp = api.PresenceStats(context.Background(), &apiproto.PresenceStatsRequest{Channel: "test"})
	assert.Equal(t, apiproto.ErrorNotAvailable, resp.Error)

	config := node.Config()
	config.Presence = true
	node.Reload(config)

	resp = api.PresenceStats(context.Background(), &apiproto.PresenceStatsRequest{Channel: "test"})
	assert.Nil(t, resp.Error)
}

func TestDisconnectAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.Disconnect(context.Background(), &apiproto.DisconnectRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)
	resp = api.Disconnect(context.Background(), &apiproto.DisconnectRequest{
		User: "test",
	})
	assert.Nil(t, resp.Error)
}

func TestUnsubscribeAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.Unsubscribe(context.Background(), &apiproto.UnsubscribeRequest{})
	assert.Equal(t, apiproto.ErrorBadRequest, resp.Error)
	resp = api.Unsubscribe(context.Background(), &apiproto.UnsubscribeRequest{
		User:    "test",
		Channel: "test",
	})
	assert.Nil(t, resp.Error)
}

func TestChannelsAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.Channels(context.Background(), &apiproto.ChannelsRequest{})
	assert.Nil(t, resp.Error)
}

func TestInfoAPI(t *testing.T) {
	node := nodeWithMemoryEngine()
	api := newAPIExecutor(node)
	resp := api.Info(context.Background(), &apiproto.InfoRequest{})
	assert.Nil(t, resp.Error)
}
