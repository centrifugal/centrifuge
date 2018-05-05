package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/proto"
	"github.com/stretchr/testify/assert"
)

func TestClientEventHub(t *testing.T) {
	h := ClientEventHub{}
	handler := func(e DisconnectEvent) DisconnectReply {
		return DisconnectReply{}
	}
	h.Disconnect(handler)
	assert.NotNil(t, h.disconnectHandler)
}

func TestSetCredentials(t *testing.T) {
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{})
	val := newCtx.Value(credentialsContextKey).(*Credentials)
	assert.NotNil(t, val)
}

func TestNewClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, err := newClient(context.Background(), node, transport)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestClientInitialState(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	assert.Equal(t, client.uid, client.ID())
	assert.NotNil(t, "", client.user)
	assert.Equal(t, 0, len(client.Channels()))
	assert.Equal(t, proto.EncodingJSON, client.Transport().Encoding())
	assert.Equal(t, "test_transport", client.Transport().Name())
	assert.False(t, client.closed)
	assert.False(t, client.authenticated)
	assert.Nil(t, client.disconnect)
}

func TestClientClosedState(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	err := client.close(nil)
	assert.NoError(t, err)
	assert.True(t, client.closed)
}

func TestClientConnectWithNoCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	_, disconnect := client.connectCmd(&proto.ConnectRequest{})
	assert.NotNil(t, disconnect)
	assert.Equal(t, disconnect, DisconnectBadRequest)
}

func TestClientConnectWithWrongCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	_, disconnect := client.connectCmd(&proto.ConnectRequest{
		Credentials: &proto.SignedCredentials{
			User: "test",
			Exp:  "",
			Info: "",
			Sign: "",
		},
	})
	assert.NotNil(t, disconnect)
	assert.Equal(t, disconnect, DisconnectInvalidSign)
}

func TestClientConnectWithValidCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.Secret = "secret"
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&proto.ConnectRequest{
		Credentials: &proto.SignedCredentials{
			User: "42",
			Exp:  "1525541722",
			Info: "",
			Sign: "46789cda3bea52eca167961dd68c4d4240b9108f196a8516d1c753259b457d12",
		},
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, client.ID(), resp.Result.Client)
	assert.Equal(t, false, resp.Result.Expires)
}

func TestClientConnectWithExpiredCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.Secret = "secret"
	node.config.ClientExpire = true
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&proto.ConnectRequest{
		Credentials: &proto.SignedCredentials{
			User: "42",
			Exp:  "1525541722",
			Info: "",
			Sign: "46789cda3bea52eca167961dd68c4d4240b9108f196a8516d1c753259b457d12",
		},
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, true, resp.Result.Expires)
	assert.Equal(t, true, resp.Result.Expired)
	assert.Equal(t, uint32(0), resp.Result.TTL)
	assert.False(t, client.authenticated)
}

func TestClientConnectContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.Secret = "secret"
	node.config.ClientExpire = true
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID: "42",
		Exp:    time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	// Set refresh handler to tell library that server-side refresh must be used.
	client.On().Refresh(func(e RefreshEvent) RefreshReply {
		return RefreshReply{}
	})

	resp, disconnect := client.connectCmd(&proto.ConnectRequest{})
	assert.Nil(t, disconnect)
	assert.Equal(t, false, resp.Result.Expires)
	assert.Equal(t, false, resp.Result.Expired)
	assert.Equal(t, uint32(0), resp.Result.TTL)
	assert.True(t, client.authenticated)
	assert.Equal(t, "42", client.UserID())
}

func TestClientConnectWithExpiredContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.Secret = "secret"
	node.config.ClientExpire = true
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID: "42",
		Exp:    time.Now().Unix() - 60,
	})
	client, _ := newClient(newCtx, node, transport)

	// Set refresh handler to tell library that server-side refresh must be used.
	client.On().Refresh(func(e RefreshEvent) RefreshReply {
		return RefreshReply{}
	})

	_, disconnect := client.connectCmd(&proto.ConnectRequest{})
	assert.NotNil(t, disconnect)
	assert.Equal(t, DisconnectExpired, disconnect)
}
