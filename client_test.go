package centrifuge

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getConnToken(user string, exp int64) string {
	claims := jwt.MapClaims{"user": user}
	if exp > 0 {
		claims["exp"] = exp
	}
	t, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("secret"))
	if err != nil {
		panic(err)
	}
	return t
}

func getSubscribeToken(channel string, client string, exp int64) string {
	claims := jwt.MapClaims{"channel": channel, "client": client}
	if exp > 0 {
		claims["exp"] = exp
	}
	t, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("secret"))
	if err != nil {
		panic(err)
	}
	return t
}

func testReplyWriter(replies *[]*protocol.Reply) *replyWriter {
	return &replyWriter{
		write: func(rep *protocol.Reply) error {
			*replies = append(*replies, rep)
			return nil
		},
		flush: func() error {
			return nil
		},
	}
}

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
	client, err := NewClient(context.Background(), node, transport)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestClientInitialState(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	assert.Equal(t, client.uid, client.ID())
	assert.NotNil(t, "", client.user)
	assert.Equal(t, 0, len(client.Channels()))
	assert.Equal(t, protocol.TypeJSON, client.Transport().Protocol())
	assert.Equal(t, "test_transport", client.Transport().Name())
	assert.False(t, client.closed)
	assert.False(t, client.authenticated)
	assert.Nil(t, client.disconnect)
}

func TestClientClosedState(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	err := client.Close(nil)
	assert.NoError(t, err)
	assert.True(t, client.closed)
}

func TestClientConnectNoCredentialsNoToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	_, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.NotNil(t, disconnect)
	assert.Equal(t, disconnect, DisconnectBadRequest)
}

func TestClientConnectNoCredentialsNoTokenInsecure(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.ClientInsecure = true
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.Nil(t, disconnect)
	assert.Nil(t, resp.Error)
	assert.NotEmpty(t, resp.Result.Client)
	assert.Empty(t, client.UserID())
}

func TestClientConnectNoCredentialsNoTokenAnonymous(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.ClientAnonymous = true
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.Nil(t, disconnect)
	assert.Nil(t, resp.Error)
	assert.NotEmpty(t, resp.Result.Client)
	assert.Empty(t, client.UserID())
}

func TestClientConnectWithMalformedToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	_, disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: "bad bad token",
	})
	assert.NotNil(t, disconnect)
	assert.Equal(t, disconnect, DisconnectInvalidToken)
}

func TestClientConnectWithValidToken(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Secret = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", 0),
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, client.ID(), resp.Result.Client)
	assert.Equal(t, false, resp.Result.Expires)
}

func TestClientConnectWithExpiringToken(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Secret = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", time.Now().Unix()+10),
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, true, resp.Result.Expires)
	assert.True(t, resp.Result.TTL > 0)
	assert.True(t, client.authenticated)
}

func TestClientConnectWithExpiredToken(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Secret = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", 1525541722),
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorTokenExpired, resp.Error)
	assert.False(t, client.authenticated)
}

func TestClientTokenRefresh(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Secret = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", 1525541722),
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorTokenExpired, resp.Error)

	refreshResp, disconnect := client.refreshCmd(&protocol.RefreshRequest{
		Token: getConnToken("42", 2525637058),
	})
	assert.Nil(t, disconnect)
	assert.NotEmpty(t, client.ID())
	assert.True(t, refreshResp.Result.Expires)
	assert.True(t, refreshResp.Result.TTL > 0)
}

func TestClientConnectContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := NewClient(newCtx, node, transport)

	// Set refresh handler to tell library that server-side refresh must be used.
	node.On().ClientRefresh(func(ctx context.Context, c *Client, e RefreshEvent) RefreshReply {
		return RefreshReply{}
	})

	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.Nil(t, disconnect)
	assert.Equal(t, false, resp.Result.Expires)
	assert.Equal(t, uint32(0), resp.Result.TTL)
	assert.True(t, client.authenticated)
	assert.Equal(t, "42", client.UserID())
}

func TestClientRefreshHandlerClosingExpiredClient(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := NewClient(newCtx, node, transport)

	node.On().ClientRefresh(func(ctx context.Context, c *Client, e RefreshEvent) RefreshReply {
		return RefreshReply{
			Expired: true,
		}
	})

	_, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.Nil(t, disconnect)
	client.expire()
	assert.True(t, client.closed)
}

func TestClientRefreshHandlerProlongatesClientSession(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := NewClient(newCtx, node, transport)

	expireAt := time.Now().Unix() + 60
	node.On().ClientRefresh(func(ctx context.Context, c *Client, e RefreshEvent) RefreshReply {
		return RefreshReply{
			ExpireAt: expireAt,
		}
	})

	_, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.Nil(t, disconnect)
	client.expire()
	assert.False(t, client.closed)
	assert.Equal(t, expireAt, client.exp)
}

func TestClientConnectWithExpiredContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() - 60,
	})
	client, _ := NewClient(newCtx, node, transport)

	// Set refresh handler to tell library that server-side refresh must be used.
	node.On().ClientRefresh(func(ctx context.Context, c *Client, e RefreshEvent) RefreshReply {
		return RefreshReply{}
	})

	resp, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorExpired, resp.Error)
}

func connectClient(t testing.TB, client *Client) *protocol.ConnectResult {
	connectResp, disconnect := client.connectCmd(&protocol.ConnectRequest{})
	assert.Nil(t, disconnect)
	assert.Nil(t, connectResp.Error)
	assert.True(t, client.authenticated)
	assert.Equal(t, client.uid, connectResp.Result.Client)
	return connectResp.Result
}

func extractSubscribeResult(replies []*protocol.Reply) *protocol.SubscribeResult {
	var res protocol.SubscribeResult
	err := json.Unmarshal(replies[0].Result, &res)
	if err != nil {
		panic(err)
	}
	return &res
}

func subscribeClient(t testing.TB, client *Client, ch string) *protocol.SubscribeResult {
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: ch,
	}, rw)
	assert.Nil(t, disconnect)
	assert.Nil(t, replies[0].Error)
	return extractSubscribeResult(replies)
}

func TestClientSubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	assert.Equal(t, 0, len(client.Channels()))

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test1",
	}, rw)
	assert.Nil(t, disconnect)
	assert.Equal(t, 1, len(replies))
	assert.Nil(t, replies[0].Error)
	res := extractSubscribeResult(replies)
	assert.Empty(t, res.Seq)
	assert.False(t, res.Recovered)
	assert.Empty(t, res.Publications)
	assert.Equal(t, 1, len(client.Channels()))

	replies = nil
	disconnect = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test2",
	}, rw)
	assert.Nil(t, disconnect)
	assert.Equal(t, 2, len(client.Channels()))
	assert.Equal(t, 1, node.Hub().NumClients())
	assert.Equal(t, 2, node.Hub().NumChannels())

	replies = nil
	disconnect = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test2",
	}, rw)
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorAlreadySubscribed, replies[0].Error)
}

func TestClientSubscribeReceivePublication(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, rw)
	assert.Nil(t, disconnect)
	assert.Nil(t, replies[0].Error)

	done := make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), "test message") {
				close(done)
			}
		}
	}()

	err := node.Publish("test", []byte(`{"text": "test message"}`))
	assert.NoError(t, err)

	select {
	case <-time.After(time.Second):
		assert.Fail(t, "timeout receiving publication")
	case <-done:
	}
}

func TestClientSubscribeReceivePublicationWithSequence(t *testing.T) {
	node := nodeWithMemoryEngine()
	config := node.Config()
	config.HistoryLifetime = 100
	config.HistorySize = 10
	node.Reload(config)
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, rw)
	assert.Nil(t, disconnect)
	assert.Nil(t, replies[0].Error)

	done := make(chan struct{})
	go func() {
		var seq uint32 = 1
		for data := range transport.sink {
			if strings.Contains(string(data), "test message") {
				dec := json.NewDecoder(strings.NewReader(string(data)))
				for {
					var push struct {
						Result struct {
							Channel string
							Data    struct {
								Seq uint32
							}
						}
					}
					err := dec.Decode(&push)
					if err == io.EOF {
						break
					}
					assert.NoError(t, err)
					if push.Result.Data.Seq != seq {
						assert.Fail(t, "wrong seq")
					}
					seq++
					if seq > 3 {
						close(done)
					}
				}
			}
		}
	}()

	// Send 3 publications, expect client to receive them with
	// incremental sequence numbers.
	err := node.Publish("test", []byte(`{"text": "test message 1"}`))
	assert.NoError(t, err)
	err = node.Publish("test", []byte(`{"text": "test message 2"}`))
	assert.NoError(t, err)
	err = node.Publish("test", []byte(`{"text": "test message 3"}`))
	assert.NoError(t, err)

	select {
	case <-time.After(time.Second):
		assert.Fail(t, "timeout receiving publications")
	case <-done:
	}
}

func TestClientSubscribePrivateChannelNoToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
	}, rw)
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorPermissionDenied, replies[0].Error)
}

func TestClientSubscribePrivateChannelWithToken(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Secret = "secret"
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$wrong_channel", "wrong client", 0),
	}, rw)
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorPermissionDenied, replies[0].Error)

	replies = nil
	disconnect = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$wrong_channel", client.ID(), 0),
	}, rw)
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorPermissionDenied, replies[0].Error)

	replies = nil
	disconnect = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$test1", client.ID(), 0),
	}, rw)
	assert.Nil(t, disconnect)
	assert.Nil(t, replies[0].Error)
}

func TestClientSubscribePrivateChannelWithExpiringToken(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Secret = "secret"
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$test1", client.ID(), 10),
	}, rw)
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorTokenExpired, replies[0].Error)

	replies = nil
	disconnect = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$test1", client.ID(), time.Now().Unix()+10),
	}, rw)
	assert.Nil(t, disconnect)
	assert.Nil(t, replies[0].Error, "token is valid and not expired yet")
	res := extractSubscribeResult(replies)
	assert.True(t, res.Expires, "expires flag must be set")
	assert.True(t, res.TTL > 0, "positive TTL must be set")
}

func TestClientSubscribeLast(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.HistorySize = 10
	config.HistoryLifetime = 60
	config.HistoryRecover = true
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := NewClient(newCtx, node, transport)
	connectClient(t, client)

	result := subscribeClient(t, client, "test")
	assert.Equal(t, uint32(0), result.Seq)

	for i := 0; i < 10; i++ {
		node.Publish("test", []byte("{}"))
	}

	client, _ = NewClient(newCtx, node, transport)
	connectClient(t, client)
	result = subscribeClient(t, client, "test")
	assert.Equal(t, uint32(10), result.Seq)
}

func TestClientUnsubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)
	subscribeClient(t, client, "test")

	unsubscribeResp, disconnect := client.unsubscribeCmd(&protocol.UnsubscribeRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, unsubscribeResp.Error)

	assert.Equal(t, 0, len(client.Channels()))
	assert.Equal(t, 1, node.Hub().NumClients())
	assert.Equal(t, 0, node.Hub().NumChannels())

	subscribeClient(t, client, "test")
	assert.Equal(t, 1, len(client.Channels()))

	err := client.Unsubscribe("test", false)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(client.Channels()))
	assert.Equal(t, 1, node.Hub().NumClients())
	assert.Equal(t, 0, node.Hub().NumChannels())
}

func TestClientPublish(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	publishResp, disconnect := client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorPermissionDenied, publishResp.Error)

	config := node.Config()
	config.Publish = true
	node.Reload(config)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, publishResp.Error)

	config = node.Config()
	config.SubscribeToPublish = true
	node.Reload(config)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorPermissionDenied, publishResp.Error)

	subscribeClient(t, client, "test")
	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, publishResp.Error)
}

type testBrokerEventHandler struct {
	// Publication must register callback func to handle Publications received.
	HandlePublicationFunc func(ch string, pub *Publication) error
	// Join must register callback func to handle Join messages received.
	HandleJoinFunc func(ch string, join *Join) error
	// Leave must register callback func to handle Leave messages received.
	HandleLeaveFunc func(ch string, leave *Leave) error
	// Control must register callback func to handle Control data received.
	HandleControlFunc func([]byte) error
}

func (b *testBrokerEventHandler) HandlePublication(ch string, pub *Publication) error {
	if b.HandlePublicationFunc != nil {
		return b.HandlePublicationFunc(ch, pub)
	}
	return nil
}

func (b *testBrokerEventHandler) HandleJoin(ch string, join *Join) error {
	if b.HandleJoinFunc != nil {
		return b.HandleJoinFunc(ch, join)
	}
	return nil
}

func (b *testBrokerEventHandler) HandleLeave(ch string, leave *Leave) error {
	if b.HandleLeaveFunc != nil {
		return b.HandleLeaveFunc(ch, leave)
	}
	return nil
}

func (b *testBrokerEventHandler) HandleControl(data []byte) error {
	if b.HandleControlFunc != nil {
		return b.HandleControlFunc(data)
	}
	return nil
}

type testClientMessage struct {
	Input     string `json:"input"`
	Timestamp int64  `json:"timestamp"`
}

func TestClientPublishHandler(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	node.broker.(*MemoryEngine).eventHandler = &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication) error {
			var msg testClientMessage
			err := json.Unmarshal(pub.Data, &msg)
			require.NoError(t, err)
			if msg.Input == "with timestamp" {
				require.True(t, msg.Timestamp > 0)
			} else {
				require.Zero(t, msg.Timestamp)
			}
			return nil
		},
	}

	config := node.Config()
	config.Publish = true
	node.Reload(config)

	subscribeClient(t, client, "test")

	client.eventHub.publishHandler = func(e PublishEvent) PublishReply {
		var msg testClientMessage
		err := json.Unmarshal(e.Data, &msg)
		require.NoError(t, err)
		if msg.Input == "with disconnect" {
			return PublishReply{
				Disconnect: DisconnectBadRequest,
			}
		}
		if msg.Input == "with error" {
			return PublishReply{
				Error: ErrorBadRequest,
			}
		}
		if msg.Input == "with timestamp" {
			msg.Timestamp = time.Now().Unix()
			data, _ := json.Marshal(msg)
			return PublishReply{Data: data}
		}
		return PublishReply{}
	}
	publishResp, disconnect := client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "no time"}`),
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, publishResp.Error)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with timestamp"}`),
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, publishResp.Error)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with error"}`),
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorBadRequest, publishResp.Error)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with disconnect"}`),
	})
	assert.Equal(t, DisconnectBadRequest, disconnect)
}

func TestClientPing(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	pingResp, disconnect := client.pingCmd(&protocol.PingRequest{})
	assert.Nil(t, disconnect)
	assert.Nil(t, pingResp.Error)
	assert.Empty(t, pingResp.Result)
}

func TestClientPingWithRecover(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.HistoryLifetime = 10
	config.HistorySize = 10
	config.HistoryRecover = true
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)
	subscribeClient(t, client, "test")

	pingResp, disconnect := client.pingCmd(&protocol.PingRequest{})
	assert.Nil(t, disconnect)
	assert.Nil(t, pingResp.Error)
	assert.Nil(t, pingResp.Result)
}

func TestClientPresence(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Presence = true
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)
	subscribeClient(t, client, "test")

	presenceResp, disconnect := client.presenceCmd(&protocol.PresenceRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, presenceResp.Error)
	assert.Equal(t, 1, len(presenceResp.Result.Presence))

	presenceStatsResp, disconnect := client.presenceStatsCmd(&protocol.PresenceStatsRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, presenceResp.Error)
	assert.Equal(t, uint32(1), presenceStatsResp.Result.NumUsers)
	assert.Equal(t, uint32(1), presenceStatsResp.Result.NumClients)

	config = node.Config()
	config.Presence = false
	node.Reload(config)

	presenceResp, disconnect = client.presenceCmd(&protocol.PresenceRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorNotAvailable, presenceResp.Error)
	assert.Nil(t, presenceResp.Result)

	presenceStatsResp, disconnect = client.presenceStatsCmd(&protocol.PresenceStatsRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorNotAvailable, presenceStatsResp.Error)
	assert.Nil(t, presenceStatsResp.Result)
}

func TestClientHistory(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.HistorySize = 10
	config.HistoryLifetime = 60
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	for i := 0; i < 10; i++ {
		node.Publish("test", []byte(`{}`))
	}

	connectClient(t, client)
	subscribeClient(t, client, "test")

	historyResp, disconnect := client.historyCmd(&protocol.HistoryRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Nil(t, historyResp.Error)
	assert.Equal(t, 10, len(historyResp.Result.Publications))

	config = node.Config()
	config.HistorySize = 0
	config.HistoryLifetime = 0
	node.Reload(config)

	historyResp, disconnect = client.historyCmd(&protocol.HistoryRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorNotAvailable, historyResp.Error)
	assert.Nil(t, historyResp.Result)
}

func TestClientHistoryDisabled(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.HistorySize = 10
	config.HistoryLifetime = 60
	config.HistoryDisableForClient = true
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	node.Publish("test", []byte(`{}`))

	connectClient(t, client)
	subscribeClient(t, client, "test")

	historyResp, disconnect := client.historyCmd(&protocol.HistoryRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorNotAvailable, historyResp.Error)
}

func TestClientPresenceDisabled(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Presence = true
	config.PresenceDisableForClient = true
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	node.Publish("test", []byte(`{}`))

	connectClient(t, client)
	subscribeClient(t, client, "test")

	presenceResp, disconnect := client.presenceCmd(&protocol.PresenceRequest{
		Channel: "test",
	})
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorNotAvailable, presenceResp.Error)
}

func TestClientCloseUnauthenticated(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.ClientStaleCloseDelay = time.Millisecond
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)
	time.Sleep(100 * time.Millisecond)
	client.mu.Lock()
	assert.True(t, client.closed)
	client.mu.Unlock()
}

func TestClientPresenceUpdate(t *testing.T) {
	node := nodeWithMemoryEngine()

	config := node.Config()
	config.Presence = true
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)
	subscribeClient(t, client, "test")

	err := client.updateChannelPresence("test")
	assert.NoError(t, err)
}

func TestClientSend(t *testing.T) {
	node := nodeWithMemoryEngine()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	err := client.Send([]byte(`{}`))
	assert.NoError(t, err)

	err = client.Close(nil)
	assert.NoError(t, err)

	err = client.Send([]byte(`{}`))
	assert.Error(t, err)
	assert.Equal(t, io.EOF, err)
}

func TestClientClose(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	err := client.Close(DisconnectShutdown)
	assert.NoError(t, err)
	assert.True(t, transport.closed)
	assert.Equal(t, DisconnectShutdown, transport.disconnect)
}

func TestClientHandleMalformedCommand(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     1,
		Method: 1000,
		Params: []byte(`{}`),
	}, rw.write, rw.flush)
	assert.Nil(t, disconnect)
	assert.Equal(t, ErrorMethodNotFound, replies[0].Error)

	replies = nil
	disconnect = client.handleCommand(&protocol.Command{
		ID:     1,
		Method: 2,
		Params: []byte(`{}`),
	}, rw.write, rw.flush)
	assert.Equal(t, DisconnectBadRequest, disconnect)
}

func TestUnique(t *testing.T) {
	pubs := []*Publication{
		{Seq: 101, Gen: 0},
		{Seq: 101, Gen: 1},
		{Seq: 101, Gen: 1},
		{Seq: 100, Gen: 2},
		{Seq: 99},
		{Seq: 98},
		{Seq: 4294967295, Gen: 0},
		{Seq: 4294967295, Gen: 1},
		{Seq: 4294967295, Gen: 4294967295},
		{Seq: 4294967295, Gen: 4294967295},
	}
	pubs = uniquePublications(pubs)
	assert.Equal(t, 8, len(pubs))
}

var recoverTests = []struct {
	Name            string
	HistorySize     int
	HistoryLifetime int
	NumPublications int
	SinceSeq        uint32
	NumRecovered    int
	Sleep           int
	Recovered       bool
}{
	{"empty_stream", 10, 60, 0, 0, 0, 0, true},
	{"from_position", 10, 60, 10, 8, 2, 0, true},
	{"from_position_that_is_too_far", 10, 60, 20, 8, 10, 0, false},
	{"same_position_no_history_expected", 10, 60, 7, 7, 0, 0, true},
	{"empty_position_recover_expected", 10, 60, 4, 0, 4, 0, true},
	{"from_position_in_expired_stream", 10, 1, 10, 8, 0, 3, false},
	{"from_same_position_in_expired_stream", 10, 1, 1, 1, 0, 3, true},
}

func TestClientSubscribeRecoverMemory(t *testing.T) {
	for _, tt := range recoverTests {
		t.Run(tt.Name, func(t *testing.T) {
			node := nodeWithMemoryEngine()

			config := node.Config()
			config.HistorySize = tt.HistorySize
			config.HistoryLifetime = tt.HistoryLifetime
			config.HistoryRecover = true
			node.Reload(config)

			transport := newTestTransport()
			ctx := context.Background()
			newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
			client, _ := NewClient(newCtx, node, transport)

			channel := "test_recovery_memory_" + tt.Name

			for i := 1; i <= tt.NumPublications; i++ {
				node.Publish(channel, []byte(`{"n": `+strconv.Itoa(i)+`}`))
			}

			time.Sleep(time.Duration(tt.Sleep) * time.Second)

			connectClient(t, client)

			replies := []*protocol.Reply{}
			rw := testReplyWriter(&replies)

			_, recoveryPosition, _ := node.historyManager.History(channel, HistoryFilter{
				Limit: 0,
				Since: nil,
			})

			disconnect := client.subscribeCmd(&protocol.SubscribeRequest{
				Channel: channel,
				Recover: true,
				Seq:     tt.SinceSeq,
				Gen:     recoveryPosition.Gen,
				Epoch:   recoveryPosition.Epoch,
			}, rw)
			assert.Nil(t, disconnect)
			assert.Nil(t, replies[0].Error)
			res := extractSubscribeResult(replies)
			assert.Equal(t, tt.NumRecovered, len(res.Publications))
			assert.Equal(t, tt.Recovered, res.Recovered)
		})
	}
}
