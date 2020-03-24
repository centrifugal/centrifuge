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
	"github.com/stretchr/testify/require"
)

func getConnToken(user string, exp int64) string {
	claims := jwt.MapClaims{"sub": user}
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
	require.NotNil(t, h.disconnectHandler)
}

func TestSetCredentials(t *testing.T) {
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{})
	val := newCtx.Value(credentialsContextKey).(*Credentials)
	require.NotNil(t, val)
}

func TestNewClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport()
	client, err := NewClient(context.Background(), node, transport)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClientInitialState(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	require.Equal(t, client.uid, client.ID())
	require.NotNil(t, "", client.user)
	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, protocol.TypeJSON, client.Transport().Protocol())
	require.Equal(t, "test_transport", client.Transport().Name())
	require.False(t, client.closed)
	require.False(t, client.authenticated)
	require.Nil(t, client.disconnect)
}

func TestClientClosedState(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	err := client.Close(nil)
	require.NoError(t, err)
	require.True(t, client.closed)
}

func TestClientConnectNoCredentialsNoToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.NotNil(t, disconnect)
	require.Equal(t, disconnect, DisconnectBadRequest)
}

func TestClientConnectNoCredentialsNoTokenInsecure(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.ClientInsecure = true
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
	result := extractConnectResult(replies)
	require.NotEmpty(t, result.Client)
	require.Empty(t, client.UserID())
}

func TestClientConnectNoCredentialsNoTokenAnonymous(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.ClientAnonymous = true
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
	result := extractConnectResult(replies)
	require.NotEmpty(t, result.Client)
	require.Empty(t, client.UserID())
}

func TestClientConnectWithMalformedToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: "bad bad token",
	}, rw)
	require.NotNil(t, disconnect)
	require.Equal(t, disconnect, DisconnectInvalidToken)
}

func TestClientConnectWithValidToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.TokenHMACSecretKey = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", 0),
	}, rw)
	require.Nil(t, disconnect)
	result := extractConnectResult(replies)
	require.Equal(t, client.ID(), result.Client)
	require.Equal(t, false, result.Expires)
}

func TestClientConnectWithExpiringToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.TokenHMACSecretKey = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", time.Now().Unix()+10),
	}, rw)
	require.Nil(t, disconnect)
	result := extractConnectResult(replies)
	require.Equal(t, true, result.Expires)
	require.True(t, result.TTL > 0)
	require.True(t, client.authenticated)
}

func TestClientConnectWithExpiredToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.TokenHMACSecretKey = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", 1525541722),
	}, rw)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorTokenExpired, replies[0].Error)
	require.False(t, client.authenticated)
}

func TestClientTokenRefresh(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.TokenHMACSecretKey = "secret"
	node.Reload(config)

	transport := newTestTransport()
	client, _ := NewClient(context.Background(), node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{
		Token: getConnToken("42", 1525541722),
	}, rw)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorTokenExpired, replies[0].Error)

	refreshResp, disconnect := client.refreshCmd(&protocol.RefreshRequest{
		Token: getConnToken("42", 2525637058),
	})
	require.Nil(t, disconnect)
	require.NotEmpty(t, client.ID())
	require.True(t, refreshResp.Result.Expires)
	require.True(t, refreshResp.Result.TTL > 0)
}

func TestClientConnectContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	result := extractConnectResult(replies)
	require.Equal(t, false, result.Expires)
	require.Equal(t, uint32(0), result.TTL)
	require.True(t, client.authenticated)
	require.Equal(t, "42", client.UserID())
}

func TestClientRefreshHandlerClosingExpiredClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	client.expire()
	require.True(t, client.closed)
}

func TestClientRefreshHandlerProlongatesClientSession(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	client.expire()
	require.False(t, client.closed)
	require.Equal(t, expireAt, client.exp)
}

func TestClientConnectWithExpiredContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorExpired, replies[0].Error)
}

func connectClient(t testing.TB, client *Client) *protocol.ConnectResult {
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
	require.True(t, client.authenticated)
	result := extractConnectResult(replies)
	require.Equal(t, client.uid, result.Client)
	return result
}

func extractSubscribeResult(replies []*protocol.Reply) *protocol.SubscribeResult {
	var res protocol.SubscribeResult
	err := json.Unmarshal(replies[0].Result, &res)
	if err != nil {
		panic(err)
	}
	return &res
}

func extractConnectResult(replies []*protocol.Reply) *protocol.ConnectResult {
	var res protocol.ConnectResult
	err := json.Unmarshal(replies[0].Result, &res)
	if err != nil {
		panic(err)
	}
	return &res
}

func subscribeClient(t testing.TB, client *Client, ch string) *protocol.SubscribeResult {
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	ctx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: ch,
	}, rw, false)
	require.Nil(t, ctx.disconnect)
	require.Nil(t, replies[0].Error)
	return extractSubscribeResult(replies)
}

func TestClientSubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	require.Equal(t, 0, len(client.Channels()))

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test1",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, 1, len(replies))
	require.Nil(t, replies[0].Error)
	res := extractSubscribeResult(replies)
	require.Empty(t, res.Seq)
	require.False(t, res.Recovered)
	require.Empty(t, res.Publications)
	require.Equal(t, 1, len(client.Channels()))

	replies = nil
	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test2",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, 2, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 2, node.Hub().NumChannels())

	replies = nil
	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test2",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, ErrorAlreadySubscribed, replies[0].Error)
}

func TestClientSubscribeReceivePublication(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, replies[0].Error)

	done := make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), "test message") {
				close(done)
			}
		}
	}()

	err := node.Publish("test", []byte(`{"text": "test message"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publication")
	case <-done:
	}
}

func TestClientSubscribeReceivePublicationWithSequence(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
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

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, replies[0].Error)

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
					require.NoError(t, err)
					if push.Result.Data.Seq != seq {
						require.Fail(t, "wrong seq")
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
	require.NoError(t, err)
	err = node.Publish("test", []byte(`{"text": "test message 2"}`))
	require.NoError(t, err)
	err = node.Publish("test", []byte(`{"text": "test message 3"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publications")
	case <-done:
	}
}

func TestServerSideSubscriptions(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	node.On().ClientConnecting(func(context.Context, TransportInfo, ConnectEvent) ConnectReply {
		return ConnectReply{
			Channels: []string{
				"server-side-1",
				"server-side-2",
			},
		}
	})
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	client.handleCommand(&protocol.Command{
		ID: 1,
	}, rw.write, rw.flush)

	done := make(chan struct{})
	go func() {
		var i int
		for data := range transport.sink {
			if strings.Contains(string(data), "test message 1") {
				i++
			}
			if strings.Contains(string(data), "test message 2") {
				i++
			}
			if strings.Contains(string(data), "test message 3") {
				i++
			}
			if i == 3 {
				close(done)
			}
		}
	}()

	client.Subscribe("server-side-3")
	err := node.Publish("server-side-1", []byte(`{"text": "test message 1"}`))
	require.NoError(t, err)
	err = node.Publish("server-side-2", []byte(`{"text": "test message 2"}`))
	require.NoError(t, err)
	err = node.Publish("server-side-3", []byte(`{"text": "test message 3"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publication")
	case <-done:
	}
}

func TestClientUserPersonalChannel(t *testing.T) {
	node := nodeWithMemoryEngine()
	config := node.Config()
	config.UserSubscribeToPersonal = true
	config.Namespaces = []ChannelNamespace{
		ChannelNamespace{
			Name:           "user",
			ChannelOptions: ChannelOptions{},
		},
	}
	node.Reload(config)

	defer node.Shutdown(context.Background())

	var tests = []struct {
		Name      string
		Namespace string
		Error     *Error
	}{
		{"ok_no_namespace", "", nil},
		{"ok_with_namespace", "user", nil},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			config := node.Config()
			config.UserSubscribeToPersonal = true
			config.UserPersonalChannelNamespace = tt.Namespace
			err := node.Reload(config)
			require.NoError(t, err)
			transport := newTestTransport()
			transport.sink = make(chan []byte, 100)
			ctx := context.Background()
			newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
			client, _ := NewClient(newCtx, node, transport)
			replies := []*protocol.Reply{}
			rw := testReplyWriter(&replies)
			client.handleCommand(&protocol.Command{
				ID: 1,
			}, rw.write, rw.flush)
			if tt.Error != nil {
				require.Equal(t, tt.Error, replies[0].Error)
			} else {
				done := make(chan struct{})
				go func() {
					for data := range transport.sink {
						if strings.Contains(string(data), "test message") {
							close(done)
						}
					}
				}()

				err := node.Publish(node.PersonalChannel("42"), []byte(`{"text": "test message"}`))
				require.NoError(t, err)

				select {
				case <-time.After(time.Second):
					require.Fail(t, "timeout receiving publication")
				case <-done:
				}
			}
		})
	}
}

func TestClientSubscribePrivateChannelNoToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, ErrorPermissionDenied, replies[0].Error)
}

func TestClientSubscribePrivateChannelWithToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.TokenHMACSecretKey = "secret"
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$wrong_channel", "wrong client", 0),
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, ErrorPermissionDenied, replies[0].Error)

	replies = nil
	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$wrong_channel", client.ID(), 0),
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, ErrorPermissionDenied, replies[0].Error)

	replies = nil
	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$test1", client.ID(), 0),
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, replies[0].Error)
}

func TestClientSubscribePrivateChannelWithExpiringToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.TokenHMACSecretKey = "secret"
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$test1", client.ID(), 10),
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, ErrorTokenExpired, replies[0].Error)

	replies = nil
	subCtx = client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "$test1",
		Token:   getSubscribeToken("$test1", client.ID(), time.Now().Unix()+10),
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, replies[0].Error, "token is valid and not expired yet")
	res := extractSubscribeResult(replies)
	require.True(t, res.Expires, "expires flag must be set")
	require.True(t, res.TTL > 0, "positive TTL must be set")
}

func TestClientSubscribeLast(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.Equal(t, uint32(0), result.Seq)

	for i := 0; i < 10; i++ {
		node.Publish("test", []byte("{}"))
	}

	client, _ = NewClient(newCtx, node, transport)
	connectClient(t, client)
	result = subscribeClient(t, client, "test")
	require.Equal(t, uint32(10), result.Seq)
}

func TestClientUnsubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)
	subscribeClient(t, client, "test")

	unsubscribeResp, disconnect := client.unsubscribeCmd(&protocol.UnsubscribeRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Nil(t, unsubscribeResp.Error)

	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 0, node.Hub().NumChannels())

	subscribeClient(t, client, "test")
	require.Equal(t, 1, len(client.Channels()))

	err := client.Unsubscribe("test")
	require.NoError(t, err)
	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 0, node.Hub().NumChannels())
}

func TestClientPublish(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	publishResp, disconnect := client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorPermissionDenied, publishResp.Error)

	config := node.Config()
	config.Publish = true
	node.Reload(config)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	require.Nil(t, disconnect)
	require.Nil(t, publishResp.Error)

	config = node.Config()
	config.SubscribeToPublish = true
	node.Reload(config)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorPermissionDenied, publishResp.Error)

	subscribeClient(t, client, "test")
	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	require.Nil(t, disconnect)
	require.Nil(t, publishResp.Error)
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
	defer node.Shutdown(context.Background())
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
	require.Nil(t, disconnect)
	require.Nil(t, publishResp.Error)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with timestamp"}`),
	})
	require.Nil(t, disconnect)
	require.Nil(t, publishResp.Error)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with error"}`),
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorBadRequest, publishResp.Error)

	publishResp, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with disconnect"}`),
	})
	require.Equal(t, DisconnectBadRequest, disconnect)
}

func TestClientPing(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	pingResp, disconnect := client.pingCmd(&protocol.PingRequest{})
	require.Nil(t, disconnect)
	require.Nil(t, pingResp.Error)
	require.Empty(t, pingResp.Result)
}

func TestClientPingWithRecover(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.Nil(t, disconnect)
	require.Nil(t, pingResp.Error)
	require.Nil(t, pingResp.Result)
}

func TestClientPresence(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.Nil(t, disconnect)
	require.Nil(t, presenceResp.Error)
	require.Equal(t, 1, len(presenceResp.Result.Presence))

	presenceStatsResp, disconnect := client.presenceStatsCmd(&protocol.PresenceStatsRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Nil(t, presenceResp.Error)
	require.Equal(t, uint32(1), presenceStatsResp.Result.NumUsers)
	require.Equal(t, uint32(1), presenceStatsResp.Result.NumClients)

	config = node.Config()
	config.Presence = false
	node.Reload(config)

	presenceResp, disconnect = client.presenceCmd(&protocol.PresenceRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable, presenceResp.Error)
	require.Nil(t, presenceResp.Result)

	presenceStatsResp, disconnect = client.presenceStatsCmd(&protocol.PresenceStatsRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable, presenceStatsResp.Error)
	require.Nil(t, presenceStatsResp.Result)
}

func TestClientHistory(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.Nil(t, disconnect)
	require.Nil(t, historyResp.Error)
	require.Equal(t, 10, len(historyResp.Result.Publications))

	config = node.Config()
	config.HistorySize = 0
	config.HistoryLifetime = 0
	node.Reload(config)

	historyResp, disconnect = client.historyCmd(&protocol.HistoryRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable, historyResp.Error)
	require.Nil(t, historyResp.Result)
}

func TestClientHistoryDisabled(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable, historyResp.Error)
}

func TestClientPresenceDisabled(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable, presenceResp.Error)
}

func TestClientCloseUnauthenticated(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	config := node.Config()
	config.ClientStaleCloseDelay = time.Millisecond
	node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)
	time.Sleep(100 * time.Millisecond)
	client.mu.Lock()
	require.True(t, client.closed)
	client.mu.Unlock()
}

func TestClientPresenceUpdate(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.NoError(t, err)
}

func TestClientSend(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	err := client.Send([]byte(`{}`))
	require.NoError(t, err)

	err = client.Close(nil)
	require.NoError(t, err)

	err = client.Send([]byte(`{}`))
	require.Error(t, err)
	require.Equal(t, io.EOF, err)
}

func TestClientClose(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)

	connectClient(t, client)

	err := client.Close(DisconnectShutdown)
	require.NoError(t, err)
	require.True(t, transport.closed)
	require.Equal(t, DisconnectShutdown, transport.disconnect)
}

func TestClientHandleMalformedCommand(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer node.Shutdown(context.Background())

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
	require.Nil(t, disconnect)
	require.Equal(t, ErrorMethodNotFound, replies[0].Error)

	replies = nil
	disconnect = client.handleCommand(&protocol.Command{
		ID:     1,
		Method: 2,
		Params: []byte(`{}`),
	}, rw.write, rw.flush)
	require.Equal(t, DisconnectBadRequest, disconnect)
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

func TestMemoryClientSubscribeRecover(t *testing.T) {
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

			subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
				Channel: channel,
				Recover: true,
				Seq:     tt.SinceSeq,
				Gen:     recoveryPosition.Gen,
				Epoch:   recoveryPosition.Epoch,
			}, rw, false)
			require.Nil(t, subCtx.disconnect)
			require.Nil(t, replies[0].Error)
			res := extractSubscribeResult(replies)
			require.Equal(t, tt.NumRecovered, len(res.Publications))
			require.Equal(t, tt.Recovered, res.Recovered)

			node.Shutdown(context.Background())
		})
	}
}
