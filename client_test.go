package centrifuge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func newClient(ctx context.Context, n *Node, t Transport) (*Client, error) {
	c, _, err := NewClient(ctx, n, t)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newTestConnectedClient(t *testing.T, n *Node, userID string) *Client {
	client := newTestClient(t, n, userID)
	connectClient(t, client)
	require.Contains(t, n.hub.users, userID)
	require.Contains(t, n.hub.conns, client.uid)
	return client
}

func newTestSubscribedClient(t *testing.T, n *Node, userID, chanID string) *Client {
	client := newTestConnectedClient(t, n, userID)
	subscribeClient(t, client, chanID)
	require.Contains(t, n.hub.subs, chanID)
	require.Contains(t, client.channels, chanID)
	return client
}

func TestSetCredentials(t *testing.T) {
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{})
	val := newCtx.Value(credentialsContextKey).(*Credentials)
	require.NotNil(t, val)
}

func TestNewClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	transport := newTestTransport(func() {})
	client, err := newClient(context.Background(), node, transport)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClientInitialState(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	require.Equal(t, client.uid, client.ID())
	require.NotNil(t, "", client.user)
	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, ProtocolTypeJSON, client.Transport().Protocol())
	require.Equal(t, "test_transport", client.Transport().Name())
	require.True(t, client.status == statusConnecting)
	require.False(t, client.authenticated)
}

func TestClientClosedState(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	err := client.close(nil)
	require.NoError(t, err)
	require.True(t, client.status == statusClosed)
}

func TestClientTimer(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.ClientStaleCloseDelay = 25 * time.Second
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	require.NotNil(t, client.timer)
	node.config.ClientStaleCloseDelay = 0
	client, _ = newClient(context.Background(), node, transport)
	require.Nil(t, client.timer)
}

func TestClientOnTimerOpClosedClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	err := client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)
	client.onTimerOp()
	require.False(t, client.timer.Stop())
}

func TestClientUnsubscribeClosedClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClient(t, client)
	subscribeClient(t, client, "test")
	err := client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)
	err = client.Unsubscribe("test")
	require.NoError(t, err)
}

func TestClientTimerSchedule(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	client.mu.Lock()
	defer client.mu.Unlock()
	client.nextExpire = time.Now().Add(5 * time.Second).UnixNano()
	client.nextPresence = time.Now().Add(10 * time.Second).UnixNano()
	client.scheduleNextTimer()
	require.NotNil(t, client.timer)
	require.Equal(t, timerOpExpire, client.timerOp)
	client.nextPresence = time.Now().Add(time.Second).UnixNano()
	client.scheduleNextTimer()
	require.NotNil(t, client.timer)
	require.Equal(t, timerOpPresence, client.timerOp)
}

func TestClientConnectNoCredentialsNoToken(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientConnectContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.NoError(t, err)
	result := extractConnectReply(rwWrapper.replies, client.Transport().Protocol())
	require.Equal(t, false, result.Expires)
	require.Equal(t, uint32(0), result.TTL)
	require.True(t, client.authenticated)
	require.Equal(t, "42", client.UserID())
}

func TestClientRefreshHandlerClosingExpiredClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(_ RefreshEvent, callback RefreshCallback) {
			callback(RefreshReply{
				Expired: true,
			}, nil)
		})
	})

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.NoError(t, err)
	client.triggerConnect()
	client.expire()
	require.True(t, client.status == statusClosed)
}

func TestClientRefreshHandlerProlongsClientSession(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	expireAt := time.Now().Unix() + 60

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{
				ExpireAt: expireAt,
			}, nil)
		})
	})

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.NoError(t, err)
	client.expire()
	require.False(t, client.status == statusClosed)
	require.Equal(t, expireAt, client.exp)
}

func TestClientConnectWithExpiredContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() - 60,
	})
	client, _ := newClient(newCtx, node, transport)

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(_ RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{}, nil)
		})
	})

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.Equal(t, ErrorExpired, err)
}

func connectClient(t testing.TB, client *Client) *protocol.ConnectResult {
	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, client.authenticated)
	result := extractConnectReply(rwWrapper.replies, client.Transport().Protocol())
	require.Equal(t, client.uid, result.Client)
	client.triggerConnect()
	client.scheduleOnConnectTimers()
	return result
}

func extractSubscribeResult(replies []*protocol.Reply, protoType ProtocolType) *protocol.SubscribeResult {
	var res protocol.SubscribeResult
	if protoType == ProtocolTypeJSON {
		err := json.Unmarshal(replies[0].Result, &res)
		if err != nil {
			panic(err)
		}
	} else {
		err := res.Unmarshal(replies[0].Result)
		if err != nil {
			panic(err)
		}
	}
	return &res
}

func extractConnectReply(replies []*protocol.Reply, protoType ProtocolType) *protocol.ConnectResult {
	var res protocol.ConnectResult
	if protoType == ProtocolTypeJSON {
		err := json.Unmarshal(replies[0].Result, &res)
		if err != nil {
			panic(err)
		}
	} else {
		err := res.Unmarshal(replies[0].Result)
		if err != nil {
			panic(err)
		}
	}
	return &res
}

func subscribeClient(t testing.TB, client *Client, ch string) *protocol.SubscribeResult {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: ch,
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	return extractSubscribeResult(rwWrapper.replies, client.Transport().Protocol())
}

func TestClientSubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClient(t, client)

	require.Equal(t, 0, len(client.Channels()))

	rwWrapper := testReplyWriterWrapper()

	err := client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test1",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, 1, len(rwWrapper.replies))
	require.Nil(t, rwWrapper.replies[0].Error)
	res := extractSubscribeResult(rwWrapper.replies, client.Transport().Protocol())
	require.Empty(t, res.Seq)
	require.False(t, res.Recovered)
	require.Empty(t, res.Publications)
	require.Equal(t, 1, len(client.Channels()))

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test2",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, 2, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 2, node.Hub().NumChannels())

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test2",
	}), rwWrapper.rw)
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestClientSubscribeEngineErrorOnSubscribe(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnSubscribe = true
	node := nodeWithEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError, event.Disconnect)
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test1",
	}), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestClientSubscribeEngineErrorOnStreamTop(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnHistory = true
	node := nodeWithEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{Recover: true},
			}, nil)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError, event.Disconnect)
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test1",
	}), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestClientSubscribeEngineErrorOnRecoverHistory(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnHistory = true
	node := nodeWithEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{Options: SubscribeOptions{Recover: true}}, nil)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError, event.Disconnect)
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test1",
		Recover: true,
	}), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestClientSubscribeValidateErrors(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.ClientChannelLimit = 1
	node.config.ChannelMaxLength = 10
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test2_very_long_channel_name",
	}), rwWrapper.rw)
	require.Equal(t, ErrorLimitExceeded, err)

	subscribeClient(t, client, "test1")

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test2",
	}), rwWrapper.rw)
	require.Equal(t, ErrorLimitExceeded, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "",
	}), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientSubscribeReceivePublication(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, SubscribeReply{}, rwWrapper.rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, rwWrapper.replies[0].Error)

	done := make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), "test message") {
				close(done)
			}
		}
	}()

	_, err := node.Publish("test", []byte(`{"text": "test message"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publication")
	case <-done:
	}
}

func TestClientSubscribeReceivePublicationWithOffset(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, SubscribeReply{}, rwWrapper.rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, rwWrapper.replies[0].Error)

	done := make(chan struct{})
	go func() {
		var offset uint64 = 1
		for data := range transport.sink {
			if strings.Contains(string(data), "test message") {
				dec := json.NewDecoder(strings.NewReader(string(data)))
				for {
					var push struct {
						Result struct {
							Channel string
							Data    struct {
								Offset uint64
							}
						}
					}
					err := dec.Decode(&push)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if push.Result.Data.Offset != offset {
						require.Fail(t, fmt.Sprintf("wrong offset: %d != %d", push.Result.Data.Offset, offset))
					}
					offset++
					if offset > 3 {
						close(done)
					}
				}
			}
		}
	}()

	// Send 3 publications, expect client to receive them with
	// incremental sequence numbers.
	_, err := node.Publish("test", []byte(`{"text": "test message 1"}`), WithHistory(10, time.Minute))
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text": "test message 2"}`), WithHistory(10, time.Minute))
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text": "test message 3"}`), WithHistory(10, time.Minute))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publications")
	case <-done:
	}
}

func TestUserConnectionLimit(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.UserConnectionLimit = 1
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	anotherClient, _ := newClient(newCtx, node, transport)
	err := anotherClient.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.Equal(t, DisconnectConnectionLimit, err)
}

type testContextKey int

var keyTest testContextKey = 1

func TestConnectingReply(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		newCtx := context.WithValue(ctx, keyTest, "val")
		return ConnectReply{
			Context: newCtx,
			Data:    []byte("{}"),
			Credentials: &Credentials{
				UserID: "12",
			},
		}, nil
	})

	done := make(chan struct{})

	node.OnConnect(func(c *Client) {
		v, ok := c.Context().Value(keyTest).(string)
		require.True(t, ok)
		require.Equal(t, "val", v)
		require.Equal(t, "12", c.UserID())
		close(done)
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestServerSideSubscriptions(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Subscriptions: map[string]SubscribeOptions{
				"server-side-1":  {},
				"$server-side-2": {},
			},
		}, nil
	})
	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	_ = client.Subscribe("server-side-3")
	_, err := node.Publish("server-side-1", []byte(`{"text": "test message 1"}`))
	require.NoError(t, err)
	_, err = node.Publish("$server-side-2", []byte(`{"text": "test message 2"}`))
	require.NoError(t, err)
	_, err = node.Publish("server-side-3", []byte(`{"text": "test message 3"}`))
	require.NoError(t, err)

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

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publication")
	case <-done:
	}
}

func TestClient_IsSubscribed(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	require.False(t, client.IsSubscribed("test"))
	_ = subscribeClient(t, client, "test")
	require.True(t, client.IsSubscribed("test"))
}

func TestClientSubscribeLast(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{Recover: true}}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	result := subscribeClient(t, client, "test")
	require.Equal(t, uint32(0), result.Seq)

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
	}

	client = newTestClient(t, node, "42")
	connectClient(t, client)
	result = subscribeClient(t, client, "test")
	require.Equal(t, uint64(10), result.Offset, fmt.Sprintf("expected: 10, got %d", result.Offset))
}

func newTestClient(t testing.TB, node *Node, userID string) *Client {
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	newCtx := SetCredentials(ctx, &Credentials{UserID: userID})
	client, err := newClient(newCtx, node, transport)
	require.NoError(t, err)
	return client
}

func getJSONEncodedParams(t testing.TB, request interface{}) []byte {
	paramsEncoder := protocol.NewJSONParamsEncoder()
	params, err := paramsEncoder.Encode(request)
	require.NoError(t, err)
	return params
}

func TestClientUnsubscribeClientSide(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	unsubscribed := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(_ UnsubscribeEvent) {
			close(unsubscribed)
		})
	})

	connectClient(t, client)
	subscribeClient(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	params := getJSONEncodedParams(t, &protocol.UnsubscribeRequest{Channel: "test"})
	err := client.handleUnsubscribe(params, rwWrapper.rw)
	require.NoError(t, err)

	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 0, node.Hub().NumChannels())

	select {
	case <-unsubscribed:
	case <-time.After(time.Second):
		t.Fatal("unsubscribe handler not called")
	}
}

func TestClientUnsubscribeServerSide(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")

	unsubscribed := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(_ UnsubscribeEvent) {
			close(unsubscribed)
		})
	})

	connectClient(t, client)
	subscribeClient(t, client, "test")
	require.Equal(t, 1, len(client.Channels()))

	err := client.Unsubscribe("test")
	require.NoError(t, err)
	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 0, node.Hub().NumChannels())

	select {
	case <-unsubscribed:
	case <-time.After(time.Second):
		t.Fatal("unsubscribe handler not called")
	}
}

func TestClientAliveHandler(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.ClientPresenceUpdateInterval = time.Millisecond

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	done := make(chan struct{})
	closed := false
	disconnected := make(chan struct{})
	numCalls := 0

	node.OnConnect(func(client *Client) {
		client.OnAlive(func() {
			numCalls++
			if numCalls >= 50 && !closed {
				close(done)
				closed = true
				client.Disconnect(DisconnectForceNoReconnect)
			}
		})

		client.OnDisconnect(func(_ DisconnectEvent) {
			close(disconnected)
		})

	})

	connectClient(t, client)
	client.triggerConnect()
	client.scheduleOnConnectTimers()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("alive handler not called")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("disconnect handler not called")
	}
}

type sliceReplyWriter struct {
	replies []*protocol.Reply
	rw      *replyWriter
}

func testReplyWriterWrapper() *sliceReplyWriter {
	replies := make([]*protocol.Reply, 0)
	wrapper := &sliceReplyWriter{
		replies: replies,
	}
	wrapper.rw = &replyWriter{
		write: func(rep *protocol.Reply) error {
			wrapper.replies = append(wrapper.replies, rep)
			return nil
		},
		flush: func() error {
			return nil
		},
		done: func() {},
	}
	return wrapper
}

func TestClientRefreshNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClient(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.RefreshRequest{}
	params := getJSONEncodedParams(t, cmd)

	err := client.handleRefresh(params, rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientRefreshEmptyToken(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{ClientSideRefresh: true}, nil
	})

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(event RefreshEvent, callback RefreshCallback) {
			callback(RefreshReply{
				ExpireAt: time.Now().Unix() + 300,
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.RefreshRequest{Token: ""}
	params := getJSONEncodedParams(t, cmd)

	err := client.handleRefresh(params, rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientRefreshUnexpected(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{ClientSideRefresh: false}, nil // we do not want client side refresh here.
	})

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(event RefreshEvent, callback RefreshCallback) {
			callback(RefreshReply{
				ExpireAt: time.Now().Unix() + 300,
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.RefreshRequest{Token: ""}
	params := getJSONEncodedParams(t, cmd)

	err := client.handleRefresh(params, rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientPublishNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClient(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	}
	params := getJSONEncodedParams(t, cmd)

	err := client.handlePublish(params, rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

type testBrokerEventHandler struct {
	// Publication must register callback func to handle Publications received.
	HandlePublicationFunc func(ch string, pub *Publication) error
	// Join must register callback func to handle Join messages received.
	HandleJoinFunc func(ch string, info *ClientInfo) error
	// Leave must register callback func to handle Leave messages received.
	HandleLeaveFunc func(ch string, info *ClientInfo) error
	// Control must register callback func to handle Control data received.
	HandleControlFunc func([]byte) error
}

func (b *testBrokerEventHandler) HandlePublication(ch string, pub *Publication) error {
	if b.HandlePublicationFunc != nil {
		return b.HandlePublicationFunc(ch, pub)
	}
	return nil
}

func (b *testBrokerEventHandler) HandleJoin(ch string, info *ClientInfo) error {
	if b.HandleJoinFunc != nil {
		return b.HandleJoinFunc(ch, info)
	}
	return nil
}

func (b *testBrokerEventHandler) HandleLeave(ch string, info *ClientInfo) error {
	if b.HandleLeaveFunc != nil {
		return b.HandleLeaveFunc(ch, info)
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
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
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

	subscribeClient(t, client, "test")

	client.eventHub.publishHandler = func(e PublishEvent, cb PublishCallback) {
		var msg testClientMessage
		err := json.Unmarshal(e.Data, &msg)
		require.NoError(t, err)
		if msg.Input == "with disconnect" {
			cb(PublishReply{}, DisconnectBadRequest)
			return
		}
		if msg.Input == "with error" {
			cb(PublishReply{}, ErrorBadRequest)
			return
		}
		if msg.Input == "with timestamp" {
			msg.Timestamp = time.Now().Unix()
			data, _ := json.Marshal(msg)
			res, err := node.Publish(e.Channel, data)
			require.NoError(t, err)
			cb(PublishReply{
				Result: &res,
			}, nil)
			return
		}
		cb(PublishReply{}, nil)
	}

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "no time"}`),
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with timestamp"}`),
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with error"}`),
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorBadRequest.toProto(), rwWrapper.replies[0].Error)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with disconnect"}`),
	}), rwWrapper.rw)
	require.NoError(t, err)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientPublishError(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnPublish = true
	node := nodeWithEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnPublish(func(event PublishEvent, cb PublishCallback) {
			require.Equal(t, "test", event.Channel)
			require.NotNil(t, event.ClientInfo)
			cb(PublishReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "no time"}`),
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientPing(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePing(getJSONEncodedParams(t, &protocol.PingRequest{}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.Empty(t, rwWrapper.replies[0].Result)
}

func TestClientPresence(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{Presence: true}}, nil)
	})

	client.OnPresence(func(e PresenceEvent, cb PresenceCallback) {
		cb(PresenceReply{}, nil)
	})
	client.OnPresenceStats(func(e PresenceStatsEvent, cb PresenceStatsCallback) {
		cb(PresenceStatsReply{}, nil)
	})

	connectClient(t, client)
	subscribeClient(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresence(getJSONEncodedParams(t, &protocol.PresenceRequest{
		Channel: "",
	}), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePresence(getJSONEncodedParams(t, &protocol.PresenceRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePresenceStats(getJSONEncodedParams(t, &protocol.PresenceStatsRequest{
		Channel: "",
	}), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePresenceStats(getJSONEncodedParams(t, &protocol.PresenceStatsRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestClientPresenceError(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnPresence = true
	node := nodeWithEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnPresence(func(event PresenceEvent, cb PresenceCallback) {
			require.Equal(t, "test", event.Channel)
			cb(PresenceReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresence(getJSONEncodedParams(t, &protocol.PresenceRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientPresenceNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClient(t, client)
	subscribeClient(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresence(getJSONEncodedParams(t, &protocol.PresenceRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientSubscribeNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientPresenceStatsNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClient(t, client)
	subscribeClient(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresenceStats(getJSONEncodedParams(t, &protocol.PresenceStatsRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientPresenceStatsError(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnPresenceStats = true
	node := nodeWithEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnPresenceStats(func(event PresenceStatsEvent, cb PresenceStatsCallback) {
			require.Equal(t, "test", event.Channel)
			cb(PresenceStatsReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresenceStats(getJSONEncodedParams(t, &protocol.PresenceStatsRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientHistory(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		cb(HistoryReply{}, nil)
	})

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte(`{}`), WithHistory(10, time.Minute))
	}

	connectClient(t, client)
	subscribeClient(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(getJSONEncodedParams(t, &protocol.HistoryRequest{
		Channel: "",
	}), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handleHistory(getJSONEncodedParams(t, &protocol.HistoryRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
	//require.Equal(t, 10, len(historyResp.Result.Publications))
}

func TestClientHistoryError(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnHistory = true
	node := nodeWithEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnHistory(func(event HistoryEvent, cb HistoryCallback) {
			require.Equal(t, "test", event.Channel)
			cb(HistoryReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(getJSONEncodedParams(t, &protocol.HistoryRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientHistoryNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClient(t, client)
	subscribeClient(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(getJSONEncodedParams(t, &protocol.HistoryRequest{
		Channel: "test",
	}), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientCloseUnauthenticated(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.ClientStaleCloseDelay = time.Millisecond

	client := newTestClient(t, node, "42")
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
	client.mu.Lock()
	require.True(t, client.status == statusClosed)
	client.mu.Unlock()
}

func TestClientHandleEmptyData(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	proceed := client.Handle(nil)
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
	proceed = client.Handle([]byte("test"))
	require.False(t, proceed)
	disconnect := client.handleCommand(&protocol.Command{})
	require.Nil(t, disconnect)
}

func TestClientHandleBrokenData(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	proceed := client.Handle([]byte(`nd3487yt734y38&**&**`))
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientHandleCommandNotAuthenticated(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	params := getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test",
	})
	cmd := &protocol.Command{ID: 1, Method: protocol.MethodTypeSubscribe, Params: params}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)
	proceed := client.Handle(data)
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientHandleUnknownMethod(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	params := getJSONEncodedParams(t, &protocol.SubscribeRequest{
		Channel: "test",
	})
	cmd := &protocol.Command{ID: 1, Method: 10000, Params: params}
	disconnect := client.handleCommand(cmd)
	require.Nil(t, disconnect)
}

func TestClientHandleCommandWithBrokenParams(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var counterMu sync.Mutex
	var numDisconnectCalls int
	var numConnectCalls int
	var wg sync.WaitGroup
	wg.Add(11)

	node.OnConnect(func(client *Client) {
		counterMu.Lock()
		numConnectCalls++
		counterMu.Unlock()

		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})

		client.OnRPC(func(e RPCEvent, cb RPCCallback) {
			cb(RPCReply{Data: []byte(`{"year": "2020"}`)}, nil)
		})

		client.OnMessage(func(event MessageEvent) {})

		client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
			cb(HistoryReply{}, nil)
		})

		client.OnPresence(func(e PresenceEvent, cb PresenceCallback) {
			cb(PresenceReply{}, nil)
		})

		client.OnPresenceStats(func(e PresenceStatsEvent, cb PresenceStatsCallback) {
			cb(PresenceStatsReply{}, nil)
		})

		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			cb(RefreshReply{}, nil)
		})

		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			cb(SubRefreshReply{}, nil)
		})

		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})

		client.OnDisconnect(func(event DisconnectEvent) {
			counterMu.Lock()
			numDisconnectCalls++
			counterMu.Unlock()
			require.Equal(t, DisconnectBadRequest, event.Disconnect)
			wg.Done()
		})
	})

	client := newTestClient(t, node, "42")

	data, err := json.Marshal(&protocol.Command{
		ID: 1, Method: protocol.MethodTypeConnect, Params: []byte("[]"),
	})
	require.NoError(t, err)
	proceed := client.Handle(data)
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
	// Check no connect and no disconnect event called.
	counterMu.Lock()
	require.Equal(t, 0, numDisconnectCalls)
	require.Equal(t, 0, numConnectCalls)
	counterMu.Unlock()

	// Now check other methods.
	methods := []protocol.MethodType{
		protocol.MethodTypeSubscribe,
		protocol.MethodTypePing,
		protocol.MethodTypePublish,
		protocol.MethodTypeUnsubscribe,
		protocol.MethodTypePresence,
		protocol.MethodTypePresenceStats,
		protocol.MethodTypeHistory,
		protocol.MethodTypeRefresh,
		protocol.MethodTypeRPC,
		protocol.MethodTypeSend,
		protocol.MethodTypeSubRefresh,
	}

	for _, method := range methods {
		client = newTestClient(t, node, "42")
		connectClient(t, client)
		data, err := json.Marshal(&protocol.Command{
			ID: 1, Method: method, Params: []byte("[]"),
		})
		require.NoError(t, err)
		proceed := client.Handle(data)
		require.False(t, proceed)
		select {
		case <-client.Context().Done():
		case <-time.After(time.Second):
			require.Fail(t, "client not closed")
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting wait group done")
	}
}

func TestClientOnAlive(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.ClientPresenceUpdateInterval = time.Second
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})
	var closeOnce sync.Once

	node.OnConnect(func(client *Client) {
		client.OnAlive(func() {
			closeOnce.Do(func() {
				close(done)
			})
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting done close")
	}
}

func TestClientHandleCommandWithoutID(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	params := getJSONEncodedParams(t, &protocol.ConnectRequest{})
	cmd := &protocol.Command{Method: protocol.MethodTypeConnect, Params: params}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)
	proceed := client.Handle(data)
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestErrorDisconnectContext(t *testing.T) {
	ctx := errorDisconnectContext(nil, DisconnectForceReconnect)
	require.Nil(t, ctx.err)
	require.Equal(t, DisconnectForceReconnect, ctx.disconnect)
	ctx = errorDisconnectContext(ErrorLimitExceeded, nil)
	require.Nil(t, ctx.disconnect)
	require.Equal(t, ErrorLimitExceeded, ctx.err)
}

func TestToClientError(t *testing.T) {
	require.Equal(t, ErrorInternal, toClientErr(errors.New("boom")))
	require.Equal(t, ErrorLimitExceeded, toClientErr(ErrorLimitExceeded))
}

func TestClientAlreadyAuthenticated(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	params := getJSONEncodedParams(t, &protocol.ConnectRequest{})
	cmd := &protocol.Command{ID: 2, Method: protocol.MethodTypeConnect, Params: params}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)
	proceed := client.Handle(data)
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientCloseExpired(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: time.Now().Unix() + 2})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)
	client.scheduleOnConnectTimers()
	client.mu.RLock()
	require.False(t, client.status == statusClosed)
	client.mu.RUnlock()
	select {
	case <-client.Context().Done():
	case <-time.After(5 * time.Second):
		require.Fail(t, "client not closed")
	}
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.True(t, client.status == statusClosed)
}

func TestClientConnectExpiredError(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: time.Now().Unix() - 2})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	err := client.connectCmd(&protocol.ConnectRequest{}, rwWrapper.rw)
	require.Equal(t, ErrorExpired, err)
	require.False(t, client.authenticated)
}

func TestClientPresenceUpdate(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{Presence: true},
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")

	connectClient(t, client)
	subscribeClient(t, client, "test")

	client.mu.RLock()
	chCtx, ok := client.channels["test"]
	client.mu.RUnlock()
	require.True(t, ok)

	err := client.updateChannelPresence("test", chCtx)
	require.NoError(t, err)
}

func TestClientSubExpired(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.ClientExpiredSubCloseDelay = 0
	node.config.ClientPresenceUpdateInterval = 10 * time.Millisecond

	doneCh := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt: time.Now().Unix() + 1,
					Presence: true,
				},
			}, nil)
		})

		client.OnDisconnect(func(event DisconnectEvent) {
			if event.Disconnect == DisconnectSubExpired {
				close(doneCh)
			}
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)
	subscribeClient(t, client, "test")

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for disconnect due to expired subscription")
	}
}

func TestClientSend(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClient(t, client)

	err := client.Send([]byte(`{}`))
	require.NoError(t, err)

	err = client.close(nil)
	require.NoError(t, err)

	err = client.Send([]byte(`{}`))
	require.Error(t, err)
	require.Equal(t, io.EOF, err)
}

func TestClientClose(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	err := client.close(DisconnectShutdown)
	require.NoError(t, err)
	require.True(t, client.transport.(*testTransport).closed)
	require.Equal(t, DisconnectShutdown, client.transport.(*testTransport).disconnect)
}

func TestClientHandleRPCNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handleRPC(getJSONEncodedParams(t, &protocol.RPCRequest{
		Method: "xxx",
	}), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientHandleRPC(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	var rpcHandlerCalled bool

	node.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, cb RPCCallback) {
			rpcHandlerCalled = true
			expectedData := []byte("{}")
			require.Equal(t, expectedData, event.Data)
			cb(RPCReply{}, nil)
		})
	})

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handleRPC(getJSONEncodedParams(t, &protocol.RPCRequest{
		Data: []byte("{}"),
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, rpcHandlerCalled)
}

func TestClientHandleSendNoHandlerSet(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSend(getJSONEncodedParams(t, &protocol.SendRequest{
		Data: []byte(`{"data":"hello"}`),
	}), rwWrapper.rw)
	require.NoError(t, err)
}

func TestClientHandleSend(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	var messageHandlerCalled bool

	client.OnMessage(func(event MessageEvent) {
		messageHandlerCalled = true
		expectedData := []byte(`{"data":"hello"}`)
		require.Equal(t, expectedData, event.Data)
	})
	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handleSend(getJSONEncodedParams(t, &protocol.SendRequest{
		Data: []byte(`{"data":"hello"}`),
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.True(t, messageHandlerCalled)
}

func TestClientHandlePublishNotAllowed(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	node.OnConnect(func(client *Client) {
		client.OnPublish(func(_ PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, ErrorPermissionDenied)
		})
	})

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Data:    []byte(`{"hello": 1}`),
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorPermissionDenied.toProto(), rwWrapper.replies[0].Error)
}

func TestClientHandlePublish(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	node.OnConnect(func(client *Client) {
		client.OnPublish(func(event PublishEvent, cb PublishCallback) {
			expectedData := []byte(`{"hello":1}`)
			require.Equal(t, expectedData, event.Data)
			require.Equal(t, "test", event.Channel)
			cb(PublishReply{}, nil)
		})
	})

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Data:    []byte(`{"hello":1}`),
		Channel: "",
	}), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(getJSONEncodedParams(t, &protocol.PublishRequest{
		Data:    []byte(`{"hello":1}`),
		Channel: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestClientSideRefresh(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
		}, nil
	})

	expireAt := time.Now().Unix() + 60

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			require.Equal(t, "test", e.Token)
			cb(RefreshReply{
				ExpireAt: expireAt,
			}, nil)
		})
	})

	connectClient(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handleRefresh(getJSONEncodedParams(t, &protocol.RefreshRequest{
		Token: "test",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestServerSideRefresh(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)

	startExpireAt := time.Now().Unix() + 1
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: startExpireAt,
	})
	client, _ := newClient(newCtx, node, transport)

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: false,
		}, nil
	})

	expireAt := time.Now().Unix() + 60

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			require.Equal(t, "", e.Token)
			require.False(t, e.ClientSideRefresh)
			cb(RefreshReply{
				ExpireAt: expireAt,
				Info:     []byte("{}"),
			}, nil)
			close(done)
		})
	})

	connectClient(t, client)

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for work done")
	case <-done:
	}

	require.True(t, client.nextExpire > startExpireAt)
	require.Equal(t, client.info, []byte("{}"))
}

func TestServerSideRefreshDisconnect(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)

	startExpireAt := time.Now().Unix() + 1
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: startExpireAt,
	})
	client, _ := newClient(newCtx, node, transport)

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: false,
		}, nil
	})

	done := make(chan struct{})
	disconnected := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			require.Equal(t, "", e.Token)
			require.False(t, e.ClientSideRefresh)
			cb(RefreshReply{}, DisconnectExpired)
			close(done)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectExpired, event.Disconnect)
			close(disconnected)
		})
	})

	connectClient(t, client)

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for work done")
	case <-done:
	}

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for client close")
	case <-disconnected:
	}
}

func TestServerSideRefreshCustomError(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)

	startExpireAt := time.Now().Unix() + 1
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: startExpireAt,
	})
	client, _ := newClient(newCtx, node, transport)

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: false,
		}, nil
	})

	done := make(chan struct{})
	disconnected := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(e RefreshEvent, cb RefreshCallback) {
			require.Equal(t, "", e.Token)
			require.False(t, e.ClientSideRefresh)
			cb(RefreshReply{}, errors.New("boom"))
			close(done)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError, event.Disconnect)
			close(disconnected)
		})
	})

	connectClient(t, client)

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for work done")
	case <-done:
	}

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for client close")
	case <-disconnected:
	}
}

func TestClientSideSubRefresh(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
		}, nil
	})

	expireAt := time.Now().Unix() + 60

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(_ SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt: time.Now().Unix() + 10,
				},
				ClientSideRefresh: true,
			}, nil)
		})

		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			require.Equal(t, "test_token", e.Token)
			cb(SubRefreshReply{
				ExpireAt: expireAt,
			}, nil)
		})
	})

	connectClient(t, client)
	subscribeClient(t, client, "test")

	rwWrapper := testReplyWriterWrapper()

	err := client.handleSubRefresh(getJSONEncodedParams(t, &protocol.SubRefreshRequest{
		Channel: "test",
		Token:   "test_token",
	}), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestCloseNoRace(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.Disconnect(DisconnectForceNoReconnect)
		time.Sleep(time.Second)
		client.OnDisconnect(func(_ DisconnectEvent) {
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClient(t, client)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for work done")
	case <-done:
	}
}

func TestClientCheckSubscriptionExpiration(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	var nowTime time.Time
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return nowTime
	}
	node.mu.Unlock()

	chanCtx := channelContext{expireAt: 100}

	// not expired.
	nowTime = time.Unix(100, 0)
	client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second, func(b bool) {
		require.True(t, b)
	})

	// simple refresh unavailable.
	nowTime = time.Unix(200, 0)
	client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second, func(b bool) {
		require.False(t, b)
	})

	// refreshed but expired.
	client.eventHub.subRefreshHandler = func(event SubRefreshEvent, cb SubRefreshCallback) {
		require.Equal(t, "channel", event.Channel)
		cb(SubRefreshReply{Expired: true}, nil)
	}
	nowTime = time.Unix(200, 0)
	client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second, func(b bool) {
		require.False(t, b)
	})

	// refreshed but not really.
	client.eventHub.subRefreshHandler = func(event SubRefreshEvent, cb SubRefreshCallback) {
		require.Equal(t, "channel", event.Channel)
		cb(SubRefreshReply{ExpireAt: 150}, nil)
	}
	nowTime = time.Unix(200, 0)
	client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second, func(b bool) {
		require.False(t, b)
	})

	// refreshed but unknown channel.
	client.eventHub.subRefreshHandler = func(event SubRefreshEvent, cb SubRefreshCallback) {
		require.Equal(t, "channel", event.Channel)
		cb(SubRefreshReply{
			ExpireAt: 250,
			Info:     []byte("info"),
		}, nil)
	}
	nowTime = time.Unix(200, 0)
	client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second, func(b bool) {
		require.True(t, b)
	})
	require.NotContains(t, client.channels, "channel")

	// refreshed.
	client.channels["channel"] = channelContext{}
	client.eventHub.subRefreshHandler = func(event SubRefreshEvent, cb SubRefreshCallback) {
		require.Equal(t, "channel", event.Channel)
		cb(SubRefreshReply{
			ExpireAt: 250,
			Info:     []byte("info"),
		}, nil)
	}
	nowTime = time.Unix(200, 0)
	client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second, func(b bool) {
		require.True(t, b)
	})
	require.Contains(t, client.channels, "channel")
	require.EqualValues(t, 250, client.channels["channel"].expireAt)
	require.Equal(t, []byte("info"), client.channels["channel"].Info)

	// Error from handler.
	client.eventHub.subRefreshHandler = func(event SubRefreshEvent, cb SubRefreshCallback) {
		cb(SubRefreshReply{}, DisconnectExpired)
	}
	nowTime = time.Unix(200, 0)
	client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second, func(b bool) {
		require.False(t, b)
	})
}

func TestClientCheckPosition(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return time.Unix(200, 0)
	}
	node.mu.Unlock()

	// no recover.
	got := client.checkPosition(300*time.Second, "channel", channelContext{})
	require.True(t, got)

	// not initial, not time to check.
	got = client.checkPosition(300*time.Second, "channel", channelContext{positionCheckTime: 50, flags: flagRecover})
	require.True(t, got)

	// invalid position.
	client.channels["channel"] = channelContext{positionCheckFailures: 2, flags: flagRecover}
	got = client.checkPosition(50*time.Second, "channel", channelContext{
		positionCheckTime: 50, flags: flagRecover,
	})
	require.False(t, got)
	require.Contains(t, client.channels, "channel")
	require.EqualValues(t, 3, client.channels["channel"].positionCheckFailures)
	require.EqualValues(t, 200, client.channels["channel"].positionCheckTime)
}

func TestErrLogLevel(t *testing.T) {
	require.Equal(t, LogLevelInfo, errLogLevel(ErrorNotAvailable))
	require.Equal(t, LogLevelError, errLogLevel(errors.New("boom")))
}
