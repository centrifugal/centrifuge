package centrifuge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

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

func newClient(ctx context.Context, n *Node, t Transport) (*Client, error) {
	c, _, err := NewClient(ctx, n, t)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newTestConnectedClient(t *testing.T, n *Node, userID string) *Client {
	transport := newTestTransport()
	ctx := SetCredentials(context.Background(), &Credentials{
		UserID: userID,
	})

	client, err := newClient(ctx, n, transport)
	require.NoError(t, err)

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
	transport := newTestTransport()
	client, err := newClient(context.Background(), node, transport)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClientInitialState(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
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
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	err := client.close(nil)
	require.NoError(t, err)
	require.True(t, client.status == statusClosed)
}

func TestClientTimer(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.ClientStaleCloseDelay = 25 * time.Second
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	require.NotNil(t, client.timer)
	node.config.ClientStaleCloseDelay = 0
	client, _ = newClient(context.Background(), node, transport)
	require.Nil(t, client.timer)
}

func TestClientTimerSchedule(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
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
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.NotNil(t, disconnect)
	require.Equal(t, disconnect, DisconnectBadRequest)
}

func TestClientConnectContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	result := extractConnectResult(replies, client.Transport().Protocol())
	require.Equal(t, false, result.Expires)
	require.Equal(t, uint32(0), result.TTL)
	require.True(t, client.authenticated)
	require.Equal(t, "42", client.UserID())
}

func TestClientRefreshHandlerClosingExpiredClient(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnRefresh(func(_ *Client, _ RefreshEvent) (RefreshReply, error) {
		return RefreshReply{
			Expired: true,
		}, nil
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	client.triggerConnect()
	client.expire()
	require.True(t, client.status == statusClosed)
}

func TestClientRefreshHandlerProlongsClientSession(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	expireAt := time.Now().Unix() + 60
	node.OnRefresh(func(_ *Client, _ RefreshEvent) (RefreshReply, error) {
		return RefreshReply{
			ExpireAt: expireAt,
		}, nil
	})

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	client.expire()
	require.False(t, client.status == statusClosed)
	require.Equal(t, expireAt, client.exp)
}

func TestClientConnectWithExpiredContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() - 60,
	})
	client, _ := newClient(newCtx, node, transport)

	node.OnRefresh(func(_ *Client, _ RefreshEvent) (RefreshReply, error) {
		return RefreshReply{}, nil
	})

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorExpired.toProto(), replies[0].Error)
}

func connectClient(t testing.TB, client *Client) *protocol.ConnectResult {
	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
	require.True(t, client.authenticated)
	result := extractConnectResult(replies, client.Transport().Protocol())
	require.Equal(t, client.uid, result.Client)
	client.triggerConnect()
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

func extractConnectResult(replies []*protocol.Reply, protoType ProtocolType) *protocol.ConnectResult {
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
	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	ctx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: ch,
	}, rw, false)
	require.Nil(t, ctx.disconnect)
	require.Nil(t, replies[0].Error)
	return extractSubscribeResult(replies, client.Transport().Protocol())
}

func TestClientSubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	require.Equal(t, 0, len(client.Channels()))

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test1",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Equal(t, 1, len(replies))
	require.Nil(t, replies[0].Error)
	res := extractSubscribeResult(replies, client.Transport().Protocol())
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
	require.Equal(t, ErrorAlreadySubscribed.toProto(), replies[0].Error)
}

func TestClientSubscribeReceivePublication(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	var replies []*protocol.Reply
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
	setTestChannelOptions(&node.config, ChannelOptions{
		HistoryLifetime: 100,
		HistorySize:     10,
	})
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, rw, false)
	require.Nil(t, subCtx.disconnect)
	require.Nil(t, replies[0].Error)

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
	_, err := node.Publish("test", []byte(`{"text": "test message 1"}`))
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text": "test message 2"}`))
	require.NoError(t, err)
	_, err = node.Publish("test", []byte(`{"text": "test message 3"}`))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout receiving publications")
	case <-done:
	}
}

func TestUserConnectionLimit(t *testing.T) {
	node := nodeWithMemoryEngine()
	node.config.ClientUserConnectionLimit = 1
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	anotherClient, _ := newClient(newCtx, node, transport)
	disconnect := anotherClient.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorLimitExceeded.toProto(), replies[0].Error)
}

func TestConnectingReply(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		newCtx := context.WithValue(ctx, "key", "val")
		return ConnectReply{
			Context: newCtx,
			Data:    []byte("{}"),
			Credentials: &Credentials{
				UserID: "12",
			},
			Events: EventAll,
		}, nil
	})

	done := make(chan struct{})

	node.OnConnect(func(c *Client) {
		v, ok := c.Context().Value("key").(string)
		require.True(t, ok)
		require.Equal(t, "val", v)
		require.Equal(t, "12", c.UserID())
		close(done)
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
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
			Channels: []string{
				"server-side-1",
				"$server-side-2",
			},
		}, nil
	})
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	_ = client.handleCommand(&protocol.Command{
		ID: 1,
	}, rw.write, rw.flush)

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

func TestClientSubscribeLast(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	setTestChannelOptions(&node.config, ChannelOptions{
		HistorySize:     10,
		HistoryLifetime: 60,
		HistoryRecover:  true,
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	result := subscribeClient(t, client, "test")
	require.Equal(t, uint32(0), result.Seq)

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte("{}"))
	}

	client, _ = newClient(newCtx, node, transport)
	connectClient(t, client)
	result = subscribeClient(t, client, "test")
	require.Equal(t, uint64(10), result.Offset)
}

func TestClientUnsubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

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

	unsubscribed := make(chan struct{})

	node.OnUnsubscribe(func(_ *Client, _ UnsubscribeEvent) {
		close(unsubscribed)
	})

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

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	done := make(chan struct{})
	closed := false
	disconnected := make(chan struct{})
	numCalls := 0

	node.OnAlive(func(_ *Client) {
		numCalls++
		if numCalls >= 50 && !closed {
			close(done)
			closed = true
			require.NoError(t, client.Disconnect(DisconnectForceNoReconnect))
		}
	})

	node.OnDisconnect(func(_ *Client, _ DisconnectEvent) {
		close(disconnected)
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

func TestClientPublishNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	publishResp, disconnect := client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), publishResp.Error)
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
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

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

	node.clientEvents.publishHandler = func(_ *Client, e PublishEvent) (PublishReply, error) {
		var msg testClientMessage
		err := json.Unmarshal(e.Data, &msg)
		require.NoError(t, err)
		if msg.Input == "with disconnect" {
			return PublishReply{}, DisconnectBadRequest
		}
		if msg.Input == "with error" {
			return PublishReply{}, ErrorBadRequest
		}
		if msg.Input == "with timestamp" {
			msg.Timestamp = time.Now().Unix()
			data, _ := json.Marshal(msg)
			res, err := node.Publish(e.Channel, data)
			return PublishReply{Result: &res}, err
		}
		return PublishReply{}, nil
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
	require.Equal(t, ErrorBadRequest.toProto(), publishResp.Error)

	_, disconnect = client.publishCmd(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with disconnect"}`),
	})
	require.Equal(t, DisconnectBadRequest, disconnect)
}

func TestClientPing(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	pingResp, disconnect := client.pingCmd(&protocol.PingRequest{})
	require.Nil(t, disconnect)
	require.Nil(t, pingResp.Error)
	require.Empty(t, pingResp.Result)
}

func TestClientPresence(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	setTestChannelOptions(&node.config, ChannelOptions{
		Presence: true,
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnPresence(func(_ *Client, _ PresenceEvent) (PresenceReply, error) {
		return PresenceReply{}, nil
	})
	node.OnPresenceStats(func(_ *Client, _ PresenceStatsEvent) (PresenceStatsReply, error) {
		return PresenceStatsReply{}, nil
	})

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
	require.Nil(t, presenceStatsResp.Error)
	require.Equal(t, uint32(1), presenceStatsResp.Result.NumUsers)
	require.Equal(t, uint32(1), presenceStatsResp.Result.NumClients)

	setTestChannelOptions(&node.config, ChannelOptions{
		Presence: false,
	})

	presenceResp, disconnect = client.presenceCmd(&protocol.PresenceRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), presenceResp.Error)
	require.Nil(t, presenceResp.Result)

	presenceStatsResp, disconnect = client.presenceStatsCmd(&protocol.PresenceStatsRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), presenceStatsResp.Error)
	require.Nil(t, presenceStatsResp.Result)
}

func TestClientHistory(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	setTestChannelOptions(&node.config, ChannelOptions{
		HistorySize:     10,
		HistoryLifetime: 60,
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	node.OnHistory(func(_ *Client, _ HistoryEvent) (HistoryReply, error) {
		return HistoryReply{}, nil
	})

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte(`{}`))
	}

	connectClient(t, client)
	subscribeClient(t, client, "test")

	historyResp, disconnect := client.historyCmd(&protocol.HistoryRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Nil(t, historyResp.Error)
	require.Equal(t, 10, len(historyResp.Result.Publications))

	setTestChannelOptions(&node.config, ChannelOptions{
		HistorySize:     0,
		HistoryLifetime: 0,
	})

	historyResp, disconnect = client.historyCmd(&protocol.HistoryRequest{
		Channel: "test",
	})
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), historyResp.Error)
	require.Nil(t, historyResp.Result)
}

func TestClientCloseUnauthenticated(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.ClientStaleCloseDelay = time.Millisecond

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	time.Sleep(100 * time.Millisecond)
	client.mu.Lock()
	require.True(t, client.status == statusClosed)
	client.mu.Unlock()
}

func TestClientCloseExpired(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: time.Now().Unix() + 2})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)
	client.scheduleOnConnectTimers()
	client.mu.RLock()
	require.False(t, client.status == statusClosed)
	client.mu.RUnlock()
	time.Sleep(4 * time.Second)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.True(t, client.status == statusClosed)
}

func TestClientConnectExpiredError(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: time.Now().Unix() - 2})
	client, _ := newClient(newCtx, node, transport)
	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorExpired.toProto(), replies[0].Error)
	require.False(t, client.authenticated)
}

func TestClientPresenceUpdate(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	setTestChannelOptions(&node.config, ChannelOptions{
		Presence: true,
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)
	subscribeClient(t, client, "test")

	err := client.updateChannelPresence("test")
	require.NoError(t, err)
}

func TestClientSend(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

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

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	err := client.close(DisconnectShutdown)
	require.NoError(t, err)
	require.True(t, transport.closed)
	require.Equal(t, DisconnectShutdown, transport.disconnect)
}

func TestClientHandlePing(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePing,
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
}

func TestClientHandleRPCNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeRPC,
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), replies[0].Error)
}

func TestClientHandleRPC(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnRPC(func(_ *Client, event RPCEvent) (RPCReply, error) {
		expectedData, _ := json.Marshal("hello")
		require.Equal(t, expectedData, event.Data)
		return RPCReply{}, nil
	})
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeRPC,
		Params: []byte(`{"data": "hello"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
}

func TestClientHandleSend(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnMessage(func(_ *Client, event MessageEvent) {
		expectedData, _ := json.Marshal("hello")
		require.Equal(t, expectedData, event.Data)
	})
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeSend,
		Params: []byte(`{"data": "hello"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
}

func TestClientHandlePublishNotAllowed(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnPublish(func(_ *Client, _ PublishEvent) (PublishReply, error) {
		return PublishReply{}, ErrorPermissionDenied
	})
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePublish,
		Params: []byte(`{"data": "hello", "channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorPermissionDenied.toProto(), replies[0].Error)
}

func TestClientHandlePublish(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnPublish(func(_ *Client, event PublishEvent) (PublishReply, error) {
		expectedData, _ := json.Marshal("hello")
		require.Equal(t, expectedData, event.Data)
		require.Equal(t, "test", event.Channel)
		return PublishReply{}, nil
	})
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePublish,
		Params: []byte(`{"data": "hello", "channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)

	node.OnPublish(func(_ *Client, event PublishEvent) (PublishReply, error) {
		return PublishReply{}, ErrorLimitExceeded
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePublish,
		Params: []byte(`{"channel": "test", "data": {}}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorLimitExceeded.toProto(), replies[0].Error)

	node.OnPublish(func(_ *Client, event PublishEvent) (PublishReply, error) {
		return PublishReply{}, DisconnectBadRequest
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePublish,
		Params: []byte(`{"channel": "test", "data": {}}`),
	}, rw.write, rw.flush)
	require.Equal(t, DisconnectBadRequest, disconnect)

	node.OnPublish(func(_ *Client, event PublishEvent) (PublishReply, error) {
		return PublishReply{}, errors.New("boom")
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePublish,
		Params: []byte(`{"channel": "test", "data": {}}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorInternal.toProto(), replies[0].Error)
}

func TestClientHandleHistoryNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeHistory,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), replies[0].Error)
}

func TestClientHandleHistoryNotAvailableDueToOption(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnHistory(func(_ *Client, _ HistoryEvent) (HistoryReply, error) {
		return HistoryReply{}, nil
	})
	connectClient(t, client)
	subscribeClient(t, client, "test")

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeHistory,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), replies[0].Error)
}

func TestClientHandleHistory(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	setTestChannelOptions(&node.config, ChannelOptions{
		HistorySize:     1,
		HistoryLifetime: 30,
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnHistory(func(_ *Client, _ HistoryEvent) (HistoryReply, error) {
		return HistoryReply{}, nil
	})
	connectClient(t, client)
	subscribeClient(t, client, "test")

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeHistory,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)

	node.OnHistory(func(_ *Client, _ HistoryEvent) (HistoryReply, error) {
		return HistoryReply{}, ErrorPermissionDenied
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeHistory,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorPermissionDenied.toProto(), replies[0].Error)

	node.OnHistory(func(_ *Client, _ HistoryEvent) (HistoryReply, error) {
		return HistoryReply{}, DisconnectBadRequest
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeHistory,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Equal(t, DisconnectBadRequest, disconnect)

	node.OnHistory(func(_ *Client, _ HistoryEvent) (HistoryReply, error) {
		return HistoryReply{}, errors.New("boom")
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeHistory,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorInternal.toProto(), replies[0].Error)
}

func TestClientHandlePresenceNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresence,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), replies[0].Error)
}

func TestClientHandlePresenceNotAvailableDueToOption(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnPresence(func(_ *Client, _ PresenceEvent) (PresenceReply, error) {
		return PresenceReply{}, nil
	})
	connectClient(t, client)
	subscribeClient(t, client, "test")

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresence,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), replies[0].Error)
}

func TestClientHandlePresence(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	setTestChannelOptions(&node.config, ChannelOptions{
		Presence: true,
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnPresence(func(_ *Client, _ PresenceEvent) (PresenceReply, error) {
		return PresenceReply{}, nil
	})
	connectClient(t, client)
	subscribeClient(t, client, "test")

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresence,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)

	node.OnPresence(func(_ *Client, _ PresenceEvent) (PresenceReply, error) {
		return PresenceReply{}, ErrorPermissionDenied
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresence,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorPermissionDenied.toProto(), replies[0].Error)

	node.OnPresence(func(_ *Client, _ PresenceEvent) (PresenceReply, error) {
		return PresenceReply{}, DisconnectBadRequest
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresence,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Equal(t, DisconnectBadRequest, disconnect)

	node.OnPresence(func(_ *Client, _ PresenceEvent) (PresenceReply, error) {
		return PresenceReply{}, errors.New("boom")
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresence,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorInternal.toProto(), replies[0].Error)
}

func TestClientHandlePresenceStatsNotAvailable(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresenceStats,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), replies[0].Error)
}

func TestClientHandlePresenceStatsNotAvailableDueToOption(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnPresenceStats(func(_ *Client, _ PresenceStatsEvent) (PresenceStatsReply, error) {
		return PresenceStatsReply{}, nil
	})
	connectClient(t, client)
	subscribeClient(t, client, "test")

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresenceStats,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorNotAvailable.toProto(), replies[0].Error)
}

func TestClientHandlePresenceStats(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	setTestChannelOptions(&node.config, ChannelOptions{
		Presence: true,
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	node.OnPresenceStats(func(_ *Client, _ PresenceStatsEvent) (PresenceStatsReply, error) {
		return PresenceStatsReply{}, nil
	})
	connectClient(t, client)
	subscribeClient(t, client, "test")

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresenceStats,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)

	node.OnPresenceStats(func(_ *Client, _ PresenceStatsEvent) (PresenceStatsReply, error) {
		return PresenceStatsReply{}, ErrorPermissionDenied
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresenceStats,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorPermissionDenied.toProto(), replies[0].Error)

	node.OnPresenceStats(func(_ *Client, _ PresenceStatsEvent) (PresenceStatsReply, error) {
		return PresenceStatsReply{}, DisconnectBadRequest
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresenceStats,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Equal(t, DisconnectBadRequest, disconnect)

	node.OnPresenceStats(func(_ *Client, _ PresenceStatsEvent) (PresenceStatsReply, error) {
		return PresenceStatsReply{}, errors.New("boom")
	})
	replies = []*protocol.Reply{}
	rw = testReplyWriter(&replies)
	disconnect = client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypePresenceStats,
		Params: []byte(`{"channel": "test"}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorInternal.toProto(), replies[0].Error)
}

func TestClientHandleMalformedCommand(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     1,
		Method: 1000,
		Params: []byte(`{}`),
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Equal(t, ErrorMethodNotFound.toProto(), replies[0].Error)

	replies = nil
	disconnect = client.handleCommand(&protocol.Command{
		ID:     1,
		Method: 2,
		Params: []byte(`{}`),
	}, rw.write, rw.flush)
	require.Equal(t, DisconnectBadRequest, disconnect)
}

func TestClientSideRefresh(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
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
	node.OnRefresh(func(_ *Client, e RefreshEvent) (RefreshReply, error) {
		require.Equal(t, "test", e.Token)
		return RefreshReply{
			ExpireAt: expireAt,
		}, nil
	})

	connectClient(t, client)

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	req := protocol.RefreshRequest{
		Token: "test",
	}
	params, _ := json.Marshal(req)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeRefresh,
		Params: params,
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
}

func TestClientSideSubRefresh(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
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

	node.OnSubscribe(func(_ *Client, _ SubscribeEvent) (SubscribeReply, error) {
		return SubscribeReply{
			ExpireAt:          time.Now().Unix() + 10,
			ClientSideRefresh: true,
		}, nil
	})

	expireAt := time.Now().Unix() + 60
	node.OnSubRefresh(func(_ *Client, e SubRefreshEvent) (SubRefreshReply, error) {
		require.Equal(t, "test_token", e.Token)
		return SubRefreshReply{
			ExpireAt: expireAt,
		}, nil
	})

	connectClient(t, client)
	subscribeClient(t, client, "test")

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)

	req := protocol.SubRefreshRequest{
		Channel: "test",
		Token:   "test_token",
	}
	params, _ := json.Marshal(req)

	disconnect := client.handleCommand(&protocol.Command{
		ID:     2,
		Method: protocol.MethodTypeSubRefresh,
		Params: params,
	}, rw.write, rw.flush)
	require.Nil(t, disconnect)
	require.Nil(t, replies[0].Error)
}

func TestCloseNoRace(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		require.NoError(t, client.Disconnect(DisconnectForceNoReconnect))
		time.Sleep(time.Second)
	})

	node.OnDisconnect(func(_ *Client, _ DisconnectEvent) {
		close(done)
	})

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := newClient(newCtx, node, transport)
	connectClient(t, client)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for work done")
	case <-done:
	}
}

func BenchmarkUUID(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		uuidObject, err := uuid.NewRandom()
		if err != nil {
			b.Fatal(err)
		}
		s := uuidObject.String()
		if s == "" {
			b.Fail()
		}
	}
	b.ReportAllocs()
}

func TestClientCheckSubscriptionExpiration(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	client.events = uint64(EventAll)

	var nowTime time.Time
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return nowTime
	}
	node.mu.Unlock()

	chanCtx := ChannelContext{expireAt: 100}

	// not expired.
	nowTime = time.Unix(100, 0)
	got := client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second)
	require.True(t, got)

	// simple refresh unavailable.
	nowTime = time.Unix(200, 0)
	got = client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second)
	require.False(t, got)

	// refreshed but expired.
	node.clientEvents.subRefreshHandler = func(client *Client, event SubRefreshEvent) (SubRefreshReply, error) {
		require.Equal(t, "channel", event.Channel)
		return SubRefreshReply{Expired: true}, nil
	}
	nowTime = time.Unix(200, 0)
	got = client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second)
	require.False(t, got)

	// refreshed but not really.
	node.clientEvents.subRefreshHandler = func(client *Client, event SubRefreshEvent) (SubRefreshReply, error) {
		require.Equal(t, "channel", event.Channel)
		return SubRefreshReply{ExpireAt: 150}, nil
	}
	nowTime = time.Unix(200, 0)
	got = client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second)
	require.False(t, got)

	// refreshed but unknown channel.
	node.clientEvents.subRefreshHandler = func(client *Client, event SubRefreshEvent) (SubRefreshReply, error) {
		require.Equal(t, "channel", event.Channel)
		return SubRefreshReply{
			ExpireAt: 250,
			Info:     []byte("info"),
		}, nil
	}
	nowTime = time.Unix(200, 0)
	got = client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second)
	require.True(t, got)
	require.NotContains(t, client.channels, "channel")

	// refreshed.
	client.channels["channel"] = ChannelContext{}
	node.clientEvents.subRefreshHandler = func(client *Client, event SubRefreshEvent) (SubRefreshReply, error) {
		require.Equal(t, "channel", event.Channel)
		return SubRefreshReply{
			ExpireAt: 250,
			Info:     []byte("info"),
		}, nil
	}
	nowTime = time.Unix(200, 0)
	got = client.checkSubscriptionExpiration("channel", chanCtx, 50*time.Second)
	require.True(t, got)
	require.Contains(t, client.channels, "channel")
	require.EqualValues(t, 250, client.channels["channel"].expireAt)
	require.Equal(t, []byte("info"), client.channels["channel"].Info)
}

func TestClientCheckPosition(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return time.Unix(200, 0)
	}

	var (
		chanOpts    ChannelOptions
		chanFound   bool
		chanOptsErr error
	)
	node.config.ChannelOptionsFunc = func(channel string) (ChannelOptions, bool, error) {
		require.Equal(t, "channel", channel)
		return chanOpts, chanFound, chanOptsErr
	}
	node.mu.Unlock()

	// channel option error.
	chanOptsErr = errors.New("oops")
	got := client.checkPosition(300*time.Second, "channel", ChannelContext{})
	require.True(t, got)

	// channel option not found.
	chanOptsErr = nil
	chanFound = false
	got = client.checkPosition(300*time.Second, "channel", ChannelContext{})
	require.True(t, got)

	// not history recover.
	chanOptsErr = nil
	chanFound = true
	got = client.checkPosition(300*time.Second, "channel", ChannelContext{})
	require.True(t, got)

	// not initial, not time to check.
	chanOptsErr = nil
	chanFound = true
	chanOpts = ChannelOptions{HistoryRecover: true}
	got = client.checkPosition(300*time.Second, "channel", ChannelContext{positionCheckTime: 50})
	require.True(t, got)

	// channel not found.
	chanOptsErr = nil
	chanFound = true
	chanOpts = ChannelOptions{HistoryRecover: true}
	got = client.checkPosition(50*time.Second, "channel", ChannelContext{
		positionCheckTime: 50,
	})
	require.True(t, got)

	// invalid position.
	chanOptsErr = nil
	chanFound = true
	chanOpts = ChannelOptions{HistoryRecover: true}
	client.channels["channel"] = ChannelContext{positionCheckFailures: 2}
	got = client.checkPosition(50*time.Second, "channel", ChannelContext{
		positionCheckTime: 50,
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
