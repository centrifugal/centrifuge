package centrifuge

import (
	"bytes"
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

func newTestConnectedClientV2(t *testing.T, n *Node, userID string) *Client {
	client := newTestClientV2(t, n, userID)
	connectClientV2(t, client)
	require.True(t, len(n.hub.UserConnections(userID)) > 0)
	return client
}

func newTestConnectedClientWithTransport(t testing.TB, ctx context.Context, n *Node, transport Transport, userID string) *Client {
	client := newTestClientCustomTransport(t, ctx, n, transport, userID)
	connectClientV2(t, client)
	require.True(t, len(n.hub.UserConnections(userID)) > 0)
	return client
}

func newTestSubscribedClientV2(t *testing.T, n *Node, userID, chanID string) *Client {
	client := newTestConnectedClientV2(t, n, userID)
	subscribeClientV2(t, client, chanID)
	require.True(t, n.hub.NumSubscribers(chanID) > 0)
	require.Contains(t, client.channels, chanID)
	return client
}

func newTestSubscribedClientWithTransport(t *testing.T, ctx context.Context, n *Node, transport Transport, userID, chanID string) *Client {
	client := newTestConnectedClientWithTransport(t, ctx, n, transport, userID)
	subscribeClientV2(t, client, chanID)
	require.True(t, n.hub.NumSubscribers(chanID) > 0)
	require.Contains(t, client.channels, chanID)
	return client
}

func TestConnectRequestToProto(t *testing.T) {
	r := ConnectRequest{
		Token: "token",
		Subs: map[string]SubscribeRequest{
			"test": {
				Recover: true,
				Offset:  1,
				Epoch:   "epoch",
			},
		}}
	protoReq := r.toProto()
	require.Equal(t, "token", protoReq.GetToken())
	require.Equal(t, uint64(1), protoReq.Subs["test"].Offset)
	require.Equal(t, "epoch", protoReq.Subs["test"].Epoch)
	require.True(t, protoReq.Subs["test"].Recover)
}

func TestSetCredentials(t *testing.T) {
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{})
	val := newCtx.Value(credentialsContextKey).(*Credentials)
	require.NotNil(t, val)
}

func TestNewClient(t *testing.T) {
	node := defaultTestNode()
	transport := newTestTransport(func() {})
	client, err := newClient(context.Background(), node, transport)
	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestClientInitialState(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	require.Equal(t, client.uid, client.ID())
	require.NotNil(t, "", client.user)
	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, ProtocolTypeJSON, client.Transport().Protocol())
	require.Equal(t, "websocket", client.Transport().Name())
	require.True(t, client.status == statusConnecting)
	require.False(t, client.authenticated)
}

func TestClientClosedState(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	err := client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)
	require.True(t, client.status == statusClosed)
}

func TestClientTimer(t *testing.T) {
	node := defaultTestNode()
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
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	err := client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)
	client.onTimerOp()
	require.False(t, client.timer.Stop())
}

func TestClientUnsubscribeClosedClient(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")
	err := client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)
	client.Unsubscribe("test")
}

func TestClientTimerSchedule(t *testing.T) {
	node := defaultTestNode()
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

func TestClientGetPingData(t *testing.T) {
	data := getPingData(true, ProtocolTypeJSON)
	require.Equal(t, jsonPingPush, data)
	data = getPingData(false, ProtocolTypeJSON)
	require.Equal(t, jsonPingReply, data)
	data = getPingData(true, ProtocolTypeProtobuf)
	require.Equal(t, protobufPingPush, data)
	data = getPingData(false, ProtocolTypeProtobuf)
	require.Equal(t, protobufPingReply, data)
}

func TestClientV2TimerSchedule(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClientV2(t, node, "42")

	client.mu.Lock()
	require.NotNil(t, client.timer)
	require.Equal(t, timerOpStale, client.timerOp)
	client.mu.Unlock()

	connectClientV2(t, client)

	client.mu.Lock()
	require.NotNil(t, client.timer)
	require.Equal(t, timerOpPing, client.timerOp)
	client.mu.Unlock()

	client.sendPing()

	client.mu.Lock()
	require.NotZero(t, client.lastPing)
	require.NotZero(t, client.nextPong)
	require.NotNil(t, client.timer)
	require.Equal(t, timerOpPong, client.timerOp)
	client.mu.Unlock()
}

func TestClientV2DisconnectNoPong(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	done := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectNoPong.Code, event.Disconnect.Code)
			close(done)
		})
	})
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setPing(2*time.Second, time.Second)
	client := newTestClientCustomTransport(t, ctx, node, transport, "42")
	connectClientV2(t, client)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("no disconnect in timeout")
	}
}

func TestClientV2PingPong(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	done := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			close(done)
		})
	})
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setPing(2*time.Second, time.Second)
	messages := make(chan []byte)
	transport.setSink(messages)
	client := newTestClientCustomTransport(t, ctx, node, transport, "42")
	go func() {
		for msg := range messages {
			if string(msg) == "{}" {
				// PING
				HandleReadFrame(client, bytes.NewReader([]byte("{}")))
			}
		}
	}()
	connectClientV2(t, client)
	select {
	case <-done:
		t.Fatal("unexpected disconnect, client must receive pong")
	case <-time.After(4 * time.Second):
	}
}

func TestClientConnectNoCredentialsNoToken(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	client, _ := newClient(context.Background(), node, transport)
	rwWrapper := testReplyWriterWrapper()
	_, err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientConnectContextCredentials(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	_, err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	result := extractConnectReply(rwWrapper.replies)
	require.Equal(t, false, result.Expires)
	require.Equal(t, uint32(0), result.Ttl)
	require.True(t, client.authenticated)
	require.Equal(t, "42", client.UserID())
}

func TestClientRefreshHandlerClosingExpiredClient(t *testing.T) {
	node := defaultTestNode()
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
	_, err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	client.triggerConnect()
	client.expire()
	require.True(t, client.status == statusClosed)
}

func TestClientRefreshHandlerProlongsClientSession(t *testing.T) {
	node := defaultTestNode()
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
	_, err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	client.expire()
	require.False(t, client.status == statusClosed)
	require.Equal(t, expireAt, client.exp)
}

func TestClientConnectWithExpiredContextCredentials(t *testing.T) {
	node := defaultTestNode()
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
	_, err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorExpired, err)
}

func extractSubscribeResult(replies []*protocol.Reply) *protocol.SubscribeResult {
	return replies[0].Subscribe
}

func extractConnectReply(replies []*protocol.Reply) *protocol.ConnectResult {
	return replies[0].Connect
}

func subscribeClientV2(t testing.TB, client *Client, ch string) *protocol.SubscribeResult {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: ch,
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].Subscribe
}

func TestClientSubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EmitJoinLeave:     true,
					PushJoinLeave:     true,
					EmitPresence:      true,
					EnablePositioning: true,
					EnableRecovery:    true,
					ChannelInfo:       []byte("{}"),
					ExpireAt:          time.Now().Unix() + 3600,
					Data:              []byte("{}"),
				},
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	require.Equal(t, 0, len(client.Channels()))

	rwWrapper := testReplyWriterWrapper()

	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, 1, len(rwWrapper.replies))
	require.Nil(t, rwWrapper.replies[0].Error)
	res := extractSubscribeResult(rwWrapper.replies)
	require.Empty(t, res.Offset)
	require.False(t, res.Recovered)
	require.Empty(t, res.Publications)
	require.Equal(t, 1, len(client.Channels()))
	require.Equal(t, 1, len(client.ChannelsWithContext()))

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test2",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, 2, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 2, node.Hub().NumChannels())

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test2",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorAlreadySubscribed, err)
}

func TestClientSubscribeBrokerErrorOnSubscribe(t *testing.T) {
	t.Parallel()
	broker := NewTestBroker()
	broker.errorOnSubscribe = true
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError.Code, event.Code)
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestClientSubscribeBrokerErrorOnStreamTop(t *testing.T) {
	t.Parallel()
	broker := NewTestBroker()
	broker.errorOnHistory = true
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{EnableRecovery: true},
			}, nil)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError.Code, event.Code)
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestClientSubscribeUnrecoverablePosition(t *testing.T) {
	broker := NewTestBroker()
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{EnableRecovery: true},
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
		Recover: true,
		Epoch:   "xxx",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, 1, len(rwWrapper.replies))
	require.Nil(t, rwWrapper.replies[0].Error)
	res := extractSubscribeResult(rwWrapper.replies)
	require.Empty(t, res.Offset)
	require.Empty(t, res.Epoch)
	require.False(t, res.Recovered)
	require.Empty(t, res.Publications)
}

func TestClientSubscribePositionedError(t *testing.T) {
	t.Parallel()
	broker := NewTestBroker()
	broker.errorOnHistory = true
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{EnablePositioning: true},
			}, nil)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError.Code, event.Code)
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestClientSubscribePositioned(t *testing.T) {
	node := nodeWithTestBroker()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{EnablePositioning: true},
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	require.Nil(t, rwWrapper.replies[0].Error)
	result := rwWrapper.replies[0].Subscribe
	require.NoError(t, err)
	require.True(t, result.Positioned)
}

func TestClientSubscribeBrokerErrorOnRecoverHistory(t *testing.T) {
	t.Parallel()
	broker := NewTestBroker()
	broker.errorOnHistory = true
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true}}, nil)
		})
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectServerError.Code, event.Code)
			close(done)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
		Recover: true,
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func testUnexpectedOffsetEpochProtocolV2(t *testing.T, offset uint64, epoch string) {
	t.Parallel()
	broker := NewTestBroker()
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true}}, nil)
		})
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			require.Equal(t, UnsubscribeCodeInsufficient, event.Code)
			close(done)
		})
	})

	client := newTestClientV2(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test",
		Recover: true,
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	err = node.handlePublication("test", &Publication{
		Offset: offset,
	}, StreamPosition{offset, epoch})
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestClientUnexpectedOffsetEpochClientV2(t *testing.T) {
	tests := []struct {
		Name   string
		Offset uint64
		Epoch  string
	}{
		{"wrong_offset", 2, ""},
		{"wrong_epoch", 1, "xyz"},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			testUnexpectedOffsetEpochProtocolV2(t, tt.Offset, tt.Epoch)
		})
	}
}

func TestClientSubscribeValidateErrors(t *testing.T) {
	node := defaultTestNode()
	node.config.ClientChannelLimit = 1
	node.config.ChannelMaxLength = 10
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test2_very_long_channel_name",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorBadRequest, err)

	subscribeClientV2(t, client, "test1")

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test2",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorLimitExceeded, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientSubscribeReceivePublication(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, SubscribeReply{}, &protocol.Command{}, false, time.Now(), rwWrapper.rw)
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
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: "test",
	}, SubscribeReply{}, &protocol.Command{}, false, time.Now(), rwWrapper.rw)
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
						Push struct {
							Channel string
							Pub     struct {
								Data   struct{}
								Offset uint64
							}
						}
					}
					err := dec.Decode(&push)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if push.Push.Pub.Offset != offset {
						require.Fail(t, fmt.Sprintf("wrong offset: %d != %d", push.Push.Pub.Offset, offset))
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
	node := defaultTestNode()
	node.config.UserConnectionLimit = 1
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	anotherClient, _ := newClient(newCtx, node, transport)
	_, err := anotherClient.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectConnectionLimit, err)
}

type testContextKey int

var keyTest testContextKey = 1

func TestConnectingReply(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
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
	connectClientV2(t, client)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	case <-done:
	}
}

func TestServerSideSubscriptions(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		Name           string
		Unidirectional bool
		ProtoVersion   ProtocolVersion
	}{
		{"bidi-v2", false, ProtocolVersion2},
		{"uni-v2", true, ProtocolVersion2},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			node := defaultTestNode()
			defer func() { _ = node.Shutdown(context.Background()) }()

			node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
				return ConnectReply{
					WriteDelay:        50 * time.Millisecond,
					ReplyWithoutQueue: true,
					Subscriptions: map[string]SubscribeOptions{
						"server-side-1":  {},
						"$server-side-2": {},
					},
				}, nil
			})
			transport := newTestTransport(func() {})
			transport.setUnidirectional(tt.Unidirectional)
			transport.setProtocolVersion(tt.ProtoVersion)
			transport.sink = make(chan []byte, 100)
			ctx := context.Background()
			newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
			client, _ := newClient(newCtx, node, transport)
			if !tt.Unidirectional {
				connectClientV2(t, client)
			} else {
				client.Connect(ConnectRequest{
					Subs: map[string]SubscribeRequest{
						"server-side-1": {
							Recover: true,
							Epoch:   "test",
							Offset:  0,
						},
					},
				})
			}

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
						return
					}
				}
			}()

			select {
			case <-time.After(time.Second):
				require.Fail(t, "timeout receiving publication")
			case <-done:
			}
		})
	}
}

func TestClientRefresh(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	transport := newTestTransport(func() {})
	transport.setUnidirectional(true)
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()

	expireAt1 := time.Now().Unix() + 100
	expireAt2 := time.Now().Unix() + 200
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: expireAt1})
	client, _ := newClient(newCtx, node, transport)

	client.Connect(ConnectRequest{})

	require.Equal(t, expireAt1, client.exp)
	err := client.Refresh()
	require.NoError(t, err)
	require.Zero(t, client.exp)

	done := make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), `"refresh"`) {
				close(done)
				return
			}
		}
	}()

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for done channel closed")
	case <-done:
	}

	done = make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), `"refresh"`) && strings.Contains(string(data), `"ttl":`) && strings.Contains(string(data), `"expires":true`) {
				close(done)
				return
			}
		}
	}()

	err = client.Refresh(WithRefreshExpireAt(expireAt2), WithRefreshInfo([]byte("info")))
	require.NoError(t, err)
	require.Equal(t, expireAt2, client.exp)
	require.Equal(t, []byte("info"), client.info)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for done channel closed 1")
	case <-done:
	}

	done = make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), `"refresh"`) && strings.Contains(string(data), `"ttl":`) && strings.Contains(string(data), `"expires":true`) {
				close(done)
				return
			}
		}
	}()

	err = node.Refresh("42", WithRefreshExpireAt(expireAt2), WithRefreshInfo([]byte("info")))
	require.NoError(t, err)
	require.Equal(t, expireAt2, client.exp)
	require.Equal(t, []byte("info"), client.info)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for done channel closed")
	case <-done:
	}

	done = make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), `"code":3005`) {
				// DisconnectExpired sent.
				close(done)
				return
			}
		}
	}()

	err = client.Refresh(WithRefreshExpired(true))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for done channel closed")
	case <-done:
	}
}

func TestClientRefreshExpireAtInThePast(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{}, nil
	})
	transport := newTestTransport(func() {})
	transport.setUnidirectional(true)
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()

	expireAt1 := time.Now().Unix() + 100
	expireAt2 := time.Now().Unix() - 200
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: expireAt1})
	client, _ := newClient(newCtx, node, transport)

	client.Connect(ConnectRequest{})

	require.Equal(t, expireAt1, client.exp)

	done := make(chan struct{})
	go func() {
		for data := range transport.sink {
			if strings.Contains(string(data), `"code":3005`) {
				close(done)
				return
			}
		}
	}()

	err := client.Refresh(WithRefreshExpireAt(expireAt2))
	require.NoError(t, err)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for done channel closed")
	case <-done:
	}
}

func TestClient_IsSubscribed(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})

	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)

	require.False(t, client.IsSubscribed("test"))
	subscribeClientV2(t, client, "test")
	require.True(t, client.IsSubscribed("test"))
}

func TestClientSubscribeLast(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{Options: SubscribeOptions{EnableRecovery: true}}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	result := subscribeClientV2(t, client, "test")
	require.Equal(t, uint64(0), result.Offset)

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte("{}"), WithHistory(10, time.Minute))
	}

	client = newTestClient(t, node, "42")
	connectClientV2(t, client)
	result = subscribeClientV2(t, client, "test")
	require.Equal(t, uint64(10), result.Offset, fmt.Sprintf("expected: 10, got %d", result.Offset))
}

func newTestClient(t testing.TB, node *Node, userID string) *Client {
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	return newTestClientCustomTransport(t, ctx, node, transport, userID)
}

func newTestClientCustomTransport(t testing.TB, ctx context.Context, node *Node, transport Transport, userID string) *Client {
	newCtx := SetCredentials(ctx, &Credentials{UserID: userID})
	client, err := newClient(newCtx, node, transport)
	require.NoError(t, err)
	return client
}

func newTestClientV2(t testing.TB, node *Node, userID string) *Client {
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	newCtx := SetCredentials(ctx, &Credentials{UserID: userID})
	client, err := newClient(newCtx, node, transport)
	require.NoError(t, err)
	return client
}

func TestClientUnsubscribeClientSide(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	unsubscribed := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			require.Equal(t, UnsubscribeCodeClient, e.Code)
			require.Nil(t, e.Disconnect)
			close(unsubscribed)
		})
	})

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	params := &protocol.UnsubscribeRequest{Channel: ""}
	err := client.handleUnsubscribe(params, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	params = &protocol.UnsubscribeRequest{Channel: "test"}
	err = client.handleUnsubscribe(params, &protocol.Command{}, time.Now(), rwWrapper.rw)
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
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")

	unsubscribed := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(e UnsubscribeEvent) {
			require.Equal(t, UnsubscribeCodeServer, e.Code)
			require.Nil(t, e.Disconnect)
			close(unsubscribed)
		})
	})

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")
	require.Equal(t, 1, len(client.Channels()))

	client.Unsubscribe("test")
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
	t.Parallel()
	node := defaultNodeNoHandlers()
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

	connectClientV2(t, client)
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
		write: func(rep *protocol.Reply) {
			d, _ := rep.MarshalVT()
			var r protocol.Reply
			_ = r.UnmarshalVT(d)
			wrapper.replies = append(wrapper.replies, &r)
		},
	}
	return wrapper
}

func TestClientRefreshNotAvailable(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.RefreshRequest{}
	err := client.handleRefresh(cmd, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientRefreshEmptyToken(t *testing.T) {
	node := defaultNodeNoHandlers()
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
	connectClientV2(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.RefreshRequest{Token: ""}
	err := client.handleRefresh(cmd, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientRefreshUnexpected(t *testing.T) {
	node := defaultNodeNoHandlers()
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
	connectClientV2(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.RefreshRequest{Token: "xxx"}
	err := client.handleRefresh(cmd, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestClientPublishNotAvailable(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	rwWrapper := testReplyWriterWrapper()

	cmd := &protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{}`),
	}
	err := client.handlePublish(cmd, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

type testBrokerEventHandler struct {
	// Publication must register callback func to handle Publications received.
	HandlePublicationFunc func(ch string, pub *Publication, sp StreamPosition) error
	// Join must register callback func to handle Join messages received.
	HandleJoinFunc func(ch string, info *ClientInfo) error
	// Leave must register callback func to handle Leave messages received.
	HandleLeaveFunc func(ch string, info *ClientInfo) error
	// Control must register callback func to handle Control data received.
	HandleControlFunc func([]byte) error
}

func (b *testBrokerEventHandler) HandlePublication(ch string, pub *Publication, sp StreamPosition) error {
	if b.HandlePublicationFunc != nil {
		return b.HandlePublicationFunc(ch, pub, sp)
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
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	node.broker.(*MemoryBroker).eventHandler = &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition) error {
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

	subscribeClientV2(t, client, "test")

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
	err := client.handlePublish(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "no time"}`),
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with timestamp"}`),
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with error"}`),
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorBadRequest.Code, rwWrapper.replies[0].Error.Code)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "with disconnect"}`),
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientPublishError(t *testing.T) {
	broker := NewTestBroker()
	broker.errorOnPublish = true
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnPublish(func(event PublishEvent, cb PublishCallback) {
			require.Equal(t, "test", event.Channel)
			require.NotNil(t, event.ClientInfo)
			cb(PublishReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePublish(&protocol.PublishRequest{
		Channel: "test",
		Data:    []byte(`{"input": "no time"}`),
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientPing(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePing(&protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Error(t, ErrorNotAvailable, err)
}

func TestClientPresence(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
	})

	client.OnPresence(func(e PresenceEvent, cb PresenceCallback) {
		cb(PresenceReply{}, nil)
	})
	client.OnPresenceStats(func(e PresenceStatsEvent, cb PresenceStatsCallback) {
		cb(PresenceStatsReply{}, nil)
	})

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresence(&protocol.PresenceRequest{
		Channel: "",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePresence(&protocol.PresenceRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)

	result := rwWrapper.replies[0].Presence
	require.Equal(t, 1, len(result.Presence))

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePresenceStats(&protocol.PresenceStatsRequest{
		Channel: "",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePresenceStats(&protocol.PresenceStatsRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestClientPresenceTakeover(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
		cb(SubscribeReply{Options: SubscribeOptions{EmitPresence: true}}, nil)
	})

	client.OnPresence(func(e PresenceEvent, cb PresenceCallback) {
		res, err := node.Presence(e.Channel)
		require.NoError(t, err)
		cb(PresenceReply{
			Result: &res,
		}, nil)
	})
	client.OnPresenceStats(func(e PresenceStatsEvent, cb PresenceStatsCallback) {
		res, err := node.PresenceStats(e.Channel)
		require.NoError(t, err)
		cb(PresenceStatsReply{
			Result: &res,
		}, nil)
	})

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresence(&protocol.PresenceRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
	result := rwWrapper.replies[0].Presence
	require.Equal(t, 1, len(result.Presence))

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePresenceStats(&protocol.PresenceStatsRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestClientPresenceError(t *testing.T) {
	presenceManager := NewTestPresenceManager()
	presenceManager.errorOnPresence = true
	node := nodeWithPresenceManager(presenceManager)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnPresence(func(event PresenceEvent, cb PresenceCallback) {
			require.Equal(t, "test", event.Channel)
			cb(PresenceReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresence(&protocol.PresenceRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientPresenceNotAvailable(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresence(&protocol.PresenceRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientSubscribeNotAvailable(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientPresenceStatsNotAvailable(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresenceStats(&protocol.PresenceStatsRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientPresenceStatsError(t *testing.T) {
	presenceManager := NewTestPresenceManager()
	presenceManager.errorOnPresenceStats = true
	node := nodeWithPresenceManager(presenceManager)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnPresenceStats(func(event PresenceStatsEvent, cb PresenceStatsCallback) {
			require.Equal(t, "test", event.Channel)
			cb(PresenceStatsReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePresenceStats(&protocol.PresenceStatsRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientHistoryNoFilter(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		require.Nil(t, e.Filter.Since)
		require.Equal(t, 0, e.Filter.Limit)
		cb(HistoryReply{}, nil)
	})

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte(`{}`), WithHistory(10, time.Minute))
	}

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(&protocol.HistoryRequest{
		Channel: "",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handleHistory(&protocol.HistoryRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
	result := rwWrapper.replies[0].History
	require.Equal(t, 0, len(result.Publications))
	require.Equal(t, uint64(10), result.Offset)
	require.NotZero(t, result.Epoch)
}

func TestClientHistoryWithLimit(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		require.Nil(t, e.Filter.Since)
		require.Equal(t, 3, e.Filter.Limit)
		cb(HistoryReply{}, nil)
	})

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte(`{}`), WithHistory(10, time.Minute))
	}

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(&protocol.HistoryRequest{
		Channel: "test",
		Limit:   3,
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
	result := rwWrapper.replies[0].History
	require.Equal(t, 3, len(result.Publications))
	require.Equal(t, uint64(10), result.Offset)
	require.NotZero(t, result.Epoch)
}

func TestClientHistoryWithSinceAndLimit(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		require.NotNil(t, e.Filter.Since)
		require.Equal(t, 2, e.Filter.Limit)
		cb(HistoryReply{}, nil)
	})

	var pubRes PublishResult
	for i := 0; i < 10; i++ {
		pubRes, _ = node.Publish("test", []byte(`{}`), WithHistory(10, time.Minute))
	}

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(&protocol.HistoryRequest{
		Channel: "test",
		Limit:   2,
		Since: &protocol.StreamPosition{
			Offset: 2,
			Epoch:  pubRes.Epoch,
		},
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
	result := rwWrapper.replies[0].History
	require.Equal(t, 2, len(result.Publications))
	require.Equal(t, uint64(4), result.Publications[1].Offset)
	require.Equal(t, uint64(10), result.Offset)
	require.NotZero(t, result.Epoch)
}

func TestClientHistoryTakeover(t *testing.T) {
	node := defaultTestNode()
	node.config.HistoryMaxPublicationLimit = 2
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		require.Nil(t, e.Filter.Since)
		require.Equal(t, 2, e.Filter.Limit)
		// Change limit here, so 3 publications returned.
		res, err := node.History(e.Channel, WithLimit(e.Filter.Limit+1), WithSince(e.Filter.Since))
		require.NoError(t, err)
		cb(HistoryReply{
			Result: &res,
		}, nil)
	})

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte(`{}`), WithHistory(10, time.Minute))
	}

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(&protocol.HistoryRequest{
		Channel: "test",
		Limit:   3,
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, rwWrapper.replies, 1)
	require.Nil(t, rwWrapper.replies[0].Error)
	result := rwWrapper.replies[0].History
	require.Equal(t, 3, len(result.Publications))
	require.Equal(t, uint64(10), result.Offset)
	require.NotZero(t, result.Epoch)
}

func TestClientHistoryUnrecoverablePositionEpoch(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	client.OnHistory(func(e HistoryEvent, cb HistoryCallback) {
		require.NotNil(t, e.Filter.Since)
		require.Equal(t, 2, e.Filter.Limit)
		result, err := node.History(e.Channel, WithLimit(e.Filter.Limit), WithSince(e.Filter.Since), WithReverse(e.Filter.Reverse))
		if err != nil {
			cb(HistoryReply{}, err)
			return
		}
		cb(HistoryReply{Result: &result}, nil)
	})

	for i := 0; i < 10; i++ {
		_, _ = node.Publish("test", []byte(`{}`), WithHistory(10, time.Minute))
	}

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(&protocol.HistoryRequest{
		Channel: "test",
		Limit:   2,
		Since: &protocol.StreamPosition{
			Offset: 2,
			Epoch:  "wrong_one",
		},
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorUnrecoverablePosition.Code, rwWrapper.replies[0].Error.Code)
}

func TestClientHistoryBrokerError(t *testing.T) {
	broker := NewTestBroker()
	broker.errorOnHistory = true
	node := nodeWithBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnHistory(func(event HistoryEvent, cb HistoryCallback) {
			require.Equal(t, "test", event.Channel)
			cb(HistoryReply{}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(&protocol.HistoryRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorInternal.toProto(), rwWrapper.replies[0].Error)
}

func TestClientHistoryNotAvailable(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleHistory(&protocol.HistoryRequest{
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientCloseUnauthenticated(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
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

func TestExtractUnidirectionalDisconnect(t *testing.T) {
	d := extractUnidirectionalDisconnect(errors.New("test"))
	require.Equal(t, DisconnectServerError, d)
	d = extractUnidirectionalDisconnect(ErrorLimitExceeded)
	require.Equal(t, DisconnectServerError, d)
	d = extractUnidirectionalDisconnect(DisconnectChannelLimit)
	require.Equal(t, DisconnectChannelLimit, d)
	d = extractUnidirectionalDisconnect(DisconnectServerError)
	require.Equal(t, DisconnectServerError, d)
	d = extractUnidirectionalDisconnect(ErrorExpired)
	require.Equal(t, DisconnectExpired, d)
}

func TestClientHandleEmptyData(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	proceed := HandleReadFrame(client, bytes.NewReader([]byte(nil)))
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
	proceed = HandleReadFrame(client, bytes.NewReader([]byte("test")))
	require.False(t, proceed)
	disconnect, proceed := client.dispatchCommand(&protocol.Command{}, 0)
	require.Nil(t, disconnect)
	require.False(t, proceed)
}

func TestClientHandleBrokenData(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	proceed := HandleReadFrame(client, bytes.NewReader([]byte(`nd3487yt734y38&**&**`)))
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientHandleCommandNotAuthenticated(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	cmd := &protocol.Command{Id: 1, Subscribe: &protocol.SubscribeRequest{
		Channel: "test",
	}}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)
	proceed := HandleReadFrame(client, bytes.NewReader(data))
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientHandleUnknownMethod(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	cmd := &protocol.Command{Id: 1}
	disconnect, proceed := client.dispatchCommand(cmd, 0)
	require.Equal(t, DisconnectBadRequest.Code, disconnect.Code)
	require.False(t, proceed)
}

func TestClientOnAlive(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
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
	connectClientV2(t, client)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting done close")
	}
}

func TestClientHandleCommandWithoutID(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	cmd := &protocol.Command{}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)
	proceed := HandleReadFrame(client, bytes.NewReader(data))
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestErrorDisconnectContext(t *testing.T) {
	ctx := errorDisconnectContext(nil, &DisconnectForceReconnect)
	require.Nil(t, ctx.err)
	require.Equal(t, DisconnectForceReconnect.Code, ctx.disconnect.Code)
	ctx = errorDisconnectContext(ErrorLimitExceeded, nil)
	require.Nil(t, ctx.disconnect)
	require.Equal(t, ErrorLimitExceeded, ctx.err)
}

func TestToClientError(t *testing.T) {
	require.Equal(t, ErrorInternal, toClientErr(errors.New("boom")))
	require.Equal(t, ErrorLimitExceeded, toClientErr(ErrorLimitExceeded))
}

func TestClientAlreadyAuthenticated(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	cmd := &protocol.Command{Id: 2, Connect: &protocol.ConnectRequest{}}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)
	proceed := HandleReadFrame(client, bytes.NewReader(data))
	require.False(t, proceed)
	select {
	case <-client.Context().Done():
	case <-time.After(time.Second):
		require.Fail(t, "client not closed")
	}
}

func TestClientCloseExpired(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: time.Now().Unix() + 2})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
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

func TestClientInfo(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", Info: []byte("info")})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(t, client)
	require.Equal(t, []byte("info"), client.Info())
}

func TestClientConnectExpiredError(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	transport := newTestTransport(func() {})
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42", ExpireAt: time.Now().Unix() - 2})
	client, _ := newClient(newCtx, node, transport)

	rwWrapper := testReplyWriterWrapper()
	_, err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorExpired, err)
	require.False(t, client.authenticated)
}

func TestClientPresenceUpdate(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{EmitPresence: true},
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	client.mu.RLock()
	chCtx, ok := client.channels["test"]
	client.mu.RUnlock()
	require.True(t, ok)

	err := client.updateChannelPresence("test", chCtx)
	require.NoError(t, err)
}

func TestClientSubExpired(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.config.ClientExpiredSubCloseDelay = 0
	node.config.ClientPresenceUpdateInterval = 10 * time.Millisecond

	doneCh := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					ExpireAt:     time.Now().Unix() + 1,
					EmitPresence: true,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			if event.Code == UnsubscribeCodeExpired {
				close(doneCh)
			}
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for disconnect due to expired subscription")
	}
}

func TestClientSend(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	connectClientV2(t, client)

	err := client.Send([]byte(`{}`))
	require.NoError(t, err)

	err = client.close(DisconnectForceNoReconnect)
	require.NoError(t, err)

	err = client.Send([]byte(`{}`))
	require.Error(t, err)
	require.Equal(t, io.EOF, err)
}

func TestClientClose(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	err := client.close(DisconnectShutdown)
	require.NoError(t, err)
	require.True(t, client.transport.(*testTransport).closed)
	require.Equal(t, DisconnectShutdown, client.transport.(*testTransport).disconnect)
}

func TestClientHandleRPCNotAvailable(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handleRPC(&protocol.RPCRequest{
		Method: "xxx",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)
}

func TestClientHandleRPC(t *testing.T) {
	node := defaultTestNode()
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

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handleRPC(&protocol.RPCRequest{
		Data: []byte("{}"),
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, rpcHandlerCalled)
}

func TestClientHandleSendNoHandlerSet(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	err := client.handleSend(&protocol.SendRequest{
		Data: []byte(`{"data":"hello"}`),
	}, &protocol.Command{}, time.Now())
	require.Error(t, ErrorNotAvailable, err)
}

func TestClientHandleSend(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	var messageHandlerCalled bool

	client.OnMessage(func(event MessageEvent) {
		messageHandlerCalled = true
		expectedData := []byte(`{"data":"hello"}`)
		require.Equal(t, expectedData, event.Data)
	})
	connectClientV2(t, client)

	err := client.handleSend(&protocol.SendRequest{
		Data: []byte(`{"data":"hello"}`),
	}, &protocol.Command{}, time.Now())
	require.NoError(t, err)
	require.True(t, messageHandlerCalled)
}

func TestClientHandlePublishNotAllowed(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	node.OnConnect(func(client *Client) {
		client.OnPublish(func(_ PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, ErrorPermissionDenied)
		})
	})

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handlePublish(&protocol.PublishRequest{
		Data:    []byte(`{"hello": 1}`),
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, ErrorPermissionDenied.Code, rwWrapper.replies[0].Error.Code)
}

func TestClientHandlePublish(t *testing.T) {
	node := defaultNodeNoHandlers()
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

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handlePublish(&protocol.PublishRequest{
		Data:    []byte(`{"hello":1}`),
		Channel: "",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handlePublish(&protocol.PublishRequest{
		Data:    []byte(`{"hello":1}`),
		Channel: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestClientSideRefresh(t *testing.T) {
	node := defaultNodeNoHandlers()
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

	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()

	err := client.handleRefresh(&protocol.RefreshRequest{
		Token: "test",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestServerSideRefresh(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
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

	connectClientV2(t, client)

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for work done")
	case <-done:
	}

	require.True(t, client.nextExpire > startExpireAt)
	require.Equal(t, client.info, []byte("{}"))
}

func TestServerSideRefreshDisconnect(t *testing.T) {
	t.Parallel()
	node := defaultNodeNoHandlers()
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
			require.Equal(t, DisconnectExpired.Code, event.Code)
			close(disconnected)
		})
	})

	connectClientV2(t, client)

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
	t.Parallel()
	node := defaultNodeNoHandlers()
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
			require.Equal(t, DisconnectServerError.Code, event.Code)
			close(disconnected)
		})
	})

	connectClientV2(t, client)

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
	node := defaultNodeNoHandlers()
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
	})

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test",
		Token:   "test_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorNotAvailable, err)

	client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
		require.Equal(t, "test_token", e.Token)
		cb(SubRefreshReply{
			ExpireAt: expireAt,
		}, nil)
	})

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test",
		Token:   "",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test1",
		Token:   "test_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, ErrorPermissionDenied, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "",
		Token:   "test_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)

	rwWrapper = testReplyWriterWrapper()
	err = client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test",
		Token:   "test_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
}

func TestClientSideSubRefreshUnexpected(t *testing.T) {
	node := defaultNodeNoHandlers()
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
				ClientSideRefresh: false,
			}, nil)
		})

		client.OnSubRefresh(func(e SubRefreshEvent, cb SubRefreshCallback) {
			require.Equal(t, "test_token", e.Token)
			cb(SubRefreshReply{
				ExpireAt: expireAt,
			}, nil)
		})
	})

	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubRefresh(&protocol.SubRefreshRequest{
		Channel: "test",
		Token:   "test_token",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.Equal(t, DisconnectBadRequest, err)
}

func TestCloseNoRace(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
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
	connectClientV2(t, client)

	select {
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for work done")
	case <-done:
	}
}

func TestClientCheckSubscriptionExpiration(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	var nowTime time.Time
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return nowTime
	}
	node.mu.Unlock()

	chanCtx := ChannelContext{expireAt: 100}

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
	client.channels["channel"] = ChannelContext{}
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
	require.Equal(t, []byte("info"), client.channels["channel"].info)

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
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return time.Unix(200, 0)
	}
	node.mu.Unlock()

	// no recover.
	got := client.checkPosition(300*time.Second, "channel", ChannelContext{})
	require.True(t, got)

	// not initial, not time to check.
	got = client.checkPosition(300*time.Second, "channel", ChannelContext{positionCheckTime: 50, flags: flagPositioning})
	require.True(t, got)
}

func TestClientIsValidPosition(t *testing.T) {
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()

	client := newTestClient(t, node, "42")

	node.mu.Lock()
	node.nowTimeGetter = func() time.Time {
		return time.Unix(200, 0)
	}
	node.mu.Unlock()

	client.channels = map[string]ChannelContext{
		"example": {
			flags:             flagSubscribed,
			positionCheckTime: 50,
			streamPosition: StreamPosition{
				Offset: 20,
				Epoch:  "test",
			},
		},
	}

	got := client.isValidPosition(StreamPosition{
		Offset: 20,
		Epoch:  "test",
	}, 200, "example")
	require.True(t, got)
	require.Equal(t, int64(200), client.channels["example"].positionCheckTime)

	got = client.isValidPosition(StreamPosition{
		Offset: 19,
		Epoch:  "test",
	}, 210, "example")
	require.True(t, got)
	require.Equal(t, int64(210), client.channels["example"].positionCheckTime)

	got = client.isValidPosition(StreamPosition{
		Offset: 21,
		Epoch:  "test",
	}, 220, "example")
	require.False(t, got)
	require.Equal(t, int64(210), client.channels["example"].positionCheckTime)

	client.channels = map[string]ChannelContext{
		"example": {
			positionCheckTime: 50,
			streamPosition: StreamPosition{
				Offset: 20,
				Epoch:  "test",
			},
		},
	}
	// no subscribed flag.
	got = client.isValidPosition(StreamPosition{
		Offset: 21,
		Epoch:  "test",
	}, 220, "example")
	require.True(t, got)

	_ = client.close(DisconnectConnectionClosed)
	// closed client.
	got = client.isValidPosition(StreamPosition{
		Offset: 21,
		Epoch:  "test",
	}, 220, "example")
	require.True(t, got)
}

func TestErrLogLevel(t *testing.T) {
	require.Equal(t, LogLevelInfo, errLogLevel(ErrorNotAvailable))
	require.Equal(t, LogLevelError, errLogLevel(errors.New("boom")))
}

func errLogLevel(err error) LogLevel {
	logLevel := LogLevelInfo
	if err != ErrorNotAvailable {
		logLevel = LogLevelError
	}
	return logLevel
}

func TestClientTransportWriteError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		Name               string
		Error              error
		ExpectedDisconnect Disconnect
	}{
		{"disconnect", DisconnectSlow, DisconnectSlow},
		{"other", errors.New("boom"), DisconnectWriteError},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			node := defaultTestNode()
			defer func() { _ = node.Shutdown(context.Background()) }()
			ctx, cancel := context.WithCancel(context.Background())
			transport := newTestTransport(cancel)
			transport.sink = make(chan []byte, 100)
			transport.writeErr = tt.Error
			transport.writeErrorContent = "test trigger message"

			doneUnsubscribe := make(chan struct{})
			doneDisconnect := make(chan struct{})

			node.OnConnect(func(client *Client) {
				client.OnUnsubscribe(func(event UnsubscribeEvent) {
					require.Equal(t, UnsubscribeCodeDisconnect, event.Code)
					require.Equal(t, tt.ExpectedDisconnect.Code, event.Disconnect.Code)
					close(doneUnsubscribe)
				})

				client.OnDisconnect(func(event DisconnectEvent) {
					require.Equal(t, tt.ExpectedDisconnect.Code, event.Code)
					close(doneDisconnect)
				})
			})

			newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
			client, _ := newClient(newCtx, node, transport)

			connectClientV2(t, client)

			rwWrapper := testReplyWriterWrapper()
			subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
				Channel: "test",
			}, SubscribeReply{}, &protocol.Command{}, false, time.Time{}, rwWrapper.rw)
			require.Nil(t, subCtx.disconnect)
			require.Nil(t, subCtx.err)
			require.Nil(t, rwWrapper.replies[0].Error)

			require.Equal(t, 1, node.Hub().NumSubscribers("test"))

			_, err := node.Publish("test", []byte(`{"text": "test trigger message"}`))
			require.NoError(t, err)

			select {
			case <-time.After(time.Second):
				require.Fail(t, "client not unsubscribed")
			case <-doneUnsubscribe:
				select {
				case <-time.After(time.Second):
					require.Fail(t, "client not closed")
				case <-doneDisconnect:
				}
			}
		})
	}
}

func TestFlagExists(t *testing.T) {
	flags := PushFlagDisconnect
	require.True(t, hasFlag(flags, PushFlagDisconnect))
}

func TestFlagNotExists(t *testing.T) {
	var flags uint64
	require.False(t, hasFlag(flags, PushFlagDisconnect))
}

func TestConcurrentSameChannelSubscribe(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	var wg sync.WaitGroup
	concurrency := 10
	wg.Add(concurrency)

	onSubscribe := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			go func() {
				cb(SubscribeReply{
					Options: SubscribeOptions{
						EnableRecovery: true,
					},
				}, nil)
				close(onSubscribe)
			}()
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	var subscribeErrors []string
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			rwWrapper := testReplyWriterWrapper()
			err := client.handleSubscribe(&protocol.SubscribeRequest{
				Channel: "test1",
				Recover: true,
				Offset:  0,
			}, &protocol.Command{}, time.Now(), rwWrapper.rw)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				subscribeErrors = append(subscribeErrors, err.Error())
			} else {
				subscribeErrors = append(subscribeErrors, "nil")
			}
		}()
	}

	wg.Wait()

	<-onSubscribe

	var n int
	for _, e := range subscribeErrors {
		if e == "105: already subscribed" {
			n++
		}
	}
	require.Equal(t, concurrency-1, n)
}

type slowHistoryBroker struct {
	startPublishingCh chan struct{}
	stopPublishingCh  chan struct{}
	*MemoryBroker
	err error
}

func (b *slowHistoryBroker) setError(err error) {
	b.err = err
}

func (b *slowHistoryBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	close(b.startPublishingCh)
	res, sp, err := b.MemoryBroker.History(ch, opts)
	<-b.stopPublishingCh
	if b.err != nil {
		return nil, StreamPosition{}, b.err
	}
	return res, sp, err
}

func TestSubscribeWithBufferedPublications(t *testing.T) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
	})
	if err != nil {
		panic(err)
	}
	startPublishingCh := make(chan struct{})
	stopPublishingCh := make(chan struct{})
	broker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(&slowHistoryBroker{startPublishingCh: startPublishingCh, stopPublishingCh: stopPublishingCh, MemoryBroker: broker})
	err = node.Run()
	require.NoError(t, err)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableRecovery: true,
				},
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	go func() {
		<-startPublishingCh
		for i := 0; i < 5; i++ {
			_, err := node.Publish("test1", []byte(`{}`), WithHistory(100, 60*time.Second))
			require.NoError(t, err)
		}
		close(stopPublishingCh)
	}()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
		Recover: true,
		Offset:  0,
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, 1, len(rwWrapper.replies))
	require.Nil(t, rwWrapper.replies[0].Error)
	res := extractSubscribeResult(rwWrapper.replies)
	require.Equal(t, uint64(0), res.Offset)
	require.True(t, res.Recovered)
	require.Len(t, res.Publications, 5)
	require.Equal(t, 1, len(client.Channels()))
}

func TestClientChannelsWhileSubscribing(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	waitCh := make(chan struct{})
	doneCh := make(chan struct{})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			go func() {
				<-waitCh
				cb(SubscribeReply{
					Options: SubscribeOptions{},
				}, nil)
				close(doneCh)
			}()
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Equal(t, 0, len(client.Channels()))
	require.False(t, client.IsSubscribed("test1"))
	close(waitCh)
	<-doneCh
	require.Equal(t, 1, len(client.Channels()))
}

func TestClientChannelsCleanupOnSubscribeError(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, ErrorInternal)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, client.channels, 0)
}

func TestClientChannelsCleanupOnSubscribeDisconnect(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, DisconnectChannelLimit)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, client.channels, 0)
}

func TestClientSubscribingChannelsCleanupOnClientClose(t *testing.T) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
	})
	if err != nil {
		panic(err)
	}
	startPublishingCh := make(chan struct{})
	stopPublishingCh := make(chan struct{})
	disconnectedCh := make(chan struct{})
	broker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(&slowHistoryBroker{startPublishingCh: startPublishingCh, stopPublishingCh: stopPublishingCh, MemoryBroker: broker})
	err = node.Run()
	require.NoError(t, err)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			go func() {
				cb(SubscribeReply{
					Options: SubscribeOptions{
						EnableRecovery: true,
					},
				}, nil)
			}()
		})

		client.OnDisconnect(func(event DisconnectEvent) {
			close(disconnectedCh)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)

	<-startPublishingCh
	close(stopPublishingCh)
	client.Disconnect()
	<-disconnectedCh
	require.Len(t, node.Hub().Channels(), 0, node.Hub().Channels())
}

func TestClientSubscribingChannelsCleanupOnHistoryError(t *testing.T) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
	})
	if err != nil {
		panic(err)
	}
	startPublishingCh := make(chan struct{})
	stopPublishingCh := make(chan struct{})
	broker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	slowBroker := &slowHistoryBroker{startPublishingCh: startPublishingCh, stopPublishingCh: stopPublishingCh, MemoryBroker: broker}
	slowBroker.setError(ErrorNotAvailable)
	node.SetBroker(slowBroker)
	err = node.Run()
	require.NoError(t, err)
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableRecovery: true,
				},
			}, nil)
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	close(stopPublishingCh)

	rwWrapper := testReplyWriterWrapper()
	err = client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: "test1",
	}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Len(t, node.Hub().Channels(), 0, node.Hub().Channels())
}

func TestClientOnStateSnapshot(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnect(func(client *Client) {
		client.OnStateSnapshot(func() (any, error) {
			return 1, nil
		})
	})

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)

	result, err := client.StateSnapshot()
	require.NoError(t, err)
	num, ok := result.(int)
	require.True(t, ok)
	require.Equal(t, 1, num)
}

func connectClientV2(t testing.TB, client *Client) {
	rwWrapper := testReplyWriterWrapper()
	_, err := client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	require.True(t, client.authenticated)
	client.triggerConnect()
	client.scheduleOnConnectTimers()
}

func newReplyDecoder(enc protocol.Type, data []byte) protocol.ReplyDecoder {
	if enc == protocol.TypeJSON {
		return protocol.NewJSONReplyDecoder(data)
	}
	return protocol.NewProtobufReplyDecoder(data)
}

func decodeReply(t *testing.T, protoType protocol.Type, data []byte) *protocol.Reply {
	decoder := newReplyDecoder(protoType, data)
	reply, err := decoder.Decode()
	require.NoError(t, err)
	return reply
}

func TestClientV2ReplyConstruction(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	clientV2 := newTestClientV2(t, node, "42")

	data, err := clientV2.getRefreshPushReply(&protocol.Refresh{})
	require.NoError(t, err)
	require.NotNil(t, decodeReply(t, protocol.TypeJSON, data).Push.Refresh)

	data, err = clientV2.getUnsubscribePushReply("test", unsubscribeServer)
	require.NoError(t, err)
	require.NotNil(t, decodeReply(t, protocol.TypeJSON, data).Push.Unsubscribe)

	data, err = clientV2.getSendPushReply([]byte("{}"))
	require.NoError(t, err)
	require.NotNil(t, decodeReply(t, protocol.TypeJSON, data).Push.Message)

	data, err = clientV2.getSubscribePushReply("test", &protocol.SubscribeResult{})
	require.NoError(t, err)
	require.NotNil(t, decodeReply(t, protocol.TypeJSON, data).Push.Subscribe)

	data, err = clientV2.getDisconnectPushReply(DisconnectForceNoReconnect)
	require.NoError(t, err)
	require.NotNil(t, decodeReply(t, protocol.TypeJSON, data).Push.Disconnect)

	reply, err := clientV2.getConnectPushReply(&protocol.ConnectResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Push.Connect)

	reply, err = clientV2.getRPCCommandReply(&protocol.RPCResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Rpc)

	reply, err = clientV2.getSubscribeCommandReply(&protocol.SubscribeResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Subscribe)

	reply, err = clientV2.getHistoryCommandReply(&protocol.HistoryResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.History)

	reply, err = clientV2.getPresenceStatsCommandReply(&protocol.PresenceStatsResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.PresenceStats)

	reply, err = clientV2.getPresenceCommandReply(&protocol.PresenceResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Presence)

	reply, err = clientV2.getPublishCommandReply(&protocol.PublishResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Publish)

	reply, err = clientV2.getUnsubscribeCommandReply(&protocol.UnsubscribeResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Unsubscribe)

	reply, err = clientV2.getSubRefreshCommandReply(&protocol.SubRefreshResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.SubRefresh)

	reply, err = clientV2.getRefreshCommandReply(&protocol.RefreshResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Refresh)

	reply, err = clientV2.getConnectCommandReply(&protocol.ConnectResult{})
	require.NoError(t, err)
	require.NotNil(t, reply.Connect)
}

func TestClient_HandleCommandV2_UnnecessaryPong(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	clientV2 := newTestClientV2(t, node, "42")

	ok := clientV2.HandleCommand(&protocol.Command{
		Connect: &protocol.ConnectRequest{},
	}, 0)
	// Interpreted as PONG in ProtocolVersion2. But it's unnecessary as ping not issued yet.
	require.False(t, ok)
}

func TestClient_HandleCommandV2_Pong(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	clientV2 := newTestClientV2(t, node, "42")

	clientV2.startWriter(0, 0, 0)
	clientV2.sendPing()
	ok := clientV2.HandleCommand(&protocol.Command{
		Connect: &protocol.ConnectRequest{},
	}, 0)
	// Interpreted as PONG in ProtocolVersion2.
	require.True(t, ok)
	require.NotZero(t, clientV2.lastSeen)
}

func TestClient_HandleCommandV2_NonAuthenticated(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	clientV2 := newTestClientV2(t, node, "42")

	ok := clientV2.HandleCommand(&protocol.Command{
		Id:        1,
		Subscribe: &protocol.SubscribeRequest{},
	}, 0)
	require.False(t, ok)
}

func TestClientLevelPing(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	done := make(chan struct{})
	node.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			PingPongConfig: &PingPongConfig{
				PingInterval: 5 * time.Second,
				PongTimeout:  3 * time.Second,
			},
		}, nil
	})
	node.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, DisconnectNoPong.Code, event.Disconnect.Code)
			close(done)
		})
	})
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setPing(0, 0)
	client := newTestClientCustomTransport(t, ctx, node, transport, "42")
	connectClientV2(t, client)
	select {
	case <-done:
	case <-time.After(9 * time.Second):
		t.Fatal("no disconnect in timeout")
	}
}

func TestNoClientLevelPing(t *testing.T) {
	t.Parallel()
	node := defaultTestNode()
	defer func() { _ = node.Shutdown(context.Background()) }()
	done := make(chan struct{})
	node.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			require.NotEqual(t, DisconnectNoPong.Code, event.Disconnect.Code)
			close(done)
		})
	})
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolVersion(ProtocolVersion2)
	transport.setPing(-1, 0)
	client := newTestClientCustomTransport(t, ctx, node, transport, "42")
	connectClientV2(t, client)
	select {
	case <-done:
		t.Fatal("should not disconnect when ping is disabled")
	case <-time.After(36 * time.Second):
	}
}

func TestClient_HandleCommandV2(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			ClientSideRefresh: true,
		}, nil
	})

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				ClientSideRefresh: true,
			}, nil)
		})
		client.OnRPC(func(event RPCEvent, callback RPCCallback) {
			callback(RPCReply{}, nil)
		})
		client.OnPublish(func(event PublishEvent, callback PublishCallback) {
			callback(PublishReply{}, nil)
		})
		client.OnMessage(func(event MessageEvent) {

		})
		client.OnPresence(func(event PresenceEvent, callback PresenceCallback) {
			callback(PresenceReply{}, nil)
		})
		client.OnPresenceStats(func(event PresenceStatsEvent, callback PresenceStatsCallback) {
			callback(PresenceStatsReply{}, nil)
		})
		client.OnHistory(func(event HistoryEvent, callback HistoryCallback) {
			callback(HistoryReply{}, nil)
		})
		client.OnRefresh(func(event RefreshEvent, callback RefreshCallback) {
			callback(RefreshReply{}, nil)
		})
		client.OnSubRefresh(func(event SubRefreshEvent, callback SubRefreshCallback) {
			callback(SubRefreshReply{}, nil)
		})
	})

	clientV2 := newTestClientV2(t, node, "42")

	ok := clientV2.HandleCommand(&protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 2,
		Subscribe: &protocol.SubscribeRequest{
			Channel: "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 3,
		Unsubscribe: &protocol.UnsubscribeRequest{
			Channel: "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 2,
		Subscribe: &protocol.SubscribeRequest{
			Channel: "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id:  4,
		Rpc: &protocol.RPCRequest{},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 5,
		Publish: &protocol.PublishRequest{
			Channel: "test",
			Data:    []byte("{}"),
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Send: &protocol.SendRequest{
			Data: []byte("test"),
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 6,
		Presence: &protocol.PresenceRequest{
			Channel: "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 7,
		PresenceStats: &protocol.PresenceStatsRequest{
			Channel: "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 8,
		History: &protocol.HistoryRequest{
			Channel: "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 9,
		Refresh: &protocol.RefreshRequest{
			Token: "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 10,
		SubRefresh: &protocol.SubRefreshRequest{
			Channel: "test",
			Token:   "test",
		},
	}, 0)
	require.True(t, ok)

	ok = clientV2.HandleCommand(&protocol.Command{
		Id:   4,
		Ping: &protocol.PingRequest{},
	}, 0)
	require.True(t, ok)

	// Only with id set - should result into a disconnect.
	ok = clientV2.HandleCommand(&protocol.Command{
		Id: 5,
	}, 0)
	require.False(t, ok)
}
