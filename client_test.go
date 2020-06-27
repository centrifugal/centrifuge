package centrifuge

import (
	"context"
	"encoding/json"
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
	return c.c, nil
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
	require.False(t, client.closed)
	require.False(t, client.authenticated)
	require.Nil(t, client.disconnect)
}

func TestClientClosedState(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	errCh := client.Close(nil)
	require.NoError(t, <-errCh)
	require.True(t, client.closed)
}

func TestClientTimer(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	require.NotNil(t, client.timer)
	config := node.Config()
	config.ClientStaleCloseDelay = 0
	_ = node.Reload(config)
	client, _ = newClient(context.Background(), node, transport)
	require.Nil(t, client.timer)
}

func TestClientTimerSchedule(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	client, _ := newClient(context.Background(), node, transport)
	client.nextExpire = time.Now().Unix() + 5
	client.nextPresence = time.Now().Unix() + 10
	client.scheduleNextTimer()
	require.NotNil(t, client.timer)
	require.Equal(t, timerOpExpire, client.timerOp)
	client.nextPresence = time.Now().Unix() + 1
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

	config := node.Config()
	_ = node.Reload(config)

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

	config := node.Config()
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)
	client.On().Refresh(func(e RefreshEvent) RefreshReply {
		return RefreshReply{
			Expired: true,
		}
	})

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	client.expire()
	require.True(t, client.closed)
}

func TestClientRefreshHandlerProlongsClientSession(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	config := node.Config()
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() + 60,
	})
	client, _ := newClient(newCtx, node, transport)

	expireAt := time.Now().Unix() + 60
	client.On().Refresh(func(e RefreshEvent) RefreshReply {
		return RefreshReply{
			ExpireAt: expireAt,
		}
	})

	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	disconnect := client.connectCmd(&protocol.ConnectRequest{}, rw)
	require.Nil(t, disconnect)
	client.expire()
	require.False(t, client.closed)
	require.Equal(t, expireAt, client.exp)
}

func TestClientConnectWithExpiredContextCredentials(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	config := node.Config()
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{
		UserID:   "42",
		ExpireAt: time.Now().Unix() - 60,
	})
	client, _ := newClient(newCtx, node, transport)

	client.On().Refresh(func(e RefreshEvent) RefreshReply {
		return RefreshReply{}
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
	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		HistoryLifetime: 100,
		HistorySize:     10,
	})
	_ = node.Reload(config)
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

func TestServerSideSubscriptions(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.On().ClientConnecting(func(context.Context, TransportInfo, ConnectEvent) ConnectReply {
		return ConnectReply{
			Channels: []string{
				"server-side-1",
				"$server-side-2",
			},
		}
	})
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	client.handleCommand(&protocol.Command{
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

	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		HistorySize:     10,
		HistoryLifetime: 60,
		HistoryRecover:  true,
	})
	_ = node.Reload(config)

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

	err := client.Unsubscribe("test")
	require.NoError(t, err)
	require.Equal(t, 0, len(client.Channels()))
	require.Equal(t, 1, node.Hub().NumClients())
	require.Equal(t, 0, node.Hub().NumChannels())
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
	HandlePublicationFunc func(ch string, pub *protocol.Publication) error
	// Join must register callback func to handle Join messages received.
	HandleJoinFunc func(ch string, join *protocol.Join) error
	// Leave must register callback func to handle Leave messages received.
	HandleLeaveFunc func(ch string, leave *protocol.Leave) error
	// Control must register callback func to handle Control data received.
	HandleControlFunc func([]byte) error
}

func (b *testBrokerEventHandler) HandlePublication(ch string, pub *protocol.Publication) error {
	if b.HandlePublicationFunc != nil {
		return b.HandlePublicationFunc(ch, pub)
	}
	return nil
}

func (b *testBrokerEventHandler) HandleJoin(ch string, join *protocol.Join) error {
	if b.HandleJoinFunc != nil {
		return b.HandleJoinFunc(ch, join)
	}
	return nil
}

func (b *testBrokerEventHandler) HandleLeave(ch string, leave *protocol.Leave) error {
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
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	connectClient(t, client)

	node.broker.(*MemoryEngine).eventHandler = &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *protocol.Publication) error {
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
	_ = node.Reload(config)

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

	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		Presence: true,
	})
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	client.On().Presence(func(event PresenceEvent) PresenceReply {
		return PresenceReply{}
	})
	client.On().PresenceStats(func(event PresenceStatsEvent) PresenceStatsReply {
		return PresenceStatsReply{}
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

	config = node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		Presence: false,
	})
	_ = node.Reload(config)

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

	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		HistorySize:     10,
		HistoryLifetime: 60,
	})
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)

	client.On().History(func(event HistoryEvent) HistoryReply {
		return HistoryReply{}
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

	config = node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		HistorySize:     0,
		HistoryLifetime: 0,
	})
	_ = node.Reload(config)

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

	config := node.Config()
	config.ClientStaleCloseDelay = time.Millisecond
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	time.Sleep(100 * time.Millisecond)
	client.mu.Lock()
	require.True(t, client.closed)
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
	require.False(t, client.closed)
	client.mu.RUnlock()
	time.Sleep(4 * time.Second)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.True(t, client.closed)
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

	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		Presence: true,
	})
	_ = node.Reload(config)

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

	errCh := client.Close(nil)
	require.NoError(t, <-errCh)

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

	errCh := client.Close(DisconnectShutdown)
	require.NoError(t, <-errCh)
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
	client.On().RPC(func(event RPCEvent) RPCReply {
		expectedData, _ := json.Marshal("hello")
		require.Equal(t, expectedData, event.Data)
		return RPCReply{}
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
	client.On().Message(func(event MessageEvent) MessageReply {
		expectedData, _ := json.Marshal("hello")
		require.Equal(t, expectedData, event.Data)
		return MessageReply{}
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
	client.On().Publish(func(event PublishEvent) PublishReply {
		return PublishReply{
			Error: ErrorPermissionDenied,
		}
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
	client.On().Publish(func(event PublishEvent) PublishReply {
		expectedData, _ := json.Marshal("hello")
		require.Equal(t, expectedData, event.Data)
		require.Equal(t, "test", event.Channel)
		return PublishReply{}
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
	client.On().History(func(event HistoryEvent) HistoryReply {
		return HistoryReply{}
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

	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		HistorySize:     1,
		HistoryLifetime: 30,
	})
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	client.On().History(func(event HistoryEvent) HistoryReply {
		return HistoryReply{}
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
	client.On().Presence(func(event PresenceEvent) PresenceReply {
		return PresenceReply{}
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

	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		Presence: true,
	})
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	client.On().Presence(func(event PresenceEvent) PresenceReply {
		return PresenceReply{}
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
	client.On().PresenceStats(func(event PresenceStatsEvent) PresenceStatsReply {
		return PresenceStatsReply{}
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

	config := node.Config()
	setTestChannelOptions(&config, ChannelOptions{
		Presence: true,
	})
	_ = node.Reload(config)

	transport := newTestTransport()
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := newClient(newCtx, node, transport)
	client.On().PresenceStats(func(event PresenceStatsEvent) PresenceStatsReply {
		return PresenceStatsReply{}
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
