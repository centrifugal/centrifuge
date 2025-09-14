package centrifuge

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/convert"

	"github.com/centrifugal/protocol"
	"github.com/segmentio/encoding/json"
	fdelta "github.com/shadowspore/fossil-delta"
	"github.com/stretchr/testify/require"
)

type testTransport struct {
	mu                sync.Mutex
	sink              chan []byte
	closed            bool
	closeCh           chan struct{}
	disconnect        Disconnect
	protoType         ProtocolType
	cancelFn          func()
	unidirectional    bool
	protocolVersion   ProtocolVersion
	writeErr          error
	writeErrorContent string
	pingInterval      time.Duration
	pongTimeout       time.Duration
}

func newTestTransport(cancelFn func()) *testTransport {
	return &testTransport{
		cancelFn:        cancelFn,
		protoType:       ProtocolTypeJSON,
		closeCh:         make(chan struct{}),
		unidirectional:  false,
		protocolVersion: ProtocolVersion2,
		pingInterval:    10 * time.Second,
		pongTimeout:     3 * time.Second,
	}
}

func (t *testTransport) setProtocolType(pType ProtocolType) {
	t.protoType = pType
}

func (t *testTransport) setProtocolVersion(v ProtocolVersion) {
	t.protocolVersion = v
}

func (t *testTransport) setUnidirectional(uni bool) {
	t.unidirectional = uni
}

func (t *testTransport) setSink(sink chan []byte) {
	t.sink = sink
}

func (t *testTransport) setPing(pingInterval, pongTimeout time.Duration) {
	t.pingInterval = pingInterval
	t.pongTimeout = pongTimeout
}

func (t *testTransport) Write(message []byte) error {
	if t.writeErr != nil {
		if t.writeErrorContent == "" || strings.Contains(string(message), t.writeErrorContent) {
			return t.writeErr
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.EOF
	}
	if t.sink != nil {
		t.sink <- message
	}
	return nil
}

func (t *testTransport) WriteMany(messages ...[]byte) error {
	if t.writeErr != nil {
		for _, message := range messages {
			if t.writeErrorContent == "" || strings.Contains(string(message), t.writeErrorContent) {
				return t.writeErr
			}
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.EOF
	}
	for _, buf := range messages {
		if t.sink != nil {
			t.sink <- buf
		}
	}
	return nil
}

func (t *testTransport) Name() string {
	return transportWebsocket
}

func (t *testTransport) Protocol() ProtocolType {
	return t.protoType
}

func (t *testTransport) Unidirectional() bool {
	return t.unidirectional
}

func (t *testTransport) Emulation() bool {
	return false
}

func (t *testTransport) ProtocolVersion() ProtocolVersion {
	return t.protocolVersion
}

func (t *testTransport) DisabledPushFlags() uint64 {
	if t.Unidirectional() {
		return 0
	}
	return PushFlagDisconnect
}

func (t *testTransport) PingPongConfig() PingPongConfig {
	return PingPongConfig{
		PingInterval: t.pingInterval,
		PongTimeout:  t.pongTimeout,
	}
}

func (t *testTransport) Close(disconnect Disconnect) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.disconnect = disconnect
	t.closed = true
	t.cancelFn()
	close(t.closeCh)
	return nil
}

func TestHub(t *testing.T) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(t, err)

	h := newHub(nil, m, 0)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	c.user = "test"
	removed := h.remove(c)
	require.False(t, removed)
	h.add(c)
	conns := h.UserConnections("test")
	require.Equal(t, 1, len(conns))
	require.Equal(t, 1, h.NumClients())
	require.Equal(t, 1, h.NumUsers())

	validUID := c.uid
	c.uid = "invalid"
	removed = h.remove(c)
	require.False(t, removed)
	require.Len(t, h.UserConnections("test"), 1)

	c.uid = validUID
	removed = h.remove(c)
	require.True(t, removed)
	require.Len(t, h.UserConnections("test"), 0)
}

func TestHubUnsubscribe(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.sink = make(chan []byte, 100)
	newTestSubscribedClientWithTransport(t, ctx, n, transport, "42", "test_channel")

	// Unsubscribe not existed user.
	err := n.hub.unsubscribe("1", "test_channel", unsubscribeServer, "", "")
	require.NoError(t, err)

	// Unsubscribe subscribed user.
	err = n.hub.unsubscribe("42", "test_channel", unsubscribeServer, "", "")
	require.NoError(t, err)

LOOP:
	for {
		select {
		case data := <-transport.sink:
			if string(data) == `{"push":{"channel":"test_channel","unsubscribe":{"code":2000,"reason":"server unsubscribe"}}}` {
				break LOOP
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no data in sink")
		}
	}

	require.Zero(t, n.hub.NumSubscribers("test_channel"))
}

func TestHubDisconnect(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	client := newTestSubscribedClientV2(t, n, "42", "test_channel")
	clientWithReconnect := newTestSubscribedClientV2(t, n, "24", "test_channel_reconnect")
	require.Len(t, n.hub.UserConnections("42"), 1)
	require.Len(t, n.hub.UserConnections("24"), 1)
	require.Equal(t, 1, n.hub.NumSubscribers("test_channel"))
	require.Equal(t, 1, n.hub.NumSubscribers("test_channel_reconnect"))

	wg := sync.WaitGroup{}
	wg.Add(2)

	client.eventHub.disconnectHandler = func(e DisconnectEvent) {
		defer wg.Done()
		require.Equal(t, DisconnectForceNoReconnect.Code, e.Disconnect.Code)
	}

	clientWithReconnect.eventHub.disconnectHandler = func(e DisconnectEvent) {
		defer wg.Done()
		require.Equal(t, DisconnectForceReconnect.Code, e.Disconnect.Code)
	}

	// Disconnect not existed user.
	err := n.hub.disconnect("1", DisconnectForceNoReconnect, "", "", nil)
	require.NoError(t, err)

	// Disconnect subscribed user.
	err = n.hub.disconnect("42", DisconnectForceNoReconnect, "", "", nil)
	require.NoError(t, err)
	select {
	case <-client.transport.(*testTransport).closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
	require.Len(t, n.hub.UserConnections("42"), 0)
	require.Equal(t, 0, n.hub.NumSubscribers("test_channel"))

	// Disconnect subscribed user with reconnect.
	err = n.hub.disconnect("24", DisconnectForceReconnect, "", "", nil)
	require.NoError(t, err)
	select {
	case <-clientWithReconnect.transport.(*testTransport).closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
	require.Len(t, n.hub.UserConnections("24"), 0)
	require.Equal(t, 0, n.hub.NumSubscribers("test_channel_reconnect"))

	wg.Wait()

	require.Len(t, n.hub.UserConnections("24"), 0)
	require.Len(t, n.hub.UserConnections("42"), 0)
	require.Equal(t, 0, n.hub.NumSubscribers("test_channel"))
	require.Equal(t, 0, n.hub.NumSubscribers("test_channel_reconnect"))
}

func TestHubDisconnect_ClientWhitelist(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	client := newTestSubscribedClientV2(t, n, "12", "test_channel")
	clientToKeep := newTestSubscribedClientV2(t, n, "12", "test_channel")

	require.Len(t, n.hub.UserConnections("12"), 2)
	require.Equal(t, 2, n.hub.NumSubscribers("test_channel"))

	shouldBeClosed := make(chan struct{})
	shouldNotBeClosed := make(chan struct{})

	client.eventHub.disconnectHandler = func(e DisconnectEvent) {
		close(shouldBeClosed)
	}

	clientToKeep.eventHub.disconnectHandler = func(e DisconnectEvent) {
		close(shouldNotBeClosed)
	}

	whitelist := []string{clientToKeep.ID()}

	// Disconnect not existed user.
	err := n.hub.disconnect("12", DisconnectConnectionLimit, "", "", whitelist)
	require.NoError(t, err)

	select {
	case <-shouldBeClosed:
		select {
		case <-shouldNotBeClosed:
			require.Fail(t, "client should not be disconnected")
		case <-time.After(time.Second):
			require.Len(t, n.hub.UserConnections("12"), 1)
			require.Equal(t, 1, n.hub.NumSubscribers("test_channel"))
		}
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	}
}

func TestHubOperationsWithClientID(t *testing.T) {
	t.Parallel()

	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	client := newTestSubscribedClientV2(t, n, "12", "test_channel")
	clientToKeep := newTestSubscribedClientV2(t, n, "12", "test_channel")

	require.Len(t, n.hub.UserConnections("12"), 2)
	require.Equal(t, 2, n.hub.NumSubscribers("test_channel"))

	shouldBeClosed := make(chan struct{})
	shouldNotBeClosed := make(chan struct{})

	client.eventHub.disconnectHandler = func(e DisconnectEvent) {
		close(shouldBeClosed)
	}

	clientToKeep.eventHub.disconnectHandler = func(e DisconnectEvent) {
		close(shouldNotBeClosed)
	}

	clientToDisconnect := client.ID()

	require.Equal(t, 2, n.hub.NumSubscriptions())
	err := n.hub.subscribe("12", "channel", clientToKeep.ID(), "")
	require.NoError(t, err)
	require.Equal(t, 3, n.hub.NumSubscriptions())
	err = n.hub.unsubscribe("12", "channel", unsubscribeServer, clientToKeep.ID(), "")
	require.NoError(t, err)
	require.Equal(t, 2, n.hub.NumSubscriptions())

	err = n.hub.disconnect("12", DisconnectConnectionLimit, clientToDisconnect, "", nil)
	require.NoError(t, err)

	select {
	case <-shouldBeClosed:
		select {
		case <-shouldNotBeClosed:
			require.Fail(t, "client should not be disconnected")
		case <-time.After(time.Second):
			require.Len(t, n.hub.UserConnections("12"), 1)
			require.Equal(t, 1, n.hub.NumSubscribers("test_channel"))
		}
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	}
}

func TestHubOperationsWithSessionID(t *testing.T) {
	t.Parallel()

	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {})

	transport := newTestTransport(func() {})
	transport.setUnidirectional(true)
	transport.setProtocolVersion(ProtocolVersion2)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "12"})
	client, _ := newClient(newCtx, n, transport)
	connectClientV2(t, client)

	transport2 := newTestTransport(func() {})
	transport2.setUnidirectional(true)
	transport2.setProtocolVersion(ProtocolVersion2)
	ctx2 := context.Background()
	newCtx2 := SetCredentials(ctx2, &Credentials{UserID: "12"})
	clientToKeep, _ := newClient(newCtx2, n, transport2)
	connectClientV2(t, clientToKeep)

	require.Len(t, n.hub.UserConnections("12"), 2)

	shouldBeClosed := make(chan struct{})
	shouldNotBeClosed := make(chan struct{})

	client.eventHub.disconnectHandler = func(e DisconnectEvent) {
		close(shouldBeClosed)
	}

	clientToKeep.eventHub.disconnectHandler = func(e DisconnectEvent) {
		close(shouldNotBeClosed)
	}

	sessionToDisconnect := client.sessionID()

	_, ok := n.hub.clientBySession(sessionToDisconnect)
	require.True(t, ok)
	require.Equal(t, 0, n.hub.NumSubscriptions())
	err := n.hub.subscribe("12", "test", "", clientToKeep.sessionID())
	require.NoError(t, err)
	require.Equal(t, 1, n.hub.NumSubscriptions())
	err = n.hub.unsubscribe("12", "test", unsubscribeServer, "", clientToKeep.sessionID())
	require.NoError(t, err)
	require.Equal(t, 0, n.hub.NumSubscriptions())

	err = n.hub.disconnect("12", DisconnectConnectionLimit, "", sessionToDisconnect, nil)
	require.NoError(t, err)

	select {
	case <-shouldBeClosed:
		select {
		case <-shouldNotBeClosed:
			require.Fail(t, "client should not be disconnected")
		case <-time.After(time.Second):
			require.Len(t, n.hub.UserConnections("12"), 1)
		}
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	}
}

func TestHubBroadcastPublication(t *testing.T) {
	tcs := []struct {
		name            string
		protocolType    ProtocolType
		protocolVersion ProtocolVersion
		uni             bool
	}{
		{name: "JSON", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2},
		{name: "Protobuf", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2},
		{name: "JSON-uni", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2, uni: true},
		{name: "Protobuf-uni", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2, uni: true},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := defaultTestNode()
			n.config.Metrics.GetChannelNamespaceLabel = func(channel string) string {
				return channel
			}
			defer func() { _ = n.Shutdown(context.Background()) }()

			ctx, cancelFn := context.WithCancel(context.Background())
			transport := newTestTransport(cancelFn)
			transport.sink = make(chan []byte, 100)
			transport.setProtocolType(tc.protocolType)
			transport.setProtocolVersion(tc.protocolVersion)
			transport.setUnidirectional(tc.uni)
			newTestSubscribedClientWithTransport(t, ctx, n, transport, "42", "test_channel")

			// Broadcast to non-existing channel.
			err := n.hub.BroadcastPublication(
				"non_existing_channel",
				&Publication{Data: []byte(`{"data": "broadcast_data"}`)},
				StreamPosition{},
			)
			require.NoError(t, err)

			// Broadcast to existing channel.
			err = n.hub.BroadcastPublication(
				"test_channel",
				&Publication{Data: []byte(`{"data": "broadcast_data"}`)},
				StreamPosition{},
			)
			require.NoError(t, err)
		LOOP:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_data") {
						break LOOP
					}
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink")
				}
			}
		})
	}
}

func deltaTestNode() *Node {
	n := defaultNodeNoHandlers()
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					EnableRecovery:    true,
					RecoveryMode:      RecoveryModeCache,
					AllowedDeltaTypes: []DeltaType{DeltaTypeFossil},
				},
			}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
	})
	return n
}

func deltaTestNodeNoRecovery() *Node {
	n := defaultNodeNoHandlers()
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{
				Options: SubscribeOptions{
					AllowedDeltaTypes: []DeltaType{DeltaTypeFossil},
				},
			}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
	})
	return n
}

func newTestSubscribedClientWithTransportDelta(t *testing.T, ctx context.Context, n *Node, transport Transport, userID, chanID string, deltaType DeltaType) *Client {
	client := newTestConnectedClientWithTransport(t, ctx, n, transport, userID)
	subscribeClientDelta(t, client, chanID, deltaType)
	require.True(t, n.hub.NumSubscribers(chanID) > 0)
	require.Contains(t, client.channels, chanID)
	return client
}

func subscribeClientDelta(t testing.TB, client *Client, ch string, deltaType DeltaType) *protocol.SubscribeResult {
	rwWrapper := testReplyWriterWrapper()
	err := client.handleSubscribe(&protocol.SubscribeRequest{
		Channel: ch,
		Delta:   string(deltaType),
	}, &protocol.Command{Id: 1}, time.Now(), rwWrapper.rw)
	require.NoError(t, err)
	require.Nil(t, rwWrapper.replies[0].Error)
	return rwWrapper.replies[0].Subscribe
}

func TestHubBroadcastPublicationDelta(t *testing.T) {
	tcs := []struct {
		name            string
		protocolType    ProtocolType
		protocolVersion ProtocolVersion
		uni             bool
	}{
		{name: "JSON", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2},
		{name: "Protobuf", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2},
		{name: "JSON-uni", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2, uni: true},
		{name: "Protobuf-uni", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2, uni: true},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := deltaTestNode()
			n.config.Metrics.GetChannelNamespaceLabel = func(channel string) string {
				return channel
			}
			defer func() { _ = n.Shutdown(context.Background()) }()

			ctx, cancelFn := context.WithCancel(context.Background())
			transport := newTestTransport(cancelFn)
			transport.sink = make(chan []byte, 100)
			transport.setProtocolType(tc.protocolType)
			transport.setProtocolVersion(tc.protocolVersion)
			transport.setUnidirectional(tc.uni)
			newTestSubscribedClientWithTransportDelta(
				t, ctx, n, transport, "42", "test_channel", DeltaTypeFossil)

			res, err := n.History("test_channel")
			require.NoError(t, err)

			err = n.hub.broadcastPublication(
				"test_channel",
				StreamPosition{Offset: 1, Epoch: res.StreamPosition.Epoch},
				&Publication{Data: []byte(`{"data": "broadcast_data"}`), Offset: 1},
				nil,
				nil,
				ChannelBatchConfig{},
			)
			require.NoError(t, err)

		LOOP:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_data") {
						break LOOP
					}
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink")
				}
			}

			// Broadcast same data to existing channel.
			err = n.hub.broadcastPublication(
				"test_channel",
				StreamPosition{Offset: 2, Epoch: res.StreamPosition.Epoch},
				&Publication{Data: []byte(`{"data": "broadcast_data"}`), Offset: 2},
				&Publication{Data: []byte(`{"data": "broadcast_data"}`), Offset: 1},
				nil, ChannelBatchConfig{},
			)
			require.NoError(t, err)

		LOOP2:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_data") {
						require.Fail(t, "should not receive same data twice - delta expected")
					}
					break LOOP2
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink 2")
				}
			}
		})
	}
}

func TestHubBroadcastPublicationDeltaAtMostOnce(t *testing.T) {
	tcs := []struct {
		name            string
		protocolType    ProtocolType
		protocolVersion ProtocolVersion
		uni             bool
	}{
		{name: "JSON", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2},
		{name: "Protobuf", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2},
		{name: "JSON-uni", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2, uni: true},
		{name: "Protobuf-uni", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2, uni: true},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := deltaTestNodeNoRecovery()
			n.config.Metrics.GetChannelNamespaceLabel = func(channel string) string {
				return channel
			}
			defer func() { _ = n.Shutdown(context.Background()) }()

			ctx, cancelFn := context.WithCancel(context.Background())
			transport := newTestTransport(cancelFn)
			transport.sink = make(chan []byte, 100)
			transport.setProtocolType(tc.protocolType)
			transport.setProtocolVersion(tc.protocolVersion)
			transport.setUnidirectional(tc.uni)
			newTestSubscribedClientWithTransportDelta(
				t, ctx, n, transport, "42", "test_channel", DeltaTypeFossil)

			res, err := n.History("test_channel")
			require.NoError(t, err)

			err = n.hub.broadcastPublication(
				"test_channel",
				StreamPosition{Offset: 1, Epoch: res.StreamPosition.Epoch},
				&Publication{Data: []byte(`{"data": "broadcast_data"}`), Offset: 1},
				nil,
				nil, ChannelBatchConfig{},
			)
			require.NoError(t, err)

		LOOP:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_data") {
						break LOOP
					}
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink")
				}
			}

			// Broadcast same data to existing channel.
			err = n.hub.broadcastPublication(
				"test_channel",
				StreamPosition{Offset: 2, Epoch: res.StreamPosition.Epoch},
				&Publication{Data: []byte(`{"data": "broadcast_data"}`), Offset: 2},
				nil,
				&Publication{Data: []byte(`{"data": "broadcast_data"}`), Offset: 1}, ChannelBatchConfig{},
			)
			require.NoError(t, err)

		LOOP2:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_data") {
						require.Fail(t, "should not receive same data twice - delta expected")
					}
					break LOOP2
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink 2")
				}
			}
		})
	}
}

func TestHubBroadcastPublicationDeltaAtMostOnceNoOffset(t *testing.T) {
	tcs := []struct {
		name            string
		protocolType    ProtocolType
		protocolVersion ProtocolVersion
		uni             bool
	}{
		{name: "JSON", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2},
		{name: "Protobuf", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2},
		{name: "JSON-uni", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2, uni: true},
		{name: "Protobuf-uni", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2, uni: true},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := deltaTestNodeNoRecovery()
			n.config.Metrics.GetChannelNamespaceLabel = func(channel string) string {
				return channel
			}
			defer func() { _ = n.Shutdown(context.Background()) }()

			ctx, cancelFn := context.WithCancel(context.Background())
			transport := newTestTransport(cancelFn)
			transport.sink = make(chan []byte, 100)
			transport.setProtocolType(tc.protocolType)
			transport.setProtocolVersion(tc.protocolVersion)
			transport.setUnidirectional(tc.uni)
			newTestSubscribedClientWithTransportDelta(
				t, ctx, n, transport, "42", "test_channel", DeltaTypeFossil)

			err := n.hub.broadcastPublication(
				"test_channel",
				StreamPosition{},
				&Publication{Data: []byte(`{"data": "broadcast_data"}`)},
				nil,
				nil, ChannelBatchConfig{},
			)
			require.NoError(t, err)

		LOOP:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_data") {
						break LOOP
					}
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink")
				}
			}

			// Broadcast same data to existing channel.
			err = n.hub.broadcastPublication(
				"test_channel",
				StreamPosition{},
				&Publication{Data: []byte(`{"data": "broadcast_data"}`)},
				nil,
				&Publication{Data: []byte(`{"data": "broadcast_data"}`)},
				ChannelBatchConfig{},
			)
			require.NoError(t, err)

		LOOP2:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_data") {
						require.Fail(t, "should not receive same data twice - delta expected")
					}
					break LOOP2
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink 2")
				}
			}
		})
	}
}

func TestHubBroadcastJoin(t *testing.T) {
	tcs := []struct {
		name            string
		protocolType    ProtocolType
		protocolVersion ProtocolVersion
		uni             bool
	}{
		{name: "JSON-V2", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2},
		{name: "Protobuf-V2", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2},
		{name: "JSON-V2-uni", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2, uni: true},
		{name: "Protobuf-V2-uni", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2, uni: true},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := defaultTestNode()
			defer func() { _ = n.Shutdown(context.Background()) }()

			ctx, cancelFn := context.WithCancel(context.Background())
			transport := newTestTransport(cancelFn)
			transport.sink = make(chan []byte, 100)
			transport.setProtocolType(tc.protocolType)
			transport.setProtocolVersion(tc.protocolVersion)
			transport.setUnidirectional(tc.uni)
			c := newTestSubscribedClientWithTransport(t, ctx, n, transport, "42", "test_channel")
			chCtx := c.channels["test_channel"]
			chCtx.flags |= flagPushJoinLeave
			c.channels["test_channel"] = chCtx

			// Broadcast to not existed channel.
			err := n.hub.broadcastJoin("not_test_channel", &ClientInfo{ClientID: "broadcast_client"}, ChannelBatchConfig{})
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastJoin("test_channel", &ClientInfo{ClientID: "broadcast_client"}, ChannelBatchConfig{})
			require.NoError(t, err)
		LOOP:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_client") {
						break LOOP
					}
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink")
				}
			}
		})
	}
}

func TestHubBroadcastLeave(t *testing.T) {
	tcs := []struct {
		name            string
		protocolType    ProtocolType
		protocolVersion ProtocolVersion
		uni             bool
	}{
		{name: "JSON-V2", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2},
		{name: "Protobuf-V2", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2},
		{name: "JSON-V2-uni", protocolType: ProtocolTypeJSON, protocolVersion: ProtocolVersion2, uni: true},
		{name: "Protobuf-V2-uni", protocolType: ProtocolTypeProtobuf, protocolVersion: ProtocolVersion2, uni: true},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := defaultTestNode()
			defer func() { _ = n.Shutdown(context.Background()) }()

			ctx, cancelFn := context.WithCancel(context.Background())
			transport := newTestTransport(cancelFn)
			transport.sink = make(chan []byte, 100)
			transport.setProtocolType(tc.protocolType)
			transport.setProtocolVersion(tc.protocolVersion)
			transport.setUnidirectional(tc.uni)
			c := newTestSubscribedClientWithTransport(t, ctx, n, transport, "42", "test_channel")
			chCtx := c.channels["test_channel"]
			chCtx.flags |= flagPushJoinLeave
			c.channels["test_channel"] = chCtx

			// Broadcast to not existed channel.
			err := n.hub.broadcastLeave("not_test_channel", &ClientInfo{ClientID: "broadcast_client"}, ChannelBatchConfig{})
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastLeave("test_channel", &ClientInfo{ClientID: "broadcast_client"}, ChannelBatchConfig{})
			require.NoError(t, err)
		LOOP:
			for {
				select {
				case data := <-transport.sink:
					if strings.Contains(string(data), "broadcast_client") {
						break LOOP
					}
				case <-time.After(2 * time.Second):
					t.Fatal("no data in sink")
				}
			}
		})
	}
}

func TestHubShutdown(t *testing.T) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(t, err)
	h := newHub(nil, m, 0)
	err = h.shutdown(context.Background())
	require.NoError(t, err)
	h = newHub(nil, m, 0)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	h.add(c)

	err = h.shutdown(context.Background())
	require.NoError(t, err)

	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = h.shutdown(ctxCanceled)
	require.EqualError(t, err, "context canceled")
}

func TestHubSubscriptions(t *testing.T) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(t, err)
	h := newHub(nil, m, 0)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)

	_, _, _ = h.addSub("test1", subInfo{client: c, deltaType: ""})
	_, _, _ = h.addSub("test2", subInfo{client: c, deltaType: ""})
	require.Equal(t, 2, h.NumChannels())
	require.Contains(t, h.Channels(), "test1")
	require.Contains(t, h.Channels(), "test2")
	require.NotZero(t, h.NumSubscribers("test1"))
	require.NotZero(t, h.NumSubscribers("test2"))

	// Not exited sub.
	empty, wasRemoved := h.removeSub("not_existed", c)
	require.True(t, empty)
	require.False(t, wasRemoved)

	// Exited sub with invalid uid.
	validUID := c.uid
	c.uid = "invalid"
	empty, wasRemoved = h.removeSub("test1", c)
	require.True(t, empty)
	require.False(t, wasRemoved)
	c.uid = validUID

	// Exited sub.
	empty, wasRemoved = h.removeSub("test1", c)
	require.True(t, empty)
	require.True(t, wasRemoved)

	// Exited sub.
	empty, wasRemoved = h.removeSub("test2", c)
	require.True(t, empty)
	require.True(t, wasRemoved)

	require.Equal(t, h.NumChannels(), 0)
	require.Zero(t, h.NumSubscribers("test1"))
	require.Zero(t, h.NumSubscribers("test2"))
}

func TestUserConnections(t *testing.T) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(t, err)
	h := newHub(nil, m, 0)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	h.add(c)

	connections := h.UserConnections(c.UserID())
	require.Equal(t, h.connShards[index(c.UserID(), numHubShards)].clients, connections)
}

func TestHubSharding(t *testing.T) {
	numUsers := numHubShards * 10
	numChannels := numHubShards * 10

	channels := make([]string, 0, numChannels)
	for i := 0; i < numChannels; i++ {
		channels = append(channels, "ch"+strconv.Itoa(i))
	}

	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	for j := 0; j < 2; j++ { // two connections from the same user.
		for i := 0; i < numUsers; i++ {
			c, err := newClient(context.Background(), n, newTestTransport(func() {}))
			require.NoError(t, err)
			c.user = strconv.Itoa(i)
			require.NoError(t, err)
			n.hub.add(c)
			for _, ch := range channels {
				_, _, _ = n.hub.addSub(ch, subInfo{client: c, deltaType: ""})
			}
		}
	}

	for i := range n.hub.connShards {
		require.NotZero(t, n.hub.connShards[i].NumClients())
		require.NotZero(t, n.hub.connShards[i].NumUsers())
	}
	for i := range n.hub.subShards {
		require.True(t, len(n.hub.subShards[i].subs) > 0)
	}

	require.Equal(t, numUsers, n.Hub().NumUsers())
	require.Equal(t, 2*numUsers, n.Hub().NumClients())
	require.Equal(t, numChannels, n.Hub().NumChannels())
}

// This benchmark allows to estimate the benefit from Hub sharding.
// As we have a broadcasting goroutine here it's not very useful to look at
// total allocations here - it's better to look at operation time.
func BenchmarkHub_Contention(b *testing.B) {
	numClients := 100
	numChannels := 128

	n := defaultTestNodeBenchmark(b)

	var clients []*Client
	var channels []string

	for i := 0; i < numChannels; i++ {
		channels = append(channels, "ch"+strconv.Itoa(i))
	}

	for i := 0; i < numClients; i++ {
		c := newTestConnectedClientWithTransport(b, context.Background(), n, newTestTransport(func() {}), "12")
		n.hub.add(c)
		clients = append(clients, c)
		for _, ch := range channels {
			_, _, _ = n.hub.addSub(ch, subInfo{client: c, deltaType: ""})
		}
	}

	pub := &Publication{
		Data: []byte(`{"input": "test"}`),
	}
	streamPosition := StreamPosition{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = n.hub.BroadcastPublication(channels[(i+numChannels/2)%numChannels], pub, streamPosition)
			}()
			_, _, _ = n.hub.addSub(channels[i%numChannels], subInfo{client: clients[i%numClients], deltaType: ""})
			wg.Wait()
		}
	})
}

var broadcastBenches = []struct {
	NumSubscribers int
}{
	{1},
	{10},
	{100},
	{1000},
	{10000},
}

// BenchmarkHub_MassiveBroadcast allows estimating time to broadcast
// a single message to many subscribers inside one channel.
func BenchmarkHub_MassiveBroadcast(b *testing.B) {
	pub := &Publication{Data: []byte(`{"input": "test"}`)}
	streamPosition := StreamPosition{}

	for _, tt := range broadcastBenches {
		numSubscribers := tt.NumSubscribers
		b.Run(fmt.Sprintf("subscribers_%d", numSubscribers), func(b *testing.B) {
			b.ReportAllocs()
			n := defaultTestNodeBenchmark(b)

			channel := "broadcast"

			sink := make(chan []byte, 10000)

			for i := 0; i < numSubscribers; i++ {
				t := newTestTransport(func() {})
				t.setSink(sink)
				c := newTestConnectedClientWithTransport(b, context.Background(), n, t, "12")
				n.hub.add(c)
				_, _, _ = n.hub.addSub(channel, subInfo{client: c, deltaType: ""})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					j := 0
					for {
						<-sink
						j++
						if j == numSubscribers {
							break
						}
					}
				}()
				_ = n.hub.broadcastPublication(channel, streamPosition, pub, nil, nil, ChannelBatchConfig{})
				wg.Wait()
			}
		})
	}
}

func TestHubBroadcastInappropriateProtocol_Publication(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	testFunc := func(client *Client) {
		done := make(chan struct{})
		client.eventHub.disconnectHandler = func(e DisconnectEvent) {
			require.Equal(t, DisconnectInappropriateProtocol.Code, e.Disconnect.Code)
			close(done)
		}
		err := n.hub.BroadcastPublication("test_channel", &Publication{
			Data: []byte(`{111`),
		}, StreamPosition{})
		require.NoError(t, err)
		waitWithTimeout(t, done)
	}

	t.Run("protocol_v2", func(t *testing.T) {
		client := newTestSubscribedClientV2(t, n, "42", "test_channel")
		testFunc(client)
	})
}

func TestHubBroadcastInappropriateProtocol_Join(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	testFunc := func(client *Client) {
		done := make(chan struct{})
		client.eventHub.disconnectHandler = func(e DisconnectEvent) {
			require.Equal(t, DisconnectInappropriateProtocol.Code, e.Disconnect.Code)
			close(done)
		}
		err := n.hub.broadcastJoin("test_channel", &ClientInfo{
			ChanInfo: []byte(`{111`),
		}, ChannelBatchConfig{})
		require.NoError(t, err)
		waitWithTimeout(t, done)
	}

	t.Run("protocol_v2", func(t *testing.T) {
		client := newTestSubscribedClientV2(t, n, "42", "test_channel")
		testFunc(client)
	})
}

func TestHubBroadcastInappropriateProtocol_Leave(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	testFunc := func(client *Client) {
		done := make(chan struct{})
		client.eventHub.disconnectHandler = func(e DisconnectEvent) {
			require.Equal(t, DisconnectInappropriateProtocol.Code, e.Disconnect.Code)
			close(done)
		}
		err := n.hub.broadcastLeave("test_channel", &ClientInfo{
			ChanInfo: []byte(`{111`),
		}, ChannelBatchConfig{})
		require.NoError(t, err)
		waitWithTimeout(t, done)
	}

	t.Run("protocol_v2", func(t *testing.T) {
		client := newTestSubscribedClientV2(t, n, "42", "test_channel")
		testFunc(client)
	})
}

var testJsonData = []byte(`{
   "_id":"662fb7df5110d6e8e9942fb2",
   "index":0,
   "guid":"a100afc6-fc35-47fd-8e3e-e8e9a81629ec",
   "isActive":true,
   "balance":"$2,784.25",
   "picture":"http://placehold.it/32x32",
   "age":21,
   "eyeColor":"green",
   "name":"Lois Norris",
   "gender":"female",
   "company":"ORGANICA",
   "email":"loisnorris@organica.com",
   "phone":"+1 (939) 451-2349",
   "address":"774 Ide Court, Sabillasville, Virginia, 4034",
   "about":"Cupidatat reprehenderit laboris aute pariatur nulla exercitation. Commodo aliqua cupidatat consectetur aliquip. Id irure nisi qui ullamco culpa reprehenderit nisi sunt consequat ipsum. Velit officia sint id voluptate anim. Sunt duis duis consequat mollit incididunt laborum enim amet ad aliqua esse nulla. Aliqua nulla adipisicing ad aliquip ut. Nostrud mollit ex aute magna culpa ea exercitation qui ex.\r\n",
   "registered":"2023-02-28T11:09:34 -02:00",
   "latitude":24.054483,
   "longitude":38.953522,
   "tags":[
      "consequat",
      "adipisicing",
      "eiusmod",
      "ipsum",
      "enim",
      "et",
      "voluptate"
   ],
   "friends":[
      {
         "id":0,
         "name":"Kaufman Randall"
      },
      {
         "id":1,
         "name":"Byrd Cooley"
      },
      {
         "id":2,
         "name":"Obrien William"
      }
   ],
   "greeting":"Hello, Lois Norris! You have 9 unread messages.",
   "favoriteFruit":"banana"
}`)

// Has some changes (in tags field, in friends field).
var testNewJsonData = []byte(`{
   "_id":"662fb7df5110d6e8e9942fb2",
   "index":0,
   "guid":"a100afc6-fc35-47fd-8e3e-e8e9a81629ec",
   "isActive":true,
   "balance":"$2,784.25",
   "picture":"http://placehold.it/32x32",
   "age":21,
   "eyeColor":"green",
   "name":"Lois Norris",
   "gender":"female",
   "company":"ORGANICA",
   "email":"loisnorris@organica.com",
   "phone":"+1 (939) 451-2349",
   "address":"774 Ide Court, Sabillasville, Virginia, 4034",
   "about":"Cupidatat reprehenderit laboris aute pariatur nulla exercitation. Commodo aliqua cupidatat consectetur aliquip. Id irure nisi qui ullamco culpa reprehenderit nisi sunt consequat ipsum. Velit officia sint id voluptate anim. Sunt duis duis consequat mollit incididunt laborum enim amet ad aliqua esse nulla. Aliqua nulla adipisicing ad aliquip ut. Nostrud mollit ex aute magna culpa ea exercitation qui ex.\r\n",
   "registered":"2023-02-28T11:09:34 -02:00",
   "latitude":24.054483,
   "longitude":38.953522,
   "tags":[
      "consequat",
      "adipisicing",
      "eiusmod"
   ],
   "friends":[
      {
         "id":0,
         "name":"Kaufman Randall"
      },
      {
         "id":1,
         "name":"Byrd Cooley"
      }
   ],
   "greeting":"Hello, Lois Norris! You have 9 unread messages.",
   "favoriteFruit":"banana"
}`)

func TestJsonStringEncode(t *testing.T) {
	testBenchmarkDeltaFossilPatch = fdelta.Create(testJsonData, testNewJsonData)
	if len(testBenchmarkDeltaFossilPatch) == 0 {
		t.Fatal("empty fossil patch")
	}
	testDeltaJsonData, err := json.Marshal(convert.BytesToString(testBenchmarkDeltaFossilPatch))
	require.NoError(t, err)
	require.NotNil(t, testDeltaJsonData)

	alternativeDeltaJsonData := json.Escape(convert.BytesToString(testBenchmarkDeltaFossilPatch))
	require.Equal(t, testDeltaJsonData, alternativeDeltaJsonData)
}

var testBenchmarkEncodeData []byte

func BenchmarkEncodeJSONString(b *testing.B) {
	jsonData := []byte(`{"input": "test"}`)
	for i := 0; i < b.N; i++ {
		testBenchmarkEncodeData = json.Escape(convert.BytesToString(jsonData))
		if len(testBenchmarkEncodeData) == 0 {
			b.Fatal("empty data")
		}
	}
}

var testBenchmarkDeltaFossilPatch []byte

func BenchmarkDeltaFossil(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testBenchmarkDeltaFossilPatch = fdelta.Create(testJsonData, testNewJsonData)
		if len(testBenchmarkDeltaFossilPatch) == 0 {
			b.Fatal("empty fossil patch")
		}
	}
}

func TestSubIDUniquenessAcrossShards(t *testing.T) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(t, err)

	h := newHub(nil, m, 0)

	// Create test clients for different shards
	clients := make([]*Client, 0)
	for i := 0; i < 10; i++ {
		c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
		require.NoError(t, err)
		clients = append(clients, c)
	}

	// Track all generated sub IDs to ensure uniqueness
	subIDs := make(map[int64]bool)
	var collectedIDs []int64

	// Generate sub IDs across multiple channels and shards
	// Use channels that will be distributed across different shards
	channels := []string{
		"channel_0",  // likely shard 0
		"channel_1",  // likely different shard
		"channel_2",  // likely different shard
		"channel_63", // likely different shard
		"test_a",     // different hash
		"test_b",     // different hash
		"test_c",     // different hash
		"user_123",   // different hash
		"user_456",   // different hash
		"room_789",   // different hash
	}

	// Create subscriptions with useID=true across different channels
	// This will force sub ID generation across different shards
	for _, channel := range channels {
		for _, client := range clients {
			subID, isNew, err := h.addSub(channel, subInfo{
				client: client,
				useID:  true,
			})
			require.NoError(t, err)

			if isNew {
				// This is a new channel, should get a unique sub ID
				require.False(t, subIDs[subID], "Sub ID %d already exists - not unique!", subID)
				subIDs[subID] = true
				collectedIDs = append(collectedIDs, subID)
			}
		}
	}

	// Verify we got unique IDs
	require.Equal(t, len(channels), len(collectedIDs), "Should have unique sub IDs for each channel")

	// Verify the ID generation pattern follows our shard-aware algorithm
	// With numHubShards=64, each shard should generate IDs like:
	// Shard 0: 0, 64, 128, 192...
	// Shard 1: 1, 65, 129, 193...
	// etc.
	for _, subID := range collectedIDs {
		shardIndex := subID % numHubShards
		require.True(t, shardIndex >= 0 && shardIndex < numHubShards,
			"Sub ID %d has invalid shard index %d", subID, shardIndex)
	}

	// Test that same channel returns same sub ID
	for _, channel := range channels[:3] { // Test first 3 channels
		for _, client := range clients[:3] { // Test first 3 clients
			subID, isNew, err := h.addSub(channel, subInfo{
				client: client,
				useID:  true,
			})
			require.NoError(t, err)
			require.False(t, isNew, "Should not be new - channel already exists")
			require.True(t, subIDs[subID], "Should return existing sub ID")
		}
	}
}

func TestSubIDShardDistribution(t *testing.T) {
	m, err := newMetricsRegistry(MetricsConfig{
		MetricsNamespace: "test",
	})
	require.NoError(t, err)

	h := newHub(nil, m, 0)

	// Create channels that hash to specific shards to test the distribution
	// We'll create enough channels to test multiple IDs per shard
	channelsPerShard := 3

	client, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)

	// Track IDs by shard index
	idsByShard := make(map[int][]int64)

	channelIndex := 0
	for shardIdx := 0; shardIdx < numHubShards; shardIdx++ {
		for i := 0; i < channelsPerShard; i++ {
			// Generate a channel name that will hash to this specific shard
			channelName := fmt.Sprintf("shard%d_ch%d", shardIdx, i)

			// Keep trying channel names until we get one that hashes to the desired shard
			for index(channelName, numHubShards) != shardIdx {
				channelName = fmt.Sprintf("shard%d_ch%d_%d", shardIdx, i, channelIndex)
				channelIndex++
			}

			// Generate sub ID for this channel
			subID, isNew, err := h.addSub(channelName, subInfo{
				client: client,
				useID:  true,
			})
			require.NoError(t, err)
			require.True(t, isNew, "Should be new channel")

			// Verify the sub ID follows our shard pattern
			expectedShardIndex := int(subID % numHubShards)
			require.Equal(t, shardIdx, expectedShardIndex,
				"Sub ID %d should belong to shard %d, but belongs to shard %d",
				subID, shardIdx, expectedShardIndex)

			idsByShard[shardIdx] = append(idsByShard[shardIdx], subID)
		}
	}

	// Verify each shard generated the expected sequence
	for shardIdx := 0; shardIdx < numHubShards; shardIdx++ {
		ids := idsByShard[shardIdx]
		require.Len(t, ids, channelsPerShard, "Should have exactly %d IDs for shard %d", channelsPerShard, shardIdx)

		// Verify the IDs follow the pattern: shardIndex, shardIndex + numHubShards, shardIndex + 2*numHubShards, etc.
		for i, id := range ids {
			expectedID := int64(shardIdx) + int64(i+1)*numHubShards
			require.Equal(t, expectedID, id,
				"Shard %d, ID %d: expected %d, got %d", shardIdx, i, expectedID, id)
		}
	}
}

func TestCompressedChannelBroadcast(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	// Create properly connected clients
	clientWithID := newTestSubscribedClientV2(t, n, "user1", "test_compressed_channel")
	clientWithoutID := newTestSubscribedClientV2(t, n, "user2", "test_compressed_channel")

	// Remove the default subscriptions and add our own with specific useID settings
	channel := "test_compressed_channel"

	// Remove existing subs and add with our desired settings
	n.hub.removeSub(channel, clientWithID)
	n.hub.removeSub(channel, clientWithoutID)

	// Add subscription with useID=true
	subIDWithID, isNewWithID, err := n.hub.addSub(channel, subInfo{
		client: clientWithID,
		useID:  true,
	})
	require.NoError(t, err)
	require.True(t, isNewWithID)
	require.NotZero(t, subIDWithID)

	// Add subscription with useID=false to same channel
	_, isNewWithoutID, err := n.hub.addSub(channel, subInfo{
		client: clientWithoutID,
		useID:  false,
	})
	require.NoError(t, err)
	require.False(t, isNewWithoutID) // Channel already exists

	// Test 1: broadcastPublication with compressChannel=false should work
	pub := &Publication{Data: []byte(`{"test": "data"}`)}
	err = n.hub.broadcastPublication(channel, StreamPosition{}, pub, nil, nil, ChannelBatchConfig{})
	require.NoError(t, err)

	// Test 2: broadcastPublication with compressChannel=true should work and include Id for useID subscribers
	err = n.hub.broadcastPublication(channel, StreamPosition{}, pub, nil, nil, ChannelBatchConfig{})
	require.NoError(t, err)

	// Test 3: Test that multiple channels get different chanIDs
	channel2 := "test_compressed_channel_2"
	subIDWithID2, isNewWithID2, err := n.hub.addSub(channel2, subInfo{
		client: clientWithID,
		useID:  true,
	})
	require.NoError(t, err)
	require.True(t, isNewWithID2)
	require.NotZero(t, subIDWithID2)
	require.NotEqual(t, subIDWithID, subIDWithID2, "Different channels should have different sub IDs")

	// Test broadcast on second channel with compression
	err = n.hub.broadcastPublication(channel2, StreamPosition{}, pub, nil, nil, ChannelBatchConfig{})
	require.NoError(t, err)
}

func TestNonUseIDSubscribersGetPreviousBehavior(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())

	// Create transport that captures messages
	transportWithoutID := newTestTransport(cancelFn)
	transportWithoutID.sink = make(chan []byte, 100)
	transportWithoutID.setProtocolType(ProtocolTypeJSON)
	transportWithoutID.setProtocolVersion(ProtocolVersion2)
	transportWithoutID.setUnidirectional(false)

	transportWithID := newTestTransport(cancelFn)
	transportWithID.sink = make(chan []byte, 100)
	transportWithID.setProtocolType(ProtocolTypeJSON)
	transportWithID.setProtocolVersion(ProtocolVersion2)
	transportWithID.setUnidirectional(false)

	// Create clients using the pattern from other tests
	clientWithoutID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithoutID, "user1", "test_validation_channel")
	clientWithID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithID, "user2", "test_validation_channel")

	channel := "test_validation_channel"

	// Remove existing subscriptions and add with our specific settings
	n.hub.removeSub(channel, clientWithoutID)
	n.hub.removeSub(channel, clientWithID)

	// Add subscription WITHOUT useID
	_, isNew1, err := n.hub.addSub(channel, subInfo{
		client: clientWithoutID,
		useID:  false, // Important: useID = false
	})
	require.NoError(t, err)
	require.True(t, isNew1)

	// Add subscription WITH useID to same channel
	_, isNew2, err := n.hub.addSub(channel, subInfo{
		client: clientWithID,
		useID:  true, // Important: useID = true
	})
	require.NoError(t, err)
	require.False(t, isNew2) // Channel already exists

	// Clear any buffered messages
	time.Sleep(100 * time.Millisecond) // Let any connection messages settle
	for {
		select {
		case <-transportWithoutID.sink:
		case <-transportWithID.sink:
		default:
			goto cleared
		}
	}
cleared:

	// Test 1: Broadcast with compressChannel=false
	// Both clients should get identical messages without Id field
	pub := &Publication{Data: []byte(`{"test": "data_no_compress"}`)}
	err = n.hub.broadcastPublication(channel, StreamPosition{}, pub, nil, nil, ChannelBatchConfig{})
	require.NoError(t, err)

	// Wait for publication messages
	var msgWithoutID, msgWithID []byte
	select {
	case msgWithoutID = <-transportWithoutID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no message received by client without useID")
	}
	select {
	case msgWithID = <-transportWithID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no message received by client with useID")
	}

	// When compressChannel=false, both should get identical messages
	require.Equal(t, msgWithoutID, msgWithID,
		"Messages should be identical when compressChannel=false:\n  without ID: %s\n  with ID: %s",
		string(msgWithoutID), string(msgWithID))

	// Verify message content
	msgStr := string(msgWithoutID)
	require.NotContains(t, msgStr, `"id":`, "Message should not contain id field when compressChannel=false")
	require.Contains(t, msgStr, "data_no_compress", "Message should contain the data")
	require.Contains(t, msgStr, channel, "Message should contain channel name")

	// Test 2: Broadcast with compressChannel=true
	// Client without useID should get same format as before (no Id field)
	// Client with useID should get Id field
	pub2 := &Publication{Data: []byte(`{"test": "data_with_compress"}`)}
	err = n.hub.broadcastPublication(channel, StreamPosition{}, pub2, nil, nil, ChannelBatchConfig{})
	require.NoError(t, err)

	// Wait for publication messages
	select {
	case msgWithoutID = <-transportWithoutID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no message received by client without useID in test 2")
	}
	select {
	case msgWithID = <-transportWithID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no message received by client with useID in test 2")
	}

	// Parse messages for detailed validation
	msgWithoutIDStr := string(msgWithoutID)
	msgWithIDStr := string(msgWithID)

	t.Logf("Message without useID: %s", msgWithoutIDStr)
	t.Logf("Message with useID: %s", msgWithIDStr)

	// Client without useID should NOT have Id field (previous behavior)
	require.NotContains(t, msgWithoutIDStr, `"id":`,
		"Client without useID should not receive Id field even when compressChannel=true")
	require.Contains(t, msgWithoutIDStr, channel,
		"Client without useID should still receive channel name")
	require.Contains(t, msgWithoutIDStr, "data_with_compress",
		"Client without useID should receive the data")

	// Client with useID should have Id field when compressChannel=true
	require.Contains(t, msgWithIDStr, `"id":`,
		"Client with useID should receive Id field when compressChannel=true")
	require.NotContains(t, msgWithIDStr, `"channel":`,
		"Client with useID should NOT receive channel field - it should be replaced by id")
	require.Contains(t, msgWithIDStr, "data_with_compress",
		"Client with useID should receive the data")

	// Messages should be different when compressChannel=true
	require.NotEqual(t, msgWithoutID, msgWithID,
		"Messages should be different when compressChannel=true due to Id field")
}

func TestDeltaPublicationsWithCompressedChannels(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())

	// Create transport that captures messages
	transportWithoutID := newTestTransport(cancelFn)
	transportWithoutID.sink = make(chan []byte, 100)
	transportWithoutID.setProtocolType(ProtocolTypeJSON)
	transportWithoutID.setProtocolVersion(ProtocolVersion2)
	transportWithoutID.setUnidirectional(false)

	transportWithID := newTestTransport(cancelFn)
	transportWithID.sink = make(chan []byte, 100)
	transportWithID.setProtocolType(ProtocolTypeJSON)
	transportWithID.setProtocolVersion(ProtocolVersion2)
	transportWithID.setUnidirectional(false)

	// Create clients
	clientWithoutID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithoutID, "user1", "test_delta_channel")
	clientWithID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithID, "user2", "test_delta_channel")

	channel := "test_delta_channel"

	// Remove existing subscriptions and add with our specific settings including delta
	n.hub.removeSub(channel, clientWithoutID)
	n.hub.removeSub(channel, clientWithID)

	// Add subscription WITHOUT useID but WITH delta
	_, isNew1, err := n.hub.addSub(channel, subInfo{
		client:    clientWithoutID,
		useID:     false, // Important: useID = false
		deltaType: DeltaTypeFossil,
	})
	require.NoError(t, err)
	require.True(t, isNew1)

	// Add subscription WITH useID and WITH delta to same channel
	_, isNew2, err := n.hub.addSub(channel, subInfo{
		client:    clientWithID,
		useID:     true, // Important: useID = true
		deltaType: DeltaTypeFossil,
	})
	require.NoError(t, err)
	require.False(t, isNew2) // Channel already exists

	// Clear any buffered messages
	time.Sleep(100 * time.Millisecond)
	for {
		select {
		case <-transportWithoutID.sink:
		case <-transportWithID.sink:
		default:
			goto cleared
		}
	}
cleared:

	// Test: Broadcast publication that can use delta - should work with compressed channels
	// Use prevPub to trigger delta compression logic
	pub := &Publication{Data: []byte(`{"test": "delta_with_compress"}`)}
	prevPub := &Publication{Data: []byte(`{"test": "previous_data"}`)}
	err = n.hub.broadcastPublication(channel, StreamPosition{}, pub, prevPub, nil, ChannelBatchConfig{})
	require.NoError(t, err)

	// Wait for messages
	var msgWithoutID, msgWithID []byte
	select {
	case msgWithoutID = <-transportWithoutID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no message received by client without useID")
	}
	select {
	case msgWithID = <-transportWithID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no message received by client with useID")
	}

	msgWithoutIDStr := string(msgWithoutID)
	msgWithIDStr := string(msgWithID)

	t.Logf("Delta message without useID: %s", msgWithoutIDStr)
	t.Logf("Delta message with useID: %s", msgWithIDStr)

	// Both should contain the publication data
	require.Contains(t, msgWithoutIDStr, "delta_with_compress", "Message should contain the data")
	require.Contains(t, msgWithIDStr, "delta_with_compress", "Message should contain the data")

	// Client without useID should have channel field, no id field
	require.Contains(t, msgWithoutIDStr, `"channel":`,
		"Client without useID should receive channel field in delta")
	require.NotContains(t, msgWithoutIDStr, `"id":`,
		"Client without useID should not receive id field in delta")

	// Client with useID should have id field, no channel field
	require.Contains(t, msgWithIDStr, `"id":`,
		"Client with useID should receive id field in delta")
	require.NotContains(t, msgWithIDStr, `"channel":`,
		"Client with useID should not receive channel field in delta - replaced by id")

	// Messages should be different due to channel vs id field
	require.NotEqual(t, msgWithoutID, msgWithID,
		"Delta messages should be different when compressChannel=true due to Id vs Channel field")
}

func TestCompressedJoinMessages(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())

	// Create transport that captures messages
	transportWithoutID := newTestTransport(cancelFn)
	transportWithoutID.sink = make(chan []byte, 100)
	transportWithoutID.setProtocolType(ProtocolTypeJSON)
	transportWithoutID.setProtocolVersion(ProtocolVersion2)
	transportWithoutID.setUnidirectional(false)

	transportWithID := newTestTransport(cancelFn)
	transportWithID.sink = make(chan []byte, 100)
	transportWithID.setProtocolType(ProtocolTypeJSON)
	transportWithID.setProtocolVersion(ProtocolVersion2)
	transportWithID.setUnidirectional(false)

	// Create clients
	clientWithoutID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithoutID, "user1", "test_join_channel")
	clientWithID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithID, "user2", "test_join_channel")

	channel := "test_join_channel"

	// Remove existing subscriptions and add with our specific settings
	n.hub.removeSub(channel, clientWithoutID)
	n.hub.removeSub(channel, clientWithID)

	// Add subscription WITHOUT useID
	_, isNew1, err := n.hub.addSub(channel, subInfo{
		client: clientWithoutID,
		useID:  false, // Important: useID = false
	})
	require.NoError(t, err)
	require.True(t, isNew1)

	// Add subscription WITH useID to same channel
	_, isNew2, err := n.hub.addSub(channel, subInfo{
		client: clientWithID,
		useID:  true, // Important: useID = true
	})
	require.NoError(t, err)
	require.False(t, isNew2) // Channel already exists

	// Enable join/leave broadcasting for both clients
	chCtx1 := clientWithoutID.channels[channel]
	chCtx1.flags |= flagPushJoinLeave
	clientWithoutID.channels[channel] = chCtx1

	chCtx2 := clientWithID.channels[channel]
	chCtx2.flags |= flagPushJoinLeave
	clientWithID.channels[channel] = chCtx2

	// Clear any buffered messages
	time.Sleep(100 * time.Millisecond)
	for {
		select {
		case <-transportWithoutID.sink:
		case <-transportWithID.sink:
		default:
			goto cleared
		}
	}
cleared:

	// Test: Broadcast join with automatic compression per subscriber
	// Client without useID should get channel field
	// Client with useID should get id field instead of channel
	clientInfo := &ClientInfo{
		ClientID: "joining_client",
		UserID:   "user_join",
	}
	err = n.hub.broadcastJoin(channel, clientInfo, ChannelBatchConfig{})
	require.NoError(t, err)

	// Wait for join messages
	var msgWithoutID, msgWithID []byte
	select {
	case msgWithoutID = <-transportWithoutID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no join message received by client without useID")
	}
	select {
	case msgWithID = <-transportWithID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no join message received by client with useID")
	}

	// Messages should be different due to compression
	require.NotEqual(t, msgWithoutID, msgWithID,
		"Join messages should be different due to compression:\n  without ID: %s\n  with ID: %s",
		string(msgWithoutID), string(msgWithID))

	// Verify content for client without useID (should have channel field)
	msgWithoutIDStr := string(msgWithoutID)
	require.NotContains(t, msgWithoutIDStr, `"id":`, "Client without useID should not have id field")
	require.Contains(t, msgWithoutIDStr, `"channel":`, "Client without useID should have channel field")
	require.Contains(t, msgWithoutIDStr, channel, "Join message should contain channel name")
	require.Contains(t, msgWithoutIDStr, "joining_client", "Join message should contain joining client ID")
	require.Contains(t, msgWithoutIDStr, `"join"`, "Message should contain join field")

	// Verify content for client with useID (should have id field, no channel field)
	msgWithIDStr := string(msgWithID)
	require.Contains(t, msgWithIDStr, `"id":`, "Client with useID should have id field")
	require.NotContains(t, msgWithIDStr, `"channel":`, "Client with useID should not have channel field")
	require.Contains(t, msgWithIDStr, "joining_client", "Join message should contain joining client ID")
	require.Contains(t, msgWithIDStr, `"join"`, "Message should contain join field")

	t.Logf("Join message without useID: %s", msgWithoutIDStr)
	t.Logf("Join message with useID: %s", msgWithIDStr)
}

func TestCompressedLeaveMessages(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	ctx, cancelFn := context.WithCancel(context.Background())

	// Create transport that captures messages
	transportWithoutID := newTestTransport(cancelFn)
	transportWithoutID.sink = make(chan []byte, 100)
	transportWithoutID.setProtocolType(ProtocolTypeJSON)
	transportWithoutID.setProtocolVersion(ProtocolVersion2)
	transportWithoutID.setUnidirectional(false)

	transportWithID := newTestTransport(cancelFn)
	transportWithID.sink = make(chan []byte, 100)
	transportWithID.setProtocolType(ProtocolTypeJSON)
	transportWithID.setProtocolVersion(ProtocolVersion2)
	transportWithID.setUnidirectional(false)

	// Create clients
	clientWithoutID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithoutID, "user1", "test_leave_channel")
	clientWithID := newTestSubscribedClientWithTransport(t, ctx, n, transportWithID, "user2", "test_leave_channel")

	channel := "test_leave_channel"

	// Remove existing subscriptions and add with our specific settings
	n.hub.removeSub(channel, clientWithoutID)
	n.hub.removeSub(channel, clientWithID)

	// Add subscription WITHOUT useID
	_, isNew1, err := n.hub.addSub(channel, subInfo{
		client: clientWithoutID,
		useID:  false, // Important: useID = false
	})
	require.NoError(t, err)
	require.True(t, isNew1)

	// Add subscription WITH useID to same channel
	_, isNew2, err := n.hub.addSub(channel, subInfo{
		client: clientWithID,
		useID:  true, // Important: useID = true
	})
	require.NoError(t, err)
	require.False(t, isNew2) // Channel already exists

	// Enable join/leave broadcasting for both clients
	chCtx1 := clientWithoutID.channels[channel]
	chCtx1.flags |= flagPushJoinLeave
	clientWithoutID.channels[channel] = chCtx1

	chCtx2 := clientWithID.channels[channel]
	chCtx2.flags |= flagPushJoinLeave
	clientWithID.channels[channel] = chCtx2

	// Clear any buffered messages
	time.Sleep(100 * time.Millisecond)
	for {
		select {
		case <-transportWithoutID.sink:
		case <-transportWithID.sink:
		default:
			goto cleared
		}
	}
cleared:

	// Test: Broadcast leave with automatic compression per subscriber
	// Client without useID should get channel field
	// Client with useID should get id field instead of channel
	clientInfo := &ClientInfo{
		ClientID: "leaving_client",
		UserID:   "user_leave",
	}
	err = n.hub.broadcastLeave(channel, clientInfo, ChannelBatchConfig{})
	require.NoError(t, err)

	// Wait for leave messages
	var msgWithoutID, msgWithID []byte
	select {
	case msgWithoutID = <-transportWithoutID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no leave message received by client without useID")
	}
	select {
	case msgWithID = <-transportWithID.sink:
	case <-time.After(2 * time.Second):
		t.Fatal("no leave message received by client with useID")
	}

	// Messages should be different due to compression
	require.NotEqual(t, msgWithoutID, msgWithID,
		"Leave messages should be different due to compression:\n  without ID: %s\n  with ID: %s",
		string(msgWithoutID), string(msgWithID))

	// Verify content for client without useID (should have channel field)
	msgWithoutIDStr := string(msgWithoutID)
	require.NotContains(t, msgWithoutIDStr, `"id":`, "Client without useID should not have id field")
	require.Contains(t, msgWithoutIDStr, `"channel":`, "Client without useID should have channel field")
	require.Contains(t, msgWithoutIDStr, channel, "Leave message should contain channel name")
	require.Contains(t, msgWithoutIDStr, "leaving_client", "Leave message should contain leaving client ID")
	require.Contains(t, msgWithoutIDStr, `"leave"`, "Message should contain leave field")

	// Verify content for client with useID (should have id field, no channel field)
	msgWithIDStr := string(msgWithID)
	require.Contains(t, msgWithIDStr, `"id":`, "Client with useID should have id field")
	require.NotContains(t, msgWithIDStr, `"channel":`, "Client with useID should not have channel field")
	require.Contains(t, msgWithIDStr, "leaving_client", "Leave message should contain leaving client ID")
	require.Contains(t, msgWithIDStr, `"leave"`, "Message should contain leave field")

	t.Logf("Leave message without useID: %s", msgWithoutIDStr)
	t.Logf("Leave message with useID: %s", msgWithIDStr)
}
