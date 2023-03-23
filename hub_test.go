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
	h := newHub(nil)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	c.user = "test"
	err = h.remove(c)
	require.NoError(t, err)
	err = h.add(c)
	require.NoError(t, err)
	conns := h.UserConnections("test")
	require.Equal(t, 1, len(conns))
	require.Equal(t, 1, h.NumClients())
	require.Equal(t, 1, h.NumUsers())

	validUID := c.uid
	c.uid = "invalid"
	err = h.remove(c)
	require.NoError(t, err)
	require.Len(t, h.UserConnections("test"), 1)

	c.uid = validUID
	err = h.remove(c)
	require.NoError(t, err)
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
			err := n.hub.broadcastJoin("not_test_channel", &ClientInfo{ClientID: "broadcast_client"})
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastJoin("test_channel", &ClientInfo{ClientID: "broadcast_client"})
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
			err := n.hub.broadcastLeave("not_test_channel", &ClientInfo{ClientID: "broadcast_client"})
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastLeave("test_channel", &ClientInfo{ClientID: "broadcast_client"})
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
	h := newHub(nil)
	err := h.shutdown(context.Background())
	require.NoError(t, err)
	h = newHub(nil)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	_ = h.add(c)

	err = h.shutdown(context.Background())
	require.NoError(t, err)

	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = h.shutdown(ctxCanceled)
	require.EqualError(t, err, "context canceled")
}

func TestHubSubscriptions(t *testing.T) {
	h := newHub(nil)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)

	_, _ = h.addSub("test1", c)
	_, _ = h.addSub("test2", c)
	require.Equal(t, 2, h.NumChannels())
	require.Contains(t, h.Channels(), "test1")
	require.Contains(t, h.Channels(), "test2")
	require.NotZero(t, h.NumSubscribers("test1"))
	require.NotZero(t, h.NumSubscribers("test2"))

	// Not exited sub.
	removed, err := h.removeSub("not_existed", c)
	require.NoError(t, err)
	require.True(t, removed)

	// Exited sub with invalid uid.
	validUID := c.uid
	c.uid = "invalid"
	removed, err = h.removeSub("test1", c)
	require.NoError(t, err)
	require.True(t, removed)
	c.uid = validUID

	// Exited sub.
	removed, err = h.removeSub("test1", c)
	require.NoError(t, err)
	require.True(t, removed)

	// Exited sub.
	removed, err = h.removeSub("test2", c)
	require.NoError(t, err)
	require.True(t, removed)

	require.Equal(t, h.NumChannels(), 0)
	require.Zero(t, h.NumSubscribers("test1"))
	require.Zero(t, h.NumSubscribers("test2"))
}

func TestUserConnections(t *testing.T) {
	h := newHub(nil)
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	_ = h.add(c)

	connections := h.UserConnections(c.UserID())
	require.Equal(t, h.connShards[index(c.UserID(), numHubShards)].conns, connections)
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
			_ = n.hub.add(c)
			for _, ch := range channels {
				_, _ = n.hub.addSub(ch, c)
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
		_ = n.hub.add(c)
		clients = append(clients, c)
		for _, ch := range channels {
			_, _ = n.hub.addSub(ch, c)
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
			_, _ = n.hub.addSub(channels[i%numChannels], clients[i%numClients])
			wg.Wait()
		}
	})
}

var broadcastBenches = []struct {
	NumSubscribers int
}{
	{1000},
	{10000},
	{100000},
}

// BenchmarkHub_MassiveBroadcast allows estimating time to broadcast
// a single message to many subscribers inside one channel.
func BenchmarkHub_MassiveBroadcast(b *testing.B) {
	pub := &Publication{Data: []byte(`{"input": "test"}`)}
	streamPosition := StreamPosition{}

	for _, tt := range broadcastBenches {
		numSubscribers := tt.NumSubscribers
		b.Run(fmt.Sprintf("%d", numSubscribers), func(b *testing.B) {
			b.ReportAllocs()
			n := defaultTestNodeBenchmark(b)

			numChannels := 64
			channels := make([]string, 0, numChannels)

			for i := 0; i < numChannels; i++ {
				channels = append(channels, "broadcast"+strconv.Itoa(i))
			}

			sink := make(chan []byte, 1024)

			for i := 0; i < numSubscribers; i++ {
				t := newTestTransport(func() {})
				t.setSink(sink)
				c := newTestConnectedClientWithTransport(b, context.Background(), n, t, "12")
				_ = n.hub.add(c)
				for _, ch := range channels {
					_, _ = n.hub.addSub(ch, c)
				}
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
				_ = n.hub.BroadcastPublication(channels[i%numChannels], pub, streamPosition)
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
		})
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
		})
		require.NoError(t, err)
		waitWithTimeout(t, done)
	}

	t.Run("protocol_v2", func(t *testing.T) {
		client := newTestSubscribedClientV2(t, n, "42", "test_channel")
		testFunc(client)
	})
}
