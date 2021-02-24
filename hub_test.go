package centrifuge

import (
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/prepared"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

type testTransport struct {
	mu         sync.Mutex
	sink       chan []byte
	closed     bool
	closeCh    chan struct{}
	disconnect *Disconnect
	protoType  ProtocolType
	cancelFn   func()
}

func newTestTransport(cancelFn func()) *testTransport {
	return &testTransport{
		cancelFn:  cancelFn,
		protoType: ProtocolTypeJSON,
		closeCh:   make(chan struct{}),
	}
}

func (t *testTransport) setProtocolType(pType ProtocolType) {
	t.protoType = pType
}

func (t *testTransport) setSink(sink chan []byte) {
	t.sink = sink
}

func (t *testTransport) Write(data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.EOF
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	if t.sink != nil {
		t.sink <- dataCopy
	}
	return nil
}

func (t *testTransport) Name() string {
	return transportWebsocket
}

func (t *testTransport) Protocol() ProtocolType {
	return t.protoType
}

func (t *testTransport) Encoding() EncodingType {
	return EncodingTypeJSON
}

func (t *testTransport) Close(disconnect *Disconnect) error {
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
	h := newHub()
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	c.user = "test"
	err = h.remove(c)
	require.NoError(t, err)
	err = h.add(c)
	require.NoError(t, err)
	conns := h.userConnections("test")
	require.Equal(t, 1, len(conns))
	require.Equal(t, 1, h.NumClients())
	require.Equal(t, 1, h.NumUsers())

	validUID := c.uid
	c.uid = "invalid"
	err = h.remove(c)
	require.NoError(t, err)
	require.Len(t, h.userConnections("test"), 1)

	c.uid = validUID
	err = h.remove(c)
	require.NoError(t, err)
	require.Len(t, h.userConnections("test"), 0)
}

func TestHubUnsubscribe(t *testing.T) {
	n := defaultTestNode()
	defer func() { _ = n.Shutdown(context.Background()) }()

	client := newTestSubscribedClient(t, n, "42", "test_channel")
	transport := client.transport.(*testTransport)
	transport.sink = make(chan []byte, 100)

	// Unsubscribe not existed user.
	err := n.hub.unsubscribe("1", "test_channel")
	require.NoError(t, err)

	// Unsubscribe subscribed user.
	err = n.hub.unsubscribe("42", "test_channel")
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.Equal(t, "{\"result\":{\"type\":3,\"channel\":\"test_channel\",\"data\":{}}}\n", string(data))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
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

	client := newTestSubscribedClient(t, n, "42", "test_channel")
	clientWithReconnect := newTestSubscribedClient(t, n, "24", "test_channel_reconnect")
	require.Len(t, n.hub.userConnections("42"), 1)
	require.Len(t, n.hub.userConnections("24"), 1)
	require.Equal(t, 1, n.hub.NumSubscribers("test_channel"))
	require.Equal(t, 1, n.hub.NumSubscribers("test_channel_reconnect"))

	wg := sync.WaitGroup{}
	wg.Add(2)

	client.eventHub.disconnectHandler = func(e DisconnectEvent) {
		defer wg.Done()
		require.False(t, e.Disconnect.Reconnect)
	}

	clientWithReconnect.eventHub.disconnectHandler = func(e DisconnectEvent) {
		defer wg.Done()
		require.True(t, e.Disconnect.Reconnect)
	}

	// Disconnect not existed user.
	err := n.hub.disconnect("1", DisconnectForceNoReconnect, nil)
	require.NoError(t, err)

	// Disconnect subscribed user.
	err = n.hub.disconnect("42", DisconnectForceNoReconnect, nil)
	require.NoError(t, err)
	select {
	case <-client.transport.(*testTransport).closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
	require.Len(t, n.hub.userConnections("42"), 0)
	require.Equal(t, 0, n.hub.NumSubscribers("test_channel"))

	// Disconnect subscribed user with reconnect.
	err = n.hub.disconnect("24", DisconnectForceReconnect, nil)
	require.NoError(t, err)
	select {
	case <-clientWithReconnect.transport.(*testTransport).closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
	require.Len(t, n.hub.userConnections("24"), 0)
	require.Equal(t, 0, n.hub.NumSubscribers("test_channel_reconnect"))

	wg.Wait()

	require.Len(t, n.hub.userConnections("24"), 0)
	require.Len(t, n.hub.userConnections("42"), 0)
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

	client := newTestSubscribedClient(t, n, "12", "test_channel")
	clientToKeep := newTestSubscribedClient(t, n, "12", "test_channel")

	require.Len(t, n.hub.userConnections("12"), 2)
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
	err := n.hub.disconnect("12", DisconnectConnectionLimit, whitelist)
	require.NoError(t, err)

	select {
	case <-shouldBeClosed:
		select {
		case <-shouldNotBeClosed:
			require.Fail(t, "client should not be disconnected")
		case <-time.After(time.Second):
			require.Len(t, n.hub.userConnections("12"), 1)
			require.Equal(t, 1, n.hub.NumSubscribers("test_channel"))
		}
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for channel close")
	}
}

func TestHubBroadcastPublication(t *testing.T) {
	tcs := []struct {
		name         string
		protocolType ProtocolType
	}{
		{name: "JSON", protocolType: ProtocolTypeJSON},
		{name: "Protobuf", protocolType: ProtocolTypeProtobuf},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := defaultTestNode()
			defer func() { _ = n.Shutdown(context.Background()) }()

			client := newTestSubscribedClient(t, n, "42", "test_channel")
			transport := client.transport.(*testTransport)
			transport.sink = make(chan []byte, 100)
			transport.protoType = tc.protocolType

			// Broadcast to not existed channel.
			err := n.hub.broadcastPublication(
				"not_test_channel",
				&protocol.Publication{Data: []byte(`{"data": "broadcast_data"}`)},
				StreamPosition{},
			)
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastPublication(
				"test_channel",
				&protocol.Publication{Data: []byte(`{"data": "broadcast_data"}`)},
				StreamPosition{},
			)
			require.NoError(t, err)
			select {
			case data := <-transport.sink:
				require.True(t, strings.Contains(string(data), "broadcast_data"))
			case <-time.After(2 * time.Second):
				t.Fatal("no data in sink")
			}
		})
	}
}

func TestHubBroadcastJoin(t *testing.T) {
	tcs := []struct {
		name         string
		protocolType ProtocolType
	}{
		{name: "JSON", protocolType: ProtocolTypeJSON},
		{name: "Protobuf", protocolType: ProtocolTypeProtobuf},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := defaultTestNode()
			defer func() { _ = n.Shutdown(context.Background()) }()

			client := newTestSubscribedClient(t, n, "42", "test_channel")
			transport := client.transport.(*testTransport)
			transport.sink = make(chan []byte, 100)
			transport.protoType = tc.protocolType

			// Broadcast to not existed channel.
			err := n.hub.broadcastJoin("not_test_channel", &protocol.Join{
				Info: protocol.ClientInfo{Client: "broadcast_client"},
			})
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastJoin("test_channel", &protocol.Join{
				Info: protocol.ClientInfo{Client: "broadcast_client"},
			})
			require.NoError(t, err)
			select {
			case data := <-transport.sink:
				require.True(t, strings.Contains(string(data), "broadcast_client"))
			case <-time.After(2 * time.Second):
				t.Fatal("no data in sink")
			}
		})
	}
}

func TestHubBroadcastLeave(t *testing.T) {
	tcs := []struct {
		name         string
		protocolType ProtocolType
	}{
		{name: "JSON", protocolType: ProtocolTypeJSON},
		{name: "Protobuf", protocolType: ProtocolTypeProtobuf},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			n := defaultTestNode()
			defer func() { _ = n.Shutdown(context.Background()) }()

			client := newTestSubscribedClient(t, n, "42", "test_channel")
			transport := client.transport.(*testTransport)
			transport.sink = make(chan []byte, 100)
			transport.protoType = tc.protocolType

			// Broadcast to not existed channel.
			err := n.hub.broadcastLeave("not_test_channel", &protocol.Leave{
				Info: protocol.ClientInfo{Client: "broadcast_client"},
			})
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastLeave("test_channel", &protocol.Leave{
				Info: protocol.ClientInfo{Client: "broadcast_client"},
			})
			require.NoError(t, err)
			select {
			case data := <-transport.sink:
				require.Contains(t, string(data), "broadcast_client")
			case <-time.After(2 * time.Second):
				t.Fatal("no data in sink")
			}
		})
	}
}

func TestHubShutdown(t *testing.T) {
	h := newHub()
	err := h.shutdown(context.Background())
	require.NoError(t, err)
	h = newHub()
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
	h := newHub()
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

func TestPreparedReply(t *testing.T) {
	reply := protocol.Reply{}
	preparedReply := prepared.NewReply(&reply, protocol.TypeJSON)
	data := preparedReply.Data()
	require.NotNil(t, data)
}

func TestUserConnections(t *testing.T) {
	h := newHub()
	c, err := newClient(context.Background(), defaultTestNode(), newTestTransport(func() {}))
	require.NoError(t, err)
	_ = h.add(c)

	connections := h.userConnections(c.UserID())
	require.Equal(t, h.connShards[index(c.UserID(), numHubShards)].conns, connections)
}

// This benchmark allows to estimate the benefit from Hub sharding.
// As we have a broadcasting goroutine here it's not very useful to look at
// total allocations here - it's better to look at operation time.
func BenchmarkHub_Contention(b *testing.B) {
	h := newHub()

	numClients := 100
	numChannels := 128

	n := defaultTestNodeBenchmark(b)

	var clients []*Client
	var channels []string

	for i := 0; i < numChannels; i++ {
		channels = append(channels, "ch"+strconv.Itoa(i))
	}

	for i := 0; i < numClients; i++ {
		c, err := newClient(context.Background(), n, newTestTransport(func() {}))
		require.NoError(b, err)
		_ = h.add(c)
		clients = append(clients, c)
		for _, ch := range channels {
			_, _ = h.addSub(ch, c)
		}
	}

	pub := &protocol.Publication{
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
				_ = h.broadcastPublication(channels[(i+numChannels/2)%numChannels], pub, streamPosition)
			}()
			_, _ = h.addSub(channels[i%numChannels], clients[i%numClients])
			wg.Wait()
		}
	})
}
