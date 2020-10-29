package centrifuge

import (
	"context"
	"io"
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
	return "test_transport"
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
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport(func() {}))
	require.NoError(t, err)
	c.user = "test"
	err = h.remove(c)
	require.NoError(t, err)
	err = h.add(c)
	require.NoError(t, err)
	require.Equal(t, len(h.users), 1)

	conns := h.userConnections("test")
	require.Equal(t, 1, len(conns))
	require.Equal(t, 1, h.NumClients())
	require.Equal(t, 1, h.NumUsers())

	validUID := c.uid
	c.uid = "invalid"
	err = h.remove(c)
	require.NoError(t, err)
	require.Len(t, h.users, 1)
	require.Len(t, conns, 1)

	c.uid = validUID
	err = h.remove(c)
	require.NoError(t, err)
	require.Equal(t, len(h.users), 0)
	require.Equal(t, 1, len(conns))
}

func TestHubUnsubscribe(t *testing.T) {
	n := nodeWithMemoryEngine()
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
	require.NotContains(t, n.hub.subs, "test_channel")
}

func TestHubDisconnect(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
	})

	client := newTestSubscribedClient(t, n, "42", "test_channel")
	clientWithReconnect := newTestSubscribedClient(t, n, "24", "test_channel_reconnect")
	require.Len(t, n.hub.conns, 2)
	require.Len(t, n.hub.users, 2)
	require.Len(t, n.hub.subs, 2)

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
	err := n.hub.disconnect("1", false)
	require.NoError(t, err)

	// Disconnect subscribed user.
	err = n.hub.disconnect("42", false)
	require.NoError(t, err)
	select {
	case <-client.transport.(*testTransport).closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
	require.NotContains(t, n.hub.conns, client.uid)
	require.NotContains(t, n.hub.users, "42")
	require.NotContains(t, n.hub.subs, "test_channel")

	// Disconnect subscribed user with reconnect.
	err = n.hub.disconnect("24", true)
	require.NoError(t, err)
	select {
	case <-clientWithReconnect.transport.(*testTransport).closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
	require.NotContains(t, n.hub.conns, clientWithReconnect.uid)
	require.NotContains(t, n.hub.users, "24")
	require.NotContains(t, n.hub.subs, "test_channel_reconnect")

	wg.Wait()

	require.Len(t, n.hub.conns, 0)
	require.Len(t, n.hub.users, 0)
	require.Len(t, n.hub.subs, 0)
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
			n := nodeWithMemoryEngine()
			defer func() { _ = n.Shutdown(context.Background()) }()

			client := newTestSubscribedClient(t, n, "42", "test_channel")
			transport := client.transport.(*testTransport)
			transport.sink = make(chan []byte, 100)
			transport.protoType = tc.protocolType

			// Broadcast to not existed channel.
			err := n.hub.broadcastPublication(
				"not_test_channel",
				&protocol.Publication{Data: []byte(`{"data": "broadcast_data"}`)},
			)
			require.NoError(t, err)

			// Broadcast to existed channel.
			err = n.hub.broadcastPublication(
				"test_channel",
				&protocol.Publication{Data: []byte(`{"data": "broadcast_data"}`)},
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
			n := nodeWithMemoryEngine()
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
			n := nodeWithMemoryEngine()
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
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport(func() {}))
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
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport(func() {}))
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
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport(func() {}))
	require.NoError(t, err)
	_ = h.add(c)

	connections := h.userConnections(c.UserID())
	require.Equal(t, h.conns, connections)
}
