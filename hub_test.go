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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

type testTransport struct {
	mu         sync.Mutex
	sink       chan []byte
	closed     bool
	closeCh    chan struct{}
	disconnect *Disconnect
	protoType  ProtocolType
}

func newTestTransport() *testTransport {
	return &testTransport{
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
	close(t.closeCh)
	return nil
}

func TestHub(t *testing.T) {
	h := newHub()
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	c.user = "test"
	_ = h.add(c)
	assert.Equal(t, len(h.users), 1)
	conns := h.userConnections("test")
	assert.Equal(t, 1, len(conns))
	assert.Equal(t, 1, h.NumClients())
	assert.Equal(t, 1, h.NumUsers())
	_ = h.remove(c)
	assert.Equal(t, len(h.users), 0)
	assert.Equal(t, 1, len(conns))
}

func TestHubUnsubscribe(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	// No such user.
	err = node.hub.unsubscribe("1", "test")
	require.NoError(t, err)
	// Subscribed user.
	err = node.hub.unsubscribe("42", "test")
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.Equal(t, "{\"result\":{\"type\":3,\"channel\":\"test\",\"data\":{}}}\n", string(data))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubDisconnect(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	// No such user.
	err = node.hub.disconnect("1", false)
	require.NoError(t, err)
	// Subscribed user.
	err = node.hub.disconnect("42", false)
	require.NoError(t, err)
	select {
	case <-transport.closeCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubBroadcastPublicationJSON(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	err = node.hub.broadcastPublication(
		"test", &protocol.Publication{Data: []byte(`{"data": "broadcasted_data"}`)}, &ChannelOptions{})
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.True(t, strings.Contains(string(data), "broadcasted_data"))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubBroadcastPublicationProtobuf(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.protoType = ProtocolTypeProtobuf
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	err = node.hub.broadcastPublication(
		"test", &protocol.Publication{Data: []byte(`{"data": "broadcasted_data"}`)}, &ChannelOptions{})
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.True(t, strings.Contains(string(data), "broadcasted_data"))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubBroadcastJoinJSON(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	transport.protoType = ProtocolTypeProtobuf
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	err = node.hub.broadcastJoin(
		"test", &protocol.Join{Info: protocol.ClientInfo{Client: "broadcast_client"}})
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.True(t, strings.Contains(string(data), "broadcast_client"))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubBroadcastJoinProtobuf(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	err = node.hub.broadcastJoin(
		"test", &protocol.Join{Info: protocol.ClientInfo{Client: "broadcast_client"}})
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.True(t, strings.Contains(string(data), "broadcast_client"))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubBroadcastLeaveJSON(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	transport.protoType = ProtocolTypeProtobuf
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	err = node.hub.broadcastLeave(
		"test", &protocol.Leave{Info: protocol.ClientInfo{Client: "broadcast_client"}})
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.True(t, strings.Contains(string(data), "broadcast_client"))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubBroadcastLeaveProtobuf(t *testing.T) {
	node := nodeWithMemoryEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	transport := newTestTransport()
	transport.sink = make(chan []byte, 100)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, err := newClient(newCtx, node, transport)
	connectClient(t, client)
	subscribeClient(t, client, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(node.hub.users), 1)
	err = node.hub.broadcastLeave(
		"test", &protocol.Leave{Info: protocol.ClientInfo{Client: "broadcast_client"}})
	require.NoError(t, err)
	select {
	case data := <-transport.sink:
		require.True(t, strings.Contains(string(data), "broadcast_client"))
	case <-time.After(2 * time.Second):
		t.Fatal("no data in sink")
	}
}

func TestHubShutdown(t *testing.T) {
	h := newHub()
	err := h.shutdown(context.Background())
	assert.NoError(t, err)
	h = newHub()
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	_ = h.add(c)
	err = h.shutdown(context.Background())
	assert.NoError(t, err)
}

func TestHubSubscriptions(t *testing.T) {
	h := newHub()
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	_, _ = h.addSub("test1", c)
	_, _ = h.addSub("test2", c)
	assert.Equal(t, 2, h.NumChannels())
	var channels []string
	channels = append(channels, h.Channels()...)
	assert.True(t, stringInSlice("test1", channels))
	assert.True(t, stringInSlice("test2", channels))
	assert.True(t, h.NumSubscribers("test1") > 0)
	assert.True(t, h.NumSubscribers("test2") > 0)
	_, _ = h.removeSub("test1", c)
	_, _ = h.removeSub("test2", c)
	assert.Equal(t, h.NumChannels(), 0)
	assert.False(t, h.NumSubscribers("test1") > 0)
	assert.False(t, h.NumSubscribers("test2") > 0)
}

func TestPreparedReply(t *testing.T) {
	reply := protocol.Reply{}
	preparedReply := prepared.NewReply(&reply, protocol.TypeJSON)
	data := preparedReply.Data()
	assert.NotNil(t, data)
}

func TestUserConnections(t *testing.T) {
	h := newHub()
	c, err := newClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	_ = h.add(c)

	connections := h.userConnections(c.UserID())
	assert.Equal(t, h.conns, connections)
}
