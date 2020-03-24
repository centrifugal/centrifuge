package centrifuge

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/assert"

	"github.com/centrifugal/centrifuge/internal/prepared"
)

type testTransport struct {
	mu         sync.Mutex
	sink       chan []byte
	closed     bool
	disconnect *Disconnect
	protoType  ProtocolType
}

func newTestTransport() *testTransport {
	return &testTransport{
		protoType: ProtocolTypeJSON,
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
	if t.sink != nil {
		t.sink <- data
	}
	return nil
}

func (t *testTransport) Name() string {
	return "test_transport"
}

func (t *testTransport) Protocol() ProtocolType {
	return protocol.TypeJSON
}

func (t *testTransport) Encoding() EncodingType {
	return protocol.EncodingTypeJSON
}

func (t *testTransport) Close(disconnect *Disconnect) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.disconnect = disconnect
	t.closed = true
	return nil
}

func TestHub(t *testing.T) {
	h := newHub()
	c, err := NewClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	c.user = "test"
	h.add(c)
	assert.Equal(t, len(h.users), 1)
	conns := h.userConnections("test")
	assert.Equal(t, 1, len(conns))
	assert.Equal(t, 1, h.NumClients())
	assert.Equal(t, 1, h.NumUsers())
	_ = h.remove(c)
	assert.Equal(t, len(h.users), 0)
	assert.Equal(t, 1, len(conns))
}

func TestHubShutdown(t *testing.T) {
	h := newHub()
	err := h.shutdown(context.Background())
	assert.NoError(t, err)
	h = newHub()
	c, err := NewClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	h.add(c)
	err = h.shutdown(context.Background())
	assert.NoError(t, err)
}

func TestHubSubscriptions(t *testing.T) {
	h := newHub()
	c, err := NewClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	h.addSub("test1", c)
	h.addSub("test2", c)
	assert.Equal(t, 2, h.NumChannels())
	channels := []string{}
	channels = append(channels, h.Channels()...)
	assert.True(t, stringInSlice("test1", channels))
	assert.True(t, stringInSlice("test2", channels))
	assert.True(t, h.NumSubscribers("test1") > 0)
	assert.True(t, h.NumSubscribers("test2") > 0)
	h.removeSub("test1", c)
	h.removeSub("test2", c)
	assert.Equal(t, h.NumChannels(), 0)
	assert.False(t, h.NumSubscribers("test1") > 0)
	assert.False(t, h.NumSubscribers("test2") > 0)
}

func TestPreparedReply(t *testing.T) {
	reply := protocol.Reply{}
	prepared := prepared.NewReply(&reply, protocol.TypeJSON)
	data := prepared.Data()
	assert.NotNil(t, data)
}

func TestUserConnections(t *testing.T) {
	h := newHub()
	c, err := NewClient(context.Background(), nodeWithMemoryEngine(), newTestTransport())
	assert.NoError(t, err)
	h.add(c)

	connections := h.userConnections(c.UserID())
	assert.Equal(t, h.conns, connections)
}
