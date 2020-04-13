package centrifuge

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/controlproto"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

type TestEngine struct {
	publishCount        int32
	publishJoinCount    int32
	publishLeaveCount   int32
	publishControlCount int32
}

func NewTestEngine() *TestEngine {
	return &TestEngine{}
}

func (e *TestEngine) Run(_ BrokerEventHandler) error {
	return nil
}

func (e *TestEngine) Publish(_ string, _ *Publication, _ *ChannelOptions) error {
	atomic.AddInt32(&e.publishCount, 1)
	return nil
}

func (e *TestEngine) PublishJoin(_ string, _ *protocol.Join, _ *ChannelOptions) error {
	atomic.AddInt32(&e.publishJoinCount, 1)
	return nil
}

func (e *TestEngine) PublishLeave(_ string, _ *protocol.Leave, _ *ChannelOptions) error {
	atomic.AddInt32(&e.publishLeaveCount, 1)
	return nil
}

func (e *TestEngine) PublishControl(_ []byte) error {
	atomic.AddInt32(&e.publishControlCount, 1)
	return nil
}

func (e *TestEngine) Subscribe(_ string) error {
	return nil
}

func (e *TestEngine) Unsubscribe(_ string) error {
	return nil
}

func (e *TestEngine) AddPresence(_ string, _ string, _ *ClientInfo, _ time.Duration) error {
	return nil
}

func (e *TestEngine) RemovePresence(_ string, _ string) error {
	return nil
}

func (e *TestEngine) Presence(_ string) (map[string]*ClientInfo, error) {
	return map[string]*protocol.ClientInfo{}, nil
}

func (e *TestEngine) PresenceStats(_ string) (PresenceStats, error) {
	return PresenceStats{}, nil
}

func (e *TestEngine) History(_ string, _ HistoryFilter) ([]*protocol.Publication, StreamPosition, error) {
	return []*protocol.Publication{}, StreamPosition{}, nil
}

func (e *TestEngine) AddHistory(_ string, pub *protocol.Publication, _ *ChannelOptions) (*Publication, error) {
	return pub, nil
}

func (e *TestEngine) RemoveHistory(_ string) error {
	return nil
}

func (e *TestEngine) Channels() ([]string, error) {
	return []string{}, nil
}

func nodeWithTestEngine() *Node {
	c := DefaultConfig
	n, err := New(c)
	if err != nil {
		panic(err)
	}
	n.SetEngine(NewTestEngine())
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func nodeWithMemoryEngine() *Node {
	c := DefaultConfig
	n, err := New(c)
	if err != nil {
		panic(err)
	}
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func TestUserAllowed(t *testing.T) {
	node := nodeWithTestEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	require.True(t, node.userAllowed("channel#1", "1"))
	require.True(t, node.userAllowed("channel", "1"))
	require.False(t, node.userAllowed("channel#1", "2"))
	require.True(t, node.userAllowed("channel#1,2", "1"))
	require.True(t, node.userAllowed("channel#1,2", "2"))
	require.False(t, node.userAllowed("channel#1,2", "3"))
}

func TestSetConfig(t *testing.T) {
	node := nodeWithTestEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()
	err := node.Reload(DefaultConfig)
	require.NoError(t, err)
}

func TestNodeRegistry(t *testing.T) {
	registry := newNodeRegistry("node1")
	nodeInfo1 := controlproto.Node{UID: "node1"}
	nodeInfo2 := controlproto.Node{UID: "node2"}
	registry.add(&nodeInfo1)
	registry.add(&nodeInfo2)
	require.Equal(t, 2, len(registry.list()))
	info := registry.get("node1")
	require.Equal(t, "node1", info.UID)
	registry.clean(10 * time.Second)
	time.Sleep(2 * time.Second)
	registry.clean(time.Second)
	// Current node info should still be in node registry - we never delete it.
	require.Equal(t, 1, len(registry.list()))
}

var testPayload = map[string]interface{}{
	"_id":        "5adece493c1a23736b037c52",
	"index":      2,
	"guid":       "478a00f4-19b1-4567-8097-013b8cc846b8",
	"isActive":   false,
	"balance":    "$2,199.02",
	"picture":    "http://placehold.it/32x32",
	"age":        25,
	"eyeColor":   "blue",
	"name":       "Swanson Walker",
	"gender":     "male",
	"company":    "SHADEASE",
	"email":      "swansonwalker@shadease.com",
	"phone":      "+1 (885) 410-3991",
	"address":    "768 Paerdegat Avenue, Gouglersville, Oklahoma, 5380",
	"registered": "2016-01-24T07:40:09 -03:00",
	"latitude":   -71.336378,
	"longitude":  -28.155956,
	"tags": []string{
		"magna",
		"nostrud",
		"irure",
		"aliquip",
		"culpa",
		"sint",
	},
	"greeting":      "Hello, Swanson Walker! You have 9 unread messages.",
	"favoriteFruit": "apple",
}

func BenchmarkNodePublishWithNoopEngine(b *testing.B) {
	node := nodeWithTestEngine()

	payload, err := json.Marshal(testPayload)
	if err != nil {
		panic(err.Error())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := node.Publish("bench", payload)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = node.Shutdown(context.Background())
}

func newFakeConn(b testing.TB, node *Node, channel string, protoType ProtocolType, sink chan []byte) {
	transport := newTestTransport()
	transport.setProtocolType(protoType)
	transport.setSink(sink)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)
	connectClient(b, client)
	var replies []*protocol.Reply
	rw := testReplyWriter(&replies)
	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: channel,
	}, rw, false)
	require.Nil(b, subCtx.disconnect)
}

func newFakeConnJSON(b testing.TB, node *Node, channel string, sink chan []byte) {
	newFakeConn(b, node, channel, ProtocolTypeJSON, sink)
}

func newFakeConnProtobuf(b testing.TB, node *Node, channel string, sink chan []byte) {
	newFakeConn(b, node, channel, ProtocolTypeProtobuf, sink)
}

func BenchmarkBroadcastMemoryEngine(b *testing.B) {
	benchmarks := []struct {
		name           string
		getFakeConn    func(b testing.TB, n *Node, channel string, sink chan []byte)
		numSubscribers int
	}{
		{"JSON", newFakeConnJSON, 1},
		{"Protobuf", newFakeConnProtobuf, 1},
		{"JSON", newFakeConnJSON, 10000},
		{"Protobuf", newFakeConnProtobuf, 10000},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%s_%d_subscribers", bm.name, bm.numSubscribers), func(b *testing.B) {
			n := nodeWithMemoryEngine()
			c := n.Config()
			c.ClientInsecure = true
			err := n.Reload(c)
			if err != nil {
				b.Fatal(err)
			}
			payload := []byte(`{"input": "test"}`)
			sink := make(chan []byte, bm.numSubscribers)
			for i := 0; i < bm.numSubscribers; i++ {
				bm.getFakeConn(b, n, "test", sink)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := n.Publish("test", payload)
				if err != nil {
					panic(err)
				}
				for j := 0; j < bm.numSubscribers; j++ {
					<-sink
				}
			}
			b.StopTimer()
			_ = n.Shutdown(context.Background())
			b.ReportAllocs()
		})
	}
}

func TestNodeHistoryIteration(t *testing.T) {
	numMessages := 10000

	e := testMemoryEngine()
	conf := e.node.Config()
	conf.HistorySize = numMessages
	conf.HistoryLifetime = 60
	conf.HistoryRecover = true
	err := e.node.Reload(conf)
	require.NoError(t, err)

	channel := "test"

	for i := 1; i <= numMessages; i++ {
		err := e.node.Publish(channel, []byte(`{}`))
		require.NoError(t, err)
	}

	pubs, _ := e.node.History(channel)
	require.Equal(t, numMessages, len(pubs))

	var n int
	var seq uint32 = 0

	for {
		pubs, _, err := e.History(channel, HistoryFilter{
			Limit: 10,
			Since: &StreamPosition{Seq: seq, Gen: 0, Epoch: ""},
		})
		seq += 10
		if err != nil {
			t.Fatal(err)
		}
		if len(pubs) == 0 {
			break
		}
		n += len(pubs)
	}
	require.Equal(t, numMessages, n)
}
