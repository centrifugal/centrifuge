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
	"github.com/stretchr/testify/assert"
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

func (e *TestEngine) Run(h BrokerEventHandler) error {
	return nil
}

func (e *TestEngine) Shutdown(ctx context.Context) error {
	return nil
}

func (e *TestEngine) Publish(ch string, pub *Publication, opts *ChannelOptions) error {
	atomic.AddInt32(&e.publishCount, 1)
	return nil
}

func (e *TestEngine) PublishJoin(ch string, join *Join, opts *ChannelOptions) error {
	atomic.AddInt32(&e.publishJoinCount, 1)
	return nil
}

func (e *TestEngine) PublishLeave(ch string, leave *Leave, opts *ChannelOptions) error {
	atomic.AddInt32(&e.publishLeaveCount, 1)
	return nil
}

func (e *TestEngine) PublishControl(msg []byte) error {
	atomic.AddInt32(&e.publishControlCount, 1)
	return nil
}

func (e *TestEngine) Subscribe(ch string) error {
	return nil
}

func (e *TestEngine) Unsubscribe(ch string) error {
	return nil
}

func (e *TestEngine) AddPresence(ch string, uid string, info *ClientInfo, expire time.Duration) error {
	return nil
}

func (e *TestEngine) RemovePresence(ch string, uid string) error {
	return nil
}

func (e *TestEngine) Presence(ch string) (map[string]*ClientInfo, error) {
	return map[string]*protocol.ClientInfo{}, nil
}

func (e *TestEngine) PresenceStats(ch string) (PresenceStats, error) {
	return PresenceStats{}, nil
}

func (e *TestEngine) History(ch string, filter HistoryFilter) ([]*protocol.Publication, StreamPosition, error) {
	return []*protocol.Publication{}, StreamPosition{}, nil
}

func (e *TestEngine) AddHistory(ch string, pub *protocol.Publication, opts *ChannelOptions) (*Publication, error) {
	return pub, nil
}

func (e *TestEngine) RemoveHistory(ch string) error {
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
	defer node.Shutdown(context.Background())
	assert.True(t, node.userAllowed("channel#1", "1"))
	assert.True(t, node.userAllowed("channel", "1"))
	assert.False(t, node.userAllowed("channel#1", "2"))
	assert.True(t, node.userAllowed("channel#1,2", "1"))
	assert.True(t, node.userAllowed("channel#1,2", "2"))
	assert.False(t, node.userAllowed("channel#1,2", "3"))
}

func TestSetConfig(t *testing.T) {
	node := nodeWithTestEngine()
	defer node.Shutdown(context.Background())
	err := node.Reload(DefaultConfig)
	assert.NoError(t, err)
}

func TestNodeRegistry(t *testing.T) {
	registry := newNodeRegistry("node1")
	nodeInfo1 := controlproto.Node{UID: "node1"}
	nodeInfo2 := controlproto.Node{UID: "node2"}
	registry.add(&nodeInfo1)
	registry.add(&nodeInfo2)
	assert.Equal(t, 2, len(registry.list()))
	info := registry.get("node1")
	assert.Equal(t, "node1", info.UID)
	registry.clean(10 * time.Second)
	time.Sleep(2 * time.Second)
	registry.clean(time.Second)
	// Current node info should still be in node registry - we never delete it.
	assert.Equal(t, 1, len(registry.list()))
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
		node.Publish("bench", payload)
	}
	b.StopTimer()
	node.Shutdown(context.Background())
}

func newFakeConn(b testing.TB, node *Node, channel string, protoType ProtocolType, sink chan []byte) {
	transport := newTestTransport()
	transport.setProtocolType(protoType)
	transport.setSink(sink)
	ctx := context.Background()
	newCtx := SetCredentials(ctx, &Credentials{UserID: "42"})
	client, _ := NewClient(newCtx, node, transport)
	connectClient(b, client)
	replies := []*protocol.Reply{}
	rw := testReplyWriter(&replies)
	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: channel,
	}, rw, false)
	assert.Nil(b, subCtx.disconnect)
}

func newFakeConnJSON(b testing.TB, node *Node, channel string, sink chan []byte) {
	newFakeConn(b, node, channel, ProtocolTypeJSON, sink)
}

func newFakeConnProtobuf(b testing.TB, node *Node, channel string, sink chan []byte) {
	newFakeConn(b, node, channel, ProtocolTypeProtobuf, sink)
}

func TestNodeHistoryIteration(t *testing.T) {
	e := testMemoryEngine()
	conf := e.node.Config()
	conf.HistorySize = 100000
	conf.HistoryLifetime = 60
	conf.HistoryRecover = true
	err := e.node.Reload(conf)
	assert.NoError(t, err)

	channel := "test"

	numMessages := 10000
	for i := 1; i <= numMessages; i++ {
		err := e.node.Publish(channel, []byte(`{}`))
		assert.NoError(t, err)
	}

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
	assert.Equal(t, numMessages, n)
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
			n.Reload(c)
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
			n.Shutdown(context.Background())
			b.ReportAllocs()
		})
	}
}
