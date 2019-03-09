package centrifuge

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/proto"
	"github.com/centrifugal/centrifuge/internal/proto/controlproto"

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

func (e *TestEngine) Publish(ch string, pub *proto.Publication, opts *ChannelOptions) error {
	atomic.AddInt32(&e.publishCount, 1)
	return nil
}

func (e *TestEngine) PublishJoin(ch string, join *proto.Join, opts *ChannelOptions) error {
	atomic.AddInt32(&e.publishJoinCount, 1)
	return nil
}

func (e *TestEngine) PublishLeave(ch string, leave *proto.Leave, opts *ChannelOptions) error {
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

func (e *TestEngine) AddPresence(ch string, uid string, info *proto.ClientInfo, expire time.Duration) error {
	return nil
}

func (e *TestEngine) RemovePresence(ch string, uid string) error {
	return nil
}

func (e *TestEngine) Presence(ch string) (map[string]*proto.ClientInfo, error) {
	return map[string]*proto.ClientInfo{}, nil
}

func (e *TestEngine) PresenceStats(ch string) (PresenceStats, error) {
	return PresenceStats{}, nil
}

func (e *TestEngine) History(ch string, filter HistoryFilter) ([]*proto.Publication, RecoveryPosition, error) {
	return []*proto.Publication{}, RecoveryPosition{}, nil
}

func (e *TestEngine) AddHistory(ch string, pub *proto.Publication, opts *ChannelOptions) (*Publication, error) {
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
	assert.True(t, node.userAllowed("channel#1", "1"))
	assert.True(t, node.userAllowed("channel", "1"))
	assert.False(t, node.userAllowed("channel#1", "2"))
	assert.True(t, node.userAllowed("channel#1,2", "1"))
	assert.True(t, node.userAllowed("channel#1,2", "2"))
	assert.False(t, node.userAllowed("channel#1,2", "3"))
}

func TestSetConfig(t *testing.T) {
	node := nodeWithTestEngine()
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
}
