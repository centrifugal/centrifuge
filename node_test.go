package centrifuge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/controlpb"
	"github.com/centrifugal/centrifuge/internal/controlproto"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

type TestBroker struct {
	errorOnRun            bool
	errorOnSubscribe      bool
	errorOnUnsubscribe    bool
	errorOnPublish        bool
	errorOnPublishJoin    bool
	errorOnPublishLeave   bool
	errorOnPublishControl bool
	errorOnHistory        bool
	errorOnRemoveHistory  bool

	publishCount        int32
	publishJoinCount    int32
	publishLeaveCount   int32
	publishControlCount int32
}

func NewTestBroker() *TestBroker {
	return &TestBroker{}
}

func (e *TestBroker) Run(_ BrokerEventHandler) error {
	if e.errorOnRun {
		return errors.New("boom")
	}
	return nil
}

func (e *TestBroker) Publish(_ string, _ []byte, _ PublishOptions) (StreamPosition, error) {
	atomic.AddInt32(&e.publishCount, 1)
	if e.errorOnPublish {
		return StreamPosition{}, errors.New("boom")
	}
	return StreamPosition{}, nil
}

func (e *TestBroker) PublishJoin(_ string, _ *ClientInfo) error {
	atomic.AddInt32(&e.publishJoinCount, 1)
	if e.errorOnPublishJoin {
		return errors.New("boom")
	}
	return nil
}

func (e *TestBroker) PublishLeave(_ string, _ *ClientInfo) error {
	atomic.AddInt32(&e.publishLeaveCount, 1)
	if e.errorOnPublishLeave {
		return errors.New("boom")
	}
	return nil
}

func (e *TestBroker) PublishControl(_ []byte, _, _ string) error {
	atomic.AddInt32(&e.publishControlCount, 1)
	if e.errorOnPublishControl {
		return errors.New("boom")
	}
	return nil
}

func (e *TestBroker) Subscribe(_ string) error {
	if e.errorOnSubscribe {
		return errors.New("boom")
	}
	return nil
}

func (e *TestBroker) Unsubscribe(_ string) error {
	if e.errorOnUnsubscribe {
		return errors.New("boom")
	}
	return nil
}

func (e *TestBroker) History(_ string, _ HistoryOptions) ([]*Publication, StreamPosition, error) {
	if e.errorOnHistory {
		return nil, StreamPosition{}, errors.New("boom")
	}
	return nil, StreamPosition{}, nil
}

func (e *TestBroker) RemoveHistory(_ string) error {
	if e.errorOnRemoveHistory {
		return errors.New("boom")
	}
	return nil
}

type TestPresenceManager struct {
	errorOnPresence       bool
	errorOnPresenceStats  bool
	errorOnAddPresence    bool
	errorOnRemovePresence bool
}

func NewTestPresenceManager() *TestPresenceManager {
	return &TestPresenceManager{}
}

func (e *TestPresenceManager) AddPresence(_ string, _ string, _ *ClientInfo) error {
	if e.errorOnAddPresence {
		return errors.New("boom")
	}
	return nil
}

func (e *TestPresenceManager) RemovePresence(_ string, _ string) error {
	if e.errorOnRemovePresence {
		return errors.New("boom")
	}
	return nil
}

func (e *TestPresenceManager) Presence(_ string) (map[string]*ClientInfo, error) {
	if e.errorOnPresence {
		return nil, errors.New("boom")
	}
	return map[string]*ClientInfo{}, nil
}

func (e *TestPresenceManager) PresenceStats(_ string) (PresenceStats, error) {
	if e.errorOnPresenceStats {
		return PresenceStats{}, errors.New("boom")
	}
	return PresenceStats{}, nil
}

func nodeWithBroker(broker Broker) *Node {
	c := Config{}
	n, err := New(c)
	if err != nil {
		panic(err)
	}
	n.SetBroker(broker)
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func nodeWithTestBroker() *Node {
	return nodeWithBroker(NewTestBroker())
}

func nodeWithPresenceManager(presenceManager PresenceManager) *Node {
	c := Config{}
	n, err := New(c)
	if err != nil {
		panic(err)
	}
	n.SetPresenceManager(presenceManager)
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func defaultNodeNoHandlers() *Node {
	n, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(entry LogEntry) {},
	})
	if err != nil {
		panic(err)
	}
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func defaultTestNodeBenchmark(b *testing.B) *Node {
	c := Config{
		LogLevel: LogLevelError,
		LogHandler: func(entry LogEntry) {
			b.Fatal(entry.Message, entry.Fields)
		},
	}
	n, err := New(c)
	if err != nil {
		panic(err)
	}
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
	})
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func defaultTestNode() *Node {
	n := defaultNodeNoHandlers()
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnPublish(func(e PublishEvent, cb PublishCallback) {
			cb(PublishReply{}, nil)
		})
	})
	return n
}

func TestErrorMessage(t *testing.T) {
	errMessage := ErrorTooManyRequests.Error()
	require.Equal(t, "111: too many requests", errMessage)
}

func TestNode_Shutdown(t *testing.T) {
	n := defaultNodeNoHandlers()
	require.NoError(t, n.Shutdown(context.Background()))
	require.True(t, n.shutdown)
	// Test second call does not return error.
	require.NoError(t, n.Shutdown(context.Background()))
}

func TestNode_shutdownCmd(t *testing.T) {
	// Testing that shutdownCmd removes node from nodes registry.
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	require.NoError(t, n.shutdownCmd(n.ID()))
	require.True(t, len(n.nodes.list()) == 0)
	require.True(t, n.nodes.size() == 0)
}

func TestClientEventHub(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	n.OnConnect(func(_ *Client) {})
	require.NotNil(t, n.clientEvents.connectHandler)
}

func TestNodeRegistry(t *testing.T) {
	registry := newNodeRegistry("node1")
	nodeInfo1 := controlpb.Node{Uid: "node1"}
	nodeInfo2 := controlpb.Node{Uid: "node2"}
	registry.add(&nodeInfo1)
	registry.add(&nodeInfo1) // Make sure update works.
	registry.add(&nodeInfo2)
	require.Equal(t, 2, len(registry.list()))
	require.Equal(t, 2, registry.size())
	info, ok := registry.get("node1")
	require.True(t, ok)
	require.Equal(t, "node1", info.Uid)
	registry.clean(10 * time.Second)
	time.Sleep(2 * time.Second)
	registry.clean(time.Second)
	// Current node info should still be in node registry - we never delete it.
	require.Equal(t, 1, len(registry.list()))
	require.Equal(t, 1, registry.size())
}

func TestNodeLogHandler(t *testing.T) {
	doneCh := make(chan struct{})
	n, _ := New(Config{
		LogLevel: LogLevelInfo,
		LogHandler: func(entry LogEntry) {
			require.Equal(t, LogLevelInfo, entry.Level)
			require.Equal(t, "test2", entry.Message)
			close(doneCh)
		},
	})
	// Debug should not be logged.
	n.Log(NewLogEntry(LogLevelDebug, "test1", nil))
	n.Log(NewLogEntry(LogLevelInfo, "test2", nil))
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout processing log in handler")
	}
}

func TestNode_SetBroker(t *testing.T) {
	n, _ := New(Config{})
	broker := testMemoryBroker()
	n.SetBroker(broker)
	require.Equal(t, n.broker, broker)
}

func TestNode_SetPresenceManager_NilPresenceManager(t *testing.T) {
	n, _ := New(Config{})
	n.SetPresenceManager(nil)
	require.NoError(t, n.addPresence("test", "uid", nil))
	require.NoError(t, n.removePresence("test", "uid"))
	_, err := n.Presence("test")
	require.Equal(t, ErrorNotAvailable, err)
	_, err = n.PresenceStats("test")
	require.Equal(t, ErrorNotAvailable, err)
}

func TestNode_LogEnabled(t *testing.T) {
	n, _ := New(Config{
		LogLevel:   LogLevelInfo,
		LogHandler: func(entry LogEntry) {},
	})
	require.False(t, n.LogEnabled(LogLevelDebug))
	require.True(t, n.LogEnabled(LogLevelInfo))
}

func TestNode_RunError(t *testing.T) {
	broker := NewTestBroker()
	broker.errorOnRun = true
	node, err := New(Config{})
	require.NoError(t, err)
	node.SetBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()
	require.Error(t, node.Run())
}

func TestNode_RunPubControlError(t *testing.T) {
	broker := NewTestBroker()
	broker.errorOnPublishControl = true
	node, err := New(Config{})
	require.NoError(t, err)
	node.SetBroker(broker)
	defer func() { _ = node.Shutdown(context.Background()) }()
	require.Error(t, node.Run())
}

func TestNode_SetPresenceManager(t *testing.T) {
	n, _ := New(Config{})
	presenceManager := testMemoryPresenceManager(t)
	n.SetPresenceManager(presenceManager)
	require.Equal(t, n.presenceManager, presenceManager)
}

func TestNode_Info(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	info, err := n.Info()
	require.NoError(t, err)
	require.Len(t, info.Nodes, 1)
}

func TestNode_handleJoin(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	err := n.handleJoin("test", &ClientInfo{})
	require.NoError(t, err)
}

func TestNode_handleLeave(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	err := n.handleLeave("test", &ClientInfo{})
	require.NoError(t, err)
}

func TestNode_Subscribe(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	done := make(chan struct{})
	n.OnConnect(func(client *Client) {
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			require.Equal(t, "42", client.UserID())
			require.Equal(t, "test_channel", event.Channel)
			require.True(t, event.ServerSide)
			require.Equal(t, UnsubscribeCodeServer, event.Code)
			close(done)
		})
	})

	newTestConnectedClientV2(t, n, "42")

	err := n.Subscribe("42", "test_channel", WithRecoverSince(&StreamPosition{0, "test"}))
	require.NoError(t, err)
	require.Equal(t, 1, n.hub.NumSubscribers("test_channel"))

	err = n.Unsubscribe("42", "test_channel")
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	require.Zero(t, n.hub.NumSubscribers("test_channel"))
}

func TestNode_Unsubscribe(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	err := n.Unsubscribe("42", "test_channel")
	require.NoError(t, err)

	done := make(chan struct{})
	n.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, cb SubscribeCallback) {
			cb(SubscribeReply{}, nil)
		})
		client.OnUnsubscribe(func(event UnsubscribeEvent) {
			require.Equal(t, "42", client.UserID())
			close(done)
		})
	})

	client := newTestSubscribedClientV2(t, n, "42", "test_channel")

	err = n.Unsubscribe("42", "test_channel")
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	require.Zero(t, n.hub.NumSubscribers("test_channel"))
	require.NotContains(t, client.channels, "test_channel")
}

func TestNode_Disconnect(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	err := n.Disconnect("42")
	require.NoError(t, err)

	done := make(chan struct{})
	n.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, "42", client.UserID())
			require.Equal(t, DisconnectBadRequest.Code, event.Code)
			close(done)
		})
	})

	newTestConnectedClientV2(t, n, "42")

	err = n.Disconnect("42", WithCustomDisconnect(DisconnectBadRequest))
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	require.True(t, len(n.hub.UserConnections("42")) == 0)
}

func TestNode_pubUnsubscribe(t *testing.T) {
	node := nodeWithTestBroker()
	defer func() { _ = node.Shutdown(context.Background()) }()

	testBroker, _ := node.broker.(*TestBroker)
	require.EqualValues(t, 1, testBroker.publishControlCount)

	err := node.pubUnsubscribe("42", "holypeka", unsubscribeServer, "", "")
	require.NoError(t, err)
	require.EqualValues(t, 2, testBroker.publishControlCount)
}

func TestNode_pubDisconnect(t *testing.T) {
	node := nodeWithTestBroker()
	defer func() { _ = node.Shutdown(context.Background()) }()

	testBroker, _ := node.broker.(*TestBroker)
	require.EqualValues(t, 1, testBroker.publishControlCount)

	err := node.pubDisconnect("42", DisconnectForceNoReconnect, "", "", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, testBroker.publishControlCount)
}

func TestNode_publishJoin(t *testing.T) {
	n := nodeWithTestBroker()
	defer func() { _ = n.Shutdown(context.Background()) }()

	testBroker, _ := n.broker.(*TestBroker)
	require.EqualValues(t, 0, testBroker.publishJoinCount)

	// Publish without options.
	err := n.publishJoin("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 1, testBroker.publishJoinCount)

	// Publish with default/correct options.
	err = n.publishJoin("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 2, testBroker.publishJoinCount)
}

func TestNode_publishLeave(t *testing.T) {
	n := nodeWithTestBroker()
	defer func() { _ = n.Shutdown(context.Background()) }()

	testBroker, _ := n.broker.(*TestBroker)
	require.EqualValues(t, 0, testBroker.publishLeaveCount)

	// Publish without options.
	err := n.publishLeave("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 1, testBroker.publishLeaveCount)

	// Publish with default/correct options.
	err = n.publishLeave("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 2, testBroker.publishLeaveCount)
}

func TestNode_RemoveHistory(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	err := n.RemoveHistory("test_user")
	require.NoError(t, err)
}

func TestNode_History_ErrorOnReverseWithZeroOffset(t *testing.T) {
	n := defaultNodeNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	_, err := n.History("test", WithReverse(true), WithSince(&StreamPosition{Offset: 0}))
	require.Equal(t, err, ErrorBadRequest)
}

func TestIndex(t *testing.T) {
	require.Equal(t, 0, index("2121", 1))
}

var testPayload = map[string]any{
	"_id":        "5adece493c1a23736b037c52",
	"index":      2,
	"guid":       "478a00f4-19b1-4567-8097-013b8cc846b8",
	"isActive":   false,
	"balance":    "$2,199.02",
	"picture":    "https://placehold.it/32x32",
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

func BenchmarkNodePublishWithNoopBroker(b *testing.B) {
	node := nodeWithTestBroker()

	payload, err := json.Marshal(testPayload)
	if err != nil {
		panic(err.Error())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := node.Publish("bench", payload)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = node.Shutdown(context.Background())
}

func newFakeConn(b testing.TB, node *Node, channel string, protoType ProtocolType, sink chan []byte, protoVersion ProtocolVersion) {
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolType(protoType)
	transport.setProtocolVersion(protoVersion)
	transport.setSink(sink)
	newCtx := SetCredentials(ctx, &Credentials{UserID: "test"})
	client, _ := newClient(newCtx, node, transport)
	connectClientV2(b, client)
	rwWrapper := testReplyWriterWrapper()
	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: channel,
	}, SubscribeReply{}, &protocol.Command{}, false, time.Now(), rwWrapper.rw)
	require.Nil(b, subCtx.disconnect)
}

func newFakeConnJSON(b testing.TB, node *Node, channel string, sink chan []byte, protoVersion ProtocolVersion) {
	newFakeConn(b, node, channel, ProtocolTypeJSON, sink, protoVersion)
}

func newFakeConnProtobuf(b testing.TB, node *Node, channel string, sink chan []byte, protoVersion ProtocolVersion) {
	newFakeConn(b, node, channel, ProtocolTypeProtobuf, sink, protoVersion)
}

func BenchmarkBroadcastMemory(b *testing.B) {
	benchmarks := []struct {
		name           string
		getFakeConn    func(b testing.TB, n *Node, channel string, sink chan []byte, protoVersion ProtocolVersion)
		numSubscribers int
		protoVersion   ProtocolVersion
	}{
		{"JSON-V1", newFakeConnJSON, 1, ProtocolVersion2},
		{"Protobuf-V1", newFakeConnProtobuf, 1, ProtocolVersion2},
		{"JSON-V1", newFakeConnJSON, 10000, ProtocolVersion2},
		{"Protobuf-V1", newFakeConnProtobuf, 10000, ProtocolVersion2},
		{"JSON-V2", newFakeConnJSON, 1, ProtocolVersion2},
		{"Protobuf-V2", newFakeConnProtobuf, 1, ProtocolVersion2},
		{"JSON-V2", newFakeConnJSON, 10000, ProtocolVersion2},
		{"Protobuf-V2", newFakeConnProtobuf, 10000, ProtocolVersion2},
	}

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%s_%d_sub", bm.name, bm.numSubscribers), func(b *testing.B) {
			n := defaultTestNodeBenchmark(b)
			payload := []byte(`{"input": "test"}`)
			sink := make(chan []byte, bm.numSubscribers)
			for i := 0; i < bm.numSubscribers; i++ {
				bm.getFakeConn(b, n, "test", sink, bm.protoVersion)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := n.Publish("test", payload)
				if err != nil {
					panic(err)
				}
				j := 0
				for {
					data := <-sink
					if bytes.Contains(data, []byte("input")) {
						j++
					}
					if j == bm.numSubscribers {
						break
					}
				}
			}
			b.StopTimer()
			_ = n.Shutdown(context.Background())
			b.ReportAllocs()
		})
	}
}

func BenchmarkHistory(b *testing.B) {
	broker := testMemoryBroker()
	numMessages := 100

	channel := "test"

	for i := 1; i <= numMessages; i++ {
		_, err := broker.node.Publish(channel, []byte(`{}`), WithHistory(numMessages, time.Minute))
		require.NoError(b, err)
	}

	b.ResetTimer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := broker.node.History(channel)
		if err != nil {
			b.Fatal(err)
		}

	}
	b.StopTimer()
	b.ReportAllocs()
}

func TestNode_handleControl(t *testing.T) {
	t.Run("BrokenData", func(t *testing.T) {
		t.Parallel()

		n := nodeWithTestBroker()
		defer func() { _ = n.Shutdown(context.Background()) }()

		err := n.handleControl([]byte("random"))
		require.EqualError(t, err, "unexpected EOF")
	})

	t.Run("Node", func(t *testing.T) {
		t.Parallel()

		n := nodeWithTestBroker()
		defer func() { _ = n.Shutdown(context.Background()) }()

		enc := controlproto.NewProtobufEncoder()
		brokenCmdBytes := []byte("random")
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Node: &controlpb.Node{
				Name: "new_node",
			},
		})
		require.NoError(t, err)

		err = n.handleControl(brokenCmdBytes)
		require.EqualError(t, err, "unexpected EOF")
		err = n.handleControl(cmdBytes)
		require.NoError(t, err)
		require.NotContains(t, n.nodes.nodes, "new_node")
	})

	t.Run("Subscribe", func(t *testing.T) {
		t.Parallel()

		n := defaultNodeNoHandlers()
		defer func() { _ = n.Shutdown(context.Background()) }()

		newTestConnectedClientV2(t, n, "42")

		enc := controlproto.NewProtobufEncoder()
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Subscribe: &controlpb.Subscribe{
				Channel: "test_channel",
				User:    "42",
				RecoverSince: &controlpb.StreamPosition{
					Offset: 0,
					Epoch:  "test",
				},
			},
		})
		require.NoError(t, err)
		err = n.handleControl(cmdBytes)
		require.NoError(t, err)
		require.Equal(t, 1, n.hub.NumSubscribers("test_channel"))
	})

	t.Run("Unsubscribe", func(t *testing.T) {
		t.Parallel()

		n := defaultNodeNoHandlers()
		done := make(chan struct{})
		n.OnConnect(func(client *Client) {
			client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
				cb(SubscribeReply{}, nil)
			})
			client.OnUnsubscribe(func(event UnsubscribeEvent) {
				require.Equal(t, "42", client.UserID())
				close(done)
			})
		})
		defer func() { _ = n.Shutdown(context.Background()) }()

		client := newTestSubscribedClientV2(t, n, "42", "test_channel")

		enc := controlproto.NewProtobufEncoder()
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Uid: client.uid,
			Unsubscribe: &controlpb.Unsubscribe{
				Channel: "test_channel",
				User:    "42",
			},
		})
		require.NoError(t, err)
		err = n.handleControl(cmdBytes)
		select {
		case <-done:
		case <-time.After(time.Second):
			require.Fail(t, "timeout")
		}
		require.NoError(t, err)
		require.Zero(t, n.hub.NumSubscribers("test_channel"))
	})

	t.Run("Shutdown", func(t *testing.T) {
		t.Parallel()

		n := defaultNodeNoHandlers()
		defer func() { _ = n.Shutdown(context.Background()) }()

		enc := controlproto.NewProtobufEncoder()
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Uid:      "",
			Shutdown: &controlpb.Shutdown{},
		})
		require.NoError(t, err)

		err = n.handleControl(cmdBytes)
		require.NoError(t, err)
	})

	t.Run("Disconnect", func(t *testing.T) {
		t.Parallel()

		n := defaultTestNode()
		done := make(chan struct{})
		n.OnConnect(func(client *Client) {
			client.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) {
				cb(SubscribeReply{}, nil)
			})
			client.OnDisconnect(func(event DisconnectEvent) {
				require.Equal(t, "42", client.UserID())
				close(done)
			})
		})
		defer func() { _ = n.Shutdown(context.Background()) }()

		client := newTestSubscribedClientV2(t, n, "42", "test_channel")

		enc := controlproto.NewProtobufEncoder()
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Uid: client.uid,
			Disconnect: &controlpb.Disconnect{
				User: "42",
			},
		})
		require.NoError(t, err)

		err = n.handleControl(cmdBytes)
		select {
		case <-done:
		case <-time.After(time.Second):
			require.Fail(t, "timeout")
		}
		require.NoError(t, err)
		require.Zero(t, n.hub.NumSubscribers("test_channel"))
		require.Zero(t, len(n.hub.UserConnections("42")))
	})

	t.Run("Refresh", func(t *testing.T) {
		t.Parallel()

		n := defaultTestNode()
		defer func() { _ = n.Shutdown(context.Background()) }()

		enc := controlproto.NewProtobufEncoder()
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Uid: "",
			Refresh: &controlpb.Refresh{
				User: "42",
			},
		})
		require.NoError(t, err)
		err = n.handleControl(cmdBytes)
		require.NoError(t, err)
	})

	t.Run("Unknown", func(t *testing.T) {
		t.Parallel()

		n := defaultTestNode()
		defer func() { _ = n.Shutdown(context.Background()) }()

		client := newTestSubscribedClientV2(t, n, "42", "test_channel")

		enc := controlproto.NewProtobufEncoder()
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Uid: client.uid,
		})
		require.NoError(t, err)

		err = n.handleControl(cmdBytes)
		require.NoError(t, err)
	})
}

func Test_infoFromProto(t *testing.T) {
	info := infoFromProto(nil)
	require.Nil(t, info)

	info = infoFromProto(&protocol.ClientInfo{
		User:   "user",
		Client: "client",
	})
	require.NotNil(t, info)
	require.Equal(t, info.UserID, "user")
	require.Equal(t, info.ClientID, "client")
	require.Nil(t, info.ConnInfo)
	require.Nil(t, info.ChanInfo)

	info = infoFromProto(&protocol.ClientInfo{
		User:     "user",
		Client:   "client",
		ConnInfo: []byte("conn_info"),
		ChanInfo: []byte("chan_info"),
	})
	require.NotNil(t, info)
	require.Equal(t, info.UserID, "user")
	require.Equal(t, info.ClientID, "client")
	require.Equal(t, info.ConnInfo, []byte("conn_info"))
	require.Equal(t, info.ChanInfo, []byte("chan_info"))
}

func Test_infoToProto(t *testing.T) {
	info := infoToProto(nil)
	require.Nil(t, info)

	info = infoToProto(&ClientInfo{
		UserID:   "user",
		ClientID: "client",
	})
	require.NotNil(t, info)
	require.Equal(t, info.User, "user")
	require.Equal(t, info.Client, "client")
	require.Nil(t, info.ConnInfo)
	require.Nil(t, info.ChanInfo)

	info = infoToProto(&ClientInfo{
		UserID:   "user",
		ClientID: "client",
		ConnInfo: []byte("conn_info"),
		ChanInfo: []byte("chan_info"),
	})
	require.NotNil(t, info)
	require.Equal(t, info.User, "user")
	require.Equal(t, info.Client, "client")
	require.Equal(t, info.ConnInfo, protocol.Raw("conn_info"))
	require.Equal(t, info.ChanInfo, protocol.Raw("chan_info"))
}

func Test_pubToProto(t *testing.T) {
	pub := pubToProto(nil)
	require.Nil(t, pub)

	pub = pubToProto(&Publication{
		Offset: 42,
		Data:   []byte("data"),
		Info: &ClientInfo{
			ClientID: "client_id",
		},
	})
	require.Equal(t, uint64(42), pub.Offset)
	require.Equal(t, protocol.Raw("data"), pub.Data)
	require.NotNil(t, pub.Info)
	require.Equal(t, pub.Info.Client, "client_id")
}

func Test_pubFromProto(t *testing.T) {
	pub := pubFromProto(nil)
	require.Nil(t, pub)

	pub = pubFromProto(&protocol.Publication{
		Data: []byte("data"),
		Info: &protocol.ClientInfo{
			Client: "client_id",
		},
		Offset: 42,
	})
	require.Equal(t, uint64(42), pub.Offset)
	require.Equal(t, []byte("data"), pub.Data)
	require.NotNil(t, pub.Info)
	require.Equal(t, pub.Info.ClientID, "client_id")
}

func TestNode_OnSurvey(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
		go func() {
			require.Nil(t, event.Data)
			require.Equal(t, "test_op", event.Op)
			callback(SurveyReply{
				Data: []byte("1"),
				Code: 1,
			})
		}()
	})

	results, err := node.Survey(context.Background(), "test_op", nil, "")
	require.NoError(t, err)
	require.Len(t, results, 1)
	res, ok := results[node.ID()]
	require.True(t, ok)
	require.Equal(t, uint32(1), res.Code)
	require.Equal(t, []byte("1"), res.Data)
}

func TestNode_OnSurveyWithNodeID(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
		go func() {
			require.Nil(t, event.Data)
			require.Equal(t, "test_op", event.Op)
			callback(SurveyReply{
				Data: []byte("1"),
				Code: 1,
			})
		}()
	})

	results, err := node.Survey(context.Background(), "test_op", nil, node.ID())
	require.NoError(t, err)
	require.Len(t, results, 1)
	res, ok := results[node.ID()]
	require.True(t, ok)
	require.Equal(t, uint32(1), res.Code)
	require.Equal(t, []byte("1"), res.Data)
}

func TestNode_OnSurvey_NoHandler(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	_, err := node.Survey(context.Background(), "test_op", nil, "")
	require.Error(t, err)
	require.Equal(t, errSurveyHandlerNotRegistered, err)
}

func TestNode_OnSurvey_Timeout(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
		}
		callback(SurveyReply{
			Data: []byte("1"),
			Code: 1,
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := node.Survey(ctx, "test_op", nil, "")
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
	close(done)
}

func TestNode_OnNotification(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	handlerCalled := false

	node.OnNotification(func(event NotificationEvent) {
		require.Equal(t, "notification", event.Op)
		require.Equal(t, []byte(`notification`), event.Data)
		require.Equal(t, node.ID(), event.FromNodeID)
		handlerCalled = true
	})

	err := node.Notify("notification", []byte(`notification`), "")
	require.NoError(t, err)
	require.True(t, handlerCalled)
}

func TestNode_OnNotification_SameNode(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	handlerCalled := false

	node.OnNotification(func(event NotificationEvent) {
		require.Equal(t, "notification", event.Op)
		require.Equal(t, []byte(`notification`), event.Data)
		require.Equal(t, node.ID(), event.FromNodeID)
		handlerCalled = true
	})

	err := node.Notify("notification", []byte(`notification`), node.ID())
	require.NoError(t, err)
	require.True(t, handlerCalled)
}

func TestNode_OnNotification_NoHandler(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	err := node.Notify("notification", []byte(`notification`), "")
	require.Equal(t, errNotificationHandlerNotRegistered, err)
}

func TestNode_handleNotification_NoHandler(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	err := node.handleNotification("test", &controlpb.Notification{})
	require.NoError(t, err)
}

func TestNode_handleSurveyRequest_NoHandler(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	err := node.handleSurveyRequest("test", &controlpb.SurveyRequest{})
	require.NoError(t, err)
}

func TestErrors(t *testing.T) {
	err := ErrorUnauthorized
	protoErr := err.toProto()
	require.Equal(t, ErrorUnauthorized.Code, protoErr.Code)
	err = ErrorUnknownChannel
	errText := err.Error()
	require.Equal(t, "102: unknown channel", errText)
}

func TestSingleFlightHistory(t *testing.T) {
	node := defaultNodeNoHandlers()
	node.config.UseSingleFlight = true
	defer func() { _ = node.Shutdown(context.Background()) }()
	result, err := node.History("test", WithLimit(1), WithSince(&StreamPosition{
		Offset: 0,
		Epoch:  "",
	}))
	require.NoError(t, err)
	require.Len(t, result.Publications, 0)
	_, _ = node.Publish("test", []byte("{}"), WithHistory(10, 60*time.Second))
	result, err = node.History("test", WithLimit(1), WithSince(&StreamPosition{
		Offset: 0,
		Epoch:  "",
	}))
	require.NoError(t, err)
	require.Len(t, result.Publications, 1)
}

func TestSingleFlightPresence(t *testing.T) {
	node := defaultNodeNoHandlers()
	node.config.UseSingleFlight = true

	node.OnConnect(func(client *Client) {
		client.OnSubscribe(func(event SubscribeEvent, callback SubscribeCallback) {
			callback(SubscribeReply{
				Options: SubscribeOptions{
					EmitPresence: true,
				},
			}, nil)
		})
	})

	defer func() { _ = node.Shutdown(context.Background()) }()
	result, err := node.Presence("test")
	require.NoError(t, err)
	require.Len(t, result.Presence, 0)

	client := newTestClient(t, node, "42")
	connectClientV2(t, client)
	subscribeClientV2(t, client, "test")

	result, err = node.Presence("test")
	require.NoError(t, err)
	require.Len(t, result.Presence, 1)

	stats, err := node.PresenceStats("test")
	require.NoError(t, err)
	require.Equal(t, 1, stats.NumClients)
	require.Equal(t, 1, stats.NumUsers)
}

func TestBrokerEventHandler_PanicsOnNil(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()
	handler := &brokerEventHandler{node: node}
	require.Panics(t, func() {
		_ = handler.HandlePublication("test", nil, StreamPosition{})
	})
	require.Panics(t, func() {
		_ = handler.HandleJoin("test", nil)
	})
	require.Panics(t, func() {
		_ = handler.HandleLeave("test", nil)
	})
}

func TestNode_OnNodeInfoSend(t *testing.T) {
	n, err := New(Config{})
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})

	n.OnNodeInfoSend(func() NodeInfoSendReply {
		close(done)
		return NodeInfoSendReply{
			Data: []byte("{}"),
		}
	})

	err = n.Run()
	require.NoError(t, err)
	defer func() { _ = n.Shutdown(context.Background()) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}

	result, err := n.Info()
	require.NoError(t, err)
	require.Equal(t, []byte("{}"), result.Nodes[0].Data)
}

func TestNode_OnTransportWrite(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnTransportWrite(func(_ *Client, event TransportWriteEvent) bool {
		if string(event.Data) == `{"push":{"message":{"data":{}}}}` {
			close(done)
		}
		return true
	})

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	sink := make(chan []byte, 1)
	transport.setSink(sink)

	client := newTestClientCustomTransport(t, ctx, node, transport, "42")

	connectClientV2(t, client)
	err := client.Send([]byte("{}"))
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	select {
	case <-sink:
	case <-time.After(time.Second):
		require.Fail(t, "timeout waiting for transport write")
	}
}

func TestNode_OnTransportWriteProtocolV2(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnTransportWrite(func(client *Client, event TransportWriteEvent) bool {
		if string(event.Data) == "{\"push\":{\"message\":{\"data\":{}}}}" {
			close(done)
		}
		return true
	})

	client := newTestClientV2(t, node, "42")
	connectClientV2(t, client)
	err := client.Send([]byte("{}"))
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
}

func TestNode_OnTransportWriteSkip(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnTransportWrite(func(_ *Client, event TransportWriteEvent) bool {
		return false
	})

	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	sink := make(chan []byte, 1)
	transport.setSink(sink)

	client := newTestClientCustomTransport(t, ctx, node, transport, "42")

	connectClientV2(t, client)
	err := client.Send([]byte("{}"))
	require.NoError(t, err)

	select {
	case data := <-sink:
		require.Fail(t, fmt.Sprintf("message written to transport – but it must not: %s", string(data)))
	case <-time.After(time.Second):
	}
}

func TestNode_OnCommandRead(t *testing.T) {
	node := defaultNodeNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	node.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{
			Credentials: &Credentials{
				UserID: "12",
			},
		}, nil
	})

	doneConnect := make(chan struct{})
	doneSubscribe := make(chan struct{})

	node.OnCommandRead(func(client *Client, event CommandReadEvent) error {
		if event.Command.Connect != nil && event.Command.Connect.Token == "123" {
			close(doneConnect)
		}
		if event.Command.Subscribe != nil && event.Command.Subscribe.Channel == "channel" {
			close(doneSubscribe)
		}
		return nil
	})

	client := newTestClientV2(t, node, "42")
	client.HandleCommand(&protocol.Command{
		Id: 1,
		Connect: &protocol.ConnectRequest{
			Token: "123",
		},
	}, 0)
	select {
	case <-doneConnect:
	case <-time.After(time.Second):
		require.Fail(t, "timeout connect")
	}

	client.HandleCommand(&protocol.Command{
		Id: 2,
		Subscribe: &protocol.SubscribeRequest{
			Channel: "channel",
		},
	}, 0)
	select {
	case <-doneSubscribe:
	case <-time.After(time.Second):
		require.Fail(t, "timeout subscribe")
	}
}
