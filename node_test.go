package centrifuge

import (
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

type TestEngine struct {
	errorOnRun            bool
	errorOnSubscribe      bool
	errorOnUnsubscribe    bool
	errorOnPublish        bool
	errorOnPublishJoin    bool
	errorOnPublishLeave   bool
	errorOnPublishControl bool
	errorOnPresence       bool
	errorOnPresenceStats  bool
	errorOnAddPresence    bool
	errorOnRemovePresence bool
	errorOnHistory        bool
	errorOnChannels       bool
	errorOnRemoveHistory  bool

	publishCount        int32
	publishJoinCount    int32
	publishLeaveCount   int32
	publishControlCount int32
}

func NewTestEngine() *TestEngine {
	return &TestEngine{}
}

func (e *TestEngine) Run(_ BrokerEventHandler) error {
	if e.errorOnRun {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) Publish(_ string, _ []byte, _ PublishOptions) (StreamPosition, error) {
	atomic.AddInt32(&e.publishCount, 1)
	if e.errorOnPublish {
		return StreamPosition{}, errors.New("boom")
	}
	return StreamPosition{}, nil
}

func (e *TestEngine) PublishJoin(_ string, _ *ClientInfo) error {
	atomic.AddInt32(&e.publishJoinCount, 1)
	if e.errorOnPublishJoin {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) PublishLeave(_ string, _ *ClientInfo) error {
	atomic.AddInt32(&e.publishLeaveCount, 1)
	if e.errorOnPublishLeave {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) PublishControl(_ []byte, _ string) error {
	atomic.AddInt32(&e.publishControlCount, 1)
	if e.errorOnPublishControl {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) Subscribe(_ string) error {
	if e.errorOnSubscribe {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) Unsubscribe(_ string) error {
	if e.errorOnUnsubscribe {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) AddPresence(_ string, _ string, _ *ClientInfo, _ time.Duration) error {
	if e.errorOnAddPresence {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) RemovePresence(_ string, _ string) error {
	if e.errorOnRemovePresence {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) Presence(_ string) (map[string]*ClientInfo, error) {
	if e.errorOnPresence {
		return nil, errors.New("boom")
	}
	return map[string]*ClientInfo{}, nil
}

func (e *TestEngine) PresenceStats(_ string) (PresenceStats, error) {
	if e.errorOnPresenceStats {
		return PresenceStats{}, errors.New("boom")
	}
	return PresenceStats{}, nil
}

func (e *TestEngine) History(_ string, _ HistoryFilter) ([]*Publication, StreamPosition, error) {
	if e.errorOnHistory {
		return nil, StreamPosition{}, errors.New("boom")
	}
	return nil, StreamPosition{}, nil
}

func (e *TestEngine) RemoveHistory(_ string) error {
	if e.errorOnRemoveHistory {
		return errors.New("boom")
	}
	return nil
}

func (e *TestEngine) Channels() ([]string, error) {
	if e.errorOnChannels {
		return nil, errors.New("boom")
	}
	return []string{}, nil
}

func nodeWithEngine(engine Engine) *Node {
	c := DefaultConfig
	n, err := New(c)
	if err != nil {
		panic(err)
	}
	n.SetEngine(engine)
	err = n.Run()
	if err != nil {
		panic(err)
	}
	return n
}

func nodeWithTestEngine() *Node {
	return nodeWithEngine(NewTestEngine())
}

func nodeWithMemoryEngineNoHandlers() *Node {
	c := DefaultConfig
	c.LogLevel = LogLevelDebug
	c.LogHandler = func(entry LogEntry) {}
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

func nodeWithMemoryEngine() *Node {
	n := nodeWithMemoryEngineNoHandlers()
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
	n := nodeWithMemoryEngineNoHandlers()
	require.NoError(t, n.Shutdown(context.Background()))
	require.True(t, n.shutdown)
	// Test second call does not return error.
	require.NoError(t, n.Shutdown(context.Background()))
}

func TestClientEventHub(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	n.OnConnect(func(_ *Client) {})
	require.NotNil(t, n.clientEvents.connectHandler)
}

func TestNodeRegistry(t *testing.T) {
	registry := newNodeRegistry("node1")
	nodeInfo1 := controlpb.Node{UID: "node1"}
	nodeInfo2 := controlpb.Node{UID: "node2"}
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

func TestNodeLogHandler(t *testing.T) {
	c := DefaultConfig
	doneCh := make(chan struct{})
	c.LogHandler = func(entry LogEntry) {
		require.Equal(t, LogLevelInfo, entry.Level)
		require.Equal(t, "test2", entry.Message)
		close(doneCh)
	}
	c.LogLevel = LogLevelInfo
	n, _ := New(c)
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
	n, _ := New(DefaultConfig)
	engine := testMemoryEngine()
	n.SetBroker(engine)
	require.Equal(t, n.broker, engine)
}

func TestNode_SetPresenceManager_NilPresenceManager(t *testing.T) {
	n, _ := New(DefaultConfig)
	n.SetPresenceManager(nil)
	require.NoError(t, n.addPresence("test", "uid", nil))
	require.NoError(t, n.removePresence("test", "uid"))
	_, err := n.Presence("test")
	require.Equal(t, ErrorNotAvailable, err)
	_, err = n.PresenceStats("test")
	require.Equal(t, ErrorNotAvailable, err)
}

func TestNode_LogEnabled(t *testing.T) {
	c := DefaultConfig
	c.LogLevel = LogLevelInfo
	c.LogHandler = func(entry LogEntry) {}
	n, _ := New(c)
	require.False(t, n.LogEnabled(LogLevelDebug))
	require.True(t, n.LogEnabled(LogLevelInfo))
}

func TestNode_RunError(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnRun = true
	node, err := New(DefaultConfig)
	require.NoError(t, err)
	node.SetEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()
	require.Error(t, node.Run())
}

func TestNode_RunPubControlError(t *testing.T) {
	engine := NewTestEngine()
	engine.errorOnPublishControl = true
	node, err := New(DefaultConfig)
	require.NoError(t, err)
	node.SetEngine(engine)
	defer func() { _ = node.Shutdown(context.Background()) }()
	require.Error(t, node.Run())
}

func TestNode_SetPresenceManager(t *testing.T) {
	n, _ := New(DefaultConfig)
	engine := testMemoryEngine()
	n.SetPresenceManager(engine)
	require.Equal(t, n.presenceManager, engine)
}

func TestNode_Info(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	info, err := n.Info()
	require.NoError(t, err)
	require.Len(t, info.Nodes, 1)
}

func TestNode_handleJoin(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	err := n.handleJoin("test", &protocol.Join{
		Info: protocol.ClientInfo{},
	})
	require.NoError(t, err)
}

func TestNode_handleLeave(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()
	err := n.handleLeave("test", &protocol.Leave{
		Info: protocol.ClientInfo{},
	})
	require.NoError(t, err)
}

func TestNode_Unsubscribe(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
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

	client := newTestSubscribedClient(t, n, "42", "test_channel")

	err = n.Unsubscribe("42", "test_channel", WithResubscribe(false))
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	require.NotContains(t, n.hub.subs, "test_channel")
	require.NotContains(t, client.channels, "test_channel")
}

func TestNode_Disconnect(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	err := n.Disconnect("42")
	require.NoError(t, err)

	done := make(chan struct{})
	n.OnConnect(func(client *Client) {
		client.OnDisconnect(func(event DisconnectEvent) {
			require.Equal(t, "42", client.UserID())
			require.Equal(t, DisconnectBadRequest, event.Disconnect)
			close(done)
		})
	})

	client := newTestConnectedClient(t, n, "42")

	err = n.Disconnect("42", WithDisconnect(DisconnectBadRequest))
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	require.NotContains(t, n.hub.conns, client.uid)
	require.NotContains(t, n.hub.users, "42")
}

func TestNode_pubUnsubscribe(t *testing.T) {
	node := nodeWithTestEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	testEngine, _ := node.broker.(*TestEngine)
	require.EqualValues(t, 1, testEngine.publishControlCount)

	err := node.pubUnsubscribe("42", "holypeka")
	require.NoError(t, err)
	require.EqualValues(t, 2, testEngine.publishControlCount)
}

func TestNode_pubDisconnect(t *testing.T) {
	node := nodeWithTestEngine()
	defer func() { _ = node.Shutdown(context.Background()) }()

	testEngine, _ := node.broker.(*TestEngine)
	require.EqualValues(t, 1, testEngine.publishControlCount)

	err := node.pubDisconnect("42", DisconnectForceNoReconnect, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, testEngine.publishControlCount)
}

func TestNode_publishJoin(t *testing.T) {
	n := nodeWithTestEngine()
	defer func() { _ = n.Shutdown(context.Background()) }()

	testEngine, _ := n.broker.(*TestEngine)
	require.EqualValues(t, 0, testEngine.publishJoinCount)

	// Publish without options.
	err := n.publishJoin("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 1, testEngine.publishJoinCount)

	// Publish with default/correct options.
	err = n.publishJoin("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 2, testEngine.publishJoinCount)
}

func TestNode_publishLeave(t *testing.T) {
	n := nodeWithTestEngine()
	defer func() { _ = n.Shutdown(context.Background()) }()

	testEngine, _ := n.broker.(*TestEngine)
	require.EqualValues(t, 0, testEngine.publishLeaveCount)

	// Publish without options.
	err := n.publishLeave("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 1, testEngine.publishLeaveCount)

	// Publish with default/correct options.
	err = n.publishLeave("test_channel", &ClientInfo{})
	require.NoError(t, err)
	require.EqualValues(t, 2, testEngine.publishLeaveCount)
}

func TestNode_RemoveHistory(t *testing.T) {
	n := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = n.Shutdown(context.Background()) }()

	err := n.RemoveHistory("test_user")
	require.NoError(t, err)
}

func TestIndex(t *testing.T) {
	require.Equal(t, 0, index("2121", 1))
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
		_, err := node.Publish("bench", payload)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = node.Shutdown(context.Background())
}

func newFakeConn(b testing.TB, node *Node, channel string, protoType ProtocolType, sink chan []byte) {
	ctx, cancelFn := context.WithCancel(context.Background())
	transport := newTestTransport(cancelFn)
	transport.setProtocolType(protoType)
	transport.setSink(sink)
	newCtx := SetCredentials(ctx, &Credentials{UserID: "test"})
	client, _ := newClient(newCtx, node, transport)
	connectClient(b, client)
	rwWrapper := testReplyWriterWrapper()
	subCtx := client.subscribeCmd(&protocol.SubscribeRequest{
		Channel: channel,
	}, SubscribeReply{}, rwWrapper.rw, false)
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
			payload := []byte(`{"input": "test"}`)
			sink := make(chan []byte, bm.numSubscribers)
			for i := 0; i < bm.numSubscribers; i++ {
				bm.getFakeConn(b, n, "test", sink)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := n.Publish("test", payload)
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

func BenchmarkHistory(b *testing.B) {
	e := testMemoryEngine()
	numMessages := 100

	channel := "test"

	for i := 1; i <= numMessages; i++ {
		_, err := e.node.Publish(channel, []byte(`{}`), WithHistory(numMessages, time.Minute))
		require.NoError(b, err)
	}

	b.ResetTimer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := e.node.History(channel)
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

		n := nodeWithTestEngine()
		defer func() { _ = n.Shutdown(context.Background()) }()

		err := n.handleControl([]byte("random"))
		require.EqualError(t, err, "unexpected EOF")
	})

	t.Run("Node", func(t *testing.T) {
		t.Parallel()

		n := nodeWithTestEngine()
		defer func() { _ = n.Shutdown(context.Background()) }()

		enc := controlproto.NewProtobufEncoder()
		brokenCmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Method: controlpb.MethodTypeNode,
			Params: []byte("random"),
		})
		require.NoError(t, err)
		paramsBytes, err := enc.EncodeNode(&controlpb.Node{
			Name: "new_node",
		})
		require.NoError(t, err)
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			Method: controlpb.MethodTypeNode,
			Params: paramsBytes,
		})
		require.NoError(t, err)

		err = n.handleControl(brokenCmdBytes)
		require.EqualError(t, err, "unexpected EOF")
		err = n.handleControl(cmdBytes)
		require.NoError(t, err)
		require.NotContains(t, n.nodes.nodes, "new_node")
	})

	t.Run("Unsubscribe", func(t *testing.T) {
		t.Parallel()

		n := nodeWithMemoryEngineNoHandlers()
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

		client := newTestSubscribedClient(t, n, "42", "test_channel")

		enc := controlproto.NewProtobufEncoder()
		brokenCmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			UID:    client.uid,
			Method: controlpb.MethodTypeUnsubscribe,
			Params: []byte("random"),
		})
		require.NoError(t, err)
		paramsBytes, err := enc.EncodeUnsubscribe(&controlpb.Unsubscribe{
			Channel: "test_channel",
			User:    "42",
		})
		require.NoError(t, err)
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			UID:    client.uid,
			Method: controlpb.MethodTypeUnsubscribe,
			Params: paramsBytes,
		})
		require.NoError(t, err)

		err = n.handleControl(brokenCmdBytes)
		require.EqualError(t, err, "unexpected EOF")
		err = n.handleControl(cmdBytes)
		select {
		case <-done:
		case <-time.After(time.Second):
			require.Fail(t, "timeout")
		}
		require.NoError(t, err)
		require.NotContains(t, n.hub.subs, "test_channel")
	})

	t.Run("Disconnect", func(t *testing.T) {
		t.Parallel()

		n := nodeWithMemoryEngine()
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

		client := newTestSubscribedClient(t, n, "42", "test_channel")

		enc := controlproto.NewProtobufEncoder()
		brokenCmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			UID:    client.uid,
			Method: controlpb.MethodTypeDisconnect,
			Params: []byte("random"),
		})
		require.NoError(t, err)
		paramsBytes, err := enc.EncodeDisconnect(&controlpb.Disconnect{
			User: "42",
		})
		require.NoError(t, err)
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			UID:    client.uid,
			Method: controlpb.MethodTypeDisconnect,
			Params: paramsBytes,
		})
		require.NoError(t, err)

		err = n.handleControl(brokenCmdBytes)
		require.EqualError(t, err, "unexpected EOF")
		err = n.handleControl(cmdBytes)
		select {
		case <-done:
		case <-time.After(time.Second):
			require.Fail(t, "timeout")
		}
		require.NoError(t, err)
		require.NotContains(t, n.hub.subs, "test_channel")
		require.NotContains(t, n.hub.users, "42")
		require.NotContains(t, n.hub.conns, client.uid)
	})

	t.Run("Unknown", func(t *testing.T) {
		t.Parallel()

		n := nodeWithMemoryEngine()
		defer func() { _ = n.Shutdown(context.Background()) }()

		client := newTestSubscribedClient(t, n, "42", "test_channel")

		enc := controlproto.NewProtobufEncoder()
		cmdBytes, err := enc.EncodeCommand(&controlpb.Command{
			UID:    client.uid,
			Method: -1,
			Params: nil,
		})
		require.NoError(t, err)

		err = n.handleControl(cmdBytes)
		require.EqualError(t, err, "control method not found: -1")
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
	node := nodeWithMemoryEngineNoHandlers()
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

	results, err := node.Survey(context.Background(), "test_op", nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	res, ok := results[node.ID()]
	require.True(t, ok)
	require.Equal(t, uint32(1), res.Code)
	require.Equal(t, []byte("1"), res.Data)
}

func TestNode_OnSurvey_NoHandler(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	_, err := node.Survey(context.Background(), "test_op", nil)
	require.Error(t, err)
	require.Equal(t, errSurveyHandlerNotRegistered, err)
}

func TestNode_OnSurvey_Timeout(t *testing.T) {
	node := nodeWithMemoryEngineNoHandlers()
	defer func() { _ = node.Shutdown(context.Background()) }()

	done := make(chan struct{})

	node.OnSurvey(func(event SurveyEvent, callback SurveyCallback) {
		go func() {
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			time.Sleep(time.Second)
			callback(SurveyReply{
				Data: []byte("1"),
				Code: 1,
			})
		}()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := node.Survey(ctx, "test_op", nil)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
	close(done)
}

func TestErrors(t *testing.T) {
	err := ErrorUnauthorized
	protoErr := err.toProto()
	require.Equal(t, uint32(ErrorUnauthorized.Code), protoErr.Code)
	err = ErrorUnknownChannel
	errText := err.Error()
	require.Equal(t, "102: unknown channel", errText)
}
