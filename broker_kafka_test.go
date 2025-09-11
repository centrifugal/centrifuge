package centrifuge

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/channelwill/cw2-live-chat-common/pkg/broker/gkafka"
	"github.com/stretchr/testify/require"
)

func testKafkaNode(t *testing.T) *Node {
	t.Helper()
	node, err := New(Config{
		LogLevel:   LogLevelDebug,
		LogHandler: func(entry LogEntry) {},
	})
	require.NoError(t, err)
	return node
}

func TestKafkaBroker_NoKafkaBrokers(t *testing.T) {
	node := testKafkaNode(t)
	_, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{}, // empty brokers
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no Kafka brokers provided")
}

func TestKafkaBroker_DefaultConfig(t *testing.T) {
	node := testKafkaNode(t)
	config := KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
		AutoCreateTopic: false, // Disable auto-create for tests
	}

	broker, err := NewKafkaBroker(node, config)
	require.NoError(t, err)
	require.NotNil(t, broker)
	require.Equal(t, "centrifuge", broker.config.TopicPrefix)
	require.Contains(t, broker.config.ConsumerGroupID, "centrifuge-node-")
	require.Equal(t, 1, broker.config.NumPartitions)
	require.Equal(t, int16(1), broker.config.ReplicationFactor)
	require.Equal(t, 24, broker.config.RetentionHours)
	require.False(t, broker.config.AutoCreateTopic)
	require.NotNil(t, broker.historyStorage)

	// Test that default history storage is memory storage
	_, ok := broker.historyStorage.(*MemoryHistoryStorage)
	require.True(t, ok, "Default history storage should be MemoryHistoryStorage")
}

func TestKafkaBroker_CustomConfig(t *testing.T) {
	node := testKafkaNode(t)
	customHistoryStorage := NewMemoryHistoryStorage()
	config := KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
		TopicPrefix:       "custom-prefix",
		ConsumerGroupID:   "custom-group",
		Name:              "test-broker",
		HistoryStorage:    customHistoryStorage,
		NumPartitions:     3,
		ReplicationFactor: 2,
		RetentionHours:    48,
		AutoCreateTopic:   false, // Disable auto-create for tests
	}

	broker, err := NewKafkaBroker(node, config)
	require.NoError(t, err)
	require.NotNil(t, broker)
	require.Equal(t, "custom-prefix", broker.config.TopicPrefix)
	require.Equal(t, "custom-group", broker.config.ConsumerGroupID)
	require.Equal(t, "test-broker", broker.config.Name)
	require.Equal(t, 3, broker.config.NumPartitions)
	require.Equal(t, int16(2), broker.config.ReplicationFactor)
	require.Equal(t, 48, broker.config.RetentionHours)
	require.False(t, broker.config.AutoCreateTopic)
	require.Equal(t, customHistoryStorage, broker.historyStorage)
}

func TestKafkaBroker_RegisterEventHandler(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	handler := &testBrokerEventHandler{}
	err = broker.RegisterBrokerEventHandler(handler)
	require.NoError(t, err)
	require.Equal(t, handler, broker.eventHandler)
	require.True(t, broker.isSubscribed)
}

func TestKafkaBroker_SubscribeUnsubscribe(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	channel := "test-channel"

	// Test subscribe
	err = broker.Subscribe(channel)
	require.NoError(t, err)

	// Check that channel is marked as subscribed
	_, exists := broker.subscriptions.Load(channel)
	require.True(t, exists)

	broker.subscriptionMu.RLock()
	subscribed := broker.subscribedChannels[channel]
	broker.subscriptionMu.RUnlock()
	require.True(t, subscribed)

	// Test unsubscribe
	err = broker.Unsubscribe(channel)
	require.NoError(t, err)

	// Check that channel is unmarked
	_, exists = broker.subscriptions.Load(channel)
	require.False(t, exists)

	broker.subscriptionMu.RLock()
	subscribed = broker.subscribedChannels[channel]
	broker.subscriptionMu.RUnlock()
	require.False(t, subscribed)
}

func TestKafkaBroker_PublishWithoutHistory(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	channel := "test-channel"
	data := []byte(`{"message": "test"}`)

	sp, suppressed, err := broker.Publish(channel, data, PublishOptions{})
	require.NoError(t, err)
	require.False(t, suppressed)
	require.Equal(t, uint64(0), sp.Offset) // No history means no offset
	require.Equal(t, "", sp.Epoch)
}

func TestKafkaBroker_PublishWithHistory(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	channel := "test-channel"
	data := []byte(`{"message": "test"}`)
	opts := PublishOptions{
		HistorySize: 10,
		HistoryTTL:  time.Minute,
	}

	sp1, suppressed, err := broker.Publish(channel, data, opts)
	require.NoError(t, err)
	require.False(t, suppressed)
	require.Equal(t, uint64(1), sp1.Offset)
	require.NotEmpty(t, sp1.Epoch)

	sp2, suppressed, err := broker.Publish(channel, data, opts)
	require.NoError(t, err)
	require.False(t, suppressed)
	require.Equal(t, uint64(2), sp2.Offset)
	require.Equal(t, sp1.Epoch, sp2.Epoch) // Same epoch for same channel

	// Test history retrieval
	pubs, sp, err := broker.History(channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 2)
	require.Equal(t, uint64(2), sp.Offset)
	require.Equal(t, sp1.Epoch, sp.Epoch)
}

func TestKafkaBroker_PublishJoinLeave(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	channel := "test-channel"
	clientInfo := &ClientInfo{
		ClientID: "client-123",
		UserID:   "user-456",
	}

	err = broker.PublishJoin(channel, clientInfo)
	require.NoError(t, err)

	err = broker.PublishLeave(channel, clientInfo)
	require.NoError(t, err)
}

func TestKafkaBroker_History(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	channel := "test-channel"
	data := []byte(`{"message": "test"}`)
	opts := PublishOptions{
		HistorySize: 5,
		HistoryTTL:  time.Minute,
	}

	// Publish some messages
	for i := 0; i < 3; i++ {
		_, _, err := broker.Publish(channel, data, opts)
		require.NoError(t, err)
	}

	// Test get all history
	pubs, sp, err := broker.History(channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3)
	require.Equal(t, uint64(3), sp.Offset)

	// Test limit
	pubs, _, err = broker.History(channel, HistoryOptions{
		Filter: HistoryFilter{Limit: 2},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 2)

	// Test since
	pubs, _, err = broker.History(channel, HistoryOptions{
		Filter: HistoryFilter{
			Limit: -1,
			Since: &StreamPosition{Offset: 1, Epoch: sp.Epoch},
		},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 2) // Should get publications 2 and 3
	require.Equal(t, uint64(2), pubs[0].Offset)
	require.Equal(t, uint64(3), pubs[1].Offset)

	// Test remove history
	err = broker.RemoveHistory(channel)
	require.NoError(t, err)

	pubs, _, err = broker.History(channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 0)
}

func TestKafkaBroker_HistorySizeLimit(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	channel := "test-channel"
	data := []byte(`{"message": "test"}`)
	opts := PublishOptions{
		HistorySize: 2, // Small history size
		HistoryTTL:  time.Minute,
	}

	// Publish more messages than history size
	_, _, err = broker.Publish(channel, data, opts)
	require.NoError(t, err)
	_, _, err = broker.Publish(channel, data, opts)
	require.NoError(t, err)
	sp5, _, err := broker.Publish(channel, data, opts)
	require.NoError(t, err)

	// Should only have 2 messages (most recent)
	pubs, sp, err := broker.History(channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 2)
	require.Equal(t, sp5.Offset, sp.Offset)     // Current offset should be 3
	require.Equal(t, uint64(2), pubs[0].Offset) // First pub should be offset 2
	require.Equal(t, uint64(3), pubs[1].Offset) // Second pub should be offset 3
}

func TestKafkaBroker_Close(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	err = broker.Close(context.Background())
	require.NoError(t, err)

	// Verify that stop channel is closed
	select {
	case <-broker.stopCh:
		// Expected
	default:
		t.Fatal("stopCh should be closed")
	}
}

func TestKafkaBroker_MessageHandling(t *testing.T) {
	node := testKafkaNode(t)
	broker, err := NewKafkaBroker(node, KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	})
	require.NoError(t, err)

	var receivedPublications int32
	var receivedJoins int32
	var receivedLeaves int32

	handler := &testBrokerEventHandler{
		HandlePublicationFunc: func(ch string, pub *Publication, sp StreamPosition, useDelta bool, prevPub *Publication) error {
			atomic.AddInt32(&receivedPublications, 1)
			require.Equal(t, "test-channel", ch)
			return nil
		},
		HandleJoinFunc: func(ch string, info *ClientInfo) error {
			atomic.AddInt32(&receivedJoins, 1)
			require.Equal(t, "test-channel", ch)
			return nil
		},
		HandleLeaveFunc: func(ch string, info *ClientInfo) error {
			atomic.AddInt32(&receivedLeaves, 1)
			require.Equal(t, "test-channel", ch)
			return nil
		},
	}

	err = broker.RegisterBrokerEventHandler(handler)
	require.NoError(t, err)

	// Subscribe to channel
	err = broker.Subscribe("test-channel")
	require.NoError(t, err)

	// Test that unsubscribed channels are ignored
	err = broker.Subscribe("ignored-channel")
	require.NoError(t, err)
	err = broker.Unsubscribe("ignored-channel")
	require.NoError(t, err)

	// Simulate message handling for subscribed channel
	pubMsg := KafkaMessage{
		Type:    MessageTypePublication,
		Channel: "test-channel",
		Data:    []byte(`{"data":"test"}`),
	}
	err = broker.handleMessage(context.Background(), mustMarshal(pubMsg))
	require.NoError(t, err)

	joinMsg := KafkaMessage{
		Type:    MessageTypeJoin,
		Channel: "test-channel",
		Data:    []byte(`{"client_id":"test"}`),
	}
	err = broker.handleMessage(context.Background(), mustMarshal(joinMsg))
	require.NoError(t, err)

	leaveMsg := KafkaMessage{
		Type:    MessageTypeLeave,
		Channel: "test-channel",
		Data:    []byte(`{"client_id":"test"}`),
	}
	err = broker.handleMessage(context.Background(), mustMarshal(leaveMsg))
	require.NoError(t, err)

	// Simulate message for unsubscribed channel (should be ignored)
	ignoredMsg := KafkaMessage{
		Type:    MessageTypePublication,
		Channel: "ignored-channel",
		Data:    []byte(`{"data":"test"}`),
	}
	err = broker.handleMessage(context.Background(), mustMarshal(ignoredMsg))
	require.NoError(t, err)

	require.Equal(t, int32(1), atomic.LoadInt32(&receivedPublications))
	require.Equal(t, int32(1), atomic.LoadInt32(&receivedJoins))
	require.Equal(t, int32(1), atomic.LoadInt32(&receivedLeaves))
}

func TestMemoryHistoryStorage(t *testing.T) {
	storage := NewMemoryHistoryStorage()
	channel := "test-channel"

	// Test empty history
	pubs, sp, err := storage.GetHistory(context.Background(), channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 0)
	require.Equal(t, uint64(0), sp.Offset)

	// Add some publications
	pub1 := &Publication{Data: []byte("test1")}
	opts := PublishOptions{HistorySize: 3, HistoryTTL: time.Minute}

	sp1, err := storage.AddToHistory(context.Background(), channel, pub1, opts)
	require.NoError(t, err)
	require.Equal(t, uint64(1), sp1.Offset)
	require.Equal(t, uint64(1), pub1.Offset)

	pub2 := &Publication{Data: []byte("test2")}
	sp2, err := storage.AddToHistory(context.Background(), channel, pub2, opts)
	require.NoError(t, err)
	require.Equal(t, uint64(2), sp2.Offset)
	require.Equal(t, sp1.Epoch, sp2.Epoch)

	// Test history retrieval
	pubs, sp, err = storage.GetHistory(context.Background(), channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 2)
	require.Equal(t, uint64(2), sp.Offset)

	// Test history limit
	for i := 0; i < 5; i++ {
		pub := &Publication{Data: []byte("test")}
		_, err := storage.AddToHistory(context.Background(), channel, pub, opts)
		require.NoError(t, err)
	}

	pubs, _, err = storage.GetHistory(context.Background(), channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 3) // Should respect history size limit

	// Test remove history
	err = storage.RemoveHistory(context.Background(), channel)
	require.NoError(t, err)

	pubs, _, err = storage.GetHistory(context.Background(), channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 0)
}

// Helper functions

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		// In tests, we can use testing.T to handle failures properly
		// For now, return empty data as this is a test helper
		return []byte{}
	}
	return data
}

func TestKafkaBroker_TopicAutoCreation(t *testing.T) {
	// Skip this test if no real Kafka cluster is available
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	node := testKafkaNode(t)
	config := KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
		TopicPrefix:       "test-auto-create",
		NumPartitions:     3,
		ReplicationFactor: 1,
		RetentionHours:    1,
		AutoCreateTopic:   true, // Enable auto-create
	}

	// This should create the topic automatically
	broker, err := NewKafkaBroker(node, config)
	if err != nil {
		// If we can't connect to Kafka, skip the test
		t.Skipf("Failed to create broker, likely no Kafka available: %v", err)
	}
	require.NotNil(t, broker)
	require.True(t, broker.config.AutoCreateTopic)
	require.NotNil(t, broker.adminClient)

	// Clean up
	defer broker.Close(context.Background())
}

func TestKafkaBroker_TopicValidation(t *testing.T) {
	node := testKafkaNode(t)

	// Test disabled auto-create
	config := KafkaBrokerConfig{
		KafkaConfig: gkafka.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
		AutoCreateTopic: false,
	}

	broker, err := NewKafkaBroker(node, config)
	require.NoError(t, err)
	require.NotNil(t, broker)
	require.False(t, broker.config.AutoCreateTopic)
	require.Nil(t, broker.adminClient)

	// Clean up
	defer broker.Close(context.Background())
}
