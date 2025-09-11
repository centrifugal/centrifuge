package centrifuge

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/centrifugal/protocol"
	commonBroker "github.com/channelwill/cw2-live-chat-common/pkg/broker"
	"github.com/channelwill/cw2-live-chat-common/pkg/broker/gkafka"
	"github.com/channelwill/cw2-live-chat-common/pkg/zaplog"
)

var _ Broker = (*KafkaBroker)(nil)

// HistoryStorage defines interface for external history storage
type HistoryStorage interface {
	// AddToHistory adds a publication to channel history
	AddToHistory(ctx context.Context, channel string, pub *Publication, opts PublishOptions) (StreamPosition, error)
	// GetHistory retrieves channel history
	GetHistory(ctx context.Context, channel string, opts HistoryOptions) ([]*Publication, StreamPosition, error)
	// RemoveHistory removes channel history
	RemoveHistory(ctx context.Context, channel string) error
}

// KafkaBroker implements Broker interface using Kafka as message transport.
type KafkaBroker struct {
	node        *Node
	config      KafkaBrokerConfig
	kafkaBroker commonBroker.Broker
	closeOnce   sync.Once
	stopCh      chan struct{}

	// Event handler for broker events
	eventHandler BrokerEventHandler

	// Channel subscriptions tracking
	subscriptions sync.Map // map[string]bool

	// Topic names
	messagesTopic string

	// History storage
	historyStorage HistoryStorage

	// Subscription management
	subscriptionMu     sync.RWMutex
	isSubscribed       bool
	subscribedChannels map[string]bool

	// Admin client for topic management
	adminClient sarama.ClusterAdmin
}

// KafkaBrokerConfig configuration for Kafka broker.
// Only contains centrifuge-specific settings, Kafka settings use gkafka.KafkaConfig
type KafkaBrokerConfig struct {
	// Kafka configuration - use gkafka.KafkaConfig directly
	KafkaConfig gkafka.KafkaConfig

	// Topic prefix to avoid conflicts
	TopicPrefix string

	// Consumer group ID for this centrifuge node
	ConsumerGroupID string

	// Name of broker for observability
	Name string

	// External history storage (optional, defaults to memory storage)
	HistoryStorage HistoryStorage

	// Number of partitions for the main topic (for better load balancing)
	NumPartitions int

	// Topic management settings
	ReplicationFactor int16 // Topic replication factor, default 1
	RetentionHours    int   // Topic retention time in hours, default 24
	AutoCreateTopic   bool  // Whether to auto-create topic, default true
}

// NewKafkaBroker creates a new Kafka broker instance using Common package implementation.
func NewKafkaBroker(n *Node, config KafkaBrokerConfig) (*KafkaBroker, error) {
	if len(config.KafkaConfig.Brokers) == 0 {
		return nil, errors.New("kafka broker: no Kafka brokers provided in configuration")
	}

	// Set default values for centrifuge-specific config
	if config.TopicPrefix == "" {
		config.TopicPrefix = "centrifuge"
	}

	if config.ConsumerGroupID == "" {
		// Use a stable consumer group ID to avoid creating new groups on every restart
		// This prevents Kafka from accumulating consumer groups and crashing
		config.ConsumerGroupID = "centrifuge-kafka-consumer-group"
	}

	if config.NumPartitions == 0 {
		config.NumPartitions = 1
	}

	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = 1
	}

	if config.RetentionHours == 0 {
		config.RetentionHours = 24
	}

	// AutoCreateTopic defaults to true
	if !config.AutoCreateTopic {
		// Only set to false if explicitly configured
	} else {
		config.AutoCreateTopic = true
	}

	// Create Kafka broker options from KafkaConfig
	opts := []gkafka.Option{
		gkafka.WithBrokers(config.KafkaConfig.Brokers...),
	}

	// Add optional configurations if provided
	if config.KafkaConfig.Username != "" {
		opts = append(opts, gkafka.WithUsername(config.KafkaConfig.Username))
	}
	if config.KafkaConfig.Password != "" {
		opts = append(opts, gkafka.WithPassword(config.KafkaConfig.Password))
	}
	if config.KafkaConfig.SecurityProtocol != "" {
		opts = append(opts, gkafka.WithSecurityProtocol(config.KafkaConfig.SecurityProtocol))
	}
	if config.KafkaConfig.SASLMechanism != "" {
		opts = append(opts, gkafka.WithSASLMechanism(config.KafkaConfig.SASLMechanism))
	}
	if config.KafkaConfig.SASLTypeSCRAMSHA != "" {
		opts = append(opts, gkafka.WithSASLTypeSCRAMSHA(config.KafkaConfig.SASLTypeSCRAMSHA))
	}
	if config.KafkaConfig.CACertPath != "" {
		opts = append(opts, gkafka.WithCACertPath(config.KafkaConfig.CACertPath))
	}
	if config.KafkaConfig.Version != "" {
		opts = append(opts, gkafka.WithVersion(config.KafkaConfig.Version))
	}
	if config.KafkaConfig.DialTimeout > 0 {
		opts = append(opts, gkafka.WithDialTimeout(config.KafkaConfig.DialTimeout))
	}
	if config.KafkaConfig.ProducerTimeout > 0 {
		opts = append(opts, gkafka.WithProducerTimeout(config.KafkaConfig.ProducerTimeout))
	}
	if config.KafkaConfig.ProducerRetryMax > 0 {
		opts = append(opts, gkafka.WithProducerRetryMax(config.KafkaConfig.ProducerRetryMax))
	}
	if config.KafkaConfig.ProducerMaxMessageBytes > 0 {
		opts = append(opts, gkafka.WithProducerMaxMessageBytes(config.KafkaConfig.ProducerMaxMessageBytes))
	}
	if config.KafkaConfig.ConsumerAutoCommitInterval > 0 {
		opts = append(opts, gkafka.WithConsumerAutoCommitInterval(config.KafkaConfig.ConsumerAutoCommitInterval))
	}
	if config.KafkaConfig.ConsumerOffsetsInitial != 0 {
		opts = append(opts, gkafka.WithConsumerOffsetsInitial(config.KafkaConfig.ConsumerOffsetsInitial))
	}
	if config.KafkaConfig.GracefulWait > 0 {
		opts = append(opts, gkafka.WithGracefulWait(config.KafkaConfig.GracefulWait))
	}

	// Create underlying Kafka broker using Common package
	kafkaBroker, err := gkafka.New(config.KafkaConfig.Brokers, opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka broker: failed to create kafka client: %w", err)
	}

	// Create admin client for topic management
	var adminClient sarama.ClusterAdmin
	if config.AutoCreateTopic {
		adminClient, err = createAdminClient(config.KafkaConfig)
		if err != nil {
			return nil, fmt.Errorf("kafka broker: failed to create admin client: %w", err)
		}
	}

	// Initialize history storage
	var historyStorage HistoryStorage
	if config.HistoryStorage != nil {
		historyStorage = config.HistoryStorage
	} else {
		// 这里不应该使用内存存储，因为如果重启会导致历史消息丢失
		zaplog.GetGlobalLogger().ErrorWithCtx(context.Background(), nil, "using memory history storage as fallback - not suitable for production distributed setups")
		return nil, fmt.Errorf("kafka broker: failed to create history storage")
	}

	b := &KafkaBroker{
		node:               n,
		config:             config,
		kafkaBroker:        kafkaBroker,
		stopCh:             make(chan struct{}),
		messagesTopic:      config.TopicPrefix + ".messages",
		historyStorage:     historyStorage,
		subscribedChannels: make(map[string]bool),
		adminClient:        adminClient,
	}

	// Auto-create topic if enabled
	if config.AutoCreateTopic {
		if err := b.ensureTopicExists(); err != nil {
			return nil, fmt.Errorf("kafka broker: failed to ensure topic exists: %w", err)
		}
	}

	return b, nil
}

// createAdminClient creates a Sarama admin client with the same configuration as the broker
func createAdminClient(kafkaConfig gkafka.KafkaConfig) (sarama.ClusterAdmin, error) {
	config := sarama.NewConfig()

	// Set version
	if kafkaConfig.Version != "" {
		version, err := sarama.ParseKafkaVersion(kafkaConfig.Version)
		if err != nil {
			return nil, fmt.Errorf("parse kafka version: %w", err)
		}
		config.Version = version
	} else {
		config.Version = sarama.V2_2_0_0 // Default version
	}

	// Set timeout
	if kafkaConfig.DialTimeout > 0 {
		config.Net.DialTimeout = kafkaConfig.DialTimeout
	} else {
		config.Net.DialTimeout = 12 * time.Second
	}

	// Set security protocol
	if kafkaConfig.SecurityProtocol == "SASL_SSL" || kafkaConfig.SecurityProtocol == "SSL" {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: false, // Always verify certificates for security
		}

		// Load CA cert if provided
		if kafkaConfig.CACertPath != "" {
			// CA cert loading logic would go here if needed
			// For now, use system root CAs with proper verification
		}
	}

	// Set SASL configuration
	if kafkaConfig.Username != "" && kafkaConfig.Password != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = kafkaConfig.Username
		config.Net.SASL.Password = kafkaConfig.Password

		switch kafkaConfig.SASLMechanism {
		case "PLAIN":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			// Don't default to PLAIN as it's insecure
			// Require explicit SASL mechanism specification
			return nil, fmt.Errorf("invalid or missing SASL mechanism: %s. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", kafkaConfig.SASLMechanism)
		}
	}

	return sarama.NewClusterAdmin(kafkaConfig.Brokers, config)
}

// ensureTopicExists creates the topic if it doesn't exist, or validates existing topic configuration
func (b *KafkaBroker) ensureTopicExists() error {
	if b.adminClient == nil {
		return errors.New("admin client not available")
	}

	// First, try to list topics to check if our topic exists
	topics, err := b.adminClient.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list topics: %w", err)
	}

	if _, exists := topics[b.messagesTopic]; exists {
		// Topic exists, validate partition count using metadata
		metadata, err := b.getTopicMetadata()
		if err != nil {
			b.node.logger.log(newLogEntry(LogLevelWarn, "failed to get topic metadata for validation", map[string]any{
				"topic":       b.messagesTopic,
				"error":       err.Error(),
				"broker_name": b.config.Name,
			}))
			// Continue anyway since topic exists
			return nil
		}

		if metadata != nil {
			actualPartitions := len(metadata.Partitions)
			if actualPartitions != b.config.NumPartitions {
				b.node.logger.log(newLogEntry(LogLevelWarn, "topic partition count mismatch", map[string]any{
					"topic":             b.messagesTopic,
					"actual_partitions": actualPartitions,
					"config_partitions": b.config.NumPartitions,
					"broker_name":       b.config.Name,
				}))
				// Note: We don't automatically modify partition count as it's a destructive operation
			} else {
				b.node.logger.log(newLogEntry(LogLevelInfo, "topic validated successfully", map[string]any{
					"topic":       b.messagesTopic,
					"partitions":  actualPartitions,
					"broker_name": b.config.Name,
				}))
			}
		}
		return nil
	}

	// Topic doesn't exist, create it
	return b.createTopic()
}

// getTopicMetadata gets metadata for the topic using a separate client connection
func (b *KafkaBroker) getTopicMetadata() (*sarama.TopicMetadata, error) {
	// Create a temporary client just for metadata
	config := sarama.NewConfig()
	config.Version = sarama.V2_2_0_0
	client, err := sarama.NewClient(b.config.KafkaConfig.Brokers, config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// Request metadata for our specific topic
	req := &sarama.MetadataRequest{
		Topics: []string{b.messagesTopic},
	}

	broker := client.Brokers()[0] // Use first available broker
	response, err := broker.GetMetadata(req)
	if err != nil {
		return nil, err
	}

	// Find our topic in the response
	for _, topic := range response.Topics {
		if topic.Name == b.messagesTopic {
			return topic, nil
		}
	}

	return nil, fmt.Errorf("topic not found in metadata response")
}

// createTopic creates a new Kafka topic with the configured settings
func (b *KafkaBroker) createTopic() error {
	topicConfig := map[string]*string{
		"retention.ms":   stringPtr(fmt.Sprintf("%d", b.config.RetentionHours*3600*1000)), // Convert hours to milliseconds
		"cleanup.policy": stringPtr("delete"),
	}

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(b.config.NumPartitions),
		ReplicationFactor: b.config.ReplicationFactor,
		ConfigEntries:     topicConfig,
	}

	if err := b.adminClient.CreateTopic(b.messagesTopic, topicDetail, false); err != nil {
		return fmt.Errorf("failed to create topic %s: %w", b.messagesTopic, err)
	}

	b.node.logger.log(newLogEntry(LogLevelInfo, "topic created successfully", map[string]any{
		"topic":              b.messagesTopic,
		"partitions":         b.config.NumPartitions,
		"replication_factor": b.config.ReplicationFactor,
		"retention_hours":    b.config.RetentionHours,
		"broker_name":        b.config.Name,
	}))

	return nil
}

// stringPtr is a helper function to create string pointers for topic configuration
func stringPtr(s string) *string {
	return &s
}

// RegisterBrokerEventHandler registers event handler.
func (b *KafkaBroker) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	b.eventHandler = h

	// Start the global message consumer when event handler is registered
	return b.startGlobalConsumer()
}

// startGlobalConsumer starts a single consumer for all messages
func (b *KafkaBroker) startGlobalConsumer() error {
	b.subscriptionMu.Lock()
	defer b.subscriptionMu.Unlock()

	if b.isSubscribed {
		return nil
	}

	// 启动单个消费者，使用修复后的非阻塞Subscribe
	err := b.kafkaBroker.Subscribe(
		context.Background(),
		b.messagesTopic,
		b.config.ConsumerGroupID,
		b.handleMessage,
		commonBroker.WithSubPullGoroutines(2),
		commonBroker.WithEnableSubUseMsgBuffer(),
		commonBroker.WithSubMsgBufferSize(1024),
		commonBroker.WithSubMsgBufferGoroutines(4),
	)

	if err != nil {
		b.node.logger.log(newErrorLogEntry(err, "kafka broker start consumer failed", map[string]any{
			"topic":       b.messagesTopic,
			"broker_name": b.config.Name,
		}))
		return err
	}

	b.isSubscribed = true
	b.node.logger.log(newLogEntry(LogLevelInfo, "kafka broker global consumer started", map[string]any{
		"topic":          b.messagesTopic,
		"consumer_group": b.config.ConsumerGroupID,
		"broker_name":    b.config.Name,
	}))

	return nil
}

// Subscribe subscribes to a channel.
func (b *KafkaBroker) Subscribe(ch string) error {
	if b.node.logEnabled(LogLevelDebug) {
		b.node.logger.log(newLogEntry(LogLevelDebug, "kafka broker subscribe to channel", map[string]any{
			"broker_name": b.config.Name,
			"channel":     ch,
		}))
	}

	// Mark as subscribed
	b.subscriptions.Store(ch, true)

	b.subscriptionMu.Lock()
	b.subscribedChannels[ch] = true
	b.subscriptionMu.Unlock()

	return nil
}

// Unsubscribe unsubscribes from a channel.
func (b *KafkaBroker) Unsubscribe(ch string) error {
	if b.node.logEnabled(LogLevelDebug) {
		b.node.logger.log(newLogEntry(LogLevelDebug, "kafka broker unsubscribe from channel", map[string]any{
			"broker_name": b.config.Name,
			"channel":     ch,
		}))
	}

	// Remove from subscriptions
	b.subscriptions.Delete(ch)

	b.subscriptionMu.Lock()
	delete(b.subscribedChannels, ch)
	b.subscriptionMu.Unlock()

	return nil
}

// Publish publishes data to a channel using Common package implementation.
func (b *KafkaBroker) Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, bool, error) {
	// Create publication message
	protoPub := &protocol.Publication{
		Data: data,
		Info: infoToProto(opts.ClientInfo),
		Tags: opts.Tags,
		Time: time.Now().UnixMilli(),
	}

	byteMessage, err := protoPub.MarshalVT()
	if err != nil {
		return StreamPosition{}, false, err
	}

	// Handle history if enabled
	var sp StreamPosition
	if opts.HistorySize > 0 && opts.HistoryTTL > 0 {
		publication := pubFromProto(protoPub)
		sp, err = b.historyStorage.AddToHistory(context.Background(), ch, publication, opts)
		if err != nil {
			return StreamPosition{}, false, fmt.Errorf("failed to add to history: %w", err)
		}
	}

	// Create message wrapper with metadata
	msgWrapper := KafkaMessage{
		Type:           MessageTypePublication,
		Channel:        ch,
		Data:           byteMessage,
		StreamPosition: sp,
	}

	msgData, err := json.Marshal(msgWrapper)
	if err != nil {
		return StreamPosition{}, false, err
	}

	// Publish using Common package broker with channel as key for proper partitioning
	err = b.kafkaBroker.Publish(
		context.Background(),
		b.messagesTopic,
		msgData,
		commonBroker.WithPublishName(ch), // Use channel as partition key
	)
	if err != nil {
		return StreamPosition{}, false, err
	}

	return sp, false, nil
}

// PublishJoin publishes join event to a channel using Common package implementation.
func (b *KafkaBroker) PublishJoin(ch string, info *ClientInfo) error {
	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	msgWrapper := KafkaMessage{
		Type:    MessageTypeJoin,
		Channel: ch,
		Data:    byteMessage,
	}

	msgData, err := json.Marshal(msgWrapper)
	if err != nil {
		return err
	}

	return b.kafkaBroker.Publish(
		context.Background(),
		b.messagesTopic,
		msgData,
		commonBroker.WithPublishName(ch),
	)
}

// PublishLeave publishes leave event to a channel using Common package implementation.
func (b *KafkaBroker) PublishLeave(ch string, info *ClientInfo) error {
	byteMessage, err := infoToProto(info).MarshalVT()
	if err != nil {
		return err
	}

	msgWrapper := KafkaMessage{
		Type:    MessageTypeLeave,
		Channel: ch,
		Data:    byteMessage,
	}

	msgData, err := json.Marshal(msgWrapper)
	if err != nil {
		return err
	}

	return b.kafkaBroker.Publish(
		context.Background(),
		b.messagesTopic,
		msgData,
		commonBroker.WithPublishName(ch),
	)
}

// History returns channel history using external storage.
func (b *KafkaBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	return b.historyStorage.GetHistory(context.Background(), ch, opts)
}

// RemoveHistory removes history for a channel using external storage.
func (b *KafkaBroker) RemoveHistory(ch string) error {
	return b.historyStorage.RemoveHistory(context.Background(), ch)
}

// Close closes the broker using Common package implementation.
func (b *KafkaBroker) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		close(b.stopCh)
		if b.kafkaBroker != nil {
			_ = b.kafkaBroker.Shutdown(ctx)
		}
		if b.adminClient != nil {
			_ = b.adminClient.Close()
		}
	})
	return nil
}

// handleMessage is the unified message handler for all channels
func (b *KafkaBroker) handleMessage(ctx context.Context, data []byte) error {
	if b.eventHandler == nil {
		return nil
	}

	var msgWrapper KafkaMessage
	if err := json.Unmarshal(data, &msgWrapper); err != nil {
		return err
	}

	// Check if we're subscribed to this channel
	if _, subscribed := b.subscriptions.Load(msgWrapper.Channel); !subscribed {
		return nil // Ignore messages for unsubscribed channels
	}

	// Route to appropriate handler based on message type
	switch msgWrapper.Type {
	case MessageTypePublication:
		return b.handlePublication(msgWrapper)
	case MessageTypeJoin:
		return b.handleJoin(msgWrapper)
	case MessageTypeLeave:
		return b.handleLeave(msgWrapper)
	default:
		return fmt.Errorf("unknown message type: %d", msgWrapper.Type)
	}
}

func (b *KafkaBroker) handlePublication(msgWrapper KafkaMessage) error {
	var pub protocol.Publication
	if err := pub.UnmarshalVT(msgWrapper.Data); err != nil {
		return err
	}

	// Set offset from stream position
	pub.Offset = msgWrapper.StreamPosition.Offset

	return b.eventHandler.HandlePublication(
		msgWrapper.Channel,
		pubFromProto(&pub),
		msgWrapper.StreamPosition,
		false, // delta not supported yet
		nil,   // prevPub not supported yet
	)
}

func (b *KafkaBroker) handleJoin(msgWrapper KafkaMessage) error {
	var info protocol.ClientInfo
	if err := info.UnmarshalVT(msgWrapper.Data); err != nil {
		return err
	}

	return b.eventHandler.HandleJoin(msgWrapper.Channel, infoFromProto(&info))
}

func (b *KafkaBroker) handleLeave(msgWrapper KafkaMessage) error {
	var info protocol.ClientInfo
	if err := info.UnmarshalVT(msgWrapper.Data); err != nil {
		return err
	}

	return b.eventHandler.HandleLeave(msgWrapper.Channel, infoFromProto(&info))
}

// KafkaMessage wraps messages sent through Kafka
type KafkaMessage struct {
	Type           MessageType    `json:"type"`
	Channel        string         `json:"channel"`
	Data           []byte         `json:"data"`
	StreamPosition StreamPosition `json:"stream_position,omitempty"`
}

// MessageType defines the type of message
type MessageType int

const (
	MessageTypePublication MessageType = iota
	MessageTypeJoin
	MessageTypeLeave
)

// MemoryHistoryStorage implements HistoryStorage interface using in-memory storage
// This is the default fallback, but should not be used in production distributed setups
type MemoryHistoryStorage struct {
	mu             sync.RWMutex
	history        map[string][]*Publication // channel -> publications
	positions      map[string]StreamPosition // channel -> current position
	historyMaxSize map[string]int            // channel -> max history size
	historyExpiry  map[string]time.Time      // channel -> expiry time
}

// NewMemoryHistoryStorage creates a new memory-based history storage
func NewMemoryHistoryStorage() *MemoryHistoryStorage {
	return &MemoryHistoryStorage{
		history:        make(map[string][]*Publication),
		positions:      make(map[string]StreamPosition),
		historyMaxSize: make(map[string]int),
		historyExpiry:  make(map[string]time.Time),
	}
}

func (m *MemoryHistoryStorage) AddToHistory(ctx context.Context, channel string, pub *Publication, opts PublishOptions) (StreamPosition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get or create current position
	currentPos, exists := m.positions[channel]
	if !exists {
		currentPos = StreamPosition{
			Offset: 0,
			Epoch:  strconv.FormatInt(time.Now().UnixNano(), 10),
		}
	}

	// Increment offset
	currentPos.Offset++

	// Create publication with offset
	pub.Offset = currentPos.Offset

	// Add to history
	history := m.history[channel]
	history = append(history, pub)

	// Apply history size limit
	if len(history) > opts.HistorySize {
		// Remove oldest messages
		history = history[len(history)-opts.HistorySize:]
	}

	// Update maps
	m.history[channel] = history
	m.positions[channel] = currentPos
	m.historyMaxSize[channel] = opts.HistorySize
	m.historyExpiry[channel] = time.Now().Add(opts.HistoryTTL)

	return currentPos, nil
}

func (m *MemoryHistoryStorage) GetHistory(ctx context.Context, channel string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Get current position
	currentPos, exists := m.positions[channel]
	if !exists {
		return nil, StreamPosition{}, nil
	}

	// Get history
	history, exists := m.history[channel]
	if !exists {
		return nil, currentPos, nil
	}

	// Clean up expired history
	if expiry, exists := m.historyExpiry[channel]; exists && time.Now().After(expiry) {
		delete(m.history, channel)
		delete(m.positions, channel)
		delete(m.historyMaxSize, channel)
		delete(m.historyExpiry, channel)
		return nil, StreamPosition{}, nil
	}

	// Apply filters
	filter := opts.Filter
	if filter.Limit == 0 {
		return nil, currentPos, nil
	}

	publications := make([]*Publication, 0)

	// Handle since position
	startIdx := 0
	if filter.Since != nil {
		for i, pub := range history {
			if pub.Offset > filter.Since.Offset {
				startIdx = i
				break
			}
		}
	}

	// Apply limit
	endIdx := len(history)
	if filter.Limit > 0 {
		requestedEnd := startIdx + filter.Limit
		if requestedEnd < endIdx {
			endIdx = requestedEnd
		}
	}

	// Extract publications
	for i := startIdx; i < endIdx; i++ {
		publications = append(publications, history[i])
	}

	// Handle reverse order
	if filter.Reverse {
		for i, j := 0, len(publications)-1; i < j; i, j = i+1, j-1 {
			publications[i], publications[j] = publications[j], publications[i]
		}
	}

	return publications, currentPos, nil
}

func (m *MemoryHistoryStorage) RemoveHistory(ctx context.Context, channel string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.history, channel)
	delete(m.positions, channel)
	delete(m.historyMaxSize, channel)
	delete(m.historyExpiry, channel)

	return nil
}

// extractChannelFromTopic extracts channel name from Kafka topic
func (b *KafkaBroker) extractChannelFromTopic(topic string, prefix string) string {
	return strings.TrimPrefix(topic, prefix)
}

// RedisHistoryStorage placeholder removed - implementation moved to history_storage_redis.go
