package centrifuge

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/channelwill/cw2-live-chat-common/pkg/storage/gredis"
	"github.com/redis/go-redis/v9"
)

var _ HistoryStorage = (*RedisHistoryStorage)(nil)

// RedisHistoryStorage implements HistoryStorage interface using Redis Streams
// This implementation provides distributed history storage for Centrifuge broker
type RedisHistoryStorage struct {
	client redis.UniversalClient
	prefix string
	name   string // Storage name for logging/metrics
}

// RedisHistoryStorageConfig configuration for Redis history storage
type RedisHistoryStorageConfig struct {
	// Redis client (can be obtained from gredis package)
	Client redis.UniversalClient

	// Key prefix for all Redis keys (default: "centrifuge")
	Prefix string

	// Storage name for observability (default: "redis-history")
	Name string
}

// NewRedisHistoryStorage creates a new Redis-based history storage using common package
func NewRedisHistoryStorage(config RedisHistoryStorageConfig) *RedisHistoryStorage {
	if config.Prefix == "" {
		config.Prefix = "centrifuge"
	}

	if config.Name == "" {
		config.Name = "redis-history"
	}

	return &RedisHistoryStorage{
		client: config.Client,
		prefix: config.Prefix,
		name:   config.Name,
	}
}

// NewRedisHistoryStorageFromConf creates storage from gredis configuration
func NewRedisHistoryStorageFromConf(redisConf gredis.RedisConf, prefix string) *RedisHistoryStorage {
	client := redisConf.InitClient()
	return NewRedisHistoryStorage(RedisHistoryStorageConfig{
		Client: client,
		Prefix: prefix,
		Name:   "redis-history",
	})
}

// AddToHistory adds a publication to channel history using Redis STREAM
func (r *RedisHistoryStorage) AddToHistory(ctx context.Context, channel string, pub *Publication, opts PublishOptions) (StreamPosition, error) {
	if opts.HistorySize <= 0 || opts.HistoryTTL <= 0 {
		return StreamPosition{}, nil
	}

	// Convert publication to protobuf for storage
	protoPub := &protocol.Publication{
		Data: pub.Data,
		Info: infoToProto(pub.Info),
		Tags: pub.Tags,
		Time: time.Now().UnixMilli(),
	}

	// Serialize publication
	pubData, err := protoPub.MarshalVT()
	if err != nil {
		return StreamPosition{}, fmt.Errorf("failed to marshal publication: %w", err)
	}

	// Redis keys
	streamKey := r.streamKey(channel)
	metaKey := r.metaKey(channel)

	// Use pipeline for atomicity
	pipe := r.client.Pipeline()

	// Add to stream with auto-generated ID
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: int64(opts.HistorySize),
		Approx: true, // Use approximate trimming for better performance
		Values: map[string]interface{}{
			"data": pubData,
		},
	})

	// Update metadata (current position and epoch)
	currentTime := time.Now().Unix()
	epoch := r.generateEpoch(channel, currentTime)

	pipe.HMSet(ctx, metaKey, map[string]interface{}{
		"epoch":      epoch,
		"updated_at": currentTime,
	})

	// Set TTL on both keys
	pipe.Expire(ctx, streamKey, opts.HistoryTTL)
	pipe.Expire(ctx, metaKey, opts.HistoryTTL)

	// Execute pipeline
	results, err := pipe.Exec(ctx)
	if err != nil {
		return StreamPosition{}, fmt.Errorf("failed to add to history: %w", err)
	}

	// Extract stream ID and convert to offset
	streamID := results[0].(*redis.StringCmd).Val()
	offset, err := r.parseStreamIDToOffset(streamID)
	if err != nil {
		return StreamPosition{}, fmt.Errorf("failed to parse stream ID: %w", err)
	}

	// Update publication with position info
	pub.Offset = offset

	return StreamPosition{
		Offset: offset,
		Epoch:  epoch,
	}, nil
}

// GetHistory retrieves channel history from Redis STREAM
func (r *RedisHistoryStorage) GetHistory(ctx context.Context, channel string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	streamKey := r.streamKey(channel)
	metaKey := r.metaKey(channel)

	// Get current position
	currentPos, err := r.getCurrentPosition(ctx, metaKey)
	if err != nil {
		return nil, StreamPosition{}, fmt.Errorf("failed to get current position: %w", err)
	}

	// If only requesting current position
	if opts.Filter.Limit == 0 {
		return nil, currentPos, nil
	}

	// Determine start and end for XRANGE/XREVRANGE
	start, end, count := r.buildRangeArgs(opts.Filter, currentPos)

	var streamResults []redis.XMessage
	var rangeErr error

	if opts.Filter.Reverse {
		// Use XREVRANGE for reverse order
		streamResults, rangeErr = r.client.XRevRangeN(ctx, streamKey, end, start, int64(count)).Result()
	} else {
		// Use XRANGE for normal order
		streamResults, rangeErr = r.client.XRangeN(ctx, streamKey, start, end, int64(count)).Result()
	}

	if rangeErr != nil && rangeErr != redis.Nil {
		return nil, currentPos, fmt.Errorf("failed to get history: %w", rangeErr)
	}

	// Convert Redis stream messages to publications
	publications := make([]*Publication, 0, len(streamResults))
	for _, msg := range streamResults {
		pub, err := r.parseStreamMessage(msg)
		if err != nil {
			continue // Skip malformed messages
		}
		publications = append(publications, pub)
	}

	return publications, currentPos, nil
}

// RemoveHistory removes channel history from Redis
func (r *RedisHistoryStorage) RemoveHistory(ctx context.Context, channel string) error {
	streamKey := r.streamKey(channel)
	metaKey := r.metaKey(channel)

	pipe := r.client.Pipeline()
	pipe.Del(ctx, streamKey)
	pipe.Del(ctx, metaKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to remove history: %w", err)
	}

	return nil
}

// Helper methods

func (r *RedisHistoryStorage) streamKey(channel string) string {
	return fmt.Sprintf("%s:history:stream:%s", r.prefix, channel)
}

func (r *RedisHistoryStorage) metaKey(channel string) string {
	return fmt.Sprintf("%s:history:meta:%s", r.prefix, channel)
}

func (r *RedisHistoryStorage) generateEpoch(channel string, timestamp int64) string {
	// Generate epoch based on channel and timestamp for uniqueness
	return fmt.Sprintf("%s-%d", r.hashChannel(channel), timestamp)
}

func (r *RedisHistoryStorage) hashChannel(channel string) string {
	// Simple hash of channel name for epoch generation
	var hash uint32
	for _, c := range channel {
		hash = hash*31 + uint32(c)
	}
	return fmt.Sprintf("%x", hash)
}

func (r *RedisHistoryStorage) parseStreamIDToOffset(streamID string) (uint64, error) {
	// Redis stream ID format: "1609459200000-0"
	// Extract timestamp part as offset
	parts := strings.Split(streamID, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid stream ID format: %s", streamID)
	}

	timestamp, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp from stream ID: %w", err)
	}

	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse sequence from stream ID: %w", err)
	}

	// Combine timestamp and sequence to create unique offset
	return timestamp*1000 + sequence, nil
}

func (r *RedisHistoryStorage) getCurrentPosition(ctx context.Context, metaKey string) (StreamPosition, error) {
	result, err := r.client.HMGet(ctx, metaKey, "epoch", "updated_at").Result()
	if err != nil {
		if err == redis.Nil {
			return StreamPosition{Offset: 0, Epoch: ""}, nil
		}
		return StreamPosition{}, err
	}

	var epoch string
	var offset uint64

	if result[0] != nil {
		epoch = result[0].(string)
	}

	if result[1] != nil {
		if timestamp, ok := result[1].(string); ok {
			if ts, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
				offset = uint64(ts)
			}
		}
	}

	return StreamPosition{
		Offset: offset,
		Epoch:  epoch,
	}, nil
}

func (r *RedisHistoryStorage) buildRangeArgs(filter HistoryFilter, currentPos StreamPosition) (start, end string, count int) {
	// Default values
	start = "-"
	end = "+"
	count = filter.Limit

	if filter.Limit < 0 {
		count = 0 // No limit
	}

	if filter.Since != nil {
		if filter.Reverse {
			// For reverse, end should be the since position
			end = r.offsetToStreamID(filter.Since.Offset)
		} else {
			// For forward, start should be after the since position
			start = r.offsetToStreamID(filter.Since.Offset + 1)
		}
	}

	return start, end, count
}

func (r *RedisHistoryStorage) offsetToStreamID(offset uint64) string {
	// Convert offset back to stream ID format
	timestamp := offset / 1000
	sequence := offset % 1000
	return fmt.Sprintf("%d-%d", timestamp, sequence)
}

func (r *RedisHistoryStorage) parseStreamMessage(msg redis.XMessage) (*Publication, error) {
	dataInterface, exists := msg.Values["data"]
	if !exists {
		return nil, fmt.Errorf("no data field in stream message")
	}

	dataStr, ok := dataInterface.(string)
	if !ok {
		return nil, fmt.Errorf("data field is not string")
	}

	// Parse protobuf publication
	var protoPub protocol.Publication
	if err := protoPub.UnmarshalVT([]byte(dataStr)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal publication: %w", err)
	}

	// Convert stream ID to offset
	offset, err := r.parseStreamIDToOffset(msg.ID)
	if err != nil {
		offset = 0 // Fallback to 0 if parsing fails
	}

	// Convert to centrifuge Publication
	pub := &Publication{
		Offset: offset,
		Data:   protoPub.Data,
		Info:   infoFromProto(protoPub.Info),
		Tags:   protoPub.Tags,
		Time:   protoPub.Time,
	}

	return pub, nil
}

// GetName returns the storage name for observability
func (r *RedisHistoryStorage) GetName() string {
	return r.name
}

// Health checks if the Redis connection is healthy
func (r *RedisHistoryStorage) Health(ctx context.Context) error {
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}
	return nil
}

// Stats returns storage statistics
func (r *RedisHistoryStorage) Stats(ctx context.Context) (map[string]interface{}, error) {
	info, err := r.client.Info(ctx, "memory", "stats").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get redis info: %w", err)
	}

	stats := map[string]interface{}{
		"storage_type": "redis",
		"storage_name": r.name,
		"prefix":       r.prefix,
		"redis_info":   info,
	}

	return stats, nil
}
