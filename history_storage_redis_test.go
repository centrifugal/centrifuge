package centrifuge

import (
	"testing"

	"github.com/channelwill/cw2-live-chat-common/pkg/storage/gredis"
	"github.com/stretchr/testify/require"
)

func TestRedisHistoryStorage_Config(t *testing.T) {
	// Test with empty config - should use defaults
	config := RedisHistoryStorageConfig{}
	storage := NewRedisHistoryStorage(config)

	require.Equal(t, "centrifuge", storage.prefix)
	require.Equal(t, "redis-history", storage.name)
	require.Nil(t, storage.client) // Client is nil in test
}

func TestRedisHistoryStorage_ConfigWithValues(t *testing.T) {
	config := RedisHistoryStorageConfig{
		Prefix: "test-prefix",
		Name:   "test-storage",
	}
	storage := NewRedisHistoryStorage(config)

	require.Equal(t, "test-prefix", storage.prefix)
	require.Equal(t, "test-storage", storage.name)
}

func TestRedisHistoryStorage_KeyGeneration(t *testing.T) {
	storage := NewRedisHistoryStorage(RedisHistoryStorageConfig{
		Prefix: "test",
	})

	channel := "chat:room:123"

	streamKey := storage.streamKey(channel)
	metaKey := storage.metaKey(channel)

	require.Equal(t, "test:history:stream:chat:room:123", streamKey)
	require.Equal(t, "test:history:meta:chat:room:123", metaKey)
}

func TestRedisHistoryStorage_FromConf(t *testing.T) {
	redisConf := gredis.RedisConf{
		Address:  "localhost:6379",
		Password: "",
		DB:       0,
	}

	storage := NewRedisHistoryStorageFromConf(redisConf, "test-prefix")

	require.Equal(t, "test-prefix", storage.prefix)
	require.Equal(t, "redis-history", storage.name)
	require.NotNil(t, storage.client)
}

func TestRedisHistoryStorage_StreamIDParsing(t *testing.T) {
	storage := NewRedisHistoryStorage(RedisHistoryStorageConfig{})

	// Test valid stream ID
	streamID := "1609459200000-0"
	offset, err := storage.parseStreamIDToOffset(streamID)
	require.NoError(t, err)
	require.Equal(t, uint64(1609459200000000), offset)

	// Test stream ID with sequence
	streamID = "1609459200000-5"
	offset, err = storage.parseStreamIDToOffset(streamID)
	require.NoError(t, err)
	require.Equal(t, uint64(1609459200000005), offset)

	// Test invalid stream ID
	streamID = "invalid"
	_, err = storage.parseStreamIDToOffset(streamID)
	require.Error(t, err)

	// Test invalid format
	streamID = "1609459200000"
	_, err = storage.parseStreamIDToOffset(streamID)
	require.Error(t, err)
}

func TestRedisHistoryStorage_OffsetToStreamID(t *testing.T) {
	storage := NewRedisHistoryStorage(RedisHistoryStorageConfig{})

	offset := uint64(1609459200000005)
	streamID := storage.offsetToStreamID(offset)
	require.Equal(t, "1609459200000-5", streamID)

	offset = uint64(1609459200000000)
	streamID = storage.offsetToStreamID(offset)
	require.Equal(t, "1609459200000-0", streamID)
}

func TestRedisHistoryStorage_EpochGeneration(t *testing.T) {
	storage := NewRedisHistoryStorage(RedisHistoryStorageConfig{})

	channel := "test-channel"
	timestamp := int64(1609459200)

	epoch1 := storage.generateEpoch(channel, timestamp)
	epoch2 := storage.generateEpoch(channel, timestamp)

	// Same channel and timestamp should generate same epoch
	require.Equal(t, epoch1, epoch2)

	// Different timestamp should generate different epoch
	epoch3 := storage.generateEpoch(channel, timestamp+1)
	require.NotEqual(t, epoch1, epoch3)

	// Different channel should generate different epoch
	epoch4 := storage.generateEpoch("other-channel", timestamp)
	require.NotEqual(t, epoch1, epoch4)
}

func TestRedisHistoryStorage_HashChannel(t *testing.T) {
	storage := NewRedisHistoryStorage(RedisHistoryStorageConfig{})

	// Test consistent hashing
	hash1 := storage.hashChannel("test-channel")
	hash2 := storage.hashChannel("test-channel")
	require.Equal(t, hash1, hash2)

	// Test different channels produce different hashes
	hash3 := storage.hashChannel("other-channel")
	require.NotEqual(t, hash1, hash3)
}

func TestRedisHistoryStorage_RangeArgsBuilder(t *testing.T) {
	storage := NewRedisHistoryStorage(RedisHistoryStorageConfig{})

	currentPos := StreamPosition{
		Offset: 1000,
		Epoch:  "test-epoch",
	}

	// Test default range
	filter := HistoryFilter{
		Limit: 10,
	}
	start, end, count := storage.buildRangeArgs(filter, currentPos)
	require.Equal(t, "-", start)
	require.Equal(t, "+", end)
	require.Equal(t, 10, count)

	// Test with since position
	filter = HistoryFilter{
		Limit: 5,
		Since: &StreamPosition{Offset: 500},
	}
	start, end, count = storage.buildRangeArgs(filter, currentPos)
	require.Equal(t, "0-501", start) // Should be after the since position
	require.Equal(t, "+", end)
	require.Equal(t, 5, count)

	// Test reverse with since position
	filter = HistoryFilter{
		Limit:   5,
		Since:   &StreamPosition{Offset: 500},
		Reverse: true,
	}
	start, end, count = storage.buildRangeArgs(filter, currentPos)
	require.Equal(t, "-", start)
	require.Equal(t, "0-500", end) // Should be the since position
	require.Equal(t, 5, count)

	// Test no limit
	filter = HistoryFilter{
		Limit: -1,
	}
	start, end, count = storage.buildRangeArgs(filter, currentPos)
	require.Equal(t, 0, count) // No limit means count = 0
}

func TestRedisHistoryStorage_GetName(t *testing.T) {
	storage := NewRedisHistoryStorage(RedisHistoryStorageConfig{
		Name: "test-storage",
	})

	require.Equal(t, "test-storage", storage.GetName())
}

// Note: The following tests would require a real Redis connection
// They are commented out but show how you would test the actual Redis operations

/*
func TestRedisHistoryStorage_Integration(t *testing.T) {
	// Skip if no Redis available
	if testing.Short() {
		t.Skip("Skipping Redis integration test in short mode")
	}

	// Create Redis client
	redisConf := gredis.RedisConf{
		Address: "localhost:6379",
		DB:      1, // Use test database
	}

	storage := NewRedisHistoryStorageFromConf(redisConf, "test")

	// Test health check
	err := storage.Health(context.Background())
	require.NoError(t, err)

	// Test add to history
	channel := "test-channel"
	pub := &Publication{
		Data: []byte("test message"),
		Info: &ClientInfo{
			ClientID: "client-123",
			UserID:   "user-456",
		},
		Tags: map[string]string{"tag1": "value1"},
	}

	opts := PublishOptions{
		HistorySize: 10,
		HistoryTTL:  time.Minute,
	}

	sp, err := storage.AddToHistory(context.Background(), channel, pub, opts)
	require.NoError(t, err)
	require.Greater(t, sp.Offset, uint64(0))
	require.NotEmpty(t, sp.Epoch)

	// Test get history
	pubs, currentSp, err := storage.GetHistory(context.Background(), channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 1)
	require.Equal(t, sp.Offset, currentSp.Offset)
	require.Equal(t, sp.Epoch, currentSp.Epoch)
	require.Equal(t, pub.Data, pubs[0].Data)

	// Test remove history
	err = storage.RemoveHistory(context.Background(), channel)
	require.NoError(t, err)

	// Verify history is removed
	pubs, _, err = storage.GetHistory(context.Background(), channel, HistoryOptions{
		Filter: HistoryFilter{Limit: -1},
	})
	require.NoError(t, err)
	require.Len(t, pubs, 0)

	// Test stats
	stats, err := storage.Stats(context.Background())
	require.NoError(t, err)
	require.Equal(t, "redis", stats["storage_type"])
	require.Equal(t, "redis-history", stats["storage_name"])
	require.Equal(t, "test", stats["prefix"])
}
*/
