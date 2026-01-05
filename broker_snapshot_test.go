//go:build integration

package centrifuge

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "embed"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

//go:embed internal/redis_lua/broker_snapshot_add.lua
var brokerSnapshotPublishScript string

//go:embed internal/redis_lua/broker_snapshot_read_ordered.lua
var brokerSnapshotReadOrderedScript string

//go:embed internal/redis_lua/broker_snapshot_read_unordered.lua
var brokerSnapshotReadUnorderedScript string

//go:embed internal/redis_lua/broker_snapshot_history_get.lua
var brokerSnapshotHistoryGetScript string

//go:embed internal/redis_lua/broker_snapshot_presence_get.lua
var brokerSnapshotPresenceGetScript string

//go:embed internal/redis_lua/broker_snapshot_presence_stats.lua
var brokerSnapshotPresenceStatsScript string

// TestBrokerSnapshot_AppendLogOnly tests append log functionality without keyed state or presence.
func TestBrokerSnapshot_AppendLogOnly(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	streamKey := prefix + ":stream:test"
	metaKey := prefix + ":meta:test"
	channel := prefix + ":channel:test"

	ctx := context.Background()
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Publish 3 messages to append log
	for i := 1; i <= 3; i++ {
		payload := "message_" + strconv.Itoa(i)
		result := runSnapshotPublishScript(t, client, &SnapshotLogParams{
			StreamKey:      streamKey,
			MetaKey:        metaKey,
			Channel:        channel,
			MessagePayload: payload,
			StreamSize:     100,
			StreamTTL:      300,
			MetaExpire:     300,
			NewEpoch:       epoch,
			PublishCommand: "publish",
		})

		require.Equal(t, int64(i), result.Offset, "offset should increment")
		require.Equal(t, epoch, result.Epoch)
		require.False(t, result.FromCache, "should not be from cache")
		require.False(t, result.Suppressed, "should not be suppressed")
	}

	// Read back the stream
	entries := client.Do(ctx, client.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(10).Build())
	require.NoError(t, entries.Error())
	arr, err := entries.ToArray()
	require.NoError(t, err)
	require.Len(t, arr, 3)

	// Verify messages - parse XRange entry format
	for i, expectedMsg := range []string{"message_3", "message_2", "message_1"} {
		entryValues, err := arr[i].ToArray()
		require.NoError(t, err)
		require.Len(t, entryValues, 2) // [id, field-values]

		fieldValues, err := entryValues[1].ToArray()
		require.NoError(t, err)

		// Find "d" field
		var found bool
		for j := 0; j < len(fieldValues); j += 2 {
			k, _ := fieldValues[j].ToString()
			if k == "d" {
				v, _ := fieldValues[j+1].ToString()
				require.Equal(t, expectedMsg, v)
				found = true
				break
			}
		}
		require.True(t, found, "field 'd' not found")
	}
}

// TestBrokerSnapshot_KeyedSnapshotSimple tests simple HASH-based keyed snapshot.
func TestBrokerSnapshot_KeyedSnapshotSimple(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotKey := prefix + ":snapshot:test"
	snapshotMetaKey := prefix + ":snapshot:meta:test"

	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Add 3 keyed state entries
	keys := []string{"client1", "client2", "client3"}
	for _, key := range keys {
		result := runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:         metaKey,
			SnapshotHashKey: snapshotKey,
			SnapshotMetaKey: snapshotMetaKey,
			MessagePayload:  key,
			NewEpoch:        epoch,
			KeyedMemberTTL:  300,
		})
		require.Greater(t, result.Offset, int64(0))
	}

	// Read back using unordered read
	readResult := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:         snapshotKey,
		MetaKey:         metaKey,
		SnapshotMetaKey: snapshotMetaKey,
		Cursor:          "0",
		Limit:           0, // Get all
		Now:             time.Now().Unix(),
		MetaTTL:         300,
		SnapshotTTL:     300,
	})

	require.Equal(t, epoch, readResult.Epoch)
	require.Equal(t, "0", readResult.Cursor)
	require.Len(t, readResult.Data, 6) // 3 key-value pairs = 6 elements

	// Verify all keys present
	dataMap := kvArrayToMap(readResult.Data)
	for _, key := range keys {
		require.Contains(t, dataMap, key)
		require.Equal(t, key, dataMap[key])
	}
}

// TestBrokerSnapshot_KeyedSnapshotOrdered tests ordered HASH+ZSET keyed snapshot.
func TestBrokerSnapshot_KeyedSnapshotOrdered(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotHashKey := prefix + ":snapshot:test"
	snapshotOrderKey := prefix + ":snapshot:order:test"
	snapshotExpireKey := prefix + ":snapshot:expire:test"

	ctx := context.Background()
	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Add 5 keyed state entries with different scores
	entries := []struct {
		key   string
		score int64
	}{
		{"client1", 100},
		{"client2", 200},
		{"client3", 300},
		{"client4", 400},
		{"client5", 500},
	}

	for _, entry := range entries {
		result := runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:           metaKey,
			SnapshotHashKey:   snapshotHashKey,
			SnapshotOrderKey:  snapshotOrderKey,
			SnapshotExpireKey: snapshotExpireKey,
			MessagePayload:    entry.key,
			Score:             entry.score,
			KeyedMemberTTL:    300,
			NewEpoch:          epoch,
		})
		require.Greater(t, result.Offset, int64(0))
	}

	// Read all (no pagination)
	readResult := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       0, // Get all
		Offset:      0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	require.Equal(t, epoch, readResult.Epoch)
	require.Len(t, readResult.Keys, 5)
	require.Len(t, readResult.Values, 5)

	// Verify descending order by score
	require.Equal(t, "client5", readResult.Keys[0])
	require.Equal(t, "client4", readResult.Keys[1])
	require.Equal(t, "client3", readResult.Keys[2])
	require.Equal(t, "client2", readResult.Keys[3])
	require.Equal(t, "client1", readResult.Keys[4])

	// Test pagination: get 2 items starting from offset 1
	readResult2 := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       2,
		Offset:      1,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	require.Len(t, readResult2.Keys, 2)
	require.Equal(t, "client4", readResult2.Keys[0])
	require.Equal(t, "client3", readResult2.Keys[1])

	// Verify expiration cleanup (add expired entry)
	expiredKey := "expired_client"
	expiredTime := now - 100 // Already expired
	client.Do(ctx, client.B().Hset().Key(snapshotHashKey).FieldValue().FieldValue(expiredKey, expiredKey).Build())
	client.Do(ctx, client.B().Zadd().Key(snapshotOrderKey).ScoreMember().ScoreMember(50, expiredKey).Build())
	client.Do(ctx, client.B().Zadd().Key(snapshotExpireKey).ScoreMember().ScoreMember(float64(expiredTime), expiredKey).Build())

	// Read again - expired entry should be cleaned up
	readResult3 := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       0,
		Offset:      0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	require.Len(t, readResult3.Keys, 5) // Expired entry removed
	for _, key := range readResult3.Keys {
		require.NotEqual(t, expiredKey, key)
	}
}

// TestBrokerSnapshot_Presence tests presence tracking functionality.
func TestBrokerSnapshot_Presence(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	presenceZSetKey := prefix + ":presence:zset:test"
	presenceHashKey := prefix + ":presence:hash:test"
	userZSetKey := prefix + ":user:zset:test"
	userHashKey := prefix + ":user:hash:test"

	ctx := context.Background()
	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)
	expireAt := now + 300

	// Add presence for 3 clients (2 users)
	clients := []struct {
		clientID string
		userID   string
		info     string
	}{
		{"client1", "user1", `{"client":"client1","user":"user1"}`},
		{"client2", "user1", `{"client":"client2","user":"user1"}`},
		{"client3", "user2", `{"client":"client3","user":"user2"}`},
	}

	for _, c := range clients {
		result := runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:          metaKey,
			PresenceZSetKey:  presenceZSetKey,
			PresenceHashKey:  presenceHashKey,
			UserZSetKey:      userZSetKey,
			UserHashKey:      userHashKey,
			MessagePayload:   c.clientID,
			PresenceInfo:     c.info,
			PresenceExpireAt: expireAt,
			TrackUser:        true,
			UserID:           c.userID,
			KeyedMemberTTL:   300,
			NewEpoch:         epoch,
		})
		require.Greater(t, result.Offset, int64(0))
	}

	// Verify presence data
	presenceData := client.Do(ctx, client.B().Hgetall().Key(presenceHashKey).Build())
	require.NoError(t, presenceData.Error())
	presenceMap, err := presenceData.AsStrMap()
	require.NoError(t, err)
	require.Len(t, presenceMap, 3)

	// Verify user tracking
	userData := client.Do(ctx, client.B().Hgetall().Key(userHashKey).Build())
	require.NoError(t, userData.Error())
	userMap, err := userData.AsStrMap()
	require.NoError(t, err)
	require.Len(t, userMap, 2)
	require.Equal(t, "2", userMap["user1"]) // 2 connections
	require.Equal(t, "1", userMap["user2"]) // 1 connection

	// Test leave message for client1
	leaveResult := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		PresenceZSetKey: presenceZSetKey,
		PresenceHashKey: presenceHashKey,
		UserZSetKey:     userZSetKey,
		UserHashKey:     userHashKey,
		MessagePayload:  "client1",
		TrackUser:       true,
		UserID:          "user1",
		IsLeave:         true,
		NewEpoch:        epoch,
	})
	require.Greater(t, leaveResult.Offset, int64(0))

	// Verify client1 removed
	presenceData2 := client.Do(ctx, client.B().Hgetall().Key(presenceHashKey).Build())
	require.NoError(t, presenceData2.Error())
	presenceMap2, err := presenceData2.AsStrMap()
	require.NoError(t, err)
	require.Len(t, presenceMap2, 2)
	require.NotContains(t, presenceMap2, "client1")

	// Verify user1 count decremented
	userData2 := client.Do(ctx, client.B().Hgetall().Key(userHashKey).Build())
	require.NoError(t, userData2.Error())
	userMap2, err := userData2.AsStrMap()
	require.NoError(t, err)
	require.Equal(t, "1", userMap2["user1"]) // Now 1 connection

	// Remove last client for user1 (client2)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		PresenceZSetKey: presenceZSetKey,
		PresenceHashKey: presenceHashKey,
		UserZSetKey:     userZSetKey,
		UserHashKey:     userHashKey,
		MessagePayload:  "client2",
		TrackUser:       true,
		UserID:          "user1",
		IsLeave:         true,
		NewEpoch:        epoch,
	})

	// Verify user1 removed entirely
	userData3 := client.Do(ctx, client.B().Hgetall().Key(userHashKey).Build())
	require.NoError(t, userData3.Error())
	userMap3, err := userData3.AsStrMap()
	require.NoError(t, err)
	require.Len(t, userMap3, 1)
	require.NotContains(t, userMap3, "user1")
	require.Contains(t, userMap3, "user2")
}

// TestBrokerSnapshot_LeaveWithKeyedSnapshot tests leave message removing keyed snapshot.
func TestBrokerSnapshot_LeaveWithKeyedSnapshot(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotKey := prefix + ":snapshot:test"

	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Add keyed state
	result := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		SnapshotHashKey: snapshotKey,
		MessagePayload:  "client1",
		NewEpoch:        epoch,
		KeyedMemberTTL:  300,
	})
	require.Greater(t, result.Offset, int64(0))

	// Verify exists
	ctx := context.Background()
	exists := client.Do(ctx, client.B().Hexists().Key(snapshotKey).Field("client1").Build())
	require.NoError(t, exists.Error())
	existsBool, err := exists.AsBool()
	require.NoError(t, err)
	require.True(t, existsBool)

	// Send leave message
	leaveResult := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		SnapshotHashKey: snapshotKey,
		MessagePayload:  "client1",
		IsLeave:         true,
		NewEpoch:        epoch,
	})
	require.Greater(t, leaveResult.Offset, int64(0))

	// Verify removed
	exists2 := client.Do(ctx, client.B().Hexists().Key(snapshotKey).Field("client1").Build())
	require.NoError(t, exists2.Error())
	exists2Bool, err := exists2.AsBool()
	require.NoError(t, err)
	require.False(t, exists2Bool)
}

// TestBrokerSnapshot_Idempotency tests idempotent publishing.
func TestBrokerSnapshot_Idempotency(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	resultKey := prefix + ":result:test"

	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// First publish with idempotency key
	result1 := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		ResultKey:       resultKey,
		MessagePayload:  "test_message",
		ResultKeyExpire: 300,
		NewEpoch:        epoch,
	})
	require.Equal(t, int64(1), result1.Offset)
	require.Equal(t, epoch, result1.Epoch)
	require.False(t, result1.FromCache)

	// Second publish with same idempotency key - should return cached result
	result2 := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		ResultKey:       resultKey,
		MessagePayload:  "test_message",
		ResultKeyExpire: 300,
		NewEpoch:        epoch,
	})
	require.Equal(t, int64(1), result2.Offset) // Same offset
	require.Equal(t, epoch, result2.Epoch)
	require.True(t, result2.FromCache) // From cache!
}

// TestBrokerSnapshot_VersionBasedIdempotency tests version-based suppression.
func TestBrokerSnapshot_VersionBasedIdempotency(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Publish version 2
	result1 := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:        metaKey,
		MessagePayload: "message_v2",
		Version:        2,
		VersionEpoch:   "epoch1",
		NewEpoch:       epoch,
	})
	require.Equal(t, int64(1), result1.Offset)
	require.False(t, result1.Suppressed)

	// Try to publish version 1 (older) - should be suppressed
	result2 := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:        metaKey,
		MessagePayload: "message_v1",
		Version:        1,
		VersionEpoch:   "epoch1",
		NewEpoch:       epoch,
	})
	require.Equal(t, int64(1), result2.Offset) // Offset should NOT increment when suppressed
	require.True(t, result2.Suppressed)        // Suppressed!

	// Publish version 3 - should succeed
	result3 := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:        metaKey,
		MessagePayload: "message_v3",
		Version:        3,
		VersionEpoch:   "epoch1",
		NewEpoch:       epoch,
	})
	require.Equal(t, int64(2), result3.Offset)
	require.False(t, result3.Suppressed)
}

// TestBrokerSnapshot_DeltaEncoding tests delta encoding functionality.
func TestBrokerSnapshot_DeltaEncoding(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	streamKey := prefix + ":stream:test"
	metaKey := prefix + ":meta:test"
	channel := prefix + ":channel:test"

	ctx := context.Background()
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Set up PUB/SUB hooks to capture published messages
	c, cancel := client.Dedicate()
	defer cancel()

	receivedMessages := make(chan string, 10)
	subscribed := make(chan struct{})

	wait := c.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(m rueidis.PubSubMessage) {
			receivedMessages <- m.Message
		},
		OnSubscription: func(s rueidis.PubSubSubscription) {
			if s.Kind == "subscribe" && s.Channel == channel {
				close(subscribed)
			}
		},
	})

	// Subscribe to channel
	err := c.Do(ctx, c.B().Subscribe().Channel(channel).Build()).Error()
	require.NoError(t, err)

	// Wait for subscription confirmation
	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription timeout")
	}

	// Publish with delta encoding
	for i := 1; i <= 3; i++ {
		payload := "message_" + strconv.Itoa(i)
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			StreamKey:      streamKey,
			MetaKey:        metaKey,
			Channel:        channel,
			MessagePayload: payload,
			StreamSize:     100,
			StreamTTL:      300,
			UseDelta:       true,
			PublishCommand: "publish",
			NewEpoch:       epoch,
		})
	}

	// Receive and verify messages
	var messages []string
	for i := 0; i < 3; i++ {
		select {
		case msg := <-receivedMessages:
			messages = append(messages, msg)
		case <-time.After(2 * time.Second):
			t.Fatal("message receive timeout")
		}
	}

	// All messages should be received
	require.Len(t, messages, 3)
	// Delta format messages should contain __d1: prefix
	for _, msg := range messages {
		require.Contains(t, msg, "__d1:")
	}

	// Clean up
	_ = c.Do(ctx, c.B().Unsubscribe().Channel(channel).Build())
	select {
	case <-wait:
	case <-time.After(1 * time.Second):
	}
}

// TestBrokerSnapshot_PubSubOnly tests PUB/SUB without any storage.
func TestBrokerSnapshot_PubSubOnly(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	channel := prefix + ":channel:test"
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	ctx := context.Background()

	// Set up PUB/SUB hooks
	c, cancel := client.Dedicate()
	defer cancel()

	receivedMessage := make(chan string, 1)
	subscribed := make(chan struct{})

	wait := c.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(m rueidis.PubSubMessage) {
			receivedMessage <- m.Message
		},
		OnSubscription: func(s rueidis.PubSubSubscription) {
			if s.Kind == "subscribe" && s.Channel == channel {
				close(subscribed)
			}
		},
	})

	// Subscribe to channel
	err := c.Do(ctx, c.B().Subscribe().Channel(channel).Build()).Error()
	require.NoError(t, err)

	// Wait for subscription confirmation
	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription timeout")
	}

	// Publish without any storage (all KEYS empty, only channel set)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		Channel:        channel,
		MessagePayload: "pubsub_only_message",
		PublishCommand: "publish",
		NewEpoch:       epoch,
	})

	// Receive message
	select {
	case msg := <-receivedMessage:
		require.Contains(t, msg, "pubsub_only_message")
	case <-time.After(2 * time.Second):
		t.Fatal("message receive timeout")
	}

	// Clean up
	_ = c.Do(ctx, c.B().Unsubscribe().Channel(channel).Build())
	select {
	case <-wait:
	case <-time.After(1 * time.Second):
	}
}

// TestBrokerSnapshot_CombinedFeatures tests using multiple features together.
func TestBrokerSnapshot_CombinedFeatures(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	streamKey := prefix + ":stream:test"
	metaKey := prefix + ":meta:test"
	snapshotKey := prefix + ":snapshot:test"
	presenceHashKey := prefix + ":presence:hash:test"
	presenceZSetKey := prefix + ":presence:zset:test"
	channel := prefix + ":channel:test"

	ctx := context.Background()
	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Test: Append log + Keyed snapshot + Presence + PUB/SUB all together
	result := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		StreamKey:        streamKey,
		MetaKey:          metaKey,
		SnapshotHashKey:  snapshotKey,
		PresenceHashKey:  presenceHashKey,
		PresenceZSetKey:  presenceZSetKey,
		Channel:          channel,
		MessagePayload:   "client1",
		PresenceInfo:     `{"client":"client1"}`,
		PresenceExpireAt: now + 300,
		StreamSize:       100,
		StreamTTL:        300,
		MetaExpire:       300,
		KeyedMemberTTL:   300,
		PublishCommand:   "publish",
		NewEpoch:         epoch,
	})

	require.Equal(t, int64(1), result.Offset)
	require.Equal(t, epoch, result.Epoch)

	// Verify append log
	entries := client.Do(ctx, client.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(1).Build())
	require.NoError(t, entries.Error())
	arr, err := entries.ToArray()
	require.NoError(t, err)
	require.Len(t, arr, 1)

	entryValues, err := arr[0].ToArray()
	require.NoError(t, err)
	fieldValues, err := entryValues[1].ToArray()
	require.NoError(t, err)

	var valClient1 string
	for i := 0; i < len(fieldValues); i += 2 {
		k, _ := fieldValues[i].ToString()
		if k == "d" {
			valClient1, _ = fieldValues[i+1].ToString()
			break
		}
	}
	require.Equal(t, "client1", valClient1)

	// Verify keyed snapshot
	snapshotExists := client.Do(ctx, client.B().Hexists().Key(snapshotKey).Field("client1").Build())
	require.NoError(t, snapshotExists.Error())
	snapshotExistsBool, err := snapshotExists.AsBool()
	require.NoError(t, err)
	require.True(t, snapshotExistsBool)

	// Verify presence
	presenceExists := client.Do(ctx, client.B().Hexists().Key(presenceHashKey).Field("client1").Build())
	require.NoError(t, presenceExists.Error())
	presenceExistsBool, err := presenceExists.AsBool()
	require.NoError(t, err)
	require.True(t, presenceExistsBool)
}

// TestBrokerSnapshot_HistoryRead tests reading history using the unified read script.
func TestBrokerSnapshot_HistoryRead(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	streamKey := prefix + ":stream:test"
	metaKey := prefix + ":meta:test"
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Write 5 messages
	for i := 1; i <= 5; i++ {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			StreamKey:      streamKey,
			MetaKey:        metaKey,
			MessagePayload: "msg_" + strconv.Itoa(i),
			StreamSize:     100,
			StreamTTL:      300,
			MetaExpire:     300,
			NewEpoch:       epoch,
		})
	}

	// Read all history
	result := runSnapshotHistoryGetScript(t, client, &SnapshotHistoryGetParams{
		StreamKey:           streamKey,
		MetaKey:             metaKey,
		IncludePublications: true,
		SinceOffset:         "-", // "-" means from beginning for XREVRANGE
		Limit:               0,   // Get all
		Reverse:             true,
		MetaExpire:          300,
		NewEpochIfEmpty:     epoch,
	})

	require.Equal(t, int64(5), result.TopOffset)
	require.Equal(t, epoch, result.Epoch)
	require.Len(t, result.Publications, 5)

	// Verify order (reverse = most recent first)
	require.Equal(t, "msg_5", result.Publications[0][1])
	require.Equal(t, "msg_4", result.Publications[1][1])
	require.Equal(t, "msg_1", result.Publications[4][1])

	// Read with limit
	resultLimited := runSnapshotHistoryGetScript(t, client, &SnapshotHistoryGetParams{
		StreamKey:           streamKey,
		MetaKey:             metaKey,
		IncludePublications: true,
		SinceOffset:         "-",
		Limit:               2,
		Reverse:             true,
		MetaExpire:          300,
		NewEpochIfEmpty:     epoch,
	})

	require.Len(t, resultLimited.Publications, 2)
	require.Equal(t, "msg_5", resultLimited.Publications[0][1])
	require.Equal(t, "msg_4", resultLimited.Publications[1][1])

	// Read only metadata (no publications)
	resultMetaOnly := runSnapshotHistoryGetScript(t, client, &SnapshotHistoryGetParams{
		StreamKey:           streamKey,
		MetaKey:             metaKey,
		IncludePublications: false,
		SinceOffset:         "-",
		Limit:               0,
		Reverse:             false,
		MetaExpire:          300,
		NewEpochIfEmpty:     epoch,
	})

	require.Equal(t, int64(5), resultMetaOnly.TopOffset)
	require.Equal(t, epoch, resultMetaOnly.Epoch)
	require.Len(t, resultMetaOnly.Publications, 0)
}

// TestBrokerSnapshot_PresenceRead tests reading full presence using the unified read script.
func TestBrokerSnapshot_PresenceRead(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	presenceZSetKey := prefix + ":presence:zset:test"
	presenceHashKey := prefix + ":presence:hash:test"
	userZSetKey := prefix + ":user:zset:test"
	userHashKey := prefix + ":user:hash:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)
	expireAt := now + 300

	// Add 4 clients (3 users)
	clients := []struct {
		clientID string
		userID   string
		info     string
	}{
		{"client1", "user1", `{"name":"Alice","conn":1}`},
		{"client2", "user1", `{"name":"Alice","conn":2}`},
		{"client3", "user2", `{"name":"Bob"}`},
		{"client4", "user3", `{"name":"Charlie"}`},
	}

	for _, c := range clients {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:          metaKey,
			PresenceZSetKey:  presenceZSetKey,
			PresenceHashKey:  presenceHashKey,
			UserZSetKey:      userZSetKey,
			UserHashKey:      userHashKey,
			MessagePayload:   c.clientID,
			PresenceInfo:     c.info,
			PresenceExpireAt: expireAt,
			TrackUser:        true,
			UserID:           c.userID,
			KeyedMemberTTL:   300,
			NewEpoch:         epoch,
		})
	}

	// Read full presence
	presenceResult := runSnapshotPresenceGetScript(t, client, &SnapshotPresenceGetParams{
		PresenceZSetKey: presenceZSetKey,
		PresenceHashKey: presenceHashKey,
		Now:             now,
		UseHExpire:      false,
	})

	require.Len(t, presenceResult.Presence, 4)
	require.Equal(t, `{"name":"Alice","conn":1}`, presenceResult.Presence["client1"])
	require.Equal(t, `{"name":"Alice","conn":2}`, presenceResult.Presence["client2"])
	require.Equal(t, `{"name":"Bob"}`, presenceResult.Presence["client3"])
	require.Equal(t, `{"name":"Charlie"}`, presenceResult.Presence["client4"])
}

// TestBrokerSnapshot_PresenceStats tests getting presence statistics.
func TestBrokerSnapshot_PresenceStats(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	presenceZSetKey := prefix + ":presence:zset:test"
	presenceHashKey := prefix + ":presence:hash:test"
	userZSetKey := prefix + ":user:zset:test"
	userHashKey := prefix + ":user:hash:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)
	expireAt := now + 300

	// Add 10 clients from 3 users
	for i := 1; i <= 10; i++ {
		userID := "user" + strconv.Itoa((i-1)%3+1) // user1, user2, user3
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:          metaKey,
			PresenceZSetKey:  presenceZSetKey,
			PresenceHashKey:  presenceHashKey,
			UserZSetKey:      userZSetKey,
			UserHashKey:      userHashKey,
			MessagePayload:   "client" + strconv.Itoa(i),
			PresenceInfo:     `{"id":"` + strconv.Itoa(i) + `"}`,
			PresenceExpireAt: expireAt,
			TrackUser:        true,
			UserID:           userID,
			KeyedMemberTTL:   300,
			NewEpoch:         epoch,
		})
	}

	// Get presence stats
	statsResult := runSnapshotPresenceStatsScript(t, client, &SnapshotPresenceStatsParams{
		PresenceZSetKey: presenceZSetKey,
		PresenceHashKey: presenceHashKey,
		UserZSetKey:     userZSetKey,
		UserHashKey:     userHashKey,
		Now:             now,
		UseHExpire:      false,
	})

	require.Equal(t, 10, statsResult.NumClients)
	require.Equal(t, 3, statsResult.NumUsers)

	// Remove some clients
	for i := 1; i <= 3; i++ {
		userID := "user" + strconv.Itoa((i-1)%3+1)
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:         metaKey,
			PresenceZSetKey: presenceZSetKey,
			PresenceHashKey: presenceHashKey,
			UserZSetKey:     userZSetKey,
			UserHashKey:     userHashKey,
			MessagePayload:  "client" + strconv.Itoa(i),
			TrackUser:       true,
			UserID:          userID,
			IsLeave:         true,
			NewEpoch:        epoch,
		})
	}

	// Get updated stats
	statsResult2 := runSnapshotPresenceStatsScript(t, client, &SnapshotPresenceStatsParams{
		PresenceZSetKey: presenceZSetKey,
		PresenceHashKey: presenceHashKey,
		UserZSetKey:     userZSetKey,
		UserHashKey:     userHashKey,
		Now:             now,
		UseHExpire:      false,
	})

	require.Equal(t, 7, statsResult2.NumClients) // 10 - 3 = 7
	require.Equal(t, 3, statsResult2.NumUsers)   // Still 3 users (each has remaining connections)
}

// TestBrokerSnapshot_SnapshotPagination tests paginating over ordered keyed snapshot.
func TestBrokerSnapshot_SnapshotPagination(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotHashKey := prefix + ":snapshot:test"
	snapshotOrderKey := prefix + ":snapshot:order:test"
	snapshotExpireKey := prefix + ":snapshot:expire:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Add 20 entries with different scores
	for i := 1; i <= 20; i++ {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:           metaKey,
			SnapshotHashKey:   snapshotHashKey,
			SnapshotOrderKey:  snapshotOrderKey,
			SnapshotExpireKey: snapshotExpireKey,
			MessagePayload:    "item_" + strconv.Itoa(i),
			Score:             int64(i * 10),
			KeyedMemberTTL:    300,
			NewEpoch:          epoch,
		})
	}

	// Page 1: Get first 5 (highest scores)
	page1 := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       5,
		Offset:      0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	require.Len(t, page1.Keys, 5)
	require.Equal(t, "item_20", page1.Keys[0]) // Highest score first
	require.Equal(t, "item_19", page1.Keys[1])
	require.Equal(t, "item_16", page1.Keys[4])

	// Page 2: Get next 5
	page2 := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       5,
		Offset:      5,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	require.Len(t, page2.Keys, 5)
	require.Equal(t, "item_15", page2.Keys[0])
	require.Equal(t, "item_14", page2.Keys[1])

	// Get all at once
	allItems := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       0, // No limit
		Offset:      0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	require.Len(t, allItems.Keys, 20)
	require.Equal(t, "item_20", allItems.Keys[0])
	require.Equal(t, "item_1", allItems.Keys[19])
}

// TestBrokerSnapshot_SnapshotTTLRefresh tests that TTL is refreshed on reads (LRU behavior).
func TestBrokerSnapshot_SnapshotTTLRefresh(t *testing.T) {
	t.Skip() // too long to execute, refactor later.

	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotKey := prefix + ":snapshot:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Write data with 5 second TTL
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		SnapshotHashKey: snapshotKey,
		MessagePayload:  "test_data",
		NewEpoch:        epoch,
		KeyedMemberTTL:  5, // 5 seconds
	})

	ctx := context.Background()

	// Verify data exists
	exists1 := client.Do(ctx, client.B().Exists().Key(snapshotKey).Build())
	require.NoError(t, exists1.Error())
	count1, _ := exists1.AsInt64()
	require.Equal(t, int64(1), count1)

	// Wait 3 seconds
	time.Sleep(3 * time.Second)

	// Read data (should refresh TTL to 5 seconds)
	runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:     snapshotKey,
		MetaKey:     metaKey,
		Cursor:      "0",
		Limit:       0,
		Now:         time.Now().Unix(),
		MetaTTL:     300,
		SnapshotTTL: 5, // Refresh to 5 seconds
	})

	// Wait another 3 seconds (total 6 from initial write, but only 3 since refresh)
	time.Sleep(3 * time.Second)

	// Data should still exist (because TTL was refreshed 3 seconds ago)
	exists2 := client.Do(ctx, client.B().Exists().Key(snapshotKey).Build())
	require.NoError(t, exists2.Error())
	count2, _ := exists2.AsInt64()
	require.Equal(t, int64(1), count2, "data should still exist after TTL refresh")

	// Wait another 3 seconds without reading (total 6 seconds since last refresh)
	time.Sleep(3 * time.Second)

	// Now data should be expired
	exists3 := client.Do(ctx, client.B().Exists().Key(snapshotKey).Build())
	require.NoError(t, exists3.Error())
	count3, _ := exists3.AsInt64()
	require.Equal(t, int64(0), count3, "data should be expired after 5 seconds without refresh")
}

// TestBrokerSnapshot_HistoryMultiPagePagination tests reading history across multiple pages.
func TestBrokerSnapshot_HistoryMultiPagePagination(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	streamKey := prefix + ":stream:test"
	metaKey := prefix + ":meta:test"
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Write 10 messages
	for i := 1; i <= 10; i++ {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			StreamKey:      streamKey,
			MetaKey:        metaKey,
			MessagePayload: "msg_" + strconv.Itoa(i),
			StreamSize:     100,
			StreamTTL:      300,
			MetaExpire:     300,
			NewEpoch:       epoch,
		})
	}

	// Read page 1: 3 messages
	page1 := runSnapshotHistoryGetScript(t, client, &SnapshotHistoryGetParams{
		StreamKey:           streamKey,
		MetaKey:             metaKey,
		IncludePublications: true,
		SinceOffset:         "-",
		Limit:               3,
		Reverse:             true,
		MetaExpire:          300,
		NewEpochIfEmpty:     epoch,
	})

	require.Len(t, page1.Publications, 3)
	require.Equal(t, "msg_10", page1.Publications[0][1])
	require.Equal(t, "msg_9", page1.Publications[1][1])
	require.Equal(t, "msg_8", page1.Publications[2][1])

	// Read page 2: next messages using last ID from page 1
	// Note: XREVRANGE is inclusive, so we need to request 4 to get 3 new ones
	lastID := page1.Publications[2][0]
	page2 := runSnapshotHistoryGetScript(t, client, &SnapshotHistoryGetParams{
		StreamKey:           streamKey,
		MetaKey:             metaKey,
		IncludePublications: true,
		SinceOffset:         lastID,
		Limit:               4, // Request 4 to account for inclusive boundary
		Reverse:             true,
		MetaExpire:          300,
		NewEpochIfEmpty:     epoch,
	})

	// First result will be msg_8 (duplicate from page 1 due to inclusive range)
	require.GreaterOrEqual(t, len(page2.Publications), 3)
	require.Equal(t, "msg_8", page2.Publications[0][1]) // Duplicate (inclusive)
	require.Equal(t, "msg_7", page2.Publications[1][1])
	require.Equal(t, "msg_6", page2.Publications[2][1])
	require.Equal(t, "msg_5", page2.Publications[3][1])

	// In practice, clients would skip the first result to avoid duplicates
	page2Deduplicated := page2.Publications[1:]
	require.Len(t, page2Deduplicated, 3)

	// Verify new messages are unique
	allIDs := make(map[string]bool)
	for _, pub := range page1.Publications {
		allIDs[pub[0]] = true
	}
	for _, pub := range page2Deduplicated {
		require.False(t, allIDs[pub[0]], "duplicate ID across pages: %s", pub[0])
	}
}

// TestBrokerSnapshot_UnorderedSnapshotMultiPageIteration tests HSCAN pagination.
func TestBrokerSnapshot_UnorderedSnapshotMultiPageIteration(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotKey := prefix + ":snapshot:test"
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Add 50 entries
	for i := 1; i <= 50; i++ {
		key := "key_" + strconv.Itoa(i)
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:         metaKey,
			SnapshotHashKey: snapshotKey,
			MessagePayload:  key,
			NewEpoch:        epoch,
			KeyedMemberTTL:  300,
		})
	}

	// Paginate through all entries
	allKeys := make(map[string]bool)
	cursor := "0"
	pageCount := 0

	for {
		result := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
			HashKey:     snapshotKey,
			MetaKey:     metaKey,
			Cursor:      cursor,
			Limit:       10, // 10 entries per page
			Now:         time.Now().Unix(),
			MetaTTL:     300,
			SnapshotTTL: 300,
		})

		pageCount++
		dataMap := kvArrayToMap(result.Data)
		for key := range dataMap {
			require.False(t, allKeys[key], "duplicate key across pages: %s", key)
			allKeys[key] = true
		}

		cursor = result.Cursor
		if cursor == "0" {
			break
		}

		require.Less(t, pageCount, 100, "pagination not terminating")
	}

	// Verify all 50 entries were returned
	require.Equal(t, 50, len(allKeys), "should return all entries across pages")
}

// TestBrokerSnapshot_ConcurrentPresenceUpdates tests concurrent join/leave operations.
func TestBrokerSnapshot_ConcurrentPresenceUpdates(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	presenceZSetKey := prefix + ":presence:zset:test"
	presenceHashKey := prefix + ":presence:hash:test"
	userZSetKey := prefix + ":user:zset:test"
	userHashKey := prefix + ":user:hash:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)
	expireAt := now + 300

	// Simulate concurrent joins from 10 clients (2 users, 5 clients each)
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			userID := "user" + strconv.Itoa((clientNum-1)/5+1) // user1 or user2
			clientID := "client" + strconv.Itoa(clientNum)
			info := `{"id":"` + clientID + `"}`

			runSnapshotPublishScript(t, client, &SnapshotLogParams{
				MetaKey:          metaKey,
				PresenceZSetKey:  presenceZSetKey,
				PresenceHashKey:  presenceHashKey,
				UserZSetKey:      userZSetKey,
				UserHashKey:      userHashKey,
				MessagePayload:   clientID,
				PresenceInfo:     info,
				PresenceExpireAt: expireAt,
				TrackUser:        true,
				UserID:           userID,
				KeyedMemberTTL:   300,
				NewEpoch:         epoch,
			})
		}(i)
	}
	wg.Wait()

	// Verify final state
	ctx := context.Background()
	presenceCount := client.Do(ctx, client.B().Hlen().Key(presenceHashKey).Build())
	require.NoError(t, presenceCount.Error())
	count, _ := presenceCount.AsInt64()
	require.Equal(t, int64(10), count, "all 10 clients should be present")

	// Verify user counts
	userData := client.Do(ctx, client.B().Hgetall().Key(userHashKey).Build())
	require.NoError(t, userData.Error())
	userMap, _ := userData.AsStrMap()
	require.Equal(t, "5", userMap["user1"], "user1 should have 5 connections")
	require.Equal(t, "5", userMap["user2"], "user2 should have 5 connections")

	// Now concurrently remove half the clients
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			userID := "user" + strconv.Itoa((clientNum-1)/5+1)
			clientID := "client" + strconv.Itoa(clientNum)

			runSnapshotPublishScript(t, client, &SnapshotLogParams{
				MetaKey:         metaKey,
				PresenceZSetKey: presenceZSetKey,
				PresenceHashKey: presenceHashKey,
				UserZSetKey:     userZSetKey,
				UserHashKey:     userHashKey,
				MessagePayload:  clientID,
				TrackUser:       true,
				UserID:          userID,
				IsLeave:         true,
				NewEpoch:        epoch,
			})
		}(i)
	}
	wg.Wait()

	// Verify final state after leaves
	presenceCount2 := client.Do(ctx, client.B().Hlen().Key(presenceHashKey).Build())
	require.NoError(t, presenceCount2.Error())
	count2, _ := presenceCount2.AsInt64()
	require.Equal(t, int64(5), count2, "5 clients should remain")

	userData2 := client.Do(ctx, client.B().Hgetall().Key(userHashKey).Build())
	require.NoError(t, userData2.Error())
	userMap2, _ := userData2.AsStrMap()
	require.Equal(t, "5", userMap2["user2"], "user2 should still have 5 connections")
	require.NotContains(t, userMap2, "user1", "user1 should be removed (0 connections)")
}

// TestBrokerSnapshot_OrderedSnapshotSameScores tests ordering when multiple entries have same score.
func TestBrokerSnapshot_OrderedSnapshotSameScores(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotHashKey := prefix + ":snapshot:test"
	snapshotOrderKey := prefix + ":snapshot:order:test"
	snapshotExpireKey := prefix + ":snapshot:expire:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Add 5 entries with same score (Redis ZSET uses lexicographic ordering for ties)
	entries := []string{"client_e", "client_a", "client_d", "client_b", "client_c"}
	for _, clientID := range entries {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:           metaKey,
			SnapshotHashKey:   snapshotHashKey,
			SnapshotOrderKey:  snapshotOrderKey,
			SnapshotExpireKey: snapshotExpireKey,
			MessagePayload:    clientID,
			Score:             100, // Same score for all
			KeyedMemberTTL:    300,
			NewEpoch:          epoch,
		})
	}

	// Read all entries
	result := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       0,
		Offset:      0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	require.Len(t, result.Keys, 5)
	// With same score, Redis uses lexicographic order (reversed since we use ZREVRANGE)
	require.Equal(t, "client_e", result.Keys[0])
	require.Equal(t, "client_d", result.Keys[1])
	require.Equal(t, "client_c", result.Keys[2])
	require.Equal(t, "client_b", result.Keys[3])
	require.Equal(t, "client_a", result.Keys[4])
}

// TestBrokerSnapshot_SimpleSnapshotExpiration tests per-entry expiration for simple HASH snapshots (no ordering).
func TestBrokerSnapshot_SimpleSnapshotExpiration(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotHashKey := prefix + ":snapshot:test"
	snapshotExpireKey := prefix + ":snapshot:expire:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Add 3 entries with different expiration times
	// Entry 1: expires in 2 seconds
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:           metaKey,
		SnapshotHashKey:   snapshotHashKey,
		SnapshotExpireKey: snapshotExpireKey,
		MessagePayload:    "short_lived",
		KeyedMemberTTL:    2,
		UseHExpire:        false, // Force ZSET-based expiration
		NewEpoch:          epoch,
	})

	// Entry 2: expires in 10 seconds
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:           metaKey,
		SnapshotHashKey:   snapshotHashKey,
		SnapshotExpireKey: snapshotExpireKey,
		MessagePayload:    "long_lived",
		KeyedMemberTTL:    10,
		UseHExpire:        false,
		NewEpoch:          epoch,
	})

	// Entry 3: already expired (for immediate cleanup test)
	ctx := context.Background()
	client.Do(ctx, client.B().Hset().Key(snapshotHashKey).FieldValue().FieldValue("already_expired", "already_expired").Build())
	client.Do(ctx, client.B().Zadd().Key(snapshotExpireKey).ScoreMember().ScoreMember(float64(now-100), "already_expired").Build())

	// Read immediately - should have 2 entries (expired one cleaned up)
	result1 := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:     snapshotHashKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Cursor:      "0",
		Limit:       0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	dataMap1 := kvArrayToMap(result1.Data)
	require.Len(t, dataMap1, 2, "should have 2 entries after cleanup")
	require.Contains(t, dataMap1, "short_lived")
	require.Contains(t, dataMap1, "long_lived")
	require.NotContains(t, dataMap1, "already_expired")

	// Wait 3 seconds for short_lived to expire
	time.Sleep(3 * time.Second)

	// Read again - short_lived should be cleaned up
	result2 := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:     snapshotHashKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Cursor:      "0",
		Limit:       0,
		Now:         time.Now().Unix(),
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	dataMap2 := kvArrayToMap(result2.Data)
	require.Len(t, dataMap2, 1, "should have 1 entry after short_lived expires")
	require.Contains(t, dataMap2, "long_lived")
	require.NotContains(t, dataMap2, "short_lived")
}

// TestBrokerSnapshot_JoinLeaveStreamLogging tests optional join/leave message logging to stream.
// Stream logging is controlled by passing/omitting StreamKey and MetaKey parameters.
func TestBrokerSnapshot_JoinLeaveStreamLogging(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	streamKey := prefix + ":stream:test"
	metaKey := prefix + ":meta:test"
	presenceHashKey := prefix + ":presence:hash:test"
	presenceZSetKey := prefix + ":presence:zset:test"

	ctx := context.Background()
	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Test 1: Join WITH stream logging (pass StreamKey and MetaKey)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		StreamKey:        streamKey,
		MetaKey:          metaKey,
		PresenceHashKey:  presenceHashKey,
		PresenceZSetKey:  presenceZSetKey,
		MessagePayload:   "client1",
		PresenceInfo:     `{"id":"client1"}`,
		PresenceExpireAt: now + 300,
		StreamSize:       100,
		StreamTTL:        300,
		MetaExpire:       300,
		KeyedMemberTTL:   300,
		NewEpoch:         epoch,
	})

	// Verify join was logged to stream
	entries1 := client.Do(ctx, client.B().Xlen().Key(streamKey).Build())
	require.NoError(t, entries1.Error())
	count1, _ := entries1.AsInt64()
	require.Equal(t, int64(1), count1, "join should be in stream")

	// Test 2: Join WITHOUT stream logging (omit StreamKey/MetaKey)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		// No StreamKey/MetaKey = don't log to stream
		PresenceHashKey:  presenceHashKey,
		PresenceZSetKey:  presenceZSetKey,
		MessagePayload:   "client2",
		PresenceInfo:     `{"id":"client2"}`,
		PresenceExpireAt: now + 300,
		KeyedMemberTTL:   300,
		NewEpoch:         epoch,
	})

	// Stream count should still be 1 (join not logged)
	entries2 := client.Do(ctx, client.B().Xlen().Key(streamKey).Build())
	require.NoError(t, entries2.Error())
	count2, _ := entries2.AsInt64()
	require.Equal(t, int64(1), count2, "join should NOT be in stream")

	// Test 3: Leave WITHOUT stream logging (omit StreamKey/MetaKey)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		// No StreamKey/MetaKey = don't log to stream
		PresenceHashKey: presenceHashKey,
		PresenceZSetKey: presenceZSetKey,
		MessagePayload:  "client1",
		IsLeave:         true,
		NewEpoch:        epoch,
	})

	// Verify presence removed but stream unchanged
	presenceExists := client.Do(ctx, client.B().Hexists().Key(presenceHashKey).Field("client1").Build())
	require.NoError(t, presenceExists.Error())
	exists, _ := presenceExists.AsBool()
	require.False(t, exists, "client1 should be removed from presence")

	entries3 := client.Do(ctx, client.B().Xlen().Key(streamKey).Build())
	require.NoError(t, entries3.Error())
	count3, _ := entries3.AsInt64()
	require.Equal(t, int64(1), count3, "leave should NOT be in stream")

	// Test 4: Leave WITH stream logging (pass StreamKey and MetaKey)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		StreamKey:       streamKey,
		MetaKey:         metaKey,
		PresenceHashKey: presenceHashKey,
		PresenceZSetKey: presenceZSetKey,
		MessagePayload:  "client2",
		StreamSize:      100,
		StreamTTL:       300,
		IsLeave:         true,
		NewEpoch:        epoch,
	})

	// Verify leave was logged to stream
	entries4 := client.Do(ctx, client.B().Xlen().Key(streamKey).Build())
	require.NoError(t, entries4.Error())
	count4, _ := entries4.AsInt64()
	require.Equal(t, int64(2), count4, "leave should be in stream")

	// Verify the leave message payload
	lastEntry := client.Do(ctx, client.B().Xrevrange().Key(streamKey).End("+").Start("-").Count(1).Build())
	require.NoError(t, lastEntry.Error())
	entryArr, _ := lastEntry.ToArray()
	require.Len(t, entryArr, 1)
	entryValues, _ := entryArr[0].ToArray()
	fieldValues, _ := entryValues[1].ToArray()
	var leavePayload string
	for i := 0; i < len(fieldValues); i += 2 {
		k, _ := fieldValues[i].ToString()
		if k == "d" {
			leavePayload, _ = fieldValues[i+1].ToString()
			break
		}
	}
	require.Equal(t, "client2", leavePayload, "leave message should contain client2")
}

// TestBrokerSnapshot_RecoveryFromSnapshot tests that snapshot reads return offset+epoch for recovery.
func TestBrokerSnapshot_RecoveryFromSnapshot(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	streamKey := prefix + ":stream:test"
	metaKey := prefix + ":meta:test"
	snapshotHashKey := prefix + ":snapshot:test"

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	// Simulate server state: mix of logged and unlogged messages
	// offset 1-3: normal messages (logged to stream)
	for i := 1; i <= 3; i++ {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			StreamKey:      streamKey,
			MetaKey:        metaKey,
			MessagePayload: "msg_" + strconv.Itoa(i),
			StreamSize:     100,
			StreamTTL:      300,
			MetaExpire:     300,
			NewEpoch:       epoch,
		})
	}

	// offset 4: join (NOT logged to stream, only in snapshot)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		SnapshotHashKey: snapshotHashKey,
		MessagePayload:  "client1",
		KeyedMemberTTL:  300,
		NewEpoch:        epoch,
	})

	// offset 5-7: more normal messages (logged to stream)
	for i := 5; i <= 7; i++ {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			StreamKey:      streamKey,
			MetaKey:        metaKey,
			MessagePayload: "msg_" + strconv.Itoa(i),
			StreamSize:     100,
			StreamTTL:      300,
			MetaExpire:     300,
			NewEpoch:       epoch,
		})
	}

	// Fresh client connects - reads snapshot to get current state
	snapshotResult := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:     snapshotHashKey,
		MetaKey:     metaKey,
		Cursor:      "0",
		Limit:       0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	// Verify snapshot returns BOTH offset and epoch
	require.Equal(t, int64(7), snapshotResult.Offset, "snapshot should return current offset")
	require.Equal(t, epoch, snapshotResult.Epoch, "snapshot should return current epoch")

	// Verify snapshot data
	dataMap := kvArrayToMap(snapshotResult.Data)
	require.Contains(t, dataMap, "client1", "snapshot should contain joined client")

	// Client can now use offset+epoch to start consuming from stream
	// Simulating: client subscribes from offset 7 with epoch
	// (In real code, client would use these to call history API with proper offset/epoch)

	// Test ordered snapshot as well
	snapshotOrderKey := prefix + ":snapshot:order:test"
	snapshotExpireKey := prefix + ":snapshot:expire:test"

	// Add to ordered snapshot
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:           metaKey,
		SnapshotHashKey:   snapshotHashKey,
		SnapshotOrderKey:  snapshotOrderKey,
		SnapshotExpireKey: snapshotExpireKey,
		MessagePayload:    "client2",
		Score:             100,
		KeyedMemberTTL:    300,
		NewEpoch:          epoch,
	})

	orderedResult := runSnapshotReadOrderedScript(t, client, &SnapshotReadOrderedParams{
		HashKey:     snapshotHashKey,
		OrderKey:    snapshotOrderKey,
		ExpireKey:   snapshotExpireKey,
		MetaKey:     metaKey,
		Limit:       0,
		Offset:      0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})

	// Verify ordered snapshot also returns offset+epoch
	require.Equal(t, int64(8), orderedResult.Offset, "ordered snapshot should return current offset")
	require.Equal(t, epoch, orderedResult.Epoch, "ordered snapshot should return current epoch")
	require.Contains(t, orderedResult.Keys, "client2")
}

// TestBrokerSnapshot_EdgeCases tests various edge cases.
func TestBrokerSnapshot_EdgeCases(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()

	metaKey := prefix + ":meta:test"
	snapshotKey := prefix + ":snapshot:test"
	presenceHashKey := prefix + ":presence:hash:test"
	presenceZSetKey := prefix + ":presence:zset:test"

	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	// Edge case 1: Leave before join (should be no-op)
	result1 := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:         metaKey,
		SnapshotHashKey: snapshotKey,
		MessagePayload:  "nonexistent_client",
		IsLeave:         true,
		NewEpoch:        epoch,
	})
	require.Greater(t, result1.Offset, int64(0))

	// Edge case 2: Join same client twice (should be idempotent for presence)
	now := time.Now().Unix()
	for i := 0; i < 2; i++ {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:          metaKey,
			PresenceHashKey:  presenceHashKey,
			PresenceZSetKey:  presenceZSetKey,
			MessagePayload:   "duplicate_client",
			PresenceInfo:     `{"id":"duplicate_client"}`,
			PresenceExpireAt: now + 300,
			KeyedMemberTTL:   300,
			NewEpoch:         epoch,
		})
	}

	ctx := context.Background()
	count := client.Do(ctx, client.B().Hlen().Key(presenceHashKey).Build())
	require.NoError(t, count.Error())
	presenceCount, _ := count.AsInt64()
	require.Equal(t, int64(1), presenceCount, "duplicate joins should not create duplicates")

	// Edge case 3: Read empty snapshot
	emptySnapshotKey := prefix + ":empty:snapshot"
	emptyMetaKey := prefix + ":empty:meta"
	emptyResult := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:     emptySnapshotKey,
		MetaKey:     emptyMetaKey,
		Cursor:      "0",
		Limit:       0,
		Now:         now,
		MetaTTL:     300,
		SnapshotTTL: 300,
	})
	require.Equal(t, "0", emptyResult.Cursor)
	require.Len(t, emptyResult.Data, 0)

	// Edge case 4: Publish with all storage disabled (just offset increment)
	minimalMetaKey := prefix + ":minimal:meta"
	minimalResult := runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:        minimalMetaKey,
		MessagePayload: "minimal_message",
		NewEpoch:       epoch,
	})
	require.Equal(t, int64(1), minimalResult.Offset)
	require.Equal(t, epoch, minimalResult.Epoch)
}

// TestBrokerSnapshot_EpochMismatchDetection tests that snapshot reads detect epoch mismatches.
func TestBrokerSnapshot_EpochMismatchDetection(t *testing.T) {
	client := setupRedisClient(t)
	defer client.Close()
	prefix := getUniquePrefix()
	ctx := context.Background()

	metaKey := prefix + ":meta:test"
	snapshotKey := prefix + ":snapshot:test"
	snapshotMetaKey := prefix + ":snapshot:meta:test"

	now := time.Now().Unix()
	epoch1 := "epoch1_" + strconv.FormatInt(now, 10)

	// Write snapshot entries in epoch1
	for i := 1; i <= 3; i++ {
		runSnapshotPublishScript(t, client, &SnapshotLogParams{
			MetaKey:         metaKey,
			SnapshotHashKey: snapshotKey,
			SnapshotMetaKey: snapshotMetaKey,
			MessagePayload:  "client" + strconv.Itoa(i),
			NewEpoch:        epoch1,
			KeyedMemberTTL:  300,
		})
	}

	// Verify snapshot has data
	result1 := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:         snapshotKey,
		MetaKey:         metaKey,
		SnapshotMetaKey: snapshotMetaKey,
		Cursor:          "0",
		Limit:           0,
		Now:             now,
		MetaTTL:         300,
		SnapshotTTL:     300,
	})
	require.Equal(t, epoch1, result1.Epoch)
	require.Len(t, result1.Data, 6) // 3 entries

	// Simulate epoch change: delete meta key (forces new epoch on next write)
	delCmd := client.B().Del().Key(metaKey).Build()
	client.Do(ctx, delCmd)

	// Write new message with different epoch (simulates stream reset)
	epoch2 := "epoch2_" + strconv.FormatInt(now+1, 10)
	runSnapshotPublishScript(t, client, &SnapshotLogParams{
		MetaKey:        metaKey,
		MessagePayload: "new_message",
		NewEpoch:       epoch2,
	})

	// Read snapshot - should detect epoch mismatch and return empty
	result2 := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:         snapshotKey,
		MetaKey:         metaKey,
		SnapshotMetaKey: snapshotMetaKey,
		Cursor:          "0",
		Limit:           0,
		Now:             now,
		MetaTTL:         300,
		SnapshotTTL:     300,
	})

	// Should return NEW epoch from stream, but EMPTY data (epoch mismatch)
	require.Equal(t, epoch2, result2.Epoch, "should return current stream epoch")
	require.Len(t, result2.Data, 0, "should return empty data due to epoch mismatch")

	// Verify snapshot data still exists in Redis (not deleted, just filtered out)
	existsCmd := client.B().Exists().Key(snapshotKey).Build()
	exists := client.Do(ctx, existsCmd)
	count, _ := exists.AsInt64()
	require.Equal(t, int64(1), count, "snapshot data should still exist")

	// Test with snapshot_meta deleted (simulates meta eviction)
	delMetaCmd := client.B().Del().Key(snapshotMetaKey).Build()
	client.Do(ctx, delMetaCmd)

	result3 := runSnapshotReadUnorderedScript(t, client, &SnapshotReadUnorderedParams{
		HashKey:         snapshotKey,
		MetaKey:         metaKey,
		SnapshotMetaKey: snapshotMetaKey,
		Cursor:          "0",
		Limit:           0,
		Now:             now,
		MetaTTL:         300,
		SnapshotTTL:     300,
	})

	// Should return empty when snapshot_meta doesn't exist
	require.Equal(t, epoch2, result3.Epoch)
	require.Len(t, result3.Data, 0, "should return empty when snapshot_meta missing")
}

// Helper types and functions

type SnapshotLogParams struct {
	StreamKey         string
	MetaKey           string
	ResultKey         string
	SnapshotHashKey   string
	SnapshotOrderKey  string
	SnapshotExpireKey string
	PresenceZSetKey   string
	PresenceHashKey   string
	UserZSetKey       string
	UserHashKey       string
	SnapshotMetaKey   string
	MessagePayload    string
	StreamSize        int
	StreamTTL         int
	Channel           string
	MetaExpire        int
	NewEpoch          string
	PublishCommand    string
	ResultKeyExpire   int
	UseDelta          bool
	Version           int
	VersionEpoch      string
	IsLeave           bool
	Score             int64
	KeyedMemberTTL    int
	UseHExpire        bool
	TrackUser         bool
	UserID            string
	PresenceInfo      string
	PresenceExpireAt  int64
}

type SnapshotLogResult struct {
	Offset     int64
	Epoch      string
	FromCache  bool
	Suppressed bool
}

type SnapshotReadOrderedParams struct {
	HashKey         string
	OrderKey        string
	ExpireKey       string
	MetaKey         string
	SnapshotMetaKey string
	Limit           int
	Offset          int
	Now             int64
	MetaTTL         int
	SnapshotTTL     int
}

type SnapshotReadOrderedResult struct {
	Offset int64
	Epoch  string
	Keys   []string
	Values []string
}

type SnapshotReadUnorderedParams struct {
	HashKey         string
	ExpireKey       string
	MetaKey         string
	SnapshotMetaKey string
	Cursor          string
	Limit           int
	Now             int64
	MetaTTL         int
	SnapshotTTL     int
}

type SnapshotReadUnorderedResult struct {
	Offset int64
	Epoch  string
	Cursor string
	Data   []string // key-value pairs as flat array
}

type SnapshotHistoryGetParams struct {
	StreamKey           string
	MetaKey             string
	IncludePublications bool
	SinceOffset         string
	Limit               int
	Reverse             bool
	MetaExpire          int
	NewEpochIfEmpty     string
}

type SnapshotHistoryGetResult struct {
	TopOffset    int64
	Epoch        string
	Publications [][]string // Each entry is [id, data]
}

type SnapshotPresenceGetParams struct {
	PresenceZSetKey string
	PresenceHashKey string
	Now             int64
	UseHExpire      bool
}

type SnapshotPresenceGetResult struct {
	Presence map[string]string // clientID -> info
}

type SnapshotPresenceStatsParams struct {
	PresenceZSetKey string
	PresenceHashKey string
	UserZSetKey     string
	UserHashKey     string
	Now             int64
	UseHExpire      bool
}

type SnapshotPresenceStatsResult struct {
	NumClients int
	NumUsers   int
}

func runSnapshotPublishScript(t *testing.T, client rueidis.Client, params *SnapshotLogParams) *SnapshotLogResult {
	t.Helper()
	ctx := context.Background()

	keys := []string{
		orEmpty(params.StreamKey),
		orEmpty(params.MetaKey),
		orEmpty(params.ResultKey),
		orEmpty(params.SnapshotHashKey),
		orEmpty(params.SnapshotOrderKey),
		orEmpty(params.SnapshotExpireKey),
		orEmpty(params.PresenceZSetKey),
		orEmpty(params.PresenceHashKey),
		orEmpty(params.UserZSetKey),
		orEmpty(params.UserHashKey),
		orEmpty(params.SnapshotMetaKey),
	}

	argv := []string{
		params.MessagePayload,
		strconv.Itoa(params.StreamSize),
		strconv.Itoa(params.StreamTTL),
		orEmpty(params.Channel),
		strconv.Itoa(params.MetaExpire),
		params.NewEpoch,
		orDefault(params.PublishCommand, "publish"),
		strconv.Itoa(params.ResultKeyExpire),
		boolToStr(params.UseDelta),
		strconv.Itoa(params.Version),
		params.VersionEpoch,
		boolToStr(params.IsLeave),
		strconv.FormatInt(params.Score, 10),
		strconv.Itoa(params.KeyedMemberTTL),
		boolToStr(params.UseHExpire),
		boolToStr(params.TrackUser),
		params.UserID,
		params.PresenceInfo,
		strconv.FormatInt(params.PresenceExpireAt, 10),
	}

	cmd := client.B().Eval().Script(brokerSnapshotPublishScript).Numkeys(int64(len(keys))).Key(keys...).Arg(argv...).Build()
	result := client.Do(ctx, cmd)
	require.NoError(t, result.Error())

	// Parse result: [offset, epoch, from_cache, suppressed]
	arrInterface, err := result.ToArray()
	require.NoError(t, err)
	require.Len(t, arrInterface, 4)

	offset, err := arrInterface[0].AsInt64()
	require.NoError(t, err)
	epoch, err := arrInterface[1].ToString()
	require.NoError(t, err)
	fromCache, err := arrInterface[2].ToString()
	require.NoError(t, err)
	suppressed, err := arrInterface[3].ToString()
	require.NoError(t, err)

	return &SnapshotLogResult{
		Offset:     offset,
		Epoch:      epoch,
		FromCache:  fromCache == "1",
		Suppressed: suppressed == "1",
	}
}

func runSnapshotReadOrderedScript(t *testing.T, client rueidis.Client, params *SnapshotReadOrderedParams) *SnapshotReadOrderedResult {
	t.Helper()
	ctx := context.Background()

	keys := []string{
		params.HashKey,
		params.OrderKey,
		params.ExpireKey,
		params.MetaKey,
		orEmpty(params.SnapshotMetaKey),
	}

	argv := []string{
		strconv.Itoa(params.Limit),
		strconv.Itoa(params.Offset),
		strconv.FormatInt(params.Now, 10),
		strconv.Itoa(params.MetaTTL),
		strconv.Itoa(params.SnapshotTTL),
	}

	cmd := client.B().Eval().Script(brokerSnapshotReadOrderedScript).Numkeys(int64(len(keys))).Key(keys...).Arg(argv...).Build()
	result := client.Do(ctx, cmd)
	require.NoError(t, result.Error())

	arr, err := result.ToArray()
	require.NoError(t, err)
	require.Len(t, arr, 4)

	offset, err := arr[0].ToString()
	require.NoError(t, err)

	epoch, err := arr[1].ToString()
	require.NoError(t, err)

	keysArr, err := arr[2].AsStrSlice()
	require.NoError(t, err)

	valuesArr, err := arr[3].AsStrSlice()
	require.NoError(t, err)

	offsetInt, _ := strconv.ParseInt(offset, 10, 64)

	return &SnapshotReadOrderedResult{
		Offset: offsetInt,
		Epoch:  epoch,
		Keys:   keysArr,
		Values: valuesArr,
	}
}

func runSnapshotReadUnorderedScript(t *testing.T, client rueidis.Client, params *SnapshotReadUnorderedParams) *SnapshotReadUnorderedResult {
	t.Helper()
	ctx := context.Background()

	keys := []string{
		params.HashKey,
		orEmpty(params.ExpireKey),
		params.MetaKey,
		orEmpty(params.SnapshotMetaKey),
	}

	argv := []string{
		params.Cursor,
		strconv.Itoa(params.Limit),
		strconv.FormatInt(params.Now, 10),
		strconv.Itoa(params.MetaTTL),
		strconv.Itoa(params.SnapshotTTL),
	}

	cmd := client.B().Eval().Script(brokerSnapshotReadUnorderedScript).Numkeys(int64(len(keys))).Key(keys...).Arg(argv...).Build()
	result := client.Do(ctx, cmd)
	require.NoError(t, result.Error())

	arr, err := result.ToArray()
	require.NoError(t, err)
	require.Len(t, arr, 4)

	offset, err := arr[0].ToString()
	require.NoError(t, err)

	epoch, err := arr[1].ToString()
	require.NoError(t, err)

	cursor, err := arr[2].ToString()
	require.NoError(t, err)

	dataArr, err := arr[3].AsStrSlice()
	require.NoError(t, err)

	offsetInt, _ := strconv.ParseInt(offset, 10, 64)

	return &SnapshotReadUnorderedResult{
		Offset: offsetInt,
		Epoch:  epoch,
		Cursor: cursor,
		Data:   dataArr,
	}
}

func runSnapshotHistoryGetScript(t *testing.T, client rueidis.Client, params *SnapshotHistoryGetParams) *SnapshotHistoryGetResult {
	t.Helper()
	ctx := context.Background()

	keys := []string{
		params.StreamKey,
		params.MetaKey,
	}

	argv := []string{
		boolToStr(params.IncludePublications),
		params.SinceOffset,
		strconv.Itoa(params.Limit),
		boolToStr(params.Reverse),
		strconv.Itoa(params.MetaExpire),
		params.NewEpochIfEmpty,
	}

	cmd := client.B().Eval().Script(brokerSnapshotHistoryGetScript).Numkeys(int64(len(keys))).Key(keys...).Arg(argv...).Build()
	result := client.Do(ctx, cmd)
	require.NoError(t, result.Error())

	arr, err := result.ToArray()
	require.NoError(t, err)
	require.Len(t, arr, 3)

	topOffset, err := arr[0].AsInt64()
	require.NoError(t, err)

	epoch, err := arr[1].ToString()
	require.NoError(t, err)

	var publications [][]string
	if !arr[2].IsNil() {
		pubsArr, err := arr[2].ToArray()
		require.NoError(t, err)

		for _, pubEntry := range pubsArr {
			entryArr, err := pubEntry.ToArray()
			require.NoError(t, err)

			if len(entryArr) >= 2 {
				id, _ := entryArr[0].ToString()
				fieldVals, err := entryArr[1].ToArray()
				require.NoError(t, err)

				var data string
				for i := 0; i < len(fieldVals); i += 2 {
					k, _ := fieldVals[i].ToString()
					if k == "d" {
						data, _ = fieldVals[i+1].ToString()
						break
					}
				}
				publications = append(publications, []string{id, data})
			}
		}
	}

	return &SnapshotHistoryGetResult{
		TopOffset:    topOffset,
		Epoch:        epoch,
		Publications: publications,
	}
}

func runSnapshotPresenceGetScript(t *testing.T, client rueidis.Client, params *SnapshotPresenceGetParams) *SnapshotPresenceGetResult {
	t.Helper()
	ctx := context.Background()

	keys := []string{
		params.PresenceZSetKey,
		params.PresenceHashKey,
	}

	argv := []string{
		strconv.FormatInt(params.Now, 10),
		boolToStr(params.UseHExpire),
	}

	cmd := client.B().Eval().Script(brokerSnapshotPresenceGetScript).Numkeys(int64(len(keys))).Key(keys...).Arg(argv...).Build()
	result := client.Do(ctx, cmd)
	require.NoError(t, result.Error())

	dataArr, err := result.AsStrSlice()
	require.NoError(t, err)

	presence := make(map[string]string)
	for i := 0; i < len(dataArr); i += 2 {
		if i+1 < len(dataArr) {
			presence[dataArr[i]] = dataArr[i+1]
		}
	}

	return &SnapshotPresenceGetResult{
		Presence: presence,
	}
}

func runSnapshotPresenceStatsScript(t *testing.T, client rueidis.Client, params *SnapshotPresenceStatsParams) *SnapshotPresenceStatsResult {
	t.Helper()
	ctx := context.Background()

	keys := []string{
		params.PresenceZSetKey,
		params.PresenceHashKey,
		params.UserZSetKey,
		params.UserHashKey,
	}

	argv := []string{
		strconv.FormatInt(params.Now, 10),
		boolToStr(params.UseHExpire),
	}

	cmd := client.B().Eval().Script(brokerSnapshotPresenceStatsScript).Numkeys(int64(len(keys))).Key(keys...).Arg(argv...).Build()
	result := client.Do(ctx, cmd)
	require.NoError(t, result.Error())

	arr, err := result.ToArray()
	require.NoError(t, err)
	require.Len(t, arr, 2)

	numClients, err := arr[0].AsInt64()
	require.NoError(t, err)

	numUsers, err := arr[1].AsInt64()
	require.NoError(t, err)

	return &SnapshotPresenceStatsResult{
		NumClients: int(numClients),
		NumUsers:   int(numUsers),
	}
}

func setupRedisClient(t *testing.T) rueidis.Client {
	t.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	require.NoError(t, err)
	return client
}

func orEmpty(s string) string {
	if s == "" {
		return ""
	}
	return s
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func boolToStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func kvArrayToMap(arr []string) map[string]string {
	m := make(map[string]string)
	for i := 0; i < len(arr); i += 2 {
		if i+1 < len(arr) {
			m[arr[i]] = arr[i+1]
		}
	}
	return m
}
