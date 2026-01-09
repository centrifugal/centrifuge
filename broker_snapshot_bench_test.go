//go:build integration

package centrifuge

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

var snapshotPublishLuaScript *rueidis.Lua

func init() {
	snapshotPublishLuaScript = rueidis.NewLuaScript(brokerSnapshotPublishScript)
}

func runSnapshotPublishScriptBench(b *testing.B, client rueidis.Client, params *SnapshotLogParams) *SnapshotLogResult {
	b.Helper()
	ctx := context.Background()

	keys := []string{
		orEmpty(params.StreamKey),
		orEmpty(params.MetaKey),
		orEmpty(params.ResultKey),
		orEmpty(params.SnapshotHashKey),
		orEmpty(params.SnapshotOrderKey),
		orEmpty(params.SnapshotExpireKey),
		orEmpty(params.UserZSetKey),
		orEmpty(params.UserHashKey),
		orEmpty(params.SnapshotMetaKey),
	}

	argv := []string{
		params.MessageKey,
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
	}

	result := snapshotPublishLuaScript.Exec(ctx, client, keys, argv)
	require.NoError(b, result.Error())

	arrInterface, err := result.ToArray()
	require.NoError(b, err)
	require.Len(b, arrInterface, 4)

	offset, err := arrInterface[0].AsInt64()
	require.NoError(b, err)
	epoch, err := arrInterface[1].ToString()
	require.NoError(b, err)
	fromCache, err := arrInterface[2].ToString()
	require.NoError(b, err)
	suppressed, err := arrInterface[3].ToString()
	require.NoError(b, err)

	return &SnapshotLogResult{
		Offset:     offset,
		Epoch:      epoch,
		FromCache:  fromCache == "1",
		Suppressed: suppressed == "1",
	}
}

func setupRedisBench(b *testing.B) (rueidis.Client, func()) {
	b.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	require.NoError(b, err)
	return client, func() {
		client.Close()
	}
}

// BenchmarkBrokerSnapshot_AppendLogOnly benchmarks append log functionality.
func BenchmarkBrokerSnapshot_AppendLogOnly(b *testing.B) {
	client, cleanup := setupRedisBench(b)
	defer cleanup()

	prefix := "bench_append_log"
	streamKey := prefix + ":stream"
	metaKey := prefix + ":meta"

	// Cleanup keys before starting.
	client.Do(context.Background(), client.B().Del().Key(streamKey, metaKey).Build())

	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			payload := "message_" + strconv.Itoa(i)
			runSnapshotPublishScriptBench(b, client, &SnapshotLogParams{
				StreamKey:      streamKey,
				MetaKey:        metaKey,
				MessageKey:     "",
				MessagePayload: payload,
				StreamSize:     10000,
				StreamTTL:      300,
				MetaExpire:     300,
				NewEpoch:       epoch,
			})
		}
	})
}

// BenchmarkBrokerSnapshot_KeyedSnapshotSimple benchmarks simple HASH-based keyed snapshot.
func BenchmarkBrokerSnapshot_KeyedSnapshotSimple(b *testing.B) {
	client, cleanup := setupRedisBench(b)
	defer cleanup()

	prefix := "bench_keyed_simple"
	metaKey := prefix + ":meta"
	snapshotKey := prefix + ":snapshot"

	client.Do(context.Background(), client.B().Del().Key(metaKey, snapshotKey).Build())

	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			key := "client" + strconv.Itoa(i)
			runSnapshotPublishScriptBench(b, client, &SnapshotLogParams{
				MetaKey:         metaKey,
				SnapshotHashKey: snapshotKey,
				MessageKey:      key,
				MessagePayload:  key,
				NewEpoch:        epoch,
				KeyedMemberTTL:  300,
			})
		}
	})
}

// BenchmarkBrokerSnapshot_KeyedSnapshotOrdered benchmarks ordered HASH+ZSET keyed snapshot.
func BenchmarkBrokerSnapshot_KeyedSnapshotOrdered(b *testing.B) {
	client, cleanup := setupRedisBench(b)
	defer cleanup()

	prefix := "bench_keyed_ordered"
	metaKey := prefix + ":meta"
	snapshotHashKey := prefix + ":snapshot"
	snapshotOrderKey := prefix + ":snapshot:order"
	snapshotExpireKey := prefix + ":snapshot:expire"

	client.Do(context.Background(), client.B().Del().Key(metaKey, snapshotHashKey, snapshotOrderKey, snapshotExpireKey).Build())

	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			key := "client" + strconv.Itoa(i)
			runSnapshotPublishScriptBench(b, client, &SnapshotLogParams{
				MetaKey:           metaKey,
				SnapshotHashKey:   snapshotHashKey,
				SnapshotOrderKey:  snapshotOrderKey,
				SnapshotExpireKey: snapshotExpireKey,
				MessageKey:        key,
				MessagePayload:    key,
				Score:             int64(i),
				KeyedMemberTTL:    300,
				NewEpoch:          epoch,
			})
		}
	})
}

// BenchmarkBrokerSnapshot_Presence benchmarks presence tracking functionality.
func BenchmarkBrokerSnapshot_Presence(b *testing.B) {
	client, cleanup := setupRedisBench(b)
	defer cleanup()

	prefix := "bench_presence"
	metaKey := prefix + ":meta"
	presenceSnapshotHashKey := prefix + ":presence:snapshot" // Members uses snapshot hash
	presenceSnapshotExpireKey := prefix + ":presence:expire" // Members uses snapshot expire zset
	userZSetKey := prefix + ":user:zset"
	userHashKey := prefix + ":user:hash"

	client.Do(context.Background(), client.B().Del().Key(metaKey, presenceSnapshotExpireKey, presenceSnapshotHashKey, userZSetKey, userHashKey).Build())

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			clientID := "client" + strconv.Itoa(i)
			userID := "user" + strconv.Itoa(i%100) // 100 different users.
			info := `{"client":"` + clientID + `","user":"` + userID + `"}`
			runSnapshotPublishScriptBench(b, client, &SnapshotLogParams{
				MetaKey:           metaKey,
				SnapshotHashKey:   presenceSnapshotHashKey,   // Members uses snapshot hash
				SnapshotExpireKey: presenceSnapshotExpireKey, // Members uses snapshot expire
				UserZSetKey:       userZSetKey,
				UserHashKey:       userHashKey,
				MessageKey:        clientID,
				MessagePayload:    info, // Members info goes in payload
				TrackUser:         true,
				UserID:            userID,
				KeyedMemberTTL:    300,
				NewEpoch:          epoch,
			})
		}
	})
}

// BenchmarkBrokerSnapshot_CombinedFeatures benchmarks using multiple features together.
func BenchmarkBrokerSnapshot_CombinedFeatures(b *testing.B) {
	client, cleanup := setupRedisBench(b)
	defer cleanup()

	prefix := "bench_combined"
	streamKey := prefix + ":stream"
	metaKey := prefix + ":meta"
	snapshotKey := prefix + ":snapshot"
	presenceSnapshotHashKey := prefix + ":presence:snapshot" // Members uses snapshot hash
	presenceSnapshotExpireKey := prefix + ":presence:expire" // Members uses snapshot expire zset
	channel := prefix + ":channel"

	client.Do(context.Background(), client.B().Del().Key(streamKey, metaKey, snapshotKey, presenceSnapshotHashKey, presenceSnapshotExpireKey).Build())

	now := time.Now().Unix()
	epoch := strconv.FormatInt(now, 10)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			clientID := "client" + strconv.Itoa(i)
			// Benchmark: keyed state update (presence is similar, just uses different keys)
			runSnapshotPublishScriptBench(b, client, &SnapshotLogParams{
				StreamKey:       streamKey,
				MetaKey:         metaKey,
				SnapshotHashKey: snapshotKey, // Keyed state snapshot
				Channel:         channel,
				MessageKey:      clientID,
				MessagePayload:  clientID,
				StreamSize:      10000,
				StreamTTL:       300,
				MetaExpire:      300,
				KeyedMemberTTL:  300,
				PublishCommand:  "publish",
				NewEpoch:        epoch,
			})
		}
	})
}

// BenchmarkBrokerSnapshot_PubSubOnly benchmarks PUB/SUB without any storage.
func BenchmarkBrokerSnapshot_PubSubOnly(b *testing.B) {
	client, cleanup := setupRedisBench(b)
	defer cleanup()

	channel := "bench_pubsub_only:channel"
	epoch := strconv.FormatInt(time.Now().Unix(), 10)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			payload := "message_" + strconv.Itoa(i)
			runSnapshotPublishScriptBench(b, client, &SnapshotLogParams{
				Channel:        channel,
				MessageKey:     "",
				MessagePayload: payload,
				PublishCommand: "publish",
				NewEpoch:       epoch,
			})
		}
	})
}
