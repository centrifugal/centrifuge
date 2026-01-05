package centrifuge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

// BenchmarkPerKeyNonOrdered benchmarks non-ordered per-key history
// using only HASH for storage (simplest approach)
func BenchmarkPerKeyNonOrdered(b *testing.B) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		b.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	channel := "bench:perkey:nonordered"
	hashKey := fmt.Sprintf("test.perkey.%s", channel)
	metaKey := fmt.Sprintf("test.perkey.meta.%s", channel)

	// Cleanup
	defer func() {
		client.Do(ctx, client.B().Del().Key(hashKey).Build())
		client.Do(ctx, client.B().Del().Key(metaKey).Build())
	}()

	b.Run("Add", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		i := 0
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i++
				key := fmt.Sprintf("key_%d", i%1000) // 1000 unique keys
				data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))

				// Simple HSET + EXPIRE
				cmds := []rueidis.Completed{
					client.B().Hset().Key(hashKey).FieldValue().FieldValue(key, string(data)).Build(),
					client.B().Expire().Key(hashKey).Seconds(300).Build(),
				}
				for _, cmd := range cmds {
					if err := client.Do(ctx, cmd).Error(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	})

	b.Run("Read", func(b *testing.B) {
		// Populate data first
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			client.Do(ctx, client.B().Hset().Key(hashKey).FieldValue().FieldValue(key, string(data)).Build())
		}

		b.ReportAllocs()
		b.ResetTimer()

		i := 0
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i++
				// HGETALL to retrieve all keys
				result := client.Do(ctx, client.B().Hgetall().Key(hashKey).Build())
				if result.Error() != nil {
					b.Fatal(result.Error())
				}

				// Parse results
				kvs, err := result.AsStrMap()
				if err != nil {
					b.Fatal(err)
				}

				if len(kvs) == 0 {
					b.Fatal("Expected results")
				}
			}
		})
	})

	b.Run("ReadPaginated", func(b *testing.B) {
		// Populate data first
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			client.Do(ctx, client.B().Hset().Key(hashKey).FieldValue().FieldValue(key, string(data)).Build())
		}

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// HSCAN with cursor for pagination (100 items per page)
				cursor := uint64(0)
				totalItems := 0

				for {
					result := client.Do(ctx, client.B().Hscan().Key(hashKey).Cursor(cursor).Count(100).Build())
					if result.Error() != nil {
						b.Fatal(result.Error())
					}

					entry, err := result.AsScanEntry()
					if err != nil {
						b.Fatal(err)
					}

					totalItems += len(entry.Elements)
					cursor = entry.Cursor

					// For benchmark, just do one page
					break
				}

				if totalItems == 0 {
					b.Fatal("Expected paginated results")
				}
			}
		})
	})
}

// BenchmarkPerKeyOrdered benchmarks ordered per-key history
// using HASH + order ZSET + expire ZSET (3 data structures)
func BenchmarkPerKeyOrdered(b *testing.B) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		b.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	channel := "bench:perkey:ordered"
	hashKey := fmt.Sprintf("test.perkey.%s", channel)
	orderKey := fmt.Sprintf("test.perkey.order.%s", channel)
	expireKey := fmt.Sprintf("test.perkey.expire.%s", channel)
	metaKey := fmt.Sprintf("test.perkey.meta.%s", channel)

	// Cleanup
	defer func() {
		client.Do(ctx, client.B().Del().Key(hashKey).Build())
		client.Do(ctx, client.B().Del().Key(orderKey).Build())
		client.Do(ctx, client.B().Del().Key(expireKey).Build())
		client.Do(ctx, client.B().Del().Key(metaKey).Build())
	}()

	b.Run("Add", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		ttl := int64(300)
		now := time.Now().Unix()

		i := 0
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i++
				key := fmt.Sprintf("key_%d", i%1000) // 1000 unique keys
				data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
				score := float64(i % 1000) // Score for ordering
				expireAt := now + ttl

				// Update all 3 structures + set TTL
				cmds := []rueidis.Completed{
					client.B().Hset().Key(hashKey).FieldValue().FieldValue(key, string(data)).Build(),
					client.B().Zadd().Key(orderKey).ScoreMember().ScoreMember(score, key).Build(),
					client.B().Zadd().Key(expireKey).ScoreMember().ScoreMember(float64(expireAt), key).Build(),
					client.B().Expire().Key(hashKey).Seconds(ttl).Build(),
					client.B().Expire().Key(orderKey).Seconds(ttl).Build(),
					client.B().Expire().Key(expireKey).Seconds(ttl).Build(),
				}

				for _, cmd := range cmds {
					if err := client.Do(ctx, cmd).Error(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	})

	b.Run("Read", func(b *testing.B) {
		// Populate data first
		now := time.Now().Unix()
		ttl := int64(300)

		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			score := float64(i)
			expireAt := now + ttl

			client.Do(ctx, client.B().Hset().Key(hashKey).FieldValue().FieldValue(key, string(data)).Build())
			client.Do(ctx, client.B().Zadd().Key(orderKey).ScoreMember().ScoreMember(score, key).Build())
			client.Do(ctx, client.B().Zadd().Key(expireKey).ScoreMember().ScoreMember(float64(expireAt), key).Build())
		}

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Step 1: Cleanup expired (in real impl this would be in Lua)
				nowTs := time.Now().Unix()
				expiredResult := client.Do(ctx, client.B().Zrangebyscore().Key(expireKey).Min("-inf").Max(fmt.Sprintf("%d", nowTs)).Build())
				if expiredResult.Error() != nil {
					b.Fatal(expiredResult.Error())
				}

				expired, _ := expiredResult.AsStrSlice()
				if len(expired) > 0 {
					// Remove from all structures (simplified for benchmark)
					client.Do(ctx, client.B().Hdel().Key(hashKey).Field(expired...).Build())
					client.Do(ctx, client.B().Zrem().Key(orderKey).Member(expired...).Build())
					client.Do(ctx, client.B().Zremrangebyscore().Key(expireKey).Min("-inf").Max(fmt.Sprintf("%d", nowTs)).Build())
				}

				// Step 2: Get ordered keys (descending by score)
				keysResult := client.Do(ctx, client.B().Zrevrange().Key(orderKey).Start(0).Stop(-1).Build())
				if keysResult.Error() != nil {
					b.Fatal(keysResult.Error())
				}

				keys, err := keysResult.AsStrSlice()
				if err != nil {
					b.Fatal(err)
				}

				if len(keys) == 0 {
					b.Fatal("Expected keys")
				}

				// Step 3: Fetch data from HASH
				if len(keys) > 0 {
					dataResult := client.Do(ctx, client.B().Hmget().Key(hashKey).Field(keys...).Build())
					if dataResult.Error() != nil {
						b.Fatal(dataResult.Error())
					}

					values, err := dataResult.AsStrSlice()
					if err != nil {
						b.Fatal(err)
					}

					if len(values) == 0 {
						b.Fatal("Expected values")
					}
				}
			}
		})
	})

	b.Run("ReadPaginated", func(b *testing.B) {
		// Populate data first
		now := time.Now().Unix()
		ttl := int64(300)

		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			score := float64(i)
			expireAt := now + ttl

			client.Do(ctx, client.B().Hset().Key(hashKey).FieldValue().FieldValue(key, string(data)).Build())
			client.Do(ctx, client.B().Zadd().Key(orderKey).ScoreMember().ScoreMember(score, key).Build())
			client.Do(ctx, client.B().Zadd().Key(expireKey).ScoreMember().ScoreMember(float64(expireAt), key).Build())
		}

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Cleanup expired
				nowTs := time.Now().Unix()
				expiredResult := client.Do(ctx, client.B().Zrangebyscore().Key(expireKey).Min("-inf").Max(fmt.Sprintf("%d", nowTs)).Build())
				if expiredResult.Error() != nil {
					b.Fatal(expiredResult.Error())
				}

				expired, _ := expiredResult.AsStrSlice()
				if len(expired) > 0 {
					client.Do(ctx, client.B().Hdel().Key(hashKey).Field(expired...).Build())
					client.Do(ctx, client.B().Zrem().Key(orderKey).Member(expired...).Build())
					client.Do(ctx, client.B().Zremrangebyscore().Key(expireKey).Min("-inf").Max(fmt.Sprintf("%d", nowTs)).Build())
				}

				// Paginated read: 100 items per page, descending
				offset := int64(0)
				limit := int64(100)
				totalItems := 0

				for {
					keysResult := client.Do(ctx, client.B().Zrevrange().Key(orderKey).Start(offset).Stop(offset+limit-1).Build())
					if keysResult.Error() != nil {
						b.Fatal(keysResult.Error())
					}

					keys, err := keysResult.AsStrSlice()
					if err != nil {
						b.Fatal(err)
					}

					if len(keys) == 0 {
						break // No more data
					}

					// Fetch data for this page
					dataResult := client.Do(ctx, client.B().Hmget().Key(hashKey).Field(keys...).Build())
					if dataResult.Error() != nil {
						b.Fatal(dataResult.Error())
					}

					values, err := dataResult.AsStrSlice()
					if err != nil {
						b.Fatal(err)
					}

					totalItems += len(values)
					offset += limit

					// For benchmark, just do one page
					break
				}

				if totalItems == 0 {
					b.Fatal("Expected paginated results")
				}
			}
		})
	})
}

// BenchmarkPerKeyNonOrderedLua benchmarks non-ordered per-key history
// using Lua script for atomic operations (as per plan section 2.2)
func BenchmarkPerKeyNonOrderedLua(b *testing.B) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		b.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	channel := "bench:perkey:nonordered:lua"
	hashKey := fmt.Sprintf("test.perkey.%s", channel)
	metaKey := fmt.Sprintf("test.perkey.meta.%s", channel)

	// Cleanup
	defer func() {
		client.Do(ctx, client.B().Del().Key(hashKey).Build())
		client.Do(ctx, client.B().Del().Key(metaKey).Build())
	}()

	// Precompile Lua scripts for efficient execution (uses EVALSHA)
	addScript := rueidis.NewLuaScript(`
local hash_key = KEYS[1]
local meta_key = KEYS[2]

local key = ARGV[1]
local pub_data = ARGV[2]
local ttl = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local meta_ttl = tonumber(ARGV[5])

-- Ensure epoch exists
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = tostring(now)
    redis.call("hset", meta_key, "e", epoch)
end
if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Update hash
redis.call("hset", hash_key, key, pub_data)
redis.call("expire", hash_key, ttl)

return {epoch, "0", "0"}
`)

	readScript := rueidis.NewLuaScript(`
local hash_key = KEYS[1]
local meta_key = KEYS[2]

local cursor = ARGV[1]
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local meta_ttl = tonumber(ARGV[4])

-- Update meta TTL
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = tostring(now)
    redis.call("hset", meta_key, "e", epoch)
end
if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Return epoch and raw HSCAN/HGETALL result (no table building)
if limit > 0 then
    local result = redis.call("hscan", hash_key, cursor, "COUNT", limit)
    return {epoch, result[1], result[2]}  -- epoch, cursor, key-value array
else
    local data = redis.call("hgetall", hash_key)
    return {epoch, "0", data}  -- epoch, cursor="0", key-value array
end
`)

	b.Run("Add", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		i := 0
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i++
				key := fmt.Sprintf("key_%d", i%1000) // 1000 unique keys
				data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
				now := time.Now().Unix()

				result := addScript.Exec(ctx, client,
					[]string{hashKey, metaKey},
					[]string{key, string(data), "300", fmt.Sprintf("%d", now), "3600"})

				if result.Error() != nil {
					b.Fatal(result.Error())
				}
			}
		})
	})

	b.Run("Read", func(b *testing.B) {
		// Populate data first
		now := time.Now().Unix()
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			addScript.Exec(ctx, client,
				[]string{hashKey, metaKey},
				[]string{key, string(data), "300", fmt.Sprintf("%d", now), "3600"})
		}

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				now := time.Now().Unix()
				result := readScript.Exec(ctx, client,
					[]string{hashKey, metaKey},
					[]string{"0", "0", fmt.Sprintf("%d", now), "3600"})

				if result.Error() != nil {
					b.Fatal(result.Error())
				}

				// Parse structured result: [epoch, cursor, [k,v,k,v,...]]
				arr, err := result.ToArray()
				if err != nil {
					b.Fatal(err)
				}

				if len(arr) < 3 {
					b.Fatal("Expected epoch, cursor, data")
				}

				// arr[2] contains key-value pairs
				kvs, err := arr[2].AsStrSlice()
				if err != nil {
					b.Fatal(err)
				}

				if len(kvs) == 0 {
					b.Fatal("Expected results")
				}
			}
		})
	})

	b.Run("ReadPaginated", func(b *testing.B) {
		// Populate data first
		now := time.Now().Unix()
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			addScript.Exec(ctx, client,
				[]string{hashKey, metaKey},
				[]string{key, string(data), "300", fmt.Sprintf("%d", now), "3600"})
		}

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				cursor := "0"
				totalItems := 0
				now := time.Now().Unix()

				for {
					result := readScript.Exec(ctx, client,
						[]string{hashKey, metaKey},
						[]string{cursor, "100", fmt.Sprintf("%d", now), "3600"})

					if result.Error() != nil {
						b.Fatal(result.Error())
					}

					// Parse structured result: [epoch, cursor, [k,v,k,v,...]]
					arr, err := result.ToArray()
					if err != nil {
						b.Fatal(err)
					}

					if len(arr) < 3 {
						b.Fatal("Invalid response")
					}

					kvs, err := arr[2].AsStrSlice()
					if err != nil {
						b.Fatal(err)
					}

					totalItems += len(kvs) / 2

					// For benchmark, just do one page
					break
				}

				if totalItems == 0 {
					b.Fatal("Expected paginated results")
				}
			}
		})
	})
}

// BenchmarkPerKeyOrderedLua benchmarks ordered per-key history
// using Lua scripts (as per plan section 8.2)
func BenchmarkPerKeyOrderedLua(b *testing.B) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		b.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	channel := "bench:perkey:ordered:lua"
	hashKey := fmt.Sprintf("test.perkey.%s", channel)
	orderKey := fmt.Sprintf("test.perkey.order.%s", channel)
	expireKey := fmt.Sprintf("test.perkey.expire.%s", channel)
	metaKey := fmt.Sprintf("test.perkey.meta.%s", channel)

	// Cleanup
	defer func() {
		client.Do(ctx, client.B().Del().Key(hashKey).Build())
		client.Do(ctx, client.B().Del().Key(orderKey).Build())
		client.Do(ctx, client.B().Del().Key(expireKey).Build())
		client.Do(ctx, client.B().Del().Key(metaKey).Build())
	}()

	// Precompile Lua scripts for efficient execution (uses EVALSHA)
	addOrderedScript := rueidis.NewLuaScript(`
local hash_key = KEYS[1]
local order_key = KEYS[2]
local expire_key = KEYS[3]
local meta_key = KEYS[4]

local key = ARGV[1]
local pub_data = ARGV[2]
local score = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local meta_ttl = tonumber(ARGV[6])

-- Ensure epoch exists
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = tostring(now)
    redis.call("hset", meta_key, "e", epoch)
end
if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Calculate expiration timestamp
local expire_at = now + ttl

-- Update all 3 structures
redis.call("hset", hash_key, key, pub_data)
redis.call("zadd", order_key, score, key)
redis.call("zadd", expire_key, expire_at, key)

-- Refresh global TTL on all structures
redis.call("expire", hash_key, ttl)
redis.call("expire", order_key, ttl)
redis.call("expire", expire_key, ttl)

return {epoch, "0", "0"}
`)

	readOrderedScript := rueidis.NewLuaScript(`
local hash_key   = KEYS[1]
local order_key  = KEYS[2]
local expire_key = KEYS[3]
local meta_key   = KEYS[4]

local limit     = tonumber(ARGV[1])
local offset    = tonumber(ARGV[2])
local now_str       = ARGV[3]
local meta_ttl  = tonumber(ARGV[4])

-- Update meta epoch + TTL
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = now_str
    redis.call("hset", meta_key, "e", epoch)
end
if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Cleanup expired entries
local expired = redis.call("zrangebyscore", expire_key, "-inf", now_str)
local expired_len = #expired
if expired_len > 0 then
    redis.call("hdel", hash_key, unpack(expired))
    redis.call("zrem", order_key, unpack(expired))
    redis.call("zremrangebyscore", expire_key, "-inf", now_str)
end

-- Fetch ordered keys (descending)
local keys
if limit > 0 then
    keys = redis.call("zrevrange", order_key, offset, offset + limit - 1)
else
    keys = redis.call("zrevrange", order_key, 0, -1)
end

local key_count = #keys
if key_count == 0 then
    return {epoch, {}, {}}
end

-- Fetch values in one call
local values = redis.call("hmget", hash_key, unpack(keys))

return {epoch, keys, values}
`)

	b.Run("Add", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		ttl := int64(300)

		i := 0
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i++
				key := fmt.Sprintf("key_%d", i%1000) // 1000 unique keys
				data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
				score := float64(i % 1000) // Score for ordering
				now := time.Now().Unix()

				result := addOrderedScript.Exec(ctx, client,
					[]string{hashKey, orderKey, expireKey, metaKey},
					[]string{key, string(data), fmt.Sprintf("%f", score), fmt.Sprintf("%d", ttl),
						fmt.Sprintf("%d", now), "3600"})

				if result.Error() != nil {
					b.Fatal(result.Error())
				}
			}
		})
	})

	b.Run("Read", func(b *testing.B) {
		// Populate data first
		now := time.Now().Unix()
		ttl := int64(300)

		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			score := float64(i)

			addOrderedScript.Exec(ctx, client,
				[]string{hashKey, orderKey, expireKey, metaKey},
				[]string{key, string(data), fmt.Sprintf("%f", score), fmt.Sprintf("%d", ttl),
					fmt.Sprintf("%d", now), "3600"})
		}

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				now := time.Now().Unix()
				result := readOrderedScript.Exec(ctx, client,
					[]string{hashKey, orderKey, expireKey, metaKey},
					[]string{"0", "0", fmt.Sprintf("%d", now), "3600"})

				if result.Error() != nil {
					b.Fatal(result.Error())
				}

				// Parse structured result: [epoch, [keys...], [values...]]
				arr, err := result.ToArray()
				if err != nil {
					b.Fatal(err)
				}

				if len(arr) < 3 {
					b.Fatal("Expected epoch, keys, values")
				}

				// arr[1] = keys, arr[2] = values
				keys, _ := arr[1].AsStrSlice()
				values, _ := arr[2].AsStrSlice()

				if len(keys) == 0 || len(values) == 0 {
					b.Fatal("Expected results")
				}
			}
		})
	})

	b.Run("ReadPaginated", func(b *testing.B) {
		// Populate data first
		now := time.Now().Unix()
		ttl := int64(300)

		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			data := []byte(fmt.Sprintf(`{"data":"value_%d"}`, i))
			score := float64(i)

			addOrderedScript.Exec(ctx, client,
				[]string{hashKey, orderKey, expireKey, metaKey},
				[]string{key, string(data), fmt.Sprintf("%f", score), fmt.Sprintf("%d", ttl),
					fmt.Sprintf("%d", now), "3600"})
		}

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				now := time.Now().Unix()
				offset := int64(0)
				limit := int64(100)
				totalItems := 0

				for {
					result := readOrderedScript.Exec(ctx, client,
						[]string{hashKey, orderKey, expireKey, metaKey},
						[]string{fmt.Sprintf("%d", limit), fmt.Sprintf("%d", offset),
							fmt.Sprintf("%d", now), "3600"})

					if result.Error() != nil {
						b.Fatal(result.Error())
					}

					// Parse structured result: [epoch, [keys...], [values...]]
					arr, err := result.ToArray()
					if err != nil {
						b.Fatal(err)
					}

					if len(arr) < 3 {
						break
					}

					keys, _ := arr[1].AsStrSlice()
					if len(keys) == 0 {
						break
					}

					totalItems += len(keys)
					offset += limit

					// For benchmark, just do one page
					break
				}

				if totalItems == 0 {
					b.Fatal("Expected paginated results")
				}
			}
		})
	})
}
