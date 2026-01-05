# Implementation Plan: Per-Key History Mode & Cursor Pagination

## Overview

Add a new per-key history storage mode alongside existing append-log (stream) mode. The per-key mode stores only the latest publication per key (hash/map semantics) with cursor-based pagination. Also add cursor pagination to Presence API.

**Key Design Decisions:**
- Channel-level configuration (via PublishOptions)
- Reuse History() API - use cursor instead of StreamPosition for per-key mode
- Key extracted from Publication.Tags field (configurable field name)
- Opaque base64-encoded cursor tokens
- Optional cursor parameter for Presence (backward compatible)
- No recovery support for per-key mode (snapshot-based)

---

## 1. Core API Changes

### 1.1 New Types (`broker.go`)

```go
// HistoryMode defines storage mode for history
type HistoryMode int

const (
    HistoryModeStream  HistoryMode = 0  // Append-log (existing, default)
    HistoryModePerKey  HistoryMode = 1  // Hash/map with latest-per-key
)
```

### 1.2 Updated HistoryFilter

```go
type HistoryFilter struct {
    Since   *StreamPosition  // For stream mode only
    Cursor  string           // For per-key mode only (opaque base64-encoded cursor)
    Limit   int              // -1=no limit, 0=position only (stream mode)
    Reverse bool             // Stream mode only
}
```

### 1.3 Updated History Return Type

**Modify existing `HistoryResult` type to add cursor:**
```go
type HistoryResult struct {
    Publications []*Publication
    Offset       uint64  // StreamPosition offset (for stream mode)
    Epoch        string  // StreamPosition epoch (for stream mode)
    NextCursor   string  // For per-key mode, empty if no more results
}
```

**Update Broker interface to return struct:**
```go
// Broker interface (CHANGED return type):
type Broker interface {
    Publish(ch string, data []byte, opts PublishOptions) (StreamPosition, bool, error)
    History(ch string, opts HistoryOptions) (HistoryResult, error)  // CHANGED
    RemoveHistory(ch string) error
}
```

Note: This is a breaking change to the Broker interface, but cleaner than alternatives.

### 1.4 PublishOptions Extension

```go
type PublishOptions struct {
    // ... existing fields (including HistoryTTL) ...
    HistoryMode HistoryMode  // Default: HistoryModeStream
    PerKey      string       // For per-key mode: the key to use (required if HistoryMode == HistoryModePerKey)
                             // TTL comes from existing HistoryTTL field
}
```

### 1.5 Presence Pagination Types (`presence.go`)

```go
type PresenceFilter struct {
    Cursor string  // Opaque base64-encoded cursor, empty = start from beginning
    Limit  int     // 0 = all (backward compatible)
}

type PresenceResult struct {
    Presence   map[string]*ClientInfo
    NextCursor string  // Empty if no more results
}

// PresenceManager interface (MODIFY existing Presence method):
type PresenceManager interface {
    Presence(ch string, filter PresenceFilter) (PresenceResult, error)  // CHANGED signature
    PresenceStats(ch string) (PresenceStats, error)  // Unchanged
    AddPresence(ch string, clientID string, info *ClientInfo) error  // Unchanged
    RemovePresence(ch string, clientID string, userID string) error  // Unchanged
}
```

Note: This changes the signature of existing `Presence()` method but doesn't add new methods.

---

## 2. Redis Backend Implementation

### 2.1 Key Structure

**Per-key history keys:**
```
{prefix}.perkey.{channel}             -> HASH: key -> marshaled Publication
{prefix}.perkey.expire.{channel}      -> ZSET: key -> expireAt (if not using HEXPIRE)
{prefix}.perkey.meta.{channel}        -> HASH: {"e": epoch}
```

**Sharded mode:**
```
{prefix}.perkey.{partition}.{channel}
{prefix}.perkey.expire.{partition}.{channel}
{prefix}.perkey.meta.{partition}.{channel}
```

### 2.2 New Lua Scripts

#### `broker_history_add_perkey.lua`

Atomically add/update per-key history entry with TTL.

**Keys:**
- KEYS[1] - per-key hash
- KEYS[2] - meta hash
- KEYS[3] - expire ZSET (if not using HEXPIRE)
- KEYS[4] - result cache key (idempotency)

**Args:**
- ARGV[1] - key (from PublishOptions.PerKey)
- ARGV[2] - marshaled Publication
- ARGV[3] - TTL seconds (from PublishOptions.HistoryTTL)
- ARGV[4] - publish channel (empty if SkipPubSub)
- ARGV[5] - meta TTL seconds
- ARGV[6] - current timestamp
- ARGV[7] - publish command ("publish" or "spublish")
- ARGV[8] - result cache TTL (idempotency)
- ARGV[9] - use HEXPIRE ("0" or "1")

**Logic:**
1. **Check result cache for idempotency** (same as stream mode)
   - If cached, return cached result immediately
2. Ensure epoch exists in meta
3. HSET key -> publication
4. Set per-field TTL (HEXPIRE) or update expire ZSET
5. Publish to PUB/SUB
6. **Cache result for idempotency** (same as stream mode)
   - Use same result key structure as stream mode
   - Allows duplicate publish detection

**Return:** `{epoch, "0", from_cache}`

**Important:** Must support all idempotency features from existing stream mode:
- Result caching with TTL
- Duplicate detection
- Same cache key format for consistency

#### `broker_history_get_perkey.lua`

Retrieve per-key history with cursor pagination.

**Keys:**
- KEYS[1] - per-key hash
- KEYS[2] - meta hash
- KEYS[3] - expire ZSET (if not using HEXPIRE)

**Args:**
- ARGV[1] - cursor (Redis HSCAN cursor, "0" for start)
- ARGV[2] - limit (0 = no pagination, return all)
- ARGV[3] - current timestamp
- ARGV[4] - meta TTL seconds
- ARGV[5] - use HEXPIRE ("0" or "1")

**Logic:**
1. Clean expired entries (if not using HEXPIRE)
2. If limit > 0: HSCAN with cursor and COUNT
3. If limit = 0: HGETALL (return all)

**Return:** `{epoch, next_cursor, [key, val, key, val, ...]}`

#### Update `presence_get.lua` for pagination (backward compatible)

Extend existing script to support cursor pagination when limit is provided.

**New Args (append to existing):**
- ARGV[...] - cursor (HSCAN cursor, "0" for start)
- ARGV[...] - limit (0 = return all, backward compatible)

**Logic Change:**
```lua
-- Existing logic for expiration cleanup remains...

-- NEW: Check if pagination requested
local limit = tonumber(ARGV[limit_index]) or 0
local cursor = ARGV[cursor_index] or "0"

if limit > 0 and use_hexpire == "1" then
    -- Pagination mode (only with HEXPIRE)
    local result = redis.call("hscan", presence_hash_key, cursor, "COUNT", limit)
    return {result[1], result[2]}  -- {next_cursor, [key, val, ...]}
elseif limit > 0 and use_hexpire == "0" then
    -- Pagination not supported without HEXPIRE
    return redis.error_reply("Pagination requires UseHashFieldTTL")
else
    -- Backward compatible: return all (existing behavior)
    local presence = redis.call("hgetall", presence_hash_key)
    return {"0", presence}  -- cursor "0" means complete
end
```

**Return:** `{next_cursor, [clientID, clientInfo, ...]}`

**Important:**
- Only supports pagination when `UseHashFieldTTL = true`
- Returns error if pagination requested without HEXPIRE
- Backward compatible: if limit=0, returns all (existing behavior)

### 2.3 RedisBroker Changes (`broker_redis.go`)

**New methods:**
- `publishPerKey()` - calls broker_history_add_perkey.lua
- `historyPerKey()` - calls broker_history_get_perkey.lua

**Modified methods:**
- `Publish()` - route to publishPerKey() if opts.HistoryMode == HistoryModePerKey
- `History()` - detect mode from filter (Cursor != nil) and route to historyPerKey()

**Config addition:**
```go
type RedisBrokerConfig struct {
    // ... existing ...
    UseHashFieldTTLForHistory bool  // Requires Redis 7.4+
}
```

### 2.4 RedisPresenceManager Changes (`presence_redis.go`)

**Update existing method signature:**
```go
func (m *RedisPresenceManager) Presence(ch string, filter PresenceFilter) (PresenceResult, error) {
    // Calls extended presence_get.lua script (backward compatible)
    // Script uses HSCAN if limit > 0 and UseHashFieldTTL=true
    // Otherwise uses HGETALL (existing behavior)
}
```

Routes to appropriate Lua script based on filter parameters.

---

## 3. Memory Backend Implementation

### 3.1 New File: `broker_memory_perkey.go`

**Purpose:** Organization - separate file for per-key mode helper methods and types.

```go
type perKeyHub struct {
    sync.RWMutex
    stores         map[string]*perKeyStore  // channel -> store
    historyMetaTTL time.Duration
    closeCh        chan struct{}
}

type perKeyStore struct {
    sync.RWMutex
    epoch       string
    entries     map[string]*perKeyEntry  // key -> entry
    expireQueue *priorityqueue.Queue     // TTL tracking
}

type perKeyEntry struct {
    pub      *Publication
    expireAt int64
}
```

**Methods:**
- `add(ch, key string, pub *Publication, ttl time.Duration) (string, error)`
- `get(ch string, cursor string, limit int) (HistoryResult, error)`

**Cursor encoding for memory backend:**
```json
{"o": 42}  // offset into sorted keys (base64-encoded)
```

Note: This file doesn't implement a separate Broker interface - just helper types/methods called by MemoryBroker.

### 3.2 MemoryBroker Changes (`broker_memory.go`)

- Add `perKeyHub` field
- Route Publish()/History() based on mode
- Implement cursor pagination by sorting map keys and using offset

### 3.3 MemoryPresenceManager Changes (`presence_memory.go`)

Update existing `Presence()` method to support pagination:
```go
func (m *MemoryPresenceManager) Presence(ch string, filter PresenceFilter) (PresenceResult, error) {
    // If filter is empty (Limit=0, Cursor=""), return all (backward compat)
    // Otherwise:
    // 1. Sort clientIDs for deterministic order
    // 2. Decode cursor to get offset: {"o": 10}
    // 3. Return slice from offset to offset+limit
    // 4. Encode next cursor if more results exist
}
```

---

## 4. Cursor Encoding/Decoding

### 4.1 New File: `cursor.go`

```go
type cursorData struct {
    RedisCursor string `json:"r,omitempty"`  // Redis HSCAN cursor
    Offset      int    `json:"o,omitempty"`  // Memory backend offset
}

func encodeCursor(data cursorData) string {
    b, _ := json.Marshal(data)
    return base64.URLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursorData, error) {
    b, err := base64.URLEncoding.DecodeString(s)
    if err != nil {
        return cursorData{}, err
    }
    var data cursorData
    err = json.Unmarshal(b, &data)
    return data, err
}
```

---

## 5. Recovery Behavior

### 5.1 Disable Recovery for Per-Key Mode

**In `client.go` (subscription handling):**

```go
// Check if channel uses per-key mode (detect from broker or channel config)
if isPerKeyMode(channel) {
    if cmd.Recover {
        return nil, nil, ErrorRecoveryNotSupported
    }
    // Disable positioning
    opts.EnablePositioning = false
}
```

**New error constant (`errors.go`):**
```go
var ErrorRecoveryNotSupported = &Error{
    Code:    105,
    Message: "recovery not supported for snapshot-based history",
}
```

**Alternative approach:** Store mode in channel options/config, check before enabling recovery.

---

## 6. Protocol Changes

### 6.1 External Protocol Package Updates

**Required changes to `github.com/centrifugal/protocol`:**

```protobuf
// In HistoryResult:
message HistoryResult {
    repeated Publication publications = 1;
    uint64 offset = 2;
    string epoch = 3;
    optional string next_cursor = 4;  // NEW
}

// In HistoryRequest:
message HistoryRequest {
    string channel = 1;
    optional int32 limit = 2;
    optional StreamPosition since = 3;
    optional bool reverse = 4;
    optional string cursor = 5;  // NEW
}

// In PresenceResult:
message PresenceResult {
    map<string, ClientInfo> presence = 1;
    optional string next_cursor = 2;  // NEW
}

// In PresenceRequest:
message PresenceRequest {
    string channel = 1;
    optional string cursor = 2;  // NEW
    optional int32 limit = 3;     // NEW
}
```

**Backward compatibility:** Optional fields allow old clients to work with new server.

---

## 7. Use Cases

### 7.1 Device/Sensor State Tracking

**Problem:** 10,000 IoT devices send state updates every 30 seconds. Only latest state matters.

**Solution with per-key mode:**
```go
// Device publishes state
broker.Publish("room:sensors", sensorData, PublishOptions{
    HistoryMode: HistoryModePerKey,
    PerKey:      "sensor_123",  // Key provided directly
    HistoryTTL:  5 * time.Minute,
})

// Client requests snapshot of all devices
result, _ := node.History("room:sensors", WithHistoryFilter(HistoryFilter{
    Limit: 100,  // Page size
}))
// Returns latest state per device, paginated

// Next page
if result.NextCursor != "" {
    nextResult, _ := node.History("room:sensors", WithHistoryFilter(HistoryFilter{
        Cursor: result.NextCursor,
        Limit:  100,
    }))
}
```

**Benefits:**
- Constant memory usage (one entry per device)
- No need to iterate through all historical updates
- Automatic TTL cleanup for inactive devices

### 7.2 Live Leaderboard/Rankings

**Problem:** Gaming leaderboard with 50k users. Only current scores matter, not history.

**Solution:**
```go
// Update player score
broker.Publish("game:leaderboard", scoreData, PublishOptions{
    HistoryMode: HistoryModePerKey,
    PerKey:      "user_789",  // Player ID as key
    HistoryTTL:  24 * time.Hour,
})

// Get top 100 (requires sorting in application layer)
result, _ := node.History("game:leaderboard")
```

### 7.3 Presence in Large Channels

**Problem:** Conference channel with 10k attendees. Listing all at once is expensive.

**Solution:**
```go
// Get first 50 attendees
result, _ := presenceManager.PresencePaginated("conference:main", PresenceFilter{
    Limit: 50,
})

// Load more as user scrolls
for result.NextCursor != "" {
    result, _ = presenceManager.PresencePaginated("conference:main", PresenceFilter{
        Cursor: result.NextCursor,
        Limit:  50,
    })
}
```

### 7.4 Real-time Collaborative Cursors

**Problem:** Track cursor positions of 500 users in document editor. Only latest position matters.

**Solution:**
```go
broker.Publish("doc:cursors", cursorData, PublishOptions{
    HistoryMode: HistoryModePerKey,
    PerKey:      "user_456",  // User ID as key
    HistoryTTL:  30 * time.Second,  // Fast TTL for inactive cursors
})
```

---

## 8. Suggested Improvements

### 8.1 Automatic Mode Detection

Instead of requiring mode in every PublishOptions, detect from channel namespace config:

```go
type ChannelOptions struct {
    // ... existing ...
    HistoryMode HistoryMode
    // Note: PerKey still provided per-publish, not configured per-channel
}

// In namespaces.go
func (n *Node) channelOpts(ch string) (ChannelOptions, bool) {
    // ... existing logic ...
}
```

**Benefit:** Cleaner API, mode configured once per channel pattern (but key still provided per-publish).

### 8.2 Ordered Key Retrieval (Simplified with Explicit Expiration Tracking)

**Approach:** Use three data structures for explicit expiration tracking - simpler than HEXPIRE.

**Key Structure:**
```
{prefix}.perkey.{channel}        -> HASH: key -> marshaled Publication
{prefix}.perkey.order.{channel}  -> ZSET: score -> key (for ordered retrieval)
{prefix}.perkey.expire.{channel} -> ZSET: expireAtUnix -> key (for TTL tracking)
{prefix}.perkey.meta.{channel}   -> HASH: {"e": epoch}
```

**PublishOptions Addition:**
```go
type PublishOptions struct {
    // ... existing ...
    PerKey      string
    PerKeyScore *float64  // Optional: if set, enables ordered mode
}
```

**Lua Script for Write (broker_history_add_perkey_ordered.lua):**
```lua
-- KEYS[1] - HASH (data)
-- KEYS[2] - order ZSET
-- KEYS[3] - expire ZSET
-- KEYS[4] - meta HASH
-- KEYS[5] - result cache key (idempotency)
-- ARGV[1] - key
-- ARGV[2] - publication data (marshaled)
-- ARGV[3] - order score
-- ARGV[4] - TTL seconds
-- ARGV[5] - current timestamp
-- ARGV[6] - meta TTL seconds
-- ARGV[7] - publish channel (empty if SkipPubSub)
-- ARGV[8] - publish command ("publish" or "spublish")
-- ARGV[9] - result cache TTL (empty if no idempotency)

local hash_key = KEYS[1]
local order_key = KEYS[2]
local expire_key = KEYS[3]
local meta_key = KEYS[4]
local result_key = KEYS[5]

local key = ARGV[1]
local pub_data = ARGV[2]
local score = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local meta_ttl = tonumber(ARGV[6])
local channel = ARGV[7]
local publish_cmd = ARGV[8]
local result_ttl = ARGV[9]

-- Check result cache for idempotency
if result_ttl ~= '' then
    local cached = redis.call("get", result_key)
    if cached then
        return {"0", cached, "1"}  -- from_cache=true
    end
end

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

-- Update all three structures
redis.call("hset", hash_key, key, pub_data)
redis.call("zadd", order_key, score, key)
redis.call("zadd", expire_key, expire_at, key)

-- Refresh global TTL on all structures (for cleanup of empty data)
redis.call("expire", hash_key, ttl)
redis.call("expire", order_key, ttl)
redis.call("expire", expire_key, ttl)

-- Publish to PUB/SUB
if channel ~= '' then
    redis.call(publish_cmd, channel, pub_data)
end

-- Cache result for idempotency
if result_ttl ~= '' then
    redis.call("set", result_key, "1", "EX", result_ttl)
end

return {epoch, "0", "0"}  -- epoch, "0" (offset N/A), from_cache=false
```

**Lua Script for Read (broker_history_get_perkey_ordered.lua):**
```lua
-- KEYS[1] - HASH (data)
-- KEYS[2] - order ZSET
-- KEYS[3] - expire ZSET
-- KEYS[4] - meta HASH
-- ARGV[1] - last_score (or "-inf" for start)
-- ARGV[2] - last_key (or "" for start)
-- ARGV[3] - limit (number of results to return)
-- ARGV[4] - reverse (0=ascending, 1=descending)
-- ARGV[5] - current timestamp
-- ARGV[6] - meta TTL seconds

local hash_key = KEYS[1]
local order_key = KEYS[2]
local expire_key = KEYS[3]
local meta_key = KEYS[4]

local last_score = ARGV[1]
local last_key = ARGV[2]
local requested_limit = tonumber(ARGV[3])
local reverse = ARGV[4]
local now = tonumber(ARGV[5])
local meta_ttl = tonumber(ARGV[6])

-- Update meta TTL
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = tostring(now)
    redis.call("hset", meta_key, "e", epoch)
end
if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Step 1: Cleanup expired entries
local expired = redis.call("zrangebyscore", expire_key, "-inf", tostring(now))
if #expired > 0 then
    redis.call("hdel", hash_key, unpack(expired))
    redis.call("zrem", order_key, unpack(expired))
    redis.call("zremrangebyscore", expire_key, "-inf", tostring(now))
end

-- Step 2: Fetch ordered keys with fetch-until-full logic
local result = {}
local result_count = 0
local batch_size = math.max(requested_limit * 2, 100)
local skip_mode = (last_key ~= "")
local next_score = last_score
local next_key = last_key
local exhausted = false

while result_count < requested_limit and not exhausted do
    -- Fetch batch from order ZSET (INCLUSIVE score range for duplicate scores)
    local batch
    if reverse == "0" then
        batch = redis.call("zrangebyscore", order_key, next_score, "+inf",
                          "LIMIT", 0, batch_size)
    else
        batch = redis.call("zrevrangebyscore", order_key, next_score, "-inf",
                          "LIMIT", 0, batch_size)
    end

    if #batch == 0 then
        exhausted = true
        break
    end

    -- Process batch: skip past last_key, collect valid entries
    for _, key in ipairs(batch) do
        -- Handle skip_mode for duplicate scores
        if skip_mode then
            if key == next_key then
                skip_mode = false
                goto continue
            else
                local key_score = redis.call("zscore", order_key, key)
                if tonumber(key_score) == tonumber(next_score) then
                    goto continue
                else
                    skip_mode = false
                end
            end
        end

        -- Fetch publication from HASH
        local pub = redis.call("hget", hash_key, key)
        if pub then
            table.insert(result, key)
            table.insert(result, pub)
            result_count = result_count + 1

            next_score = redis.call("zscore", order_key, key)
            next_key = key

            if result_count >= requested_limit then
                break
            end
        end

        ::continue::
    end

    -- Check if exhausted
    if #batch < batch_size then
        exhausted = true
    end
end

-- Encode cursor
local next_cursor = ""
if not exhausted and result_count >= requested_limit then
    next_cursor = cjson.encode({ls = tonumber(next_score), lk = next_key})
end

return {epoch, next_cursor, result}
```

**Benefits:**
- ✅ Works with any Redis version (no HEXPIRE needed)
- ✅ Explicit expiration - cleaner than HEXPIRE + fallback
- ✅ Ordered retrieval (ascending/descending by score)
- ✅ Guarantees exactly N results (fetch-until-full)
- ✅ Handles duplicate scores via skip_mode

**Trade-offs:**
- 3 data structures vs 1 HASH (extra memory)
- More write operations per publish
- Worth it for ordered use cases (leaderboards, time-series)

**Example:**
```go
broker.Publish("game:leaderboard", data, PublishOptions{
    HistoryMode:  HistoryModePerKey,
    PerKey:       "player_123",
    PerKeyScore:  Float64Ptr(9500.0),  // Enables ordered mode
    HistoryTTL:   24 * time.Hour,
})
```

### 8.3 Batch Key Retrieval

Add API to fetch specific keys (instead of HSCAN pagination):

```go
type HistoryFilter struct {
    // ... existing ...
    Keys []string  // NEW: Fetch specific keys only (per-key mode)
}
```

**Implementation:** Use HMGET instead of HSCAN.

**Use case:** Fetch state for specific subset of devices.

### 8.4 Stats for Per-Key Mode

Add method to get key count and memory usage:

```go
type PerKeyStats struct {
    NumKeys    int
    MemoryUsed int64  // Estimate
    OldestKey  string
    NewestKey  string
}

// Broker interface addition:
HistoryStats(ch string) (PerKeyStats, error)
```

**Redis implementation:** HLEN, MEMORY USAGE commands.

### 8.5 Migration Helper

Provide utility to convert from stream to per-key mode:

```go
func (n *Node) MigrateHistoryMode(ch string, keyExtractor func(*Publication) string) error {
    // 1. Fetch all stream history
    // 2. Extract keys, keep latest per key
    // 3. Publish to per-key mode
    // 4. Remove old stream
}
```

---

## 9. Testing Strategy

### 9.1 Unit Tests

**New test files:**
- `broker_redis_perkey_test.go` - Per-key mode Redis tests
- `broker_memory_perkey_test.go` - Per-key mode memory tests
- `cursor_test.go` - Cursor encoding/decoding tests
- `presence_paginated_test.go` - Presence pagination tests

**Key test cases:**
- Per-key publish overwrites previous value for same key
- Different keys coexist in same channel
- TTL expiration (HEXPIRE and ZSET fallback)
- Cursor pagination (empty, partial, complete, invalid cursor)
- Epoch generation and consistency
- Idempotency with result cache
- Missing key field handling (should error)
- Empty key value handling
- Presence pagination with dynamic presence changes

### 9.2 Integration Tests

**Test file:** `integration_perkey_test.go`

- Test with real Redis (UseHashFieldTTL true/false)
- Test mixed channels (some stream, some per-key)
- Test concurrent access patterns
- Test HSCAN consistency with concurrent modifications
- Test cursor across server restarts (should fail gracefully)

### 9.3 Backward Compatibility Tests

- Old clients must work with new server (ignore cursor fields)
- Stream mode must work unchanged
- Ensure no performance regression for stream mode
- Test protocol parsing with missing cursor fields

### 9.4 Benchmark Tests

**New file:** `bench_perkey_test.go`

```go
func BenchmarkPerKeyPublish(b *testing.B)
func BenchmarkPerKeyHistory(b *testing.B)
func BenchmarkStreamVsPerKey(b *testing.B)
func BenchmarkPresencePaginated(b *testing.B)
```

---

## 10. Implementation Phases

### Phase 1: Foundation (Core Types & Cursor)
**Files:**
- `broker.go` - Add HistoryMode, update PublishOptions with PerKey field, modify Broker.History() to return HistoryResult, update HistoryFilter
- `cursor.go` (new) - Cursor encoding/decoding utilities
- `presence.go` - Add PresenceFilter, PresenceResult, modify PresenceManager.Presence() signature
- `cursor_test.go` (new) - Test cursor functions

**Deliverable:** Type system ready, cursor encoding working. Note: Breaking changes to Broker and PresenceManager interfaces.

### Phase 2: Redis Per-Key History
**Files:**
- `internal/redis_lua/broker_history_add_perkey.lua` (new)
- `internal/redis_lua/broker_history_get_perkey.lua` (new)
- `broker_redis.go` - publishPerKey(), historyPerKey(), route in Publish()/History()
- `broker_redis_perkey_test.go` (new)

**Deliverable:** Per-key history working in Redis with HEXPIRE support.

### Phase 3: Memory Per-Key History
**Files:**
- `broker_memory_perkey.go` (new) - perKeyHub, perKeyStore
- `broker_memory.go` - integrate perKeyHub, route based on mode
- `broker_memory_perkey_test.go` (new)

**Deliverable:** Per-key history working in memory backend.

### Phase 4: Presence Pagination
**Files:**
- `internal/redis_lua/presence_get.lua` - Extend existing script to support HSCAN pagination (backward compatible)
- `presence_redis.go` - Update Presence() method to pass cursor/limit parameters
- `presence_memory.go` - Update Presence() method to support pagination
- `presence_paginated_test.go` (new)

**Deliverable:** Presence pagination working for both backends via updated Presence() method. Redis pagination only works with UseHashFieldTTL=true.

### Phase 5: Recovery & Client Integration
**Files:**
- `client.go` - Disable recovery for per-key mode
- `errors.go` - Add ErrorRecoveryNotSupported
- `node.go` - Update History() wrapper if needed
- `client_test.go` - Test recovery prevention

**Deliverable:** Recovery correctly disabled for per-key channels.

### Phase 6: Protocol & Documentation
**Files:**
- Update `github.com/centrifugal/protocol` (external)
- Update examples
- Add migration guide
- Add API documentation

**Deliverable:** Full feature ready for release.

### Phase 7: Testing & Benchmarks
**Files:**
- `integration_perkey_test.go` (new)
- `bench_perkey_test.go` (new)
- Add test coverage for edge cases

**Deliverable:** Fully tested, benchmarked, production-ready.

---

## 11. Edge Cases & Considerations

### 11.1 Key Validation Failure

**Problem:** PerKey is empty when HistoryMode is PerKey.

**Solution:** Return error from Publish(), don't silently fail.

```go
if opts.HistoryMode == HistoryModePerKey {
    if opts.PerKey == "" {
        return StreamPosition{}, false, errors.New("per-key mode requires non-empty PerKey field")
    }
}
```

### 11.2 Cursor Invalidation

**Problem:** Redis restart, cluster reshard, or TTL expiration invalidates cursor.

**Solution:** Return error, client restarts from beginning.

```go
if decodedCursor.RedisCursor == "" {
    return HistoryResult{}, errors.New("invalid cursor")
}
```

### 11.3 HSCAN Consistency

**Problem:** Redis HSCAN doesn't guarantee point-in-time snapshot. Keys added/removed during scan may be missed or duplicated.

**Solution:** Document as limitation. For most use cases (sensor states, presence), eventual consistency is acceptable.

### 11.4 Mixed Mode Prevention

**Problem:** Accidentally publishing in different modes to same channel corrupts data.

**Solution:** Track mode per channel in metadata, validate on publish:

```lua
-- In Lua script, check meta for existing mode
local existing_mode = redis.call("hget", meta_key, "mode")
if existing_mode and existing_mode ~= ARGV[mode_index] then
    return redis.error_reply("history mode mismatch")
end
redis.call("hset", meta_key, "mode", ARGV[mode_index])
```

### 11.5 Memory Backend Cursor Stability

**Problem:** In-memory map iteration order is non-deterministic in Go.

**Solution:** Sort keys before pagination:

```go
keys := make([]string, 0, len(store.entries))
for k := range store.entries {
    keys = append(keys, k)
}
sort.Strings(keys)  // Deterministic order

offset := cursor.Offset
for i := offset; i < len(keys) && i < offset+limit; i++ {
    results = append(results, store.entries[keys[i]].pub)
}
```

### 11.6 Large Hash Performance

**Problem:** HSCAN on very large hash (100k+ keys) may be slow.

**Mitigation:**
- Use HSCAN COUNT parameter to control iteration chunk size
- Consider HSCAN only for initial page, then use HMGET with known keys
- Document recommended max keys per channel (~10k)

### 11.7 TTL Drift

**Problem:** ZSET fallback mode requires cleanup on read. If no reads occur, expired keys stay in hash.

**Solution:** Not needed for Redis backend! The ZSET itself has a TTL that gets updated on each publish. If keys stop being updated, the ZSET TTL won't be refreshed and the entire ZSET (and hash) will eventually expire together. This provides automatic cleanup.

**For memory backend:** Rely on TTL-based expiration queue (priority queue) to automatically remove expired entries, similar to how existing historyHub works.

---

## 12. Critical Files Summary

### Core Interface & Types
- `broker.go`
- `presence.go`
- `cursor.go` (new)

### Redis Backend
- `broker_redis.go`
- `presence_redis.go`
- `internal/redis_lua/broker_history_add_perkey.lua` (new)
- `internal/redis_lua/broker_history_get_perkey.lua` (new)
- `internal/redis_lua/presence_get.lua` (extend existing for pagination)

### Memory Backend
- `broker_memory.go`
- `broker_memory_perkey.go` (new)
- `presence_memory.go`

### Client & Recovery
- `client.go`
- `errors.go`
- `node.go`

### Configuration
- `config.go`
- `options.go`

### Tests
- `broker_redis_perkey_test.go` (new)
- `broker_memory_perkey_test.go` (new)
- `presence_paginated_test.go` (new)
- `cursor_test.go` (new)
- `integration_perkey_test.go` (new)
- `bench_perkey_test.go` (new)

### External Dependencies
- `github.com/centrifugal/protocol` - Protocol definitions (requires updates)

---

## Summary

This plan provides a complete architecture for adding per-key history mode with cursor pagination to Centrifuge. The design:

1. **Modifies existing interfaces** - Changes Broker.History() and PresenceManager.Presence() signatures, but doesn't add new methods
2. **Channel-level configuration** - Mode determined per-channel via PublishOptions
3. **Follows existing patterns** - Per-field TTL implementation matches presence system, idempotency support matches stream mode
4. **Backward compatible where possible** - Optional protocol fields, existing stream mode unchanged, but interface changes are breaking
5. **Production-ready** - Handles edge cases, includes comprehensive testing strategy, supports idempotency
6. **Well-scoped use cases** - Device state, leaderboards, presence, collaborative tools

**Breaking Changes:**
- `Broker.History()` return type changes from `([]*Publication, StreamPosition, error)` to `(HistoryResult, error)`
- `PresenceManager.Presence()` signature changes from `(ch string)` to `(ch string, filter PresenceFilter)` and returns `PresenceResult` instead of `map[string]*ClientInfo`

The implementation can proceed in phases, with each phase delivering working, testable functionality.
