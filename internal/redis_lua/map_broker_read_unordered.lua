-- Read unordered keyed state with cursor-based pagination.
-- KEYS[1] = state hash key
-- KEYS[2] = state expire zset key (optional, empty '' to disable expiration cleanup)
-- KEYS[3] = meta key
-- KEYS[4] = state meta key
-- ARGV[1] = cursor (use "0" to start, returned cursor for next page)
-- ARGV[2] = limit (0 = return all via HGETALL, >0 = use HSCAN)
-- ARGV[3] = now (current timestamp)
-- ARGV[4] = meta_ttl (seconds, 0 to disable)
-- ARGV[5] = state_ttl (seconds, 0 to disable - refreshes TTL on read)

local hash_key = KEYS[1]
local expire_key = KEYS[2]
local meta_key = KEYS[3]
local state_meta_key = KEYS[4]

local cursor = ARGV[1]
local limit = tonumber(ARGV[2])
local now_str = ARGV[3]
local now = tonumber(now_str)
local meta_ttl = tonumber(ARGV[4])
local state_ttl = tonumber(ARGV[5])

-- Update meta TTL and get current offset
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = now_str
    redis.call("hset", meta_key, "e", epoch)
end

local offset = redis.call("hget", meta_key, "s")
if not offset then
    offset = "0"
end

if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Validate state epoch against stream epoch
if state_meta_key ~= '' then
    local state_meta_exists = redis.call("exists", state_meta_key)
    if state_meta_exists == 0 then
        -- No state metadata = state is invalid/evicted
        return {offset, epoch, "0", {}}
    end

    local state_epoch = redis.call("hget", state_meta_key, "epoch")
    if state_epoch ~= epoch then
        -- Epoch mismatch = state is stale (from old epoch)
        return {offset, epoch, "0", {}}
    end
end

-- IMPORTANT: We do NOT cleanup expired entries here.
-- Expired entries MUST be removed by the cleanup worker (broker_state_cleanup.lua)
-- which generates LEAVE events to the stream, updates user tracking, etc.
-- Inline cleanup would bypass LEAVE event generation, breaking convergence guarantees.

-- Refresh state TTL on read (LRU behavior)
if state_ttl > 0 then
    redis.call("expire", hash_key, state_ttl)
    if expire_key ~= '' then
        redis.call("expire", expire_key, state_ttl)
    end
    if state_meta_key ~= '' then
        redis.call("expire", state_meta_key, state_ttl)
    end
end

-- Return offset, epoch and raw HSCAN/HGETALL result (no table building)
if limit > 0 then
    local result = redis.call("hscan", hash_key, cursor, "COUNT", limit)
    return {offset, epoch, result[1], result[2]}  -- offset, epoch, cursor, key-value array
else
    local data = redis.call("hgetall", hash_key)
    return {offset, epoch, "0", data}  -- offset, epoch, cursor="0", key-value array
end
