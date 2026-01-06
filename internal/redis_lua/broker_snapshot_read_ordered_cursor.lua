-- Read ordered keyed snapshot with CURSOR-BASED pagination and expiration cleanup.
-- KEYS[1] = snapshot hash key
-- KEYS[2] = snapshot order zset key
-- KEYS[3] = snapshot expire zset key
-- KEYS[4] = meta key
-- KEYS[5] = snapshot meta key
-- ARGV[1] = limit (0 = no limit, return all remaining)
-- ARGV[2] = cursor (score upper bound, exclusive; use "+" for first page, or returned next_cursor)
-- ARGV[3] = now (current timestamp for expiration cleanup)
-- ARGV[4] = meta_ttl (seconds, 0 to disable)
-- ARGV[5] = snapshot_ttl (seconds, 0 to disable - refreshes TTL on read)

local hash_key = KEYS[1]
local order_key = KEYS[2]
local expire_key = KEYS[3]
local meta_key = KEYS[4]
local snapshot_meta_key = KEYS[5]

local limit = tonumber(ARGV[1])
local cursor = ARGV[2]
local now_str = ARGV[3]
local meta_ttl = tonumber(ARGV[4])
local snapshot_ttl = tonumber(ARGV[5])

-- Update meta epoch + TTL and get current stream offset
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = now_str
    redis.call("hset", meta_key, "e", epoch)
end

local stream_offset = redis.call("hget", meta_key, "s")
if not stream_offset then
    stream_offset = "0"
end

if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Validate snapshot epoch against stream epoch
if snapshot_meta_key ~= '' then
    local snapshot_meta_exists = redis.call("exists", snapshot_meta_key)
    if snapshot_meta_exists == 0 then
        -- No snapshot metadata = snapshot is invalid/evicted
        return {stream_offset, epoch, {}, {}, "+"}
    end

    local snapshot_epoch = redis.call("hget", snapshot_meta_key, "epoch")
    if snapshot_epoch ~= epoch then
        -- Epoch mismatch = snapshot is stale
        return {stream_offset, epoch, {}, {}, "+"}
    end
end

-- Refresh snapshot TTL on read (LRU behavior)
if snapshot_ttl > 0 then
    redis.call("expire", hash_key, snapshot_ttl)
    redis.call("expire", order_key, snapshot_ttl)
    redis.call("expire", expire_key, snapshot_ttl)
    if snapshot_meta_key ~= '' then
        redis.call("expire", snapshot_meta_key, snapshot_ttl)
    end
end

-- Cleanup expired entries
local expired = redis.call("zrangebyscore", expire_key, "-inf", now_str)
local expired_len = #expired
if expired_len > 0 then
    redis.call("hdel", hash_key, unpack(expired))
    redis.call("zrem", order_key, unpack(expired))
    redis.call("zremrangebyscore", expire_key, "-inf", now_str)
end

-- Determine range bounds for descending order
local min_score = "-inf"
local max_score = cursor == "+" and "+inf" or "(" .. cursor  -- exclusive upper bound if cursor provided

-- Fetch keys in descending score order
local range_args = {order_key, max_score, min_score, "WITHSCORES", "LIMIT", 0}
if limit > 0 then
    table.insert(range_args, limit)
else
    table.insert(range_args, 1000000)  -- large number to get all
end

local keys_with_scores = redis.call("zrevrangebyscore", unpack(range_args))

-- Extract only keys and the lowest score for next cursor
local keys = {}
local next_cursor = "+"
if #keys_with_scores > 0 then
    for i = 1, #keys_with_scores, 2 do
        table.insert(keys, keys_with_scores[i])
    end
    -- The last element is the lowest score in this page
    local lowest_score = keys_with_scores[#keys_with_scores]
    next_cursor = tostring(lowest_score)
end

local key_count = #keys
if key_count == 0 then
    return {stream_offset, epoch, {}, {}, "+"}
end

-- Fetch values in one call
local values = redis.call("hmget", hash_key, unpack(keys))

-- Return: offset, epoch, keys, values, next_cursor
return {stream_offset, epoch, keys, values, next_cursor}