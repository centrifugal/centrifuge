-- Read ordered keyed snapshot with pagination and expiration cleanup.
-- KEYS[1] = snapshot hash key
-- KEYS[2] = snapshot order zset key
-- KEYS[3] = snapshot expire zset key
-- KEYS[4] = meta key
-- KEYS[5] = snapshot meta key
-- ARGV[1] = limit (0 = no limit, return all)
-- ARGV[2] = offset (for pagination)
-- ARGV[3] = now (current timestamp for expiration cleanup)
-- ARGV[4] = meta_ttl (seconds, 0 to disable)
-- ARGV[5] = snapshot_ttl (seconds, 0 to disable - refreshes TTL on read)

local hash_key = KEYS[1]
local order_key = KEYS[2]
local expire_key = KEYS[3]
local meta_key = KEYS[4]
local snapshot_meta_key = KEYS[5]

local limit = tonumber(ARGV[1])
local page_offset = tonumber(ARGV[2])
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
        return {stream_offset, epoch, {}, {}}
    end

    local snapshot_epoch = redis.call("hget", snapshot_meta_key, "epoch")
    if snapshot_epoch ~= epoch then
        -- Epoch mismatch = snapshot is stale (from old epoch)
        return {stream_offset, epoch, {}, {}}
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

-- Fetch ordered keys (descending)
local keys
if limit > 0 then
    keys = redis.call("zrevrange", order_key, page_offset, page_offset + limit - 1)
else
    keys = redis.call("zrevrange", order_key, 0, -1)
end

local key_count = #keys
if key_count == 0 then
    return {stream_offset, epoch, {}, {}}
end

-- Fetch values in one call
local values = redis.call("hmget", hash_key, unpack(keys))

return {stream_offset, epoch, keys, values}
