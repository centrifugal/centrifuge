-- Read unordered keyed snapshot with cursor-based pagination.
-- KEYS[1] = snapshot hash key
-- KEYS[2] = snapshot expire zset key (optional, empty '' to disable expiration cleanup)
-- KEYS[3] = meta key
-- ARGV[1] = cursor (use "0" to start, returned cursor for next page)
-- ARGV[2] = limit (0 = return all via HGETALL, >0 = use HSCAN)
-- ARGV[3] = now (current timestamp)
-- ARGV[4] = meta_ttl (seconds, 0 to disable)
-- ARGV[5] = snapshot_ttl (seconds, 0 to disable - refreshes TTL on read)

local hash_key = KEYS[1]
local expire_key = KEYS[2]
local meta_key = KEYS[3]

local cursor = ARGV[1]
local limit = tonumber(ARGV[2])
local now_str = ARGV[3]
local now = tonumber(now_str)
local meta_ttl = tonumber(ARGV[4])
local snapshot_ttl = tonumber(ARGV[5])

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

-- Cleanup expired entries (if expire_key provided)
if expire_key ~= '' then
    local expired = redis.call("zrangebyscore", expire_key, "-inf", now_str)
    local expired_len = #expired
    if expired_len > 0 then
        redis.call("hdel", hash_key, unpack(expired))
        redis.call("zremrangebyscore", expire_key, "-inf", now_str)
    end
end

-- Refresh snapshot TTL on read (LRU behavior)
if snapshot_ttl > 0 then
    redis.call("expire", hash_key, snapshot_ttl)
    if expire_key ~= '' then
        redis.call("expire", expire_key, snapshot_ttl)
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
