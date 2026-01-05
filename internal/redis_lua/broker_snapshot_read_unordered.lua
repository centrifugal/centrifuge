-- Read unordered keyed snapshot with cursor-based pagination.
-- KEYS[1] = snapshot hash key
-- KEYS[2] = meta key
-- ARGV[1] = cursor (use "0" to start, returned cursor for next page)
-- ARGV[2] = limit (0 = return all via HGETALL, >0 = use HSCAN)
-- ARGV[3] = now (current timestamp)
-- ARGV[4] = meta_ttl (seconds, 0 to disable)
-- ARGV[5] = snapshot_ttl (seconds, 0 to disable - refreshes TTL on read)

local hash_key = KEYS[1]
local meta_key = KEYS[2]

local cursor = ARGV[1]
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local meta_ttl = tonumber(ARGV[4])
local snapshot_ttl = tonumber(ARGV[5])

-- Update meta TTL
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = tostring(now)
    redis.call("hset", meta_key, "e", epoch)
end
if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
end

-- Refresh snapshot TTL on read (LRU behavior)
if snapshot_ttl > 0 then
    redis.call("expire", hash_key, snapshot_ttl)
end

-- Return epoch and raw HSCAN/HGETALL result (no table building)
if limit > 0 then
    local result = redis.call("hscan", hash_key, cursor, "COUNT", limit)
    return {epoch, result[1], result[2]}  -- epoch, cursor, key-value array
else
    local data = redis.call("hgetall", hash_key)
    return {epoch, "0", data}  -- epoch, cursor="0", key-value array
end
