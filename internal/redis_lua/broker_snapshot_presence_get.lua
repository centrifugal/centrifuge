-- Get presence information from unified snapshot structure.
-- KEYS[1] = presence zset key
-- KEYS[2] = presence hash key
-- ARGV[1] = current timestamp in seconds
-- ARGV[2] = use hash field TTL "0" or "1"

local presence_zset_key = KEYS[1]
local presence_hash_key = KEYS[2]
local now = ARGV[1]
local use_hexpire = ARGV[2]

-- Cleanup expired entries if not using HEXPIRE
if use_hexpire == '0' and presence_zset_key ~= '' then
    local expired = redis.call("zrangebyscore", presence_zset_key, "0", now)
    if #expired > 0 then
        for num = 1, #expired do
            redis.call("hdel", presence_hash_key, expired[num])
        end
        redis.call("zremrangebyscore", presence_zset_key, "0", now)
    end
end

-- Return all presence data
return redis.call("hgetall", presence_hash_key)
