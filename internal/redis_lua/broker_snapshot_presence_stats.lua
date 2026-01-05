-- Get presence stats from unified snapshot structure.
-- KEYS[1] = presence zset key
-- KEYS[2] = presence hash key
-- KEYS[3] = per-user zset key
-- KEYS[4] = per-user hash key
-- ARGV[1] = current timestamp in seconds
-- ARGV[2] = use hash field TTL "0" or "1"

local presence_zset_key = KEYS[1]
local presence_hash_key = KEYS[2]
local user_zset_key = KEYS[3]
local user_hash_key = KEYS[4]
local now = ARGV[1]
local use_hexpire = ARGV[2]

-- Cleanup expired client entries if not using HEXPIRE
if use_hexpire == '0' and presence_zset_key ~= '' then
    local expired = redis.call("zrangebyscore", presence_zset_key, "0", now)
    if #expired > 0 then
        for num = 1, #expired do
            redis.call("hdel", presence_hash_key, expired[num])
        end
        redis.call("zremrangebyscore", presence_zset_key, "0", now)
    end
end

-- Cleanup expired user entries if not using HEXPIRE
if use_hexpire == '0' and user_zset_key ~= '' then
    local userExpired = redis.call("zrangebyscore", user_zset_key, "0", now)
    if #userExpired > 0 then
        for num = 1, #userExpired do
            redis.call("hdel", user_hash_key, userExpired[num])
        end
        redis.call("zremrangebyscore", user_zset_key, "0", now)
    end
end

-- Get counts
local clientCount = redis.call("hlen", presence_hash_key)
local userCount = 0
if user_hash_key ~= '' then
    userCount = redis.call("hlen", user_hash_key)
end

return { clientCount, userCount }
