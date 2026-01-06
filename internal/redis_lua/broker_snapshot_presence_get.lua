-- Get presence information from unified snapshot structure.
-- Presence is stored using snapshot keys (HASH + expire ZSET).
-- KEYS[1] = snapshot hash key (used for presence data)
-- KEYS[2] = snapshot expire zset key (used for presence expiration)
-- ARGV[1] = current timestamp in seconds
-- ARGV[2] = use hash field TTL "0" or "1"

local snapshot_hash_key = KEYS[1]
local snapshot_expire_key = KEYS[2]
local now = ARGV[1]
local use_hexpire = ARGV[2]

-- Cleanup expired entries if not using HEXPIRE
if use_hexpire == '0' and snapshot_expire_key ~= '' then
    local expired = redis.call("zrangebyscore", snapshot_expire_key, "0", now)
    if #expired > 0 then
        for num = 1, #expired do
            redis.call("hdel", snapshot_hash_key, expired[num])
        end
        redis.call("zremrangebyscore", snapshot_expire_key, "0", now)
    end
end

-- Return all presence data
return redis.call("hgetall", snapshot_hash_key)
