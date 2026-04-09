--[[
Find expired entries in keyed state (read-only, no mutations).

Returns a list of expired entry keys and their current state values.
The caller (Go) is responsible for constructing removal publications
and calling the batch-remove script to atomically delete entries,
write to stream, and publish via PUB/SUB.

Returns:
  { key1, state_value1, expire_score1, key2, state_value2, expire_score2, ... }

Each triplet contains:
  - key: the expired entry key
  - state_value: raw state hash value ("offset:epoch:protobuf") or "" if not found
  - expire_score: the expire ZSET score for race detection in the remove phase
--]]

-- ==== KEYS ====
-- KEYS[1] = state hash key
-- KEYS[2] = state expire zset key

-- ==== ARGV ====
-- ARGV[1] = now (unix timestamp milliseconds)
-- ARGV[2] = batch_size (max entries to process)

local state_hash_key = KEYS[1]
local state_expire_key = KEYS[2]

local now = tonumber(ARGV[1])
local batch_size = tonumber(ARGV[2])

-- Find expired entries
local expired = redis.call("zrangebyscore", state_expire_key, 0, now, "LIMIT", 0, batch_size)

if #expired == 0 then
    return {}
end

local result = {}

for _, entry_key in ipairs(expired) do
    -- Verify entry is still expired (race check)
    local current_score = redis.call("zscore", state_expire_key, entry_key)
    if current_score and tonumber(current_score) <= now then
        -- Read state value to extract tags in Go
        local state_value = redis.call("hget", state_hash_key, entry_key) or ""
        result[#result+1] = entry_key
        result[#result+1] = state_value
        result[#result+1] = current_score
    end
end

return result
