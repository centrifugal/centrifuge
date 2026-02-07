-- Get stats from keyed snapshot structure.
-- This is a read-only operation - cleanup is handled by keyed_engine_cleanup.lua
--
-- Generic design:
-- - Works for any keyed state (presence, cursors, etc.)
-- - NumKeys = total entries in snapshot
-- - NumAggregated = unique aggregation values (e.g., unique users for presence)
--
-- KEYS[1] = snapshot hash key
-- KEYS[2] = aggregation hash key (optional, for aggregation counts)

local snapshot_hash_key = KEYS[1]
local aggregation_hash_key = KEYS[2]

-- Count total entries in snapshot
local key_count = redis.call("hlen", snapshot_hash_key)

-- Count unique aggregation values (e.g., unique users for presence)
local aggregation_count = 0
if aggregation_hash_key ~= '' then
    aggregation_count = redis.call("hlen", aggregation_hash_key)
end

return { key_count, aggregation_count }
