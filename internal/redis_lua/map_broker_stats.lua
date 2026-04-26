-- Get stats from keyed state structure.
-- This is a read-only operation - cleanup is handled by map_broker_cleanup.lua
--
-- KEYS[1] = state hash key

local state_hash_key = KEYS[1]

-- Count total entries in state
local key_count = redis.call("hlen", state_hash_key)

return { key_count }
