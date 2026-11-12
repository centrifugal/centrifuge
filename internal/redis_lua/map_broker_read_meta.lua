-- Get or create stream metadata (epoch, top_offset)
-- KEYS[1] = meta key
-- KEYS[2] = stream key (dropped with a meta of another lineage)
-- KEYS[3] = state meta key (state rebound to a new lineage's epoch)
-- KEYS[4] = lineage token key
-- ARGV[1] = meta_expire (milliseconds, 0 to disable)
-- ARGV[2] = new_epoch_if_empty

local meta_key = KEYS[1]
local stream_key = KEYS[2]
local state_meta_key = KEYS[3]
local token, ancestor = lineage_token(KEYS[4])
local meta_expire = ARGV[1]
local new_epoch_if_empty = ARGV[2]

local current_epoch, dropped = lineage_map_meta(meta_key, stream_key, token, ancestor)
local top_offset = redis.call("hget", meta_key, "s")

if current_epoch == false then
    current_epoch = new_epoch_if_empty
    lineage_map_create(meta_key, state_meta_key, current_epoch, token, dropped)
end

if top_offset == false then
    top_offset = 0
end

if meta_expire ~= '0' then
    redis.call("pexpire", meta_key, meta_expire)
end

return { top_offset, current_epoch .. lineage_map_obs(meta_key) }
