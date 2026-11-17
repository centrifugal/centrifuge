-- Read state history stream.
-- KEYS[1] = stream key
-- KEYS[2] = meta key
-- KEYS[3] = state meta key (state rebound to a new lineage's epoch)
-- KEYS[4] = lineage token key
-- ARGV[1] = include_publications ("0" or "1")
-- ARGV[2] = since_offset
-- ARGV[3] = limit (0 = no limit)
-- ARGV[4] = reverse ("0" or "1")
-- ARGV[5] = meta_expire (milliseconds, 0 to disable)
-- ARGV[6] = new_epoch_if_empty

local stream_key = KEYS[1]
local meta_key = KEYS[2]
local state_meta_key = KEYS[3]
local token, ancestor = lineage_token(KEYS[4])
local include_publications = ARGV[1]
local since_offset = ARGV[2]
local limit = ARGV[3]
local reverse = ARGV[4]
local meta_expire = ARGV[5]
local new_epoch_if_empty = ARGV[6]

-- Get or create meta
local current_epoch, dropped = lineage_map_meta(meta_key, stream_key, token, ancestor)
local top_offset = redis.call("hget", meta_key, "s")

if current_epoch == false then
    current_epoch = new_epoch_if_empty
    top_offset = 0
    lineage_map_create(meta_key, state_meta_key, current_epoch, token, dropped)
    -- meta_key was missing — wipe stream_key too. Each xadd entry stores its
    -- epoch in the "e" field, but the Go consumer reads only "d" (data) and
    -- discards "e", so old-epoch entries lingering due to lazy eviction would
    -- otherwise be returned by xrange under the new epoch wrapper. Mirrors
    -- the publish-path safety net (`if top_offset == 1 then del stream_key`).
    redis.call("del", stream_key)
end

if top_offset == false then
    top_offset = 0
end

if meta_expire ~= '0' then
    redis.call("pexpire", meta_key, meta_expire)
end

local pubs = {}

if include_publications ~= "0" then
    if reverse == "0" then
        -- Forward read: always from since_offset
        if limit ~= "0" then
            pubs = redis.call("xrange", stream_key, since_offset, "+", "COUNT", limit)
        else
            pubs = redis.call("xrange", stream_key, since_offset, "+")
        end
    else
        -- Reverse read: compute get_offset
        local get_offset = top_offset
        if since_offset ~= "-" and since_offset ~= "0" then
            get_offset = since_offset
        end
        if limit ~= "0" then
            pubs = redis.call("xrevrange", stream_key, get_offset, "-", "COUNT", limit)
        else
            pubs = redis.call("xrevrange", stream_key, get_offset, "-")
        end
    end
end

return { top_offset, current_epoch .. lineage_map_obs(meta_key) .. lineage_reset_ext(dropped), pubs }
