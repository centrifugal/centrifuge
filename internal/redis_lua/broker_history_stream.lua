local stream_key = KEYS[1]
local meta_key = KEYS[2]
local include_publications = ARGV[1]
local since_offset = ARGV[2]
local limit = ARGV[3]
local reverse = ARGV[4]
local meta_expire = ARGV[5]
local new_epoch_if_empty = ARGV[6]
-- ARGV[7]: the TTL of a meta with no publications (seconds, "0": meta_expire).
-- ARGV[8]: "1" extends a meta only when less than half its TTL is left.
local empty_expire = tonumber(ARGV[7] or "0")
local extend_below_half = ARGV[8] == "1"

local token_key = KEYS[3]

-- See add_history in broker_history_add_stream_fn.lua for the lineage token.
local token, ancestor = lineage_token(token_key)
local lineage_reset = false
local stream_meta = redis.call("hmget", meta_key, "e", "s", LINEAGE_FIELD, "w")
local current_epoch, top_offset = stream_meta[1], stream_meta[2]

if token ~= "" and current_epoch ~= false and stream_meta[3] ~= token then
    if stream_meta[3] == false or stream_meta[3] == ancestor then
        redis.call("hset", meta_key, LINEAGE_FIELD, token)
    else
        redis.call("del", meta_key, stream_key)
        current_epoch = false
        stream_meta[4] = false
        lineage_reset = true
    end
end

if current_epoch == false then
    current_epoch = new_epoch_if_empty
    top_offset = 0
    if token ~= "" then
        redis.call("hset", meta_key, "e", current_epoch, LINEAGE_FIELD, token)
    else
        redis.call("hset", meta_key, "e", current_epoch)
    end
end

if top_offset == false then
    top_offset = 0
end

if meta_expire ~= '0' then
    local target = tonumber(meta_expire)
    if empty_expire > 0 and empty_expire < target and tonumber(top_offset) == 0 then
        target = empty_expire
    end
    if extend_below_half then
        -- A meta above the target is shortened (an empty meta from before
        -- the short TTL); one with more than half the target left is kept.
        local ttl = redis.call("ttl", meta_key)
        if ttl < 0 or ttl > target or ttl * 2 < target then
            redis.call("expire", meta_key, target)
        end
    else
        redis.call("expire", meta_key, target)
    end
end

local pubs = nil

if include_publications ~= "0" then
    if limit ~= "0" then
        if reverse == "0" then
            pubs = redis.call("xrange", stream_key, since_offset, "+", "COUNT", limit)
        else
            local get_offset = top_offset
            if since_offset ~= "0" then
                get_offset = since_offset
            end
            pubs = redis.call("xrevrange", stream_key, get_offset, "-", "COUNT", limit)
        end
    else
        if reverse == "0" then
            pubs = redis.call("xrange", stream_key, since_offset, "+")
        else
            local get_offset = top_offset
            if since_offset ~= "0" then
                get_offset = since_offset
            end
            pubs = redis.call("xrevrange", stream_key, get_offset, "-")
        end
    end
end

return { top_offset, current_epoch .. lineage_obs(stream_meta[4]) .. lineage_reset_ext(lineage_reset), pubs }
