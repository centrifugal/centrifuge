-- Get or create stream metadata (epoch, top_offset)
-- KEYS[1] = meta key
-- ARGV[1] = meta_expire (milliseconds, 0 to disable)
-- ARGV[2] = new_epoch_if_empty

local meta_key = KEYS[1]
local meta_expire = ARGV[1]
local new_epoch_if_empty = ARGV[2]

local stream_meta = redis.call("hmget", meta_key, "e", "s")
local current_epoch, top_offset = stream_meta[1], stream_meta[2]

if current_epoch == false then
    current_epoch = new_epoch_if_empty
    redis.call("hset", meta_key, "e", current_epoch)
end

if top_offset == false then
    top_offset = 0
end

if meta_expire ~= '0' then
    redis.call("pexpire", meta_key, meta_expire)
end

return { top_offset, current_epoch }
