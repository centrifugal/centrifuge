local stream_key = KEYS[1]
local meta_key = KEYS[2]
local result_key = KEYS[3]
local message_payload = ARGV[1]
local stream_size = ARGV[2]
local stream_ttl = ARGV[3]
local channel = ARGV[4]
local meta_expire = ARGV[5]
local new_epoch_if_empty = ARGV[6]
local publish_command = ARGV[7]
local result_key_expire = ARGV[8]

if result_key_expire ~= '' then
    local cached_result = redis.call("hmget", result_key, "e", "s")
    local result_epoch, result_offset = cached_result[1], cached_result[2]
    if result_epoch ~= false then
        return {result_offset, result_epoch, "1"}
    end
end

local current_epoch = redis.call("hget", meta_key, "e")
if current_epoch == false then
  current_epoch = new_epoch_if_empty
  redis.call("hset", meta_key, "e", current_epoch)
end

local top_offset = redis.call("hincrby", meta_key, "s", 1)

if meta_expire ~= '0' then
  redis.call("expire", meta_key, meta_expire)
end

redis.call("xadd", stream_key, "MAXLEN", stream_size, top_offset, "d", message_payload)
redis.call("expire", stream_key, stream_ttl)

if channel ~= '' then
  local payload = "__" .. "p1:" .. top_offset .. ":" .. current_epoch .. "__" .. message_payload
  redis.call(publish_command, channel, payload)
end

if result_key_expire ~= '' then
  redis.call("hset", result_key, "e", current_epoch, "s", top_offset)
  redis.call("expire", result_key, result_key_expire)
end

return {top_offset, current_epoch, "0"}
