local list_key = KEYS[1]
local meta_key = KEYS[2]
local message_payload = ARGV[1]
local ltrim_right_bound = ARGV[2]
local list_ttl = ARGV[3]
local channel = ARGV[4]
local meta_expire = ARGV[5]
local new_epoch_if_empty = ARGV[6]
local publish_command = ARGV[7]

local current_epoch = redis.call("hget", meta_key, "e")
if current_epoch == false then
  current_epoch = new_epoch_if_empty
  redis.call("hset", meta_key, "e", current_epoch)
end

local top_offset = redis.call("hincrby", meta_key, "s", 1)

if meta_expire ~= '0' then
  redis.call("expire", meta_key, meta_expire)
end

local payload = "__" .. "p1:" .. top_offset .. ":" .. current_epoch .. "__" .. message_payload
redis.call("lpush", list_key, payload)
redis.call("ltrim", list_key, 0, ltrim_right_bound)
redis.call("expire", list_key, list_ttl)

if channel ~= '' then
  redis.call(publish_command, channel, payload)
end

return {top_offset, current_epoch}
