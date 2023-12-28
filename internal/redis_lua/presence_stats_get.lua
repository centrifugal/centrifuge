-- Get presence stats information.
-- KEYS[1] - presence set key
-- KEYS[2] - presence hash key
-- KEYS[3] - per-user zset key
-- KEYS[4] - per-user hash key
-- ARGV[1] - current timestamp in seconds
local expired = redis.call("zrangebyscore", KEYS[1], "0", ARGV[1])
if #expired > 0 then
  for num = 1, #expired do
    redis.call("hdel", KEYS[2], expired[num])
  end
  redis.call("zremrangebyscore", KEYS[1], "0", ARGV[1])
end

local userExpired = redis.call("zrangebyscore", KEYS[3], "0", ARGV[1])
if #userExpired > 0 then
  for num = 1, #userExpired do
    redis.call("hdel", KEYS[4], userExpired[num])
  end
  redis.call("zremrangebyscore", KEYS[3], "0", ARGV[1])
end

local clientCount = redis.call("hlen", KEYS[2])
local userCount = redis.call("hlen", KEYS[4])

return {clientCount, userCount}
