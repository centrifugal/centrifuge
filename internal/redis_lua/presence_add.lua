-- Add/update presence information.
-- KEYS[1] - presence set key
-- KEYS[2] - presence hash key
-- ARGV[1] - key expire seconds
-- ARGV[2] - expire at for set member
-- ARGV[3] - client ID
-- ARGV[4] - info payload
redis.call("zadd", KEYS[1], ARGV[2], ARGV[3])
redis.call("hset", KEYS[2], ARGV[3], ARGV[4])
redis.call("expire", KEYS[1], ARGV[1])
redis.call("expire", KEYS[2], ARGV[1])
