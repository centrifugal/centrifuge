-- Remove client presence.
-- KEYS[1] - presence set key
-- KEYS[2] - presence hash key
-- ARGV[1] - client ID
redis.call("hdel", KEYS[2], ARGV[1])
redis.call("zrem", KEYS[1], ARGV[1])
