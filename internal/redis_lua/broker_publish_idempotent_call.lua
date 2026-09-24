-- Caller for one publication. The logic lives in
-- broker_publish_idempotent_fn.lua, which is prepended to this by the Go side.
--
-- KEYS[2], passed with sharded PUB/SUB, never exists and only routes the call
-- while its slot migrates - see idempotentRouteKey.
return publish_idempotent(KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4])
