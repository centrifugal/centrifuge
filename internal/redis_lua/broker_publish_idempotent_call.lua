-- Caller for one publication. The logic lives in
-- broker_publish_idempotent_fn.lua, which is prepended to this by the Go side.
return publish_idempotent(KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4])
