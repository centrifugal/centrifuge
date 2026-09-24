-- Caller for one publication. The logic lives in
-- broker_history_add_stream_fn.lua, which is prepended to this by the Go side.
return add_history(
    KEYS[1], KEYS[2], KEYS[3],
    ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5],
    ARGV[6], ARGV[7], ARGV[8], ARGV[9],
    ARGV[10], ARGV[11]
)
