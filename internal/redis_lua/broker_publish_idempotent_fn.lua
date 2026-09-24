-- publish_idempotent publishes one publication whose result is remembered, so
-- that a retry carrying the same idempotency key does not publish it twice.
--
-- The logic is a function with one definition, and the caller supplies the key
-- and arguments explicitly - nothing here reads KEYS or ARGV.
--
-- The remembered result is written after the publication and never before: it
-- is what says a publication happened, so writing it first would let a failure
-- leave a result claiming a publication nobody received.
local function publish_idempotent(result_key, payload, channel, publish_command, result_key_expire)
    if result_key_expire ~= '' then
        local stream_meta = redis.call("hmget", result_key, "e", "s")
        local result_epoch, result_offset = stream_meta[1], stream_meta[2]
        if result_epoch ~= false then
            return { result_offset, result_epoch }
        end
    end

    local res
    if channel ~= '' then
        res = redis.call(publish_command, channel, payload)
    end

    if result_key_expire ~= '' then
        redis.call("hset", result_key, "e", "")
        redis.call("expire", result_key, result_key_expire)
    end

    return res
end
