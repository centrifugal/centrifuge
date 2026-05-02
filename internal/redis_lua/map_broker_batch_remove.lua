--[[
Batch remove expired entries from keyed state and publish removal events.

Takes pre-constructed removal protobuf payloads from Go (which include tags)
and atomically:
  1. Re-verifies each entry is still expired (race detection via expire score)
  2. Removes from state hash, expire ZSET, order ZSET, and version fields
  3. Writes removal event to stream (if not streamless)
  4. Publishes removal event via PUB/SUB

Returns: { removed_count, final_offset_string, epoch }
--]]

-- ==== KEYS ====
-- KEYS[1] = state hash key
-- KEYS[2] = state expire zset key
-- KEYS[3] = stream key (for removal events)
-- KEYS[4] = stream meta key (for offset/epoch)
-- KEYS[5] = cleanup registration zset key (for scheduling)
-- KEYS[6] = state order zset key (always passed, ZREM is no-op if key doesn't exist)
-- KEYS[7] = state meta key (for per-key version cleanup)

-- ==== ARGV ====
-- ARGV[1]  = num_entries (number of entries to process)
-- ARGV[2]  = channel (for PUBLISH)
-- ARGV[3]  = publish_command ("PUBLISH" or "SPUBLISH", empty to disable)
-- ARGV[4]  = stream_size (MAXLEN for stream)
-- ARGV[5]  = stream_ttl (milliseconds)
-- ARGV[6]  = meta_expire (milliseconds, "0" to disable)
-- ARGV[7]  = new_epoch_if_empty
-- ARGV[8]  = channel_for_cleanup_zset
-- ARGV[9]  = streamless ("1" = skip stream/meta operations, "0" = normal)
-- ARGV[10..] = triplets of (entry_key, removal_payload, expected_expire_score) per entry

local state_hash_key = KEYS[1]
local state_expire_key = KEYS[2]
local stream_key = KEYS[3]
local meta_key = KEYS[4]
local cleanup_registration_key = KEYS[5]
local state_order_key = KEYS[6]
local state_meta_key = KEYS[7]

local num_entries = tonumber(ARGV[1])
local channel = ARGV[2]
local publish_command = ARGV[3]
local stream_size = ARGV[4]
local stream_ttl = ARGV[5]
local meta_expire = ARGV[6]
local new_epoch_if_empty = ARGV[7]
local channel_for_cleanup = ARGV[8]
local streamless = ARGV[9] == "1"

-- Helper: update cleanup registration ZSET with the earliest expiry.
local function update_cleanup_registration()
    if cleanup_registration_key == '' then return end
    local earliest = redis.call("zrange", state_expire_key, 0, 0, "WITHSCORES")
    if #earliest >= 2 then
        redis.call("zadd", cleanup_registration_key, earliest[2], channel_for_cleanup)
    else
        redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
    end
end

if num_entries == 0 then
    update_cleanup_registration()
    return { 0, "0", "" }
end

-- Get or create epoch (only in streamed mode)
local current_epoch
local top_offset = 0

if not streamless then
    current_epoch = redis.call("hget", meta_key, "e")
    if not current_epoch then
        current_epoch = new_epoch_if_empty
        redis.call("hset", meta_key, "e", current_epoch)
    end
else
    current_epoch = new_epoch_if_empty
end

local removed_count = 0

for i = 0, num_entries - 1 do
    local base = 10 + i * 3
    local entry_key = ARGV[base]
    local removal_payload = ARGV[base + 1]
    local expected_score = ARGV[base + 2]

    -- Race condition check: verify entry still has the same expire score
    local current_score = redis.call("zscore", state_expire_key, entry_key)
    if current_score and current_score == expected_score then
        -- Still the same expired entry, safe to remove

        if not streamless then
            -- Increment offset for the removal event
            top_offset = redis.call("hincrby", meta_key, "s", 1)

            -- Set meta TTL if needed
            if meta_expire ~= '0' then
                redis.call("pexpire", meta_key, meta_expire)
            end

            -- Write removal event to stream
            redis.call("xadd", stream_key, "MAXLEN", "~", stream_size, top_offset, "e", current_epoch, "d", removal_payload)
            if tonumber(stream_ttl) > 0 then
                redis.call("pexpire", stream_key, tonumber(stream_ttl))
            end
        end

        -- Publish removal event
        if channel ~= '' and publish_command ~= '' then
            local pub_payload = top_offset .. ":" .. current_epoch .. ":" .. removal_payload
            redis.call(publish_command, channel, pub_payload)
        end

        -- Remove from state hash, expire zset, and order zset.
        redis.call("hdel", state_hash_key, entry_key)
        redis.call("zrem", state_expire_key, entry_key)
        redis.call("zrem", state_order_key, entry_key)
        -- Clean up per-key version fields from state meta
        if state_meta_key ~= '' then
            redis.call("hdel", state_meta_key, "v:" .. entry_key, "ve:" .. entry_key)
        end

        removed_count = removed_count + 1
    end
end

-- Update cleanup registration with the earliest remaining expiry.
update_cleanup_registration()

local final_offset = 0
if not streamless then
    final_offset = redis.call("hget", meta_key, "s") or 0
end

return { removed_count, tostring(final_offset), current_epoch }
