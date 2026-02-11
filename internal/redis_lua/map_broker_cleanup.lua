--[[
Cleanup expired entries from keyed state and generate removal events.

This script:
1. Finds expired entries in the expire ZSET
2. For each expired entry:
   - Writes removal event to stream with new offset
   - Removes from state hash
   - Removes from expire ZSET
   - Publishes removal event via PUB/SUB
3. Updates cleanup registration ZSET with next expiry time
4. Returns count of removed entries

Storage format in state hash: offset:epoch:publication_bytes
--]]

-- ==== KEYS ====
-- KEYS[1] = state hash key
-- KEYS[2] = state expire zset key
-- KEYS[3] = stream key (for removal events)
-- KEYS[4] = stream meta key (for offset/epoch)
-- KEYS[5] = cleanup registration zset key (for scheduling)

-- ==== ARGV ====
-- ARGV[1]  = now (unix timestamp)
-- ARGV[2]  = batch_size (max entries to process)
-- ARGV[3]  = channel (for PUBLISH)
-- ARGV[4]  = publish_command ("PUBLISH" or "SPUBLISH", empty to disable)
-- ARGV[5]  = stream_size (MAXLEN for stream)
-- ARGV[6]  = stream_ttl (seconds)
-- ARGV[7]  = meta_expire (seconds, "0" to disable)
-- ARGV[8]  = new_epoch_if_empty
-- ARGV[9]  = use_hexpire ("0" or "1")
-- ARGV[10] = channel_for_cleanup_zset (channel name to update in cleanup ZSET)
-- ARGV[11] = force_consistency ("0" or "1" - force epoch change on eviction)
-- ARGV[12] = nil_key (slot-aligned placeholder for unused KEYS in cluster mode, empty '' to disable)

local state_hash_key = KEYS[1]
local state_expire_key = KEYS[2]
local stream_key = KEYS[3]
local meta_key = KEYS[4]
local cleanup_registration_key = KEYS[5]

-- In Redis Cluster, all KEYS must hash to the same slot. For unused keys we pass
-- a slot-aligned placeholder instead of '' (empty string hashes to slot 0). Convert
-- those placeholders back to '' so all existing conditional checks still work.
local nil_key = ARGV[12] or ""
if nil_key ~= "" then
    if stream_key == nil_key then stream_key = '' end
    if meta_key == nil_key then meta_key = '' end
    if cleanup_registration_key == nil_key then cleanup_registration_key = '' end
end

local now = tonumber(ARGV[1])
local batch_size = tonumber(ARGV[2])
local channel = ARGV[3]
local publish_command = ARGV[4]
local stream_size = ARGV[5]
local stream_ttl = ARGV[6]
local meta_expire = ARGV[7]
local new_epoch_if_empty = ARGV[8]
local channel_for_cleanup = ARGV[10]

-- Helper: encode an integer as a protobuf varint
local function encode_varint(value)
    local parts = {}
    while value > 127 do
        parts[#parts+1] = string.char(128 + (value % 128))
        value = math.floor(value / 128)
    end
    parts[#parts+1] = string.char(value)
    return table.concat(parts)
end

-- Helper function to construct minimal LEAVE Publication protobuf
local function construct_minimal_leave(key, timestamp_seconds)
    local parts = {}

    -- Field 11: key (string), wire type 2
    -- tag = (11 << 3) | 2 = 0x5A
    parts[#parts+1] = string.char(0x5A)
    parts[#parts+1] = encode_varint(#key)
    parts[#parts+1] = key

    -- Field 12: removed (bool), wire type 0
    -- tag = (12 << 3) | 0 = 0x60
    parts[#parts+1] = string.char(0x60, 0x01)

    -- Field 9: time (int64, Unix ms), wire type 0
    -- tag = (9 << 3) | 0 = 0x48
    parts[#parts+1] = string.char(0x48)
    parts[#parts+1] = encode_varint(timestamp_seconds * 1000)

    return table.concat(parts)
end

-- Helper: update cleanup registration ZSET with the earliest expiry in the expire ZSET.
-- Uses ZRANGE 0 0 WITHSCORES to find the absolute earliest entry (expired or future),
-- ensuring remaining expired entries are not lost when batch_size limits processing.
local function update_cleanup_registration()
    if cleanup_registration_key == '' then return end
    local earliest = redis.call("zrange", state_expire_key, 0, 0, "WITHSCORES")
    if #earliest >= 2 then
        redis.call("zadd", cleanup_registration_key, earliest[2], channel_for_cleanup)
    else
        redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
    end
end

-- Find expired entries
local expired = redis.call("zrangebyscore", state_expire_key, 0, now, "LIMIT", 0, batch_size)

if #expired == 0 then
    -- No expired entries, update cleanup registration with next expiry
    update_cleanup_registration()
    return { 0, "0", "", "ok" }
end

-- Determine if stream is enabled (stream_size > 0).
local has_stream = tonumber(stream_size) > 0

-- Get or create epoch
local current_epoch
local top_offset = 0

if has_stream and meta_key ~= '' then
    current_epoch = redis.call("hget", meta_key, "e")
    if not current_epoch then
        current_epoch = new_epoch_if_empty
        redis.call("hset", meta_key, "e", current_epoch)
    end
else
    current_epoch = new_epoch_if_empty
end

local removed_count = 0

for _, entry_key in ipairs(expired) do
    -- Race condition check: verify entry is still expired
    local current_score = redis.call("zscore", state_expire_key, entry_key)
    if current_score and tonumber(current_score) <= now then
        -- Still expired, safe to remove

        -- Construct minimal removal payload (key + removed + timestamp)
        local removal_payload = construct_minimal_leave(entry_key, now)

        if meta_key ~= '' then
            -- Increment offset for the removal event
            top_offset = redis.call("hincrby", meta_key, "s", 1)

            -- Set meta TTL if needed
            if meta_expire ~= '0' then
                redis.call("expire", meta_key, meta_expire)
            end
        end

        -- Write removal event to stream
        if stream_key ~= '' then
            redis.call("xadd", stream_key, "MAXLEN", stream_size, top_offset, "e", current_epoch, "d", removal_payload)
            if tonumber(stream_ttl) > 0 then
                redis.call("expire", stream_key, tonumber(stream_ttl))
            end
        end

        -- Publish removal event
        if channel ~= '' and publish_command ~= '' then
            local pub_payload = top_offset .. ":" .. current_epoch .. ":" .. removal_payload
            redis.call(publish_command, channel, pub_payload)
        end

        -- Remove from state hash and expire zset
        redis.call("hdel", state_hash_key, entry_key)
        redis.call("zrem", state_expire_key, entry_key)

        removed_count = removed_count + 1
    end
    -- If current_score > now, entry was re-added, skip silently
end

-- Update cleanup registration with the earliest remaining expiry.
update_cleanup_registration()

local final_offset = 0
if meta_key ~= '' then
    final_offset = redis.call("hget", meta_key, "s") or 0
end

return { removed_count, tostring(final_offset), current_epoch }
