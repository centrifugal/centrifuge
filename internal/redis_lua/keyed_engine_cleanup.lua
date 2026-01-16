--[[
Cleanup expired entries from keyed snapshot and generate removal events.

This script:
1. Finds expired entries in the expire ZSET
2. For each expired entry:
   - Writes removal event to stream with new offset
   - Removes from snapshot hash and aggregation mapping
   - Removes from expire ZSET
   - Updates aggregation counts
   - Publishes removal event via PUB/SUB
3. Updates cleanup registration ZSET with next expiry time
4. Returns count of removed entries

Storage format in snapshot hash: offset:epoch:publication_bytes

Generic design:
- Works for any keyed state (presence, cursors, etc.)
- Key = any unique identifier (e.g., clientID for presence, userID for cursors)
- Aggregation = optional grouping (e.g., userID for presence to count unique users)
--]]

-- ==== KEYS ====
-- KEYS[1] = snapshot hash key
-- KEYS[2] = snapshot expire zset key
-- KEYS[3] = stream key (for removal events)
-- KEYS[4] = stream meta key (for offset/epoch)
-- KEYS[5] = aggregation zset key (for aggregation TTL, optional)
-- KEYS[6] = aggregation hash key (for aggregation counts, optional)
-- KEYS[7] = cleanup registration zset key (for scheduling)
-- KEYS[8] = aggregation mapping hash key (entry_key -> aggregation_value, optional)

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

local snapshot_hash_key = KEYS[1]
local snapshot_expire_key = KEYS[2]
local stream_key = KEYS[3]
local meta_key = KEYS[4]
local aggregation_zset_key = KEYS[5]
local aggregation_hash_key = KEYS[6]
local cleanup_registration_key = KEYS[7]
local aggregation_mapping_key = KEYS[8]

local now = tonumber(ARGV[1])
local batch_size = tonumber(ARGV[2])
local channel = ARGV[3]
local publish_command = ARGV[4]
local stream_size = ARGV[5]
local stream_ttl = ARGV[6]
local meta_expire = ARGV[7]
local new_epoch_if_empty = ARGV[8]
local use_hexpire = ARGV[9]
local channel_for_cleanup = ARGV[10]
local force_consistency = ARGV[11]

-- Helper function to construct minimal LEAVE Publication protobuf
local function construct_minimal_leave(key, timestamp_seconds)
    local pb = ""

    -- Field 11: key (string), wire type 2
    -- tag = (11 << 3) | 2 = 0x5A
    pb = pb .. string.char(0x5A)
    pb = pb .. string.char(#key)
    pb = pb .. key

    -- Field 12: removed (bool), wire type 0
    -- tag = (12 << 3) | 0 = 0x60
    pb = pb .. string.char(0x60)
    pb = pb .. string.char(0x01)

    -- Field 9: time (int64, Unix ms), wire type 0
    -- tag = (9 << 3) | 0 = 0x48
    local time_ms = timestamp_seconds * 1000
    pb = pb .. string.char(0x48)

    -- inline varint encoding
    while time_ms > 127 do
        pb = pb .. string.char(128 + (time_ms % 128))
        time_ms = math.floor(time_ms / 128)
    end
    pb = pb .. string.char(time_ms)

    return pb
end

-- Find expired entries
local expired = redis.call("zrangebyscore", snapshot_expire_key, 0, now, "LIMIT", 0, batch_size)

if #expired == 0 then
    -- No expired entries, update cleanup registration with next expiry
    if cleanup_registration_key ~= '' then
        local next_expiry = redis.call("zrangebyscore", snapshot_expire_key, now, "+inf", "WITHSCORES", "LIMIT", 0, 1)
        if #next_expiry >= 2 then
            redis.call("zadd", cleanup_registration_key, next_expiry[2], channel_for_cleanup)
        else
            redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
        end
    end
    return { 0, "0", "", "ok" }
end

-- Get or create epoch
local current_epoch = redis.call("hget", meta_key, "e")
if not current_epoch then
    current_epoch = new_epoch_if_empty
    redis.call("hset", meta_key, "e", current_epoch)
end

local removed_count = 0

for _, entry_key in ipairs(expired) do
    -- Race condition check: verify entry is still expired
    local current_score = redis.call("zscore", snapshot_expire_key, entry_key)
    if current_score and tonumber(current_score) <= now then
        -- Still expired, safe to remove

        -- Construct minimal removal payload (key + removed + timestamp)
        local removal_payload = construct_minimal_leave(entry_key, now)

        -- Increment offset for the removal event
        local top_offset = redis.call("hincrby", meta_key, "s", 1)

        -- Set meta TTL if needed
        if meta_expire ~= '0' then
            redis.call("expire", meta_key, meta_expire)
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

        -- Get aggregation value from mapping (e.g., userID for presence, optional)
        local aggregation_value = nil
        if aggregation_mapping_key ~= '' and aggregation_hash_key ~= '' then
            aggregation_value = redis.call("hget", aggregation_mapping_key, entry_key)
        end

        -- Remove from snapshot and aggregation mapping
        redis.call("hdel", snapshot_hash_key, entry_key)
        if aggregation_mapping_key ~= '' then
            redis.call("hdel", aggregation_mapping_key, entry_key)
        end
        redis.call("zrem", snapshot_expire_key, entry_key)

        -- Update aggregation counts - decrement for this aggregation value
        if aggregation_value and aggregation_hash_key ~= '' then
            local entry_count = redis.call("hincrby", aggregation_hash_key, aggregation_value, -1)
            if entry_count <= 0 then
                -- Last entry for this aggregation value, remove from tracking
                if use_hexpire == '0' and aggregation_zset_key ~= '' then
                    redis.call("zrem", aggregation_zset_key, aggregation_value)
                end
                redis.call("hdel", aggregation_hash_key, aggregation_value)
            end
        end

        removed_count = removed_count + 1
    end
    -- If current_score > now, entry was re-added, skip silently
end

-- Update cleanup registration with next expiry time
if cleanup_registration_key ~= '' then
    local next_expiry = redis.call("zrangebyscore", snapshot_expire_key, now, "+inf", "WITHSCORES", "LIMIT", 0, 1)
    if #next_expiry >= 2 then
        redis.call("zadd", cleanup_registration_key, next_expiry[2], channel_for_cleanup)
    else
        redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
    end
end

return { removed_count, tostring(redis.call("hget", meta_key, "s") or 0), current_epoch }
