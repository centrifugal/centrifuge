--[[
Cleanup expired entries from presence/keyed snapshot and generate LEAVE events.

This script:
1. Finds expired entries in the expire ZSET
2. For each expired entry (with race condition check):
   - Writes LEAVE event to stream with new offset
   - Removes from snapshot hash and leave hash
   - Removes from expire ZSET
   - Updates aggregation (user tracking)
   - Publishes LEAVE via PUB/SUB
3. Updates cleanup registration ZSET with next expiry time
4. Returns count of removed entries

Storage format in snapshot hash: offset:epoch:client_info_bytes
Storage in leave hash: client_id -> pre-constructed leave Push bytes

Race condition handling:
- Between finding expired entries and processing, a client may rejoin
- We verify ZSCORE is still <= now before removing
- If client rejoined (new ZSCORE > now), we skip it
--]]

-- ==== KEYS ====
-- KEYS[1] = snapshot hash key
-- KEYS[2] = snapshot expire zset key
-- KEYS[3] = stream key (for LEAVE events)
-- KEYS[4] = stream meta key (for offset/epoch)
-- KEYS[5] = aggregation zset key (for user tracking TTL, optional)
-- KEYS[6] = aggregation hash key (for user count, optional)
-- KEYS[7] = cleanup registration zset key (for scheduling)
-- KEYS[8] = leave payload hash key (pre-constructed LEAVE Push bytes)
-- KEYS[9] = aggregation mapping hash key (client_id -> aggregation_value, optional)

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

local snapshot_hash_key = KEYS[1]
local snapshot_expire_key = KEYS[2]
local stream_key = KEYS[3]
local meta_key = KEYS[4]
local aggregation_zset_key = KEYS[5]
local aggregation_hash_key = KEYS[6]
local cleanup_registration_key = KEYS[7]
local leave_hash_key = KEYS[8]
local aggregation_mapping_key = KEYS[9]

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
    return { 0, "0", "" }
end

-- Get or create epoch
local current_epoch = redis.call("hget", meta_key, "e")
if not current_epoch then
    current_epoch = new_epoch_if_empty
    redis.call("hset", meta_key, "e", current_epoch)
end

local removed_count = 0

for _, client_id in ipairs(expired) do
    -- Race condition check: verify entry is still expired
    local current_score = redis.call("zscore", snapshot_expire_key, client_id)
    if current_score and tonumber(current_score) <= now then
        -- Still expired, safe to remove

        -- Get pre-constructed leave payload
        local leave_payload = redis.call("hget", leave_hash_key, client_id)
        if leave_payload then
            -- Increment offset for the LEAVE event
            local top_offset = redis.call("hincrby", meta_key, "s", 1)

            -- Set meta TTL if needed
            if meta_expire ~= '0' then
                redis.call("expire", meta_key, meta_expire)
            end

            -- Get aggregation value for user tracking (stored as suffix in expire ZSET member)
            -- We'll handle aggregation via the leave hash which stores user info

            -- Write LEAVE event to stream
            if stream_key ~= '' then
                redis.call("xadd", stream_key, "MAXLEN", stream_size, top_offset, "e", current_epoch, "d", leave_payload)
                if tonumber(stream_ttl) > 0 then
                    redis.call("expire", stream_key, tonumber(stream_ttl))
                end
            end

            -- Publish LEAVE event
            if channel ~= '' and publish_command ~= '' then
                local pub_payload = top_offset .. ":" .. current_epoch .. ":" .. leave_payload
                redis.call(publish_command, channel, pub_payload)
            end

            -- Get aggregation value (e.g., user_id) from mapping for user tracking
            local aggregation_value = nil
            if aggregation_mapping_key ~= '' and aggregation_hash_key ~= '' then
                aggregation_value = redis.call("hget", aggregation_mapping_key, client_id)
            end

            -- Remove from snapshot, leave hash, and aggregation mapping
            redis.call("hdel", snapshot_hash_key, client_id)
            redis.call("hdel", leave_hash_key, client_id)
            if aggregation_mapping_key ~= '' then
                redis.call("hdel", aggregation_mapping_key, client_id)
            end
            redis.call("zrem", snapshot_expire_key, client_id)

            -- Update aggregation (user tracking) - decrement user count
            if aggregation_value and aggregation_hash_key ~= '' then
                local connectionsCount = redis.call("hincrby", aggregation_hash_key, aggregation_value, -1)
                if connectionsCount <= 0 then
                    -- Last connection for this user, remove from tracking
                    if use_hexpire == '0' and aggregation_zset_key ~= '' then
                        redis.call("zrem", aggregation_zset_key, aggregation_value)
                    end
                    redis.call("hdel", aggregation_hash_key, aggregation_value)
                end
            end

            removed_count = removed_count + 1
        else
            -- No leave payload (shouldn't happen), just clean up
            redis.call("hdel", snapshot_hash_key, client_id)
            redis.call("zrem", snapshot_expire_key, client_id)
        end
    end
    -- If current_score > now, client rejoined, skip silently
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
