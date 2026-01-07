--[[
Unified publish script supporting:
1. Append log with offset/epoch for continuity.
2. Keyed snapshot (simple HASH or ordered HASH+ZSET) with optional TTL - used for both state and presence.
3. Per-user tracking for presence (connection count optimization).
4. Leave messages (remove from keyed snapshot + user tracking).
5. Idempotency via result key.
6. Delta or full payload publishing.
--]]

-- ==== KEYS ====
-- KEYS[1] = append log stream key (optional, empty '' to disable)
-- KEYS[2] = append log meta key (optional, empty '' to disable)
-- KEYS[3] = result key for idempotency (optional, empty '' to disable)
-- KEYS[4] = snapshot hash key (optional, empty '' to disable) - used for both keyed state AND presence
-- KEYS[5] = snapshot order zset key (optional, empty '' to disable)
-- KEYS[6] = snapshot expire zset key (optional, empty '' to disable)
-- KEYS[7] = per-user zset key (optional, empty '' to disable) - for presence user tracking
-- KEYS[8] = per-user hash key (optional, empty '' to disable) - for presence user tracking
-- KEYS[9] = snapshot meta key (optional, empty '' to disable)

-- ==== ARGV ====
-- ARGV[1]  = message_key (Key for snapshot field - Client ID for presence, state key for keyed state)
-- ARGV[2]  = message_payload (Data for stream and publish, also for snapshot if message_key_payload is empty)
-- ARGV[3]  = stream_size (MAXLEN)
-- ARGV[4]  = stream_ttl (seconds)
-- ARGV[5]  = channel (for PUBLISH, empty '' to disable)
-- ARGV[6]  = meta_expire (seconds, "0" to disable)
-- ARGV[7]  = new_epoch_if_empty
-- ARGV[8]  = publish_command (e.g., "publish")
-- ARGV[9]  = result_key_expire (for idempotency, empty '' to disable)
-- ARGV[10] = use_delta ("0" or "1")
-- ARGV[11] = version ("0" to disable version-based idempotency)
-- ARGV[12] = version_epoch
-- ARGV[13] = is_leave ("0" or "1")
-- ARGV[14] = score (for ordered keyed state)
-- ARGV[15] = keyed_member_ttl (seconds for keyed snapshot TTL)
-- ARGV[16] = use_hexpire ("0" or "1" - Redis 7.4+ per-field TTL)
-- ARGV[17] = track_user ("0" or "1" - enable user tracking for presence)
-- ARGV[18] = user_id (user identifier for presence tracking)
-- ARGV[19] = message_key_payload (optional payload for snapshot only, empty '' to use message_payload)

-- Local variables from KEYS
local stream_key = KEYS[1]
local meta_key = KEYS[2]
local result_key = KEYS[3]
local snapshot_hash_key = KEYS[4]
local snapshot_order_key = KEYS[5]
local snapshot_expire_key = KEYS[6]
local user_zset_key = KEYS[7]
local user_hash_key = KEYS[8]
local snapshot_meta_key = KEYS[9]

-- Local variables from ARGV
local message_key = ARGV[1]
local message_payload = ARGV[2]
local stream_size = ARGV[3]
local stream_ttl = ARGV[4]
local channel = ARGV[5]
local meta_expire = ARGV[6]
local new_epoch_if_empty = ARGV[7]
local publish_command = ARGV[8]
local result_key_expire = ARGV[9]
local use_delta = ARGV[10]
local version = ARGV[11]
local version_epoch = ARGV[12]
local is_leave = ARGV[13]
local score = ARGV[14]
local keyed_member_ttl = ARGV[15]
local use_hexpire = ARGV[16]
local track_user = ARGV[17]
local user_id = ARGV[18]
local message_key_payload = ARGV[19]

-- Determine which payload to use for snapshot storage
local snapshot_payload = message_payload
if message_key_payload and message_key_payload ~= '' then
    snapshot_payload = message_key_payload
end

-- ==== Step 0: Idempotency check ====
if result_key_expire ~= '' and result_key ~= '' then
    local epoch = redis.call("hget", result_key, "e")
    if epoch then
        local offset = redis.call("hget", result_key, "s")
        return { offset, epoch, "1", "0" }
    end
end

-- ==== Step 1: Ensure epoch exists and increment offset (only if append log enabled) ====
local current_epoch = "0"
local top_offset = 0

if meta_key ~= '' then
    current_epoch = redis.call("hget", meta_key, "e")
    if not current_epoch then
        current_epoch = new_epoch_if_empty
        redis.call("hset", meta_key, "e", current_epoch)
    end

    -- ==== Step 2: Version-based idempotency check (BEFORE incrementing offset) ====
    if version ~= "0" then
        local prev_vals = redis.call("hmget", meta_key, "v", "ve", "s")
        local prev_version, prev_version_epoch, current_offset = prev_vals[1], prev_vals[2], prev_vals[3]
        if prev_version then
            if (version_epoch == "" or version_epoch == prev_version_epoch) and (tonumber(prev_version) >= tonumber(version)) then
                -- Suppressed: return current offset without incrementing
                local offset_num = 0
                if current_offset then
                    offset_num = tonumber(current_offset)
                end
                return { offset_num, current_epoch, "0", "1" }
            end
        end
    end

    -- ==== Step 3: Increment append log offset (only if not suppressed) ====
    top_offset = redis.call("hincrby", meta_key, "s", 1)

    -- Set meta TTL if needed
    if meta_expire ~= '0' then
        redis.call("expire", meta_key, meta_expire)
    end

    -- Update version tracking after successful increment
    if version ~= "0" then
        redis.call("hset", meta_key, "v", version, "ve", version_epoch)
    end
else
    -- No append log, use epoch from ARGV
    current_epoch = new_epoch_if_empty
    top_offset = 0
end

-- ==== Step 4: Handle leave message ====
if is_leave == "1" then
    -- Remove from keyed snapshot (works for both state and presence)
    if snapshot_hash_key ~= '' then
        -- Check if client exists (for user tracking)
        local clientExists = false
        if track_user ~= '0' and user_hash_key ~= '' then
            clientExists = redis.call("hexists", snapshot_hash_key, message_key) == 1
        end

        -- Remove from snapshot
        redis.call("hdel", snapshot_hash_key, message_key)
        if snapshot_order_key ~= '' then
            redis.call("zrem", snapshot_order_key, message_key)
        end
        if snapshot_expire_key ~= '' then
            redis.call("zrem", snapshot_expire_key, message_key)
        end

        -- Update user tracking if enabled
        if track_user ~= '0' and clientExists and user_hash_key ~= '' then
            local connectionsCount = redis.call("hincrby", user_hash_key, user_id, -1)
            if connectionsCount <= 0 then
                if use_hexpire == '0' and user_zset_key ~= '' then
                    redis.call("zrem", user_zset_key, user_id)
                end
                redis.call("hdel", user_hash_key, user_id)
            end
        end
    end
end

-- ==== Step 5: Add/update keyed snapshot ====
-- Skip for leave messages (they already removed entries in Step 4)
local wasNewClient = false
if snapshot_hash_key ~= '' and is_leave ~= "1" then
    -- Check if snapshot is from a different epoch - clear if so
    if snapshot_meta_key ~= '' then
        local old_snapshot_epoch = redis.call("hget", snapshot_meta_key, "epoch")
        if old_snapshot_epoch and old_snapshot_epoch ~= current_epoch then
            -- Epoch changed - clear old snapshot data
            redis.call("del", snapshot_hash_key)
            if snapshot_order_key ~= '' then
                redis.call("del", snapshot_order_key)
            end
            if snapshot_expire_key ~= '' then
                redis.call("del", snapshot_expire_key)
            end
            redis.call("del", snapshot_meta_key)
            -- Clear user tracking data from previous epoch
            if user_hash_key ~= '' then
                redis.call("del", user_hash_key)
            end
            if user_zset_key ~= '' then
                redis.call("del", user_zset_key)
            end
        end
    end

    -- Check if this is a new client (for user tracking)
    if track_user ~= '0' then
        wasNewClient = redis.call("hexists", snapshot_hash_key, message_key) == 0
    end

    if snapshot_order_key ~= '' and snapshot_expire_key ~= '' then
        -- Ordered keyed state (HASH + ZSET)
        local now = tonumber(redis.call("time")[1])
        local expire_at = now + tonumber(keyed_member_ttl)
        redis.call("hset", snapshot_hash_key, message_key, snapshot_payload)
        redis.call("zadd", snapshot_order_key, tonumber(score), message_key)
        redis.call("zadd", snapshot_expire_key, expire_at, message_key)
        local ttl = tonumber(keyed_member_ttl)
        redis.call("expire", snapshot_hash_key, ttl)
        redis.call("expire", snapshot_order_key, ttl)
        redis.call("expire", snapshot_expire_key, ttl)
    else
        -- Simple HASH keyed snapshot (unordered)
        redis.call("hset", snapshot_hash_key, message_key, snapshot_payload)
        if tonumber(keyed_member_ttl) > 0 then
            if use_hexpire == "1" then
                -- Redis 7.4+ HEXPIRE per-field TTL
                redis.call("hexpire", snapshot_hash_key, tonumber(keyed_member_ttl), "FIELDS", "1", message_key)
            else
                -- Fallback for Redis < 7.4: use expire ZSET for per-entry expiration
                if snapshot_expire_key ~= '' then
                    local now = tonumber(redis.call("time")[1])
                    local expire_at = now + tonumber(keyed_member_ttl)
                    redis.call("zadd", snapshot_expire_key, expire_at, message_key)
                    redis.call("expire", snapshot_expire_key, tonumber(keyed_member_ttl))
                end
                -- Still set whole-hash TTL as safety net
                redis.call("expire", snapshot_hash_key, tonumber(keyed_member_ttl))
            end
        end
    end

    -- Update snapshot metadata with current epoch
    if snapshot_meta_key ~= '' then
        local now = tonumber(redis.call("time")[1])
        redis.call("hset", snapshot_meta_key, "epoch", current_epoch)
        redis.call("hset", snapshot_meta_key, "updated_at", tostring(now))
        -- Snapshot meta should persist as long or longer than snapshot data
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("expire", snapshot_meta_key, tonumber(keyed_member_ttl))
        end
    end
end

-- ==== Step 6: User tracking (for presence) ====
-- Update per-user connection count if tracking is enabled and this was a new client
if track_user ~= '0' and user_hash_key ~= '' and wasNewClient and is_leave ~= "1" then
    redis.call("hincrby", user_hash_key, user_id, 1)
    if tonumber(keyed_member_ttl) > 0 then
        redis.call("expire", user_hash_key, tonumber(keyed_member_ttl))
    end
    if use_hexpire == '0' and user_zset_key ~= '' then
        local now = tonumber(redis.call("time")[1])
        local expire_at = now + tonumber(keyed_member_ttl)
        redis.call("zadd", user_zset_key, expire_at, user_id)
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("expire", user_zset_key, tonumber(keyed_member_ttl))
        end
    else
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("hexpire", user_hash_key, tonumber(keyed_member_ttl), "FIELDS", "1", user_id)
        end
    end
end

-- ==== Step 7: Update append log (if stream keys provided) ====
if stream_key ~= '' and meta_key ~= '' then
    if top_offset == 1 then
        -- Offset is 1 - could be first message ever OR epoch change
        -- Only clear if there's old data from a DIFFERENT epoch
        local should_clear = false
        if snapshot_meta_key ~= '' then
            local old_epoch = redis.call("hget", snapshot_meta_key, "epoch")
            if old_epoch and old_epoch ~= current_epoch then
                -- Epoch changed - old snapshot exists from different epoch
                should_clear = true
            end
        end

        -- Delete the existing stream (always on offset 1)
        redis.call("del", stream_key)

        if should_clear then
            -- Clear snapshot data from previous epoch
            if snapshot_hash_key ~= '' then
                redis.call("del", snapshot_hash_key)
            end
            if snapshot_order_key ~= '' then
                redis.call("del", snapshot_order_key)
            end
            if snapshot_expire_key ~= '' then
                redis.call("del", snapshot_expire_key)
            end
            if snapshot_meta_key ~= '' then
                redis.call("del", snapshot_meta_key)
            end
            -- Clear user tracking data from previous epoch
            if user_hash_key ~= '' then
                redis.call("del", user_hash_key)
            end
            if user_zset_key ~= '' then
                redis.call("del", user_zset_key)
            end
        end
    end

    local xadd_args = { stream_key, "MAXLEN", stream_size, top_offset, "e", current_epoch, "d", message_payload }
    redis.call("xadd", unpack(xadd_args))
    if tonumber(stream_ttl) > 0 then
        redis.call("expire", stream_key, tonumber(stream_ttl))
    end
end

-- ==== Step 8: Publish to channel (optional delta) ====
if channel ~= '' then
    local payload
    if use_delta == "1" and stream_key ~= '' then
        local prev_payload = ""
        if top_offset > 1 then
            local prev_entries = redis.call("xrevrange", stream_key, "+", "-", "COUNT", 1)
            if #prev_entries > 0 then
                local fields_and_values = prev_entries[1][2]
                for i = 1, #fields_and_values, 2 do
                    if fields_and_values[i] == "d" then
                        prev_payload = fields_and_values[i + 1]
                        break
                    end
                end
            end
        end
        payload = "__d1:" .. top_offset .. ":" .. current_epoch .. ":" .. #prev_payload .. ":" .. prev_payload .. ":" .. #message_payload .. ":" .. message_payload
    else
        payload = "__p1:" .. top_offset .. ":" .. current_epoch .. "__" .. message_payload
    end
    redis.call(publish_command, channel, payload)
end

-- ==== Step 9: Store result key for idempotency ====
if result_key_expire ~= '' and result_key ~= '' then
    redis.call("hset", result_key, "e", current_epoch, "s", top_offset)
    redis.call("expire", result_key, result_key_expire)
end

-- ==== Step 10: Return top_offset and epoch ====
return { top_offset, current_epoch, "0", "0" }
