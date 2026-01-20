--[[
Unified publish script supporting:
1. Append log with offset/epoch for continuity.
2. Keyed snapshot (simple HASH or ordered HASH+ZSET) with optional TTL - used for both state and presence.
3. Per-user tracking for presence (connection count optimization).
4. Leave messages (remove from keyed snapshot + user tracking).
5. Idempotency via result key.
6. Delta or full payload publishing with simplified format.

Publishing format (via PUBLISH/SPUBLISH):
- Non-delta: "offset:epoch:protobuf" where protobuf is protocol.Publication
- Delta: "d:offset:epoch:prev_len:prev_pub:curr_len:curr_pub"
  Both prev and curr are protocol.Publication messages (fetched from stream)
--]]

-- ==== KEYS ====
-- KEYS[1] = append log stream key (optional, empty '' to disable)
-- KEYS[2] = append log meta key (optional, empty '' to disable)
-- KEYS[3] = result key for idempotency (optional, empty '' to disable)
-- KEYS[4] = snapshot hash key (optional, empty '' to disable) - used for both keyed state AND presence
-- KEYS[5] = snapshot order zset key (optional, empty '' to disable)
-- KEYS[6] = snapshot expire zset key (optional, empty '' to disable)
-- KEYS[7] = aggregation zset key (optional, empty '' to disable) - for generic aggregations (e.g., user tracking in presence)
-- KEYS[8] = aggregation hash key (optional, empty '' to disable) - for generic aggregations (e.g., user tracking in presence)
-- KEYS[9] = snapshot meta key (optional, empty '' to disable)
-- KEYS[10] = cleanup registration zset key (optional, empty '' to disable) - for scheduling cleanup
-- KEYS[11] = aggregation mapping hash key (optional, empty '' to disable) - stores client_id -> aggregation_value mapping for cleanup

-- ==== ARGV ====
-- ARGV[1]  = message_key (Key for snapshot field - Client ID for presence, state key for keyed state)
-- ARGV[2]  = message_payload (protocol.Publication for stream and publishing)
-- ARGV[3]  = stream_size (MAXLEN)
-- ARGV[4]  = stream_ttl (seconds)
-- ARGV[5]  = channel (for PUBLISH, empty '' to disable)
-- ARGV[6]  = meta_expire (seconds, "0" to disable)
-- ARGV[7]  = new_epoch_if_empty
-- ARGV[8]  = publish_command (e.g., "PUBLISH" or "SPUBLISH", empty '' to disable)
-- ARGV[9]  = result_key_expire (for idempotency, empty '' to disable)
-- ARGV[10] = use_delta ("0" or "1")
-- ARGV[11] = version ("0" to disable version-based idempotency)
-- ARGV[12] = version_epoch
-- ARGV[13] = is_leave ("0" or "1")
-- ARGV[14] = score (for ordered keyed state)
-- ARGV[15] = keyed_member_ttl (seconds for keyed snapshot TTL)
-- ARGV[16] = use_hexpire ("0" or "1" - Redis 7.4+ per-field TTL)
-- ARGV[17] = aggregation_key (aggregation identifier, empty "" to disable - e.g., "user_id" for presence)
-- ARGV[18] = aggregation_value (value to aggregate by - e.g., actual user ID for presence user counting)
-- ARGV[19] = message_key_payload (optional alternative payload for snapshot storage - not used when message_payload is Publication)
-- ARGV[20] = channel_for_cleanup (channel name for cleanup registration, empty '' to disable)
-- ARGV[21] = key_mode ("" for replace/always, "if_new" only if key doesn't exist, "if_exists" only if key exists)

-- Local variables from KEYS
local stream_key = KEYS[1]
local meta_key = KEYS[2]
local result_key = KEYS[3]
local snapshot_hash_key = KEYS[4]
local snapshot_order_key = KEYS[5]
local snapshot_expire_key = KEYS[6]
local aggregation_zset_key = KEYS[7]
local aggregation_hash_key = KEYS[8]
local snapshot_meta_key = KEYS[9]
local cleanup_registration_key = KEYS[10]
local aggregation_mapping_key = KEYS[11]

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
local aggregation_key = ARGV[17]
local aggregation_value = ARGV[18]
local message_key_payload = ARGV[19]
local channel_for_cleanup = ARGV[20]
local key_mode = ARGV[21] or ""

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

    -- ==== Step 2b: KeyMode check (BEFORE incrementing offset) ====
    if key_mode ~= "" and message_key ~= "" and snapshot_hash_key ~= "" and is_leave ~= "1" then
        local key_exists = redis.call("hexists", snapshot_hash_key, message_key) == 1
        if key_mode == "if_new" and key_exists then
            -- KeyModeIfNew but key already exists - suppress
            local current_offset = redis.call("hget", meta_key, "s") or 0
            return { tonumber(current_offset), current_epoch, "0", "1" }
        end
        if key_mode == "if_exists" and not key_exists then
            -- KeyModeIfExists but key doesn't exist - suppress
            local current_offset = redis.call("hget", meta_key, "s") or 0
            return { tonumber(current_offset), current_epoch, "0", "1" }
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
        if aggregation_key ~= '0' and aggregation_hash_key ~= '' then
            clientExists = redis.call("hexists", snapshot_hash_key, message_key) == 1
        end

        -- Remove from snapshot
        redis.call("hdel", snapshot_hash_key, message_key)
        if aggregation_mapping_key ~= '' then
            redis.call("hdel", aggregation_mapping_key, message_key)
        end
        if snapshot_order_key ~= '' then
            redis.call("zrem", snapshot_order_key, message_key)
        end
        if snapshot_expire_key ~= '' then
            redis.call("zrem", snapshot_expire_key, message_key)
        end

        -- Update cleanup registration: remove channel if no entries left, or update to next expiry
        if cleanup_registration_key ~= '' and channel_for_cleanup ~= '' and snapshot_expire_key ~= '' then
            local remaining = redis.call("zcard", snapshot_expire_key)
            if remaining == 0 then
                -- No entries left, remove channel from cleanup registration
                redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
            else
                -- Update to next earliest expiry
                local next_expiry = redis.call("zrange", snapshot_expire_key, 0, 0, "WITHSCORES")
                if #next_expiry >= 2 then
                    redis.call("zadd", cleanup_registration_key, next_expiry[2], channel_for_cleanup)
                else
                    -- Race: entry was removed between ZCARD and ZRANGE, remove from cleanup
                    redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
                end
            end
        end

        -- Update user tracking if enabled
        if aggregation_key ~= '0' and clientExists and aggregation_hash_key ~= '' then
            local connectionsCount = redis.call("hincrby", aggregation_hash_key, aggregation_value, -1)
            if connectionsCount <= 0 then
                if use_hexpire == '0' and aggregation_zset_key ~= '' then
                    redis.call("zrem", aggregation_zset_key, aggregation_value)
                end
                redis.call("hdel", aggregation_hash_key, aggregation_value)
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
            if aggregation_hash_key ~= '' then
                redis.call("del", aggregation_hash_key)
            end
            if aggregation_zset_key ~= '' then
                redis.call("del", aggregation_zset_key)
            end
            -- Clear aggregation mapping from previous epoch
            if aggregation_mapping_key ~= '' then
                redis.call("del", aggregation_mapping_key)
            end
            -- Remove channel from cleanup registration (no entries left after epoch change)
            if cleanup_registration_key ~= '' and channel_for_cleanup ~= '' then
                redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
            end
        end
    end

    -- Check if this is a new client (for user tracking)
    if aggregation_key ~= '0' then
        wasNewClient = redis.call("hexists", snapshot_hash_key, message_key) == 0
    end

    local now = tonumber(redis.call("time")[1])
    local expire_at = now + tonumber(keyed_member_ttl)
    local ttl = tonumber(keyed_member_ttl)

    if snapshot_order_key ~= '' and snapshot_expire_key ~= '' then
        -- Ordered keyed state (HASH + ZSET)
        -- Store revision with payload: offset:epoch:payload
        local snapshot_value = top_offset .. ":" .. current_epoch .. ":" .. snapshot_payload
        redis.call("hset", snapshot_hash_key, message_key, snapshot_value)
        redis.call("zadd", snapshot_order_key, tonumber(score), message_key)
        redis.call("zadd", snapshot_expire_key, expire_at, message_key)
        redis.call("expire", snapshot_hash_key, ttl)
        redis.call("expire", snapshot_order_key, ttl)
        redis.call("expire", snapshot_expire_key, ttl)
    else
        -- Simple HASH keyed snapshot (unordered)
        -- Store revision with payload: offset:epoch:payload
        local snapshot_value = top_offset .. ":" .. current_epoch .. ":" .. snapshot_payload
        redis.call("hset", snapshot_hash_key, message_key, snapshot_value)
        if ttl > 0 then
            -- Always populate expire ZSET for cleanup script to discover expirations
            -- This enables guaranteed LEAVE events even when using HEXPIRE
            if snapshot_expire_key ~= '' then
                redis.call("zadd", snapshot_expire_key, expire_at, message_key)
                redis.call("expire", snapshot_expire_key, ttl)
            end

            if use_hexpire == "1" then
                -- Redis 7.4+ HEXPIRE per-field TTL (defense in depth)
                redis.call("hexpire", snapshot_hash_key, ttl, "FIELDS", "1", message_key)
            else
                -- Still set whole-hash TTL as safety net
                redis.call("expire", snapshot_hash_key, ttl)
            end
        end
    end

    -- Store aggregation mapping for cleanup (enables user count updates on TTL expiry)
    if aggregation_mapping_key ~= '' and aggregation_key ~= '0' and aggregation_value ~= '' then
        redis.call("hset", aggregation_mapping_key, message_key, aggregation_value)
        if ttl > 0 then
            if use_hexpire == "1" then
                -- Redis 7.4+ per-field TTL for aggregation mappings
                redis.call("hexpire", aggregation_mapping_key, ttl, "FIELDS", "1", message_key)
            else
                -- Whole hash TTL as fallback
                redis.call("expire", aggregation_mapping_key, ttl)
            end
        end
    end

    -- Register channel for cleanup with earliest expiry time
    if cleanup_registration_key ~= '' and channel_for_cleanup ~= '' and ttl > 0 then
        -- Use NX to only set if score doesn't exist or is higher (earlier expiry wins)
        local current_score = redis.call("zscore", cleanup_registration_key, channel_for_cleanup)
        if not current_score or tonumber(current_score) > expire_at then
            redis.call("zadd", cleanup_registration_key, expire_at, channel_for_cleanup)
        end
    end

    -- Update snapshot metadata with current epoch
    if snapshot_meta_key ~= '' then
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
if aggregation_key ~= '0' and aggregation_hash_key ~= '' and wasNewClient and is_leave ~= "1" then
    redis.call("hincrby", aggregation_hash_key, aggregation_value, 1)
    if tonumber(keyed_member_ttl) > 0 then
        redis.call("expire", aggregation_hash_key, tonumber(keyed_member_ttl))
    end
    if use_hexpire == '0' and aggregation_zset_key ~= '' then
        local now = tonumber(redis.call("time")[1])
        local expire_at = now + tonumber(keyed_member_ttl)
        redis.call("zadd", aggregation_zset_key, expire_at, aggregation_value)
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("expire", aggregation_zset_key, tonumber(keyed_member_ttl))
        end
    else
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("hexpire", aggregation_hash_key, tonumber(keyed_member_ttl), "FIELDS", "1", aggregation_value)
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
            if aggregation_hash_key ~= '' then
                redis.call("del", aggregation_hash_key)
            end
            if aggregation_zset_key ~= '' then
                redis.call("del", aggregation_zset_key)
            end
            -- Clear aggregation mapping from previous epoch
            if aggregation_mapping_key ~= '' then
                redis.call("del", aggregation_mapping_key)
            end
            -- Remove channel from cleanup registration (no entries left after epoch change)
            if cleanup_registration_key ~= '' and channel_for_cleanup ~= '' then
                redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
            end
        end
    end

    local xadd_args = { stream_key, "MAXLEN", stream_size, top_offset, "e", current_epoch, "d", message_payload }
    redis.call("xadd", unpack(xadd_args))
    if tonumber(stream_ttl) > 0 then
        redis.call("expire", stream_key, tonumber(stream_ttl))
    end
end

-- ==== Step 8: Publish to channel (simplified format) ====
if channel ~= '' and publish_command ~= '' then
    local payload
    if use_delta == "1" and stream_key ~= '' then
        -- Delta format: d:offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
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
        payload = "d:" .. top_offset .. ":" .. current_epoch .. ":" .. #prev_payload .. ":" .. prev_payload .. ":" .. #message_payload .. ":" .. message_payload
    else
        -- Non-delta format: offset:epoch:protobuf
        payload = top_offset .. ":" .. current_epoch .. ":" .. message_payload
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
