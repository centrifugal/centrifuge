--[[
Unified publish script supporting:
1. Append log with offset/epoch for continuity.
2. Keyed snapshot (simple HASH or ordered HASH+ZSET) with optional TTL.
3. Presence tracking per client and per user.
4. Leave messages (remove presence + keyed state).
5. Idempotency via result key.
6. Delta or full payload publishing.
--]]

-- ==== KEYS ====
-- KEYS[1] = append log stream key (optional, empty '' to disable)
-- KEYS[2] = append log meta key (optional, empty '' to disable)
-- KEYS[3] = result key for idempotency (optional, empty '' to disable)
-- KEYS[4] = snapshot hash key (optional, empty '' to disable)
-- KEYS[5] = snapshot order zset key (optional, empty '' to disable)
-- KEYS[6] = snapshot expire zset key (optional, empty '' to disable)
-- KEYS[7] = presence zset key (optional, empty '' to disable)
-- KEYS[8] = presence hash key (optional, empty '' to disable)
-- KEYS[9] = per-user zset key (optional, empty '' to disable)
-- KEYS[10] = per-user hash key (optional, empty '' to disable)

-- ==== ARGV ====
-- ARGV[1]  = message_payload (or client_id for presence operations)
-- ARGV[2]  = stream_size (MAXLEN)
-- ARGV[3]  = stream_ttl (seconds)
-- ARGV[4]  = channel (for PUBLISH, empty '' to disable)
-- ARGV[5]  = meta_expire (seconds, "0" to disable)
-- ARGV[6]  = new_epoch_if_empty
-- ARGV[7]  = publish_command (e.g., "publish")
-- ARGV[8]  = result_key_expire (for idempotency, empty '' to disable)
-- ARGV[9]  = use_delta ("0" or "1")
-- ARGV[10] = version ("0" to disable version-based idempotency)
-- ARGV[11] = version_epoch
-- ARGV[12] = is_leave ("0" or "1")
-- ARGV[13] = score (for ordered keyed state)
-- ARGV[14] = keyed_member_ttl (seconds for keyed snapshot TTL)
-- ARGV[15] = use_hexpire ("0" or "1" - Redis 7.4+ per-field TTL)
-- ARGV[16] = track_user ("0" or "1")
-- ARGV[17] = user_id
-- ARGV[18] = presence_info (client info payload for presence)
-- ARGV[19] = presence_expire_at (expire timestamp for presence ZADD)

-- Local variables from KEYS
local stream_key = KEYS[1]
local meta_key = KEYS[2]
local result_key = KEYS[3]
local snapshot_hash_key = KEYS[4]
local snapshot_order_key = KEYS[5]
local snapshot_expire_key = KEYS[6]
local presence_zset_key = KEYS[7]
local presence_hash_key = KEYS[8]
local user_zset_key = KEYS[9]
local user_hash_key = KEYS[10]

-- Local variables from ARGV
local message_payload = ARGV[1]
local stream_size = ARGV[2]
local stream_ttl = ARGV[3]
local channel = ARGV[4]
local meta_expire = ARGV[5]
local new_epoch_if_empty = ARGV[6]
local publish_command = ARGV[7]
local result_key_expire = ARGV[8]
local use_delta = ARGV[9]
local version = ARGV[10]
local version_epoch = ARGV[11]
local is_leave = ARGV[12]
local score = ARGV[13]
local keyed_member_ttl = ARGV[14]
local use_hexpire = ARGV[15]
local track_user = ARGV[16]
local user_id = ARGV[17]
local presence_info = ARGV[18]
local presence_expire_at = ARGV[19]

-- ==== Step 0: Idempotency check ====
if result_key_expire ~= '' and result_key ~= '' then
    local cached = redis.call("hmget", result_key, "e", "s")
    if cached[1] then
        -- Already processed this message, return stored offset/epoch
        return { cached[2], cached[1], "1", "0" }
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
    -- Remove keyed snapshot entry if exists
    if snapshot_hash_key ~= '' then
        redis.call("hdel", snapshot_hash_key, message_payload)
        if snapshot_order_key ~= '' then
            redis.call("zrem", snapshot_order_key, message_payload)
        end
        if snapshot_expire_key ~= '' then
            redis.call("zrem", snapshot_expire_key, message_payload)
        end
    end

    -- Remove presence
    if presence_hash_key ~= '' then
        local clientExists = false
        if track_user ~= '0' then
            clientExists = redis.call("hexists", presence_hash_key, message_payload) == 1
        end

        redis.call("hdel", presence_hash_key, message_payload)
        if use_hexpire == '0' and presence_zset_key ~= '' then
            redis.call("zrem", presence_zset_key, message_payload)
        end

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

    -- Return leave info (no append log write for leave)
    return { top_offset, current_epoch, "0", "0" }
end

-- ==== Step 5: Add/update keyed snapshot ====
if snapshot_hash_key ~= '' then
    if snapshot_order_key ~= '' and snapshot_expire_key ~= '' then
        -- Ordered keyed state (HASH + ZSET)
        local now = tonumber(redis.call("time")[1])
        local expire_at = now + tonumber(keyed_member_ttl)
        redis.call("hset", snapshot_hash_key, message_payload, message_payload)
        redis.call("zadd", snapshot_order_key, tonumber(score), message_payload)
        redis.call("zadd", snapshot_expire_key, expire_at, message_payload)
        local ttl = tonumber(keyed_member_ttl)
        redis.call("expire", snapshot_hash_key, ttl)
        redis.call("expire", snapshot_order_key, ttl)
        redis.call("expire", snapshot_expire_key, ttl)
    else
        -- Simple HASH keyed snapshot
        redis.call("hset", snapshot_hash_key, message_payload, message_payload)
        if tonumber(keyed_member_ttl) > 0 then
            if use_hexpire == "1" then
                -- Redis 7.4+ HEXPIRE per-field
                redis.call("hexpire", snapshot_hash_key, tonumber(keyed_member_ttl), "FIELDS", "1", message_payload)
            else
                -- fallback: expire entire hash
                redis.call("expire", snapshot_hash_key, tonumber(keyed_member_ttl))
            end
        end
    end
end

-- ==== Step 6: Add/update presence (for non-leave messages) ====
if presence_hash_key ~= '' and presence_info ~= '' then
    local isNewClient = false
    if track_user ~= '0' then
        isNewClient = redis.call("hexists", presence_hash_key, message_payload) == 0
    end

    -- Add per-client presence
    redis.call("hset", presence_hash_key, message_payload, presence_info)
    if tonumber(keyed_member_ttl) > 0 then
        redis.call("expire", presence_hash_key, tonumber(keyed_member_ttl))
    end
    if use_hexpire == '0' and presence_zset_key ~= '' then
        redis.call("zadd", presence_zset_key, presence_expire_at, message_payload)
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("expire", presence_zset_key, tonumber(keyed_member_ttl))
        end
    else
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("hexpire", presence_hash_key, tonumber(keyed_member_ttl), "FIELDS", "1", message_payload)
        end
    end

    -- Add per-user information
    if track_user ~= '0' and user_hash_key ~= '' then
        if isNewClient then
            redis.call("hincrby", user_hash_key, user_id, 1)
        end
        if tonumber(keyed_member_ttl) > 0 then
            redis.call("expire", user_hash_key, tonumber(keyed_member_ttl))
        end
        if use_hexpire == '0' and user_zset_key ~= '' then
            redis.call("zadd", user_zset_key, presence_expire_at, user_id)
            if tonumber(keyed_member_ttl) > 0 then
                redis.call("expire", user_zset_key, tonumber(keyed_member_ttl))
            end
        else
            if tonumber(keyed_member_ttl) > 0 then
                redis.call("hexpire", user_hash_key, tonumber(keyed_member_ttl), "FIELDS", "1", user_id)
            end
        end
    end
end

-- ==== Step 7: Update append log (only if enabled) ====
if stream_key ~= '' and meta_key ~= '' then
    if top_offset == 1 then
        -- If a new epoch starts (thus top_offset is 1), try to delete the existing stream
        redis.call("del", stream_key)
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
    redis.call("expire", result_key, tonumber(result_key_expire))
end

-- ==== Step 10: Return top_offset and epoch ====
return { top_offset, current_epoch, "0", "0" }
