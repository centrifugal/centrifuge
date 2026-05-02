--[[
Unified publish script supporting:
1. Append log with offset/epoch for continuity.
2. Keyed state (simple HASH or ordered HASH+ZSET) with optional TTL - used for both state and presence.
3. Leave messages (remove from keyed state).
4. Idempotency via result key.
5. Delta or full payload publishing with simplified format.

Publishing format (via PUBLISH/SPUBLISH):
- Non-delta: "offset:epoch:protobuf" where protobuf is protocol.Publication
- Delta: "d:offset:epoch:prev_len:prev_pub:curr_len:curr_pub"
  Both prev and curr are protocol.Publication messages (fetched from stream)
--]]

-- ==== KEYS ====
-- KEYS[1] = append log stream key (optional, empty '' to disable)
-- KEYS[2] = append log meta key (optional, empty '' to disable)
-- KEYS[3] = result key for idempotency (optional, empty '' to disable)
-- KEYS[4] = state hash key (optional, empty '' to disable) - used for both keyed state AND presence
-- KEYS[5] = state order zset key (optional, empty '' to disable)
-- KEYS[6] = state expire zset key (optional, empty '' to disable)
-- KEYS[7] = state meta key (optional, empty '' to disable)
-- KEYS[8] = cleanup registration zset key (optional, empty '' to disable) - for scheduling cleanup

-- ==== ARGV ====
-- ARGV[1]  = message_key (Key for state field - Client ID for presence, state key for keyed state)
-- ARGV[2]  = message_payload (protocol.Publication for stream and publishing)
-- ARGV[3]  = stream_size (MAXLEN)
-- ARGV[4]  = stream_ttl (milliseconds)
-- ARGV[5]  = channel (for PUBLISH, empty '' to disable)
-- ARGV[6]  = meta_expire (milliseconds, "0" to disable)
-- ARGV[7]  = new_epoch_if_empty
-- ARGV[8]  = publish_command (e.g., "PUBLISH" or "SPUBLISH", empty '' to disable)
-- ARGV[9]  = result_key_expire (for idempotency, empty '' to disable)
-- ARGV[10] = use_delta ("0" or "1")
-- ARGV[11] = version ("0" to disable version-based idempotency)
-- ARGV[12] = version_epoch
-- ARGV[13] = is_leave ("0" or "1")
-- ARGV[14] = score (for ordered keyed state)
-- ARGV[15] = keyed_member_ttl (milliseconds for keyed state TTL)
-- ARGV[16] = use_hpexpire ("0" or "1" - Redis 7.4+ per-field TTL)
-- ARGV[17] = channel_for_cleanup (channel name for cleanup registration, empty '' to disable)
-- ARGV[18] = key_mode ("" for replace/always, "if_new" only if key doesn't exist, "if_exists" only if key exists)
-- ARGV[19] = refresh_ttl_on_suppress ("0" or "1" - refresh TTL even when suppressed by key_mode)
-- ARGV[20] = expected_offset (for CAS, empty '' to disable)
-- ARGV[21] = expected_epoch (for CAS, empty '' to disable)
-- ARGV[22] = state_payload (for state storage, empty '' to use message_payload)
-- ARGV[23] = nil_key (slot-aligned placeholder for unused KEYS in cluster mode, empty '' to disable)
-- ARGV[24] = version_field (pre-computed "v:KEY" for per-key version, empty '' to disable)
-- ARGV[25] = version_epoch_field (pre-computed "ve:KEY" for per-key version epoch, empty '' to disable)
-- ARGV[26] = now (current time in milliseconds, from Go)

-- Local variables from KEYS
local stream_key = KEYS[1]
local meta_key = KEYS[2]
local result_key = KEYS[3]
local state_hash_key = KEYS[4]
local state_order_key = KEYS[5]
local state_expire_key = KEYS[6]
local state_meta_key = KEYS[7]
local cleanup_registration_key = KEYS[8]

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
local use_hpexpire = ARGV[16]
local channel_for_cleanup = ARGV[17]
local key_mode = ARGV[18] or ""
local refresh_ttl_on_suppress = ARGV[19] or "0"
local expected_offset = ARGV[20] or ""
local expected_epoch = ARGV[21] or ""
local state_payload_arg = ARGV[22] or ""

-- In Redis Cluster, all KEYS must hash to the same slot. For unused keys we pass
-- a slot-aligned placeholder instead of '' (empty string hashes to slot 0). Convert
-- those placeholders back to '' so all existing conditional checks still work.
local nil_key = ARGV[23] or ""
local version_field = ARGV[24] or ""
local version_epoch_field = ARGV[25] or ""
local now = tonumber(ARGV[26])
if nil_key ~= "" then
    if stream_key == nil_key then stream_key = '' end
    if meta_key == nil_key then meta_key = '' end
    if result_key == nil_key then result_key = '' end
    if state_hash_key == nil_key then state_hash_key = '' end
    if state_order_key == nil_key then state_order_key = '' end
    if state_expire_key == nil_key then state_expire_key = '' end
    if state_meta_key == nil_key then state_meta_key = '' end
    if cleanup_registration_key == nil_key then cleanup_registration_key = '' end
end

-- Determine which payload to use for state storage
-- If state_payload_arg is provided, use it; otherwise use message_payload
local state_payload = state_payload_arg
if state_payload == "" then
    state_payload = message_payload
end

-- ==== Step 0: Idempotency check ====
if result_key_expire ~= '' and result_key ~= '' then
    local epoch = redis.call("hget", result_key, "e")
    if epoch then
        local offset = redis.call("hget", result_key, "s")
        return { offset, epoch, "idempotency" }
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

    -- ==== Step 2: Epoch change check for state (moved here for version check correctness) ====
    -- The invariant: state_hash_key data is only valid when state_meta_key.epoch
    -- matches current_epoch. When that invariant doesn't hold, state is from a
    -- dead epoch and must be wiped before the KeyMode/version checks below —
    -- otherwise zombie keys would suppress legitimate fresh-epoch publishes.
    --
    -- Three failure modes the wipe must catch:
    --   (a) state_meta_key.epoch is set and explicitly differs from current.
    --   (b) state_meta_key was evicted entirely while state_hash_key lingered
    --       (lazy eviction skew on a fresh meta).
    --   (c) fresh_meta with empty state_meta_key — same as (b), reached when
    --       meta_key was the one that disappeared first.
    --
    -- Unifying check: if state_hash_key has any entries and we cannot confirm
    -- state_meta_key.epoch == current_epoch, wipe.
    local should_wipe_state = false
    if state_hash_key ~= '' then
        local has_state_data = tonumber(redis.call("hlen", state_hash_key) or 0) > 0
        if has_state_data then
            local state_epoch_valid = false
            if state_meta_key ~= '' then
                local stored_epoch = redis.call("hget", state_meta_key, "epoch")
                if stored_epoch and stored_epoch == current_epoch then
                    state_epoch_valid = true
                end
            end
            if not state_epoch_valid then
                should_wipe_state = true
            end
        end
    end
    if should_wipe_state then
        -- Clear all keyed-state structures (including per-key versions stored in state_meta_key).
        redis.call("del", state_hash_key)
        if state_order_key ~= '' then
            redis.call("del", state_order_key)
        end
        if state_expire_key ~= '' then
            redis.call("del", state_expire_key)
        end
        if state_meta_key ~= '' then
            redis.call("del", state_meta_key)
        end
        -- Remove channel from cleanup registration (no entries left after epoch change)
        if cleanup_registration_key ~= '' and channel_for_cleanup ~= '' then
            redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
        end
    end

    -- ==== Step 2a: Per-key version check (BEFORE incrementing offset) ====
    if version ~= "0" and version_field ~= "" and state_meta_key ~= "" then
        local prev_vals = redis.call("hmget", state_meta_key, version_field, version_epoch_field)
        local prev_version, prev_version_epoch = prev_vals[1], prev_vals[2]
        if prev_version then
            if (version_epoch == "" or version_epoch == prev_version_epoch) and (tonumber(prev_version) >= tonumber(version)) then
                -- Suppressed: return current offset without incrementing
                local current_offset = redis.call("hget", meta_key, "s") or 0
                return { tonumber(current_offset), current_epoch, "version" }
            end
        end
    end

    -- ==== Step 2b: KeyMode check (BEFORE incrementing offset) ====
    if key_mode ~= "" and message_key ~= "" and state_hash_key ~= "" and is_leave ~= "1" then
        local key_exists = redis.call("hexists", state_hash_key, message_key) == 1
        if key_mode == "if_new" and key_exists then
            -- KeyModeIfNew but key already exists - suppress
            -- But optionally refresh TTL if refresh_ttl_on_suppress is set
            if refresh_ttl_on_suppress == "1" and tonumber(keyed_member_ttl) > 0 then
                local expire_at = now + tonumber(keyed_member_ttl)
                local ttl = tonumber(keyed_member_ttl)

                -- Update expire zset with new expiry time (per-key tracking)
                if state_expire_key ~= '' then
                    redis.call("zadd", state_expire_key, expire_at, message_key)
                end

                -- Update cleanup registration with new expiry
                if cleanup_registration_key ~= '' and channel_for_cleanup ~= '' then
                    local current_score = redis.call("zscore", cleanup_registration_key, channel_for_cleanup)
                    if not current_score or tonumber(current_score) > expire_at then
                        redis.call("zadd", cleanup_registration_key, expire_at, channel_for_cleanup)
                    end
                end

                -- Refresh per-field TTL if using HPEXPIRE
                if use_hpexpire == "1" then
                    redis.call("hpexpire", state_hash_key, ttl, "FIELDS", "1", message_key)
                end
                -- Container-level safety net EXPIRE using MetaTTL (not per-key TTL).
                -- Without refreshing meta_key + stream_key here, an idle channel
                -- whose keys are being kept alive can still lose meta + stream to
                -- TTL, forcing an epoch reset on the next publish.
                if meta_expire ~= '0' then
                    local me = tonumber(meta_expire)
                    if state_expire_key ~= '' then
                        redis.call("pexpire", state_expire_key, me)
                    end
                    if use_hpexpire ~= "1" then
                        redis.call("pexpire", state_hash_key, me)
                    end
                    if meta_key ~= '' then
                        redis.call("pexpire", meta_key, me)
                    end
                end
                if stream_key ~= '' and tonumber(stream_ttl) > 0 then
                    redis.call("pexpire", stream_key, tonumber(stream_ttl))
                end
            end
            local current_offset = redis.call("hget", meta_key, "s") or 0
            return { tonumber(current_offset), current_epoch, "key_exists" }
        end
        if key_mode == "if_exists" and not key_exists then
            -- KeyModeIfExists but key doesn't exist - suppress
            local current_offset = redis.call("hget", meta_key, "s") or 0
            return { tonumber(current_offset), current_epoch, "key_not_found" }
        end
    end

    -- ==== Step 2c: CAS position check (BEFORE incrementing offset) ====
    if expected_offset ~= "" and expected_epoch ~= "" and message_key ~= "" and state_hash_key ~= "" then
        local current_value = redis.call("hget", state_hash_key, message_key)

        -- First check epoch matches current channel epoch
        if expected_epoch ~= current_epoch then
            local current_offset = redis.call("hget", meta_key, "s") or 0
            -- Return current value for immediate retry (4th element)
            return { tonumber(current_offset), current_epoch, "position_mismatch", current_value or "" }
        end

        if current_value then
            -- Parse offset from "offset:epoch:payload"
            local colon_pos = string.find(current_value, ":")
            if colon_pos then
                local current_key_offset = tonumber(string.sub(current_value, 1, colon_pos - 1))
                if current_key_offset ~= tonumber(expected_offset) then
                    local current_offset = redis.call("hget", meta_key, "s") or 0
                    -- Return current value for immediate retry (4th element)
                    return { tonumber(current_offset), current_epoch, "position_mismatch", current_value }
                end
            else
                -- Invalid format - mismatch
                local current_offset = redis.call("hget", meta_key, "s") or 0
                return { tonumber(current_offset), current_epoch, "position_mismatch", current_value }
            end
        else
            -- Key doesn't exist but expected position was provided
            local current_offset = redis.call("hget", meta_key, "s") or 0
            return { tonumber(current_offset), current_epoch, "position_mismatch", "" }
        end
    end

    -- ==== Step 2d: Leave key existence check (BEFORE incrementing offset) ====
    if is_leave == "1" and message_key ~= "" and state_hash_key ~= "" then
        if redis.call("hexists", state_hash_key, message_key) == 0 then
            local current_offset = redis.call("hget", meta_key, "s") or 0
            return { tonumber(current_offset), current_epoch, "key_not_found" }
        end
    end

    -- ==== Step 3: Increment append log offset (only if not suppressed) ====
    top_offset = redis.call("hincrby", meta_key, "s", 1)

    -- Set meta TTL if needed
    if meta_expire ~= '0' then
        redis.call("pexpire", meta_key, meta_expire)
    end

    -- Update per-key version tracking in state_meta_key (tied to state lifecycle)
    if version ~= "0" and version_field ~= "" and state_meta_key ~= "" then
        redis.call("hset", state_meta_key, version_field, version, version_epoch_field, version_epoch)
    end
else
    -- No append log, use epoch from ARGV
    current_epoch = new_epoch_if_empty
    top_offset = 0

    -- Leave key existence check (no stream case)
    if is_leave == "1" and message_key ~= "" and state_hash_key ~= "" then
        if redis.call("hexists", state_hash_key, message_key) == 0 then
            return { 0, current_epoch, "key_not_found" }
        end
    end
end

-- ==== Step 4: Handle leave message ====
if is_leave == "1" then
    -- Remove from keyed state (works for both state and presence)
    if state_hash_key ~= '' then
        -- Remove from state
        redis.call("hdel", state_hash_key, message_key)
        if state_order_key ~= '' then
            redis.call("zrem", state_order_key, message_key)
        end
        if state_expire_key ~= '' then
            redis.call("zrem", state_expire_key, message_key)
        end
        -- Clean up per-key version fields from state meta (tied to state lifecycle)
        if version_field ~= "" and state_meta_key ~= "" then
            redis.call("hdel", state_meta_key, version_field, version_epoch_field)
        end

        -- Update cleanup registration: remove channel if no entries left, or update to next expiry
        if cleanup_registration_key ~= '' and channel_for_cleanup ~= '' and state_expire_key ~= '' then
            local remaining = redis.call("zcard", state_expire_key)
            if remaining == 0 then
                -- No entries left, remove channel from cleanup registration
                redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
            else
                -- Update to next earliest expiry
                local next_expiry = redis.call("zrange", state_expire_key, 0, 0, "WITHSCORES")
                if #next_expiry >= 2 then
                    redis.call("zadd", cleanup_registration_key, next_expiry[2], channel_for_cleanup)
                else
                    -- Race: entry was removed between ZCARD and ZRANGE, remove from cleanup
                    redis.call("zrem", cleanup_registration_key, channel_for_cleanup)
                end
            end
        end
    end
end

-- For key-based delta: fetch raw previous state value before updating.
-- Returned to Go for delta computation (Go parses "offset:epoch:payload" format).
local prev_key_value = nil
if use_delta == "1" and message_key ~= "" and state_hash_key ~= "" and state_payload_arg == "" and is_leave ~= "1" then
    prev_key_value = redis.call("hget", state_hash_key, message_key)
end

-- ==== Step 5: Add/update keyed state ====
-- Skip for leave messages (they already removed entries in Step 4)
if state_hash_key ~= '' and is_leave ~= "1" then
    -- Epoch change check already done in Step 2 (moved there for version check correctness)

    local expire_at = now + tonumber(keyed_member_ttl)
    local ttl = tonumber(keyed_member_ttl)

    if state_order_key ~= '' and state_expire_key ~= '' then
        -- Ordered keyed state (HASH + ZSET)
        -- Store revision with payload: offset:epoch:payload
        local state_value = top_offset .. ":" .. current_epoch .. ":" .. state_payload
        redis.call("hset", state_hash_key, message_key, state_value)
        redis.call("zadd", state_order_key, tonumber(score), message_key)
        if ttl > 0 then
            redis.call("zadd", state_expire_key, expire_at, message_key)
        end
        -- Container-level safety net EXPIRE using MetaTTL (not per-key TTL)
        if meta_expire ~= '0' then
            local me = tonumber(meta_expire)
            redis.call("pexpire", state_hash_key, me)
            redis.call("pexpire", state_order_key, me)
            redis.call("pexpire", state_expire_key, me)
        end
    else
        -- Simple HASH keyed state (unordered)
        -- Store revision with payload: offset:epoch:payload
        local state_value = top_offset .. ":" .. current_epoch .. ":" .. state_payload
        redis.call("hset", state_hash_key, message_key, state_value)
        if ttl > 0 then
            -- Always populate expire ZSET for cleanup script to discover expirations
            -- This enables guaranteed LEAVE events even when using HPEXPIRE
            if state_expire_key ~= '' then
                redis.call("zadd", state_expire_key, expire_at, message_key)
            end

            if use_hpexpire == "1" then
                -- Redis 7.4+ HPEXPIRE per-field TTL (defense in depth)
                redis.call("hpexpire", state_hash_key, ttl, "FIELDS", "1", message_key)
            end
        end
        -- Container-level safety net EXPIRE using MetaTTL (not per-key TTL)
        if meta_expire ~= '0' then
            local me = tonumber(meta_expire)
            if state_expire_key ~= '' then
                redis.call("pexpire", state_expire_key, me)
            end
            if use_hpexpire ~= "1" then
                redis.call("pexpire", state_hash_key, me)
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

    -- Update state metadata with current epoch
    if state_meta_key ~= '' then
        redis.call("hset", state_meta_key, "epoch", current_epoch)
        redis.call("hset", state_meta_key, "updated_at", tostring(now))
        -- State meta should persist as long as stream meta (meta_expire).
        -- Do NOT fall back to keyed_member_ttl - it's per-key TTL (e.g. 30s for
        -- presence) which is too short and causes premature epoch invalidation.
        if meta_expire ~= '0' then
            redis.call("pexpire", state_meta_key, tonumber(meta_expire))
        end
    end
end

-- ==== Step 6: Update append log (if stream keys provided) ====
if stream_key ~= '' and meta_key ~= '' then
    if top_offset == 1 then
        -- Offset is 1 - delete existing stream to start fresh.
        -- State clearing on epoch change is already handled in Step 5.
        redis.call("del", stream_key)
    end

    local xadd_args = { stream_key, "MAXLEN", "~", stream_size, top_offset, "e", current_epoch, "d", message_payload }
    redis.call("xadd", unpack(xadd_args))
    if tonumber(stream_ttl) > 0 then
        redis.call("pexpire", stream_key, tonumber(stream_ttl))
    end
end

-- ==== Step 7: Publish to channel (simplified format) ====
if channel ~= '' and publish_command ~= '' then
    local payload
    if prev_key_value then
        -- Key-based delta: prev_key_value is raw state value ("offset:epoch:protobuf").
        -- Go parses it on the receiving side.
        payload = "d:" .. top_offset .. ":" .. current_epoch .. ":" .. #prev_key_value .. ":" .. prev_key_value .. ":" .. #message_payload .. ":" .. message_payload
    else
        -- Non-delta format: offset:epoch:protobuf
        payload = top_offset .. ":" .. current_epoch .. ":" .. message_payload
    end
    redis.call(publish_command, channel, payload)
end

-- ==== Step 8: Store result key for idempotency ====
if result_key_expire ~= '' and result_key ~= '' then
    redis.call("hset", result_key, "e", current_epoch, "s", top_offset)
    redis.call("pexpire", result_key, result_key_expire)
end

-- ==== Step 9: Return top_offset and epoch ====
return { top_offset, current_epoch, "" }
