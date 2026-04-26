-- Read ordered keyed state with key-based cursor pagination for continuity.
-- Uses (score, key) cursor instead of integer offset to ensure no entries are
-- skipped when the state changes during pagination.
--
-- Supports both DESC (default) and ASC ordering via the asc parameter.
-- DESC: (score DESC, key DESC) — ZREVRANGE, higher scores first.
-- ASC:  (score ASC, key ASC)   — ZRANGE, lower scores first.
--
-- KEYS[1] = state hash key
-- KEYS[2] = state order zset key
-- KEYS[3] = state expire zset key
-- KEYS[4] = meta key
-- KEYS[5] = state meta key
-- ARGV[1] = limit (0 = no limit, return all)
-- ARGV[2] = cursor_score (empty string for first page, score part of cursor)
-- ARGV[3] = cursor_key (empty string for first page, key part of cursor)
-- ARGV[4] = now (current timestamp for expiration cleanup)
-- ARGV[5] = meta_ttl (milliseconds, 0 to disable)
-- ARGV[6] = state_ttl (milliseconds, 0 to disable - refreshes TTL on read)
-- ARGV[7] = streamless ("1" = skip meta/epoch logic, "0" = normal streamed mode)
-- ARGV[8] = asc ("1" = ascending order, "0" or absent = descending order)

local hash_key = KEYS[1]
local order_key = KEYS[2]
local expire_key = KEYS[3]
local meta_key = KEYS[4]
local state_meta_key = KEYS[5]

local limit = tonumber(ARGV[1])
local cursor_score_str = ARGV[2]
local cursor_key = ARGV[3]
local now_str = ARGV[4]
local meta_ttl = tonumber(ARGV[5])
local state_ttl = tonumber(ARGV[6])
local streamless = ARGV[7] == "1"
local asc = ARGV[8] == "1"

local epoch = ""
local stream_offset = "0"

if not streamless then
    -- Update meta epoch + TTL and get current stream offset
    if meta_key ~= '' then
        epoch = redis.call("hget", meta_key, "e")
        if not epoch then
            epoch = now_str
            redis.call("hset", meta_key, "e", epoch)
        end

        stream_offset = redis.call("hget", meta_key, "s")
        if not stream_offset then
            stream_offset = "0"
        end

        if meta_ttl > 0 then
            redis.call("pexpire", meta_key, meta_ttl)
        end
    end

    -- Validate state epoch against stream epoch
    if state_meta_key ~= '' then
        local state_meta_exists = redis.call("exists", state_meta_key)
        if state_meta_exists == 0 then
            return {stream_offset, epoch, {}, {}, "", ""}
        end

        local state_epoch = redis.call("hget", state_meta_key, "epoch")
        if state_epoch ~= epoch then
            return {stream_offset, epoch, {}, {}, "", ""}
        end
    end
end

-- Refresh state TTL on read (LRU behavior)
if state_ttl > 0 then
    redis.call("pexpire", hash_key, state_ttl)
    redis.call("pexpire", order_key, state_ttl)
    redis.call("pexpire", expire_key, state_ttl)
    if not streamless and state_meta_key ~= '' then
        redis.call("pexpire", state_meta_key, state_ttl)
    end
end

-- NOTE: We intentionally do NOT cleanup expired entries inline here.
-- Expired entries MUST be removed by the cleanup worker (map_broker_cleanup.lua)
-- which generates LEAVE events for the stream and publishes removal notifications.
-- Silently removing entries here would cause clients to miss removal events.

local fetch_limit = limit
if limit > 0 then
    fetch_limit = limit + 1  -- Fetch one extra to detect if there's more
end

local keys_to_fetch = {}
local last_score = nil

if cursor_score_str == "" then
    -- First page.
    local all_keys
    if asc then
        if fetch_limit > 0 then
            all_keys = redis.call("zrange", order_key, 0, fetch_limit - 1, "WITHSCORES")
        else
            all_keys = redis.call("zrange", order_key, 0, -1, "WITHSCORES")
        end
    else
        if fetch_limit > 0 then
            all_keys = redis.call("zrevrange", order_key, 0, fetch_limit - 1, "WITHSCORES")
        else
            all_keys = redis.call("zrevrange", order_key, 0, -1, "WITHSCORES")
        end
    end

    -- Parse result (key, score, key, score, ...)
    for i = 1, #all_keys, 2 do
        keys_to_fetch[#keys_to_fetch + 1] = all_keys[i]
        last_score = tonumber(all_keys[i + 1])
    end
else
    -- Subsequent page: need entries after cursor in the requested direction.
    local cursor_score = tonumber(cursor_score_str)

    -- Step 1: Get entries from cursor's score bucket with key past cursor_key.
    local same_score = redis.call("zrangebyscore", order_key, cursor_score, cursor_score)

    if asc then
        -- ASC: keep entries with key > cursor_key
        for i = 1, #same_score do
            local k = same_score[i]
            if k > cursor_key then
                keys_to_fetch[#keys_to_fetch + 1] = k
            end
        end
        -- Sort by key ASC
        table.sort(keys_to_fetch, function(a, b) return a < b end)
    else
        -- DESC: keep entries with key < cursor_key
        for i = 1, #same_score do
            local k = same_score[i]
            if k < cursor_key then
                keys_to_fetch[#keys_to_fetch + 1] = k
            end
        end
        -- Sort by key DESC
        table.sort(keys_to_fetch, function(a, b) return a > b end)
    end

    last_score = cursor_score

    -- Step 2: Get entries past cursor_score in the requested direction.
    local remaining_needed = 0
    if limit > 0 then
        remaining_needed = fetch_limit - #keys_to_fetch
    end

    if limit == 0 or remaining_needed > 0 then
        local other_entries
        if asc then
            -- ASC: entries with score > cursor_score
            if remaining_needed > 0 then
                other_entries = redis.call("zrangebyscore", order_key, "(" .. cursor_score_str, "+inf", "WITHSCORES", "LIMIT", 0, remaining_needed)
            else
                other_entries = redis.call("zrangebyscore", order_key, "(" .. cursor_score_str, "+inf", "WITHSCORES")
            end
        else
            -- DESC: entries with score < cursor_score
            if remaining_needed > 0 then
                other_entries = redis.call("zrevrangebyscore", order_key, "(" .. cursor_score_str, "-inf", "WITHSCORES", "LIMIT", 0, remaining_needed)
            else
                other_entries = redis.call("zrevrangebyscore", order_key, "(" .. cursor_score_str, "-inf", "WITHSCORES")
            end
        end

        for i = 1, #other_entries, 2 do
            keys_to_fetch[#keys_to_fetch + 1] = other_entries[i]
            last_score = tonumber(other_entries[i + 1])
        end
    end
end

-- Determine if there are more entries and trim to limit
local has_more = false
local key_count = #keys_to_fetch

if key_count == 0 then
    return {stream_offset, epoch, {}, {}, "", ""}
end

if limit > 0 and key_count > limit then
    has_more = true
    -- Trim to limit
    local trimmed = {}
    for i = 1, limit do
        trimmed[i] = keys_to_fetch[i]
    end
    keys_to_fetch = trimmed
    key_count = limit
    -- Get the actual last score for cursor
    last_score = tonumber(redis.call("zscore", order_key, keys_to_fetch[key_count]))
end

-- Fetch values in one call
local values = redis.call("hmget", hash_key, unpack(keys_to_fetch))

-- Build next cursor from last entry
local next_cursor_score = ""
local next_cursor_key = ""
if has_more then
    next_cursor_score = tostring(last_score)
    next_cursor_key = keys_to_fetch[key_count]
end

return {stream_offset, epoch, keys_to_fetch, values, next_cursor_score, next_cursor_key}
