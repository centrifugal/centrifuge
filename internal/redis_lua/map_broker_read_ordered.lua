-- Read ordered keyed state with key-based cursor pagination for continuity.
-- Uses (score, key) cursor instead of integer offset to ensure no entries are
-- skipped when the state changes during pagination.
--
-- Ordering: (score DESC, key DESC) - matches Redis native ZREVRANGE ordering.
-- This allows direct use of Redis commands without Lua filtering.
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
-- ARGV[5] = meta_ttl (seconds, 0 to disable)
-- ARGV[6] = state_ttl (seconds, 0 to disable - refreshes TTL on read)

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

-- Update meta epoch + TTL and get current stream offset
local epoch = redis.call("hget", meta_key, "e")
if not epoch then
    epoch = now_str
    redis.call("hset", meta_key, "e", epoch)
end

local stream_offset = redis.call("hget", meta_key, "s")
if not stream_offset then
    stream_offset = "0"
end

if meta_ttl > 0 then
    redis.call("expire", meta_key, meta_ttl)
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

-- Refresh state TTL on read (LRU behavior)
if state_ttl > 0 then
    redis.call("expire", hash_key, state_ttl)
    redis.call("expire", order_key, state_ttl)
    redis.call("expire", expire_key, state_ttl)
    if state_meta_key ~= '' then
        redis.call("expire", state_meta_key, state_ttl)
    end
end

-- Cleanup expired entries
local expired = redis.call("zrangebyscore", expire_key, "-inf", now_str)
if #expired > 0 then
    redis.call("hdel", hash_key, unpack(expired))
    redis.call("zrem", order_key, unpack(expired))
    redis.call("zremrangebyscore", expire_key, "-inf", now_str)
end

-- Key-based cursor pagination using native Redis ordering (score DESC, key DESC).
-- For cursor (cursor_score, cursor_key), we need entries "after" it:
--   score < cursor_score, OR (score == cursor_score AND key < cursor_key)
--
-- Strategy:
-- 1. Get entries with score < cursor_score (use exclusive bound)
-- 2. Get entries with score == cursor_score AND key < cursor_key (filter in Lua, but only same-score bucket)
-- This minimizes Lua filtering to just the cursor's score bucket.

local fetch_limit = limit
if limit > 0 then
    fetch_limit = limit + 1  -- Fetch one extra to detect if there's more
end

local keys_to_fetch = {}
local last_score = nil

if cursor_score_str == "" then
    -- First page: simple ZREVRANGE
    local all_keys
    if fetch_limit > 0 then
        all_keys = redis.call("zrevrange", order_key, 0, fetch_limit - 1, "WITHSCORES")
    else
        all_keys = redis.call("zrevrange", order_key, 0, -1, "WITHSCORES")
    end

    -- Parse result (key, score, key, score, ...)
    for i = 1, #all_keys, 2 do
        keys_to_fetch[#keys_to_fetch + 1] = all_keys[i]
        last_score = tonumber(all_keys[i + 1])
    end
else
    -- Subsequent page: need entries after cursor (score DESC, key DESC)
    local cursor_score = tonumber(cursor_score_str)

    -- Step 1: Get entries from cursor's score bucket with key < cursor_key
    -- ZREVRANGEBYLEX only works when all scores are same, so we use ZRANGEBYLEX on same-score subset
    -- Actually, we need to filter manually for same-score entries

    -- First, get entries with score == cursor_score
    local same_score = redis.call("zrangebyscore", order_key, cursor_score, cursor_score)

    -- Filter: keep entries with key < cursor_key (for DESC order, smaller keys come later)
    for i = 1, #same_score do
        local k = same_score[i]
        if k < cursor_key then
            keys_to_fetch[#keys_to_fetch + 1] = k
        end
    end

    -- Sort by key DESC (to match ZREVRANGE ordering within same score)
    table.sort(keys_to_fetch, function(a, b) return a > b end)

    last_score = cursor_score

    -- Step 2: Get entries with score < cursor_score
    local remaining_needed = 0
    if limit > 0 then
        remaining_needed = fetch_limit - #keys_to_fetch
    end

    if limit == 0 or remaining_needed > 0 then
        local lower_entries
        if remaining_needed > 0 then
            lower_entries = redis.call("zrevrangebyscore", order_key, "(" .. cursor_score_str, "-inf", "WITHSCORES", "LIMIT", 0, remaining_needed)
        else
            lower_entries = redis.call("zrevrangebyscore", order_key, "(" .. cursor_score_str, "-inf", "WITHSCORES")
        end

        for i = 1, #lower_entries, 2 do
            keys_to_fetch[#keys_to_fetch + 1] = lower_entries[i]
            last_score = tonumber(lower_entries[i + 1])
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
