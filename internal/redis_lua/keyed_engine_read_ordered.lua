-- Read ordered keyed snapshot with key-based cursor pagination for continuity.
-- Uses (score, key) cursor instead of integer offset to ensure no entries are
-- skipped when the snapshot changes during pagination.
--
-- KEYS[1] = snapshot hash key
-- KEYS[2] = snapshot order zset key
-- KEYS[3] = snapshot expire zset key
-- KEYS[4] = meta key
-- KEYS[5] = snapshot meta key
-- ARGV[1] = limit (0 = no limit, return all)
-- ARGV[2] = cursor_score (empty string for first page, score part of cursor)
-- ARGV[3] = cursor_key (empty string for first page, key part of cursor)
-- ARGV[4] = now (current timestamp for expiration cleanup)
-- ARGV[5] = meta_ttl (seconds, 0 to disable)
-- ARGV[6] = snapshot_ttl (seconds, 0 to disable - refreshes TTL on read)

local hash_key = KEYS[1]
local order_key = KEYS[2]
local expire_key = KEYS[3]
local meta_key = KEYS[4]
local snapshot_meta_key = KEYS[5]

local limit = tonumber(ARGV[1])
local cursor_score_str = ARGV[2]
local cursor_key = ARGV[3]
local now_str = ARGV[4]
local meta_ttl = tonumber(ARGV[5])
local snapshot_ttl = tonumber(ARGV[6])

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

-- Validate snapshot epoch against stream epoch
if snapshot_meta_key ~= '' then
    local snapshot_meta_exists = redis.call("exists", snapshot_meta_key)
    if snapshot_meta_exists == 0 then
        -- No snapshot metadata = snapshot is invalid/evicted
        return {stream_offset, epoch, {}, {}, "", ""}
    end

    local snapshot_epoch = redis.call("hget", snapshot_meta_key, "epoch")
    if snapshot_epoch ~= epoch then
        -- Epoch mismatch = snapshot is stale (from old epoch)
        return {stream_offset, epoch, {}, {}, "", ""}
    end
end

-- Refresh snapshot TTL on read (LRU behavior)
if snapshot_ttl > 0 then
    redis.call("expire", hash_key, snapshot_ttl)
    redis.call("expire", order_key, snapshot_ttl)
    redis.call("expire", expire_key, snapshot_ttl)
    if snapshot_meta_key ~= '' then
        redis.call("expire", snapshot_meta_key, snapshot_ttl)
    end
end

-- Cleanup expired entries
local expired = redis.call("zrangebyscore", expire_key, "-inf", now_str)
local expired_len = #expired
if expired_len > 0 then
    redis.call("hdel", hash_key, unpack(expired))
    redis.call("zrem", order_key, unpack(expired))
    redis.call("zremrangebyscore", expire_key, "-inf", now_str)
end

-- Key-based cursor pagination:
-- For ordered snapshots sorted by (score DESC, key ASC within same score),
-- find entries "after" cursor where:
--   score < cursor_score, OR
--   (score == cursor_score AND key > cursor_key)
--
-- Redis ZSET sorts by (score ASC, key LEX ASC), so ZREVRANGE gives (score DESC, key LEX DESC).
-- Within same score, we need entries with key > cursor_key, but ZREVRANGE gives key DESC.
-- So we fetch extra entries and filter in Lua.

local result_keys = {}
local fetch_limit = limit
if limit > 0 then
    -- Fetch extra to account for filtering within same score
    fetch_limit = limit + 100
end

local all_keys
if cursor_score_str == "" then
    -- First page: start from highest score
    if fetch_limit > 0 then
        all_keys = redis.call("zrevrange", order_key, 0, fetch_limit - 1, "WITHSCORES")
    else
        all_keys = redis.call("zrevrange", order_key, 0, -1, "WITHSCORES")
    end
else
    -- Subsequent pages: get entries with score <= cursor_score
    local cursor_score = tonumber(cursor_score_str)
    if fetch_limit > 0 then
        all_keys = redis.call("zrevrangebyscore", order_key, cursor_score, "-inf", "WITHSCORES", "LIMIT", 0, fetch_limit)
    else
        all_keys = redis.call("zrevrangebyscore", order_key, cursor_score, "-inf", "WITHSCORES")
    end
end

-- Parse WITHSCORES result (key, score, key, score, ...)
-- Filter based on cursor position
local keys_to_fetch = {}
local scores_map = {}
local cursor_score = nil
if cursor_score_str ~= "" then
    cursor_score = tonumber(cursor_score_str)
end

for i = 1, #all_keys, 2 do
    local key = all_keys[i]
    local score = tonumber(all_keys[i + 1])

    -- Check if this entry is "after" the cursor
    local include = true
    if cursor_score ~= nil then
        if score > cursor_score then
            -- score > cursor_score: this entry is "before" cursor, skip
            include = false
        elseif score == cursor_score then
            -- Same score: include only if key > cursor_key (lexicographic)
            if key <= cursor_key then
                include = false
            end
        end
        -- score < cursor_score: include (this entry is "after" cursor)
    end

    if include then
        table.insert(keys_to_fetch, key)
        scores_map[key] = score

        -- Stop if we have enough
        if limit > 0 and #keys_to_fetch >= limit then
            break
        end
    end
end

local key_count = #keys_to_fetch
if key_count == 0 then
    return {stream_offset, epoch, {}, {}, "", ""}
end

-- Fetch values in one call
local values = redis.call("hmget", hash_key, unpack(keys_to_fetch))

-- Build next cursor from last entry
local next_cursor_score = ""
local next_cursor_key = ""

-- Check if there might be more entries
local has_more = false
if limit > 0 then
    local last_key = keys_to_fetch[key_count]
    local last_score = scores_map[last_key]

    -- Check if there are more entries after this one
    local remaining = redis.call("zrevrangebyscore", order_key, last_score, "-inf", "LIMIT", 0, 2, "WITHSCORES")
    -- Count entries that would come after our last entry
    local count_after = 0
    for i = 1, #remaining, 2 do
        local k = remaining[i]
        local s = tonumber(remaining[i + 1])
        if s < last_score or (s == last_score and k > last_key) then
            count_after = count_after + 1
        end
    end

    if count_after > 0 or #remaining > 2 then
        has_more = true
        next_cursor_score = tostring(last_score)
        next_cursor_key = last_key
    end
end

return {stream_offset, epoch, keys_to_fetch, values, next_cursor_score, next_cursor_key}
