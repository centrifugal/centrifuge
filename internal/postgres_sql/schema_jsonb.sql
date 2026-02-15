-- Auto-generated from schema.sql — do not edit.
-- Stream Table (Change History + Fan-out)
CREATE TABLE IF NOT EXISTS cf_map_stream (
    id              BIGSERIAL PRIMARY KEY,
    channel         TEXT NOT NULL,
    channel_offset  BIGINT NOT NULL,
    epoch           TEXT NOT NULL DEFAULT '',
    key             TEXT NOT NULL,
    data            JSONB,
    tags            JSONB,
    client_id       TEXT,
    user_id         TEXT,
    conn_info       JSONB,
    chan_info        JSONB,
    subscribed_at   TIMESTAMPTZ,
    removed         BOOLEAN DEFAULT FALSE,
    score           BIGINT,
    previous_data   JSONB,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    shard_id        SMALLINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS cf_map_stream_channel_offset_idx ON cf_map_stream (channel, channel_offset);
CREATE INDEX IF NOT EXISTS cf_map_stream_channel_id_idx ON cf_map_stream (channel, id DESC);
CREATE INDEX IF NOT EXISTS cf_map_stream_expires_idx ON cf_map_stream (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS cf_map_stream_shard_cursor_idx ON cf_map_stream (shard_id, id);

-- Note: REPLICA IDENTITY FULL is NOT needed. Publications use
-- WITH (publish = 'insert') so DELETE/UPDATE old-row logging is skipped.

-- ============================================================================
-- Outbox Cursor Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS cf_map_outbox_cursor (
    shard_id          INTEGER PRIMARY KEY,
    last_processed_id BIGINT NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Snapshot Table (Current State)
-- ============================================================================

CREATE TABLE IF NOT EXISTS cf_map_state (
    channel             TEXT NOT NULL,
    key                 TEXT NOT NULL,
    data                JSONB,
    tags                JSONB,
    client_id           TEXT,
    user_id             TEXT,
    conn_info           JSONB,
    chan_info            JSONB,
    subscribed_at       TIMESTAMPTZ,
    score               BIGINT,
    key_version         BIGINT DEFAULT 0,
    key_version_epoch   TEXT,
    key_offset          BIGINT NOT NULL,
    expires_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (channel, key)
);

CREATE INDEX IF NOT EXISTS cf_map_state_ordered_idx
    ON cf_map_state (channel, score DESC, key)
    WHERE score IS NOT NULL;
CREATE INDEX IF NOT EXISTS cf_map_state_expires_idx
    ON cf_map_state (expires_at)
    WHERE expires_at IS NOT NULL;

-- ============================================================================
-- Stream Metadata Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS cf_map_meta (
    channel         TEXT PRIMARY KEY,
    top_offset      BIGINT NOT NULL DEFAULT 0,
    epoch           TEXT NOT NULL DEFAULT '',
    version         BIGINT DEFAULT 0,
    version_epoch   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    expires_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS cf_map_meta_expires_idx
    ON cf_map_meta (expires_at)
    WHERE expires_at IS NOT NULL;

-- ============================================================================
-- Idempotency Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS cf_map_idempotency (
    channel         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    result_offset   BIGINT NOT NULL,
    result_id       BIGINT NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (channel, idempotency_key)
);

CREATE INDEX IF NOT EXISTS cf_map_idempotency_expires_idx ON cf_map_idempotency (expires_at);

-- ============================================================================
-- Functions
-- ============================================================================

CREATE OR REPLACE FUNCTION cf_map_publish(
    p_channel TEXT,
    p_key TEXT,
    p_data JSONB,
    p_tags JSONB DEFAULT NULL,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_conn_info JSONB DEFAULT NULL,
    p_chan_info JSONB DEFAULT NULL,
    p_subscribed_at TIMESTAMPTZ DEFAULT NULL,
    p_key_mode TEXT DEFAULT NULL,
    p_key_ttl INTERVAL DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_stream_size INT DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL,
    p_expected_offset BIGINT DEFAULT NULL,
    p_score BIGINT DEFAULT NULL,
    p_version BIGINT DEFAULT NULL,
    p_version_epoch TEXT DEFAULT NULL,
    p_key_version BIGINT DEFAULT NULL,
    p_key_version_epoch TEXT DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_refresh_ttl_on_suppress BOOLEAN DEFAULT FALSE,
    p_use_delta BOOLEAN DEFAULT FALSE,
    p_num_shards INTEGER DEFAULT 16,
    p_stream_data JSONB DEFAULT NULL
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT,
    suppressed BOOLEAN,
    suppress_reason TEXT,
    current_data JSONB,
    current_offset BIGINT
) AS $$
DECLARE
    v_offset BIGINT;
    v_id BIGINT;
    v_epoch TEXT;
    v_exists BOOLEAN;
    v_current_offset BIGINT;
    v_current_data JSONB;
    v_previous_data JSONB;
    v_current_version BIGINT;
    v_stream_version BIGINT;
    v_stream_version_epoch TEXT;
    v_shard_id INTEGER;
BEGIN
    -- Calculate shard_id from channel hash
    v_shard_id := abs(hashtext(p_channel)) % p_num_shards;

    -- 1. Get or create stream metadata
    INSERT INTO cf_map_meta (channel, top_offset, epoch, updated_at)
    VALUES (p_channel, 0, substr(md5(random()::text || random()::text), 1, 8), NOW())
    ON CONFLICT (channel) DO NOTHING;

    SELECT top_offset, m.epoch, COALESCE(version, 0), version_epoch
    INTO v_offset, v_epoch, v_stream_version, v_stream_version_epoch
    FROM cf_map_meta m WHERE m.channel = p_channel FOR UPDATE;

    -- 2. Check idempotency
    IF p_idempotency_key IS NOT NULL THEN
        SELECT result_offset INTO v_current_offset
        FROM cf_map_idempotency
        WHERE channel = p_channel AND idempotency_key = p_idempotency_key
          AND expires_at > NOW();
        IF FOUND THEN
            RETURN QUERY SELECT NULL::BIGINT, v_current_offset, v_epoch, TRUE,
                'idempotency'::TEXT, NULL::JSONB, NULL::BIGINT;
            RETURN;
        END IF;
    END IF;

    -- 3. CAS check (ExpectedPosition)
    IF p_expected_offset IS NOT NULL THEN
        SELECT key_offset, sn.data INTO v_current_offset, v_current_data
        FROM cf_map_state sn WHERE sn.channel = p_channel AND sn.key = p_key;
        IF NOT FOUND OR v_current_offset != p_expected_offset THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE,
                'position_mismatch'::TEXT, v_current_data, v_current_offset;
            RETURN;
        END IF;
    END IF;

    -- 4. KeyMode check
    IF p_key_mode IS NOT NULL THEN
        SELECT EXISTS(SELECT 1 FROM cf_map_state WHERE channel = p_channel AND key = p_key) INTO v_exists;
        IF p_key_mode = 'if_new' AND v_exists THEN
            IF p_refresh_ttl_on_suppress AND p_key_ttl IS NOT NULL THEN
                UPDATE cf_map_state SET expires_at = NOW() + p_key_ttl, updated_at = NOW()
                WHERE channel = p_channel AND key = p_key;
            END IF;
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'key_exists'::TEXT, NULL::JSONB, NULL::BIGINT;
            RETURN;
        END IF;
        IF p_key_mode = 'if_exists' AND NOT v_exists THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'key_not_found'::TEXT, NULL::JSONB, NULL::BIGINT;
            RETURN;
        END IF;
    END IF;

    -- 5. Stream-level version check
    IF p_version IS NOT NULL THEN
        IF (p_version_epoch IS NULL OR p_version_epoch = v_stream_version_epoch)
           AND p_version <= v_stream_version THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'version'::TEXT, NULL::JSONB, NULL::BIGINT;
            RETURN;
        END IF;
    END IF;

    -- 6. All checks passed - increment offset and update version
    UPDATE cf_map_meta SET
        top_offset = top_offset + 1,
        version = GREATEST(COALESCE(version, 0), COALESCE(p_version, 0)),
        version_epoch = COALESCE(p_version_epoch, version_epoch),
        expires_at = COALESCE(CASE WHEN p_meta_ttl IS NOT NULL THEN NOW() + p_meta_ttl ELSE NULL END, expires_at),
        updated_at = NOW()
    WHERE channel = p_channel
    RETURNING top_offset INTO v_offset;

    -- 6b. Fetch previous data for key-based delta (before UPSERT overwrites it)
    IF p_use_delta AND p_key IS NOT NULL AND p_key != '' THEN
        SELECT sn.data INTO v_previous_data
        FROM cf_map_state sn WHERE sn.channel = p_channel AND sn.key = p_key;
    END IF;

    -- 7. Update snapshot
    INSERT INTO cf_map_state (
        channel, key, data, tags, client_id, user_id, conn_info, chan_info, subscribed_at,
        score, key_version, key_version_epoch, key_offset, expires_at, updated_at
    ) VALUES (
        p_channel, p_key, p_data, p_tags, p_client_id, p_user_id, p_conn_info, p_chan_info, p_subscribed_at,
        p_score, p_key_version, p_key_version_epoch, v_offset,
        CASE WHEN p_key_ttl IS NOT NULL THEN NOW() + p_key_ttl ELSE NULL END, NOW()
    )
    ON CONFLICT (channel, key) DO UPDATE SET
        data = EXCLUDED.data, tags = EXCLUDED.tags,
        client_id = EXCLUDED.client_id, user_id = EXCLUDED.user_id,
        conn_info = EXCLUDED.conn_info, chan_info = EXCLUDED.chan_info, subscribed_at = EXCLUDED.subscribed_at,
        score = EXCLUDED.score, key_version = EXCLUDED.key_version, key_version_epoch = EXCLUDED.key_version_epoch,
        key_offset = EXCLUDED.key_offset, expires_at = EXCLUDED.expires_at, updated_at = NOW();

    -- 8. Insert into stream (include epoch, shard_id, and previous_data for delta)
    INSERT INTO cf_map_stream (
        channel, channel_offset, epoch, key, data, tags, client_id, user_id, conn_info, chan_info, subscribed_at, score, previous_data, expires_at, shard_id
    ) VALUES (
        p_channel, v_offset, v_epoch, p_key, COALESCE(p_stream_data, p_data), p_tags, p_client_id, p_user_id, p_conn_info, p_chan_info, p_subscribed_at, p_score, v_previous_data,
        CASE WHEN p_stream_ttl IS NOT NULL THEN NOW() + p_stream_ttl ELSE NULL END,
        v_shard_id
    ) RETURNING cf_map_stream.id INTO v_id;

    -- 9. Trim stream if needed (cursor-aware: never trim undelivered rows).
    -- Uses channel_offset arithmetic instead of OFFSET scan: O(1) vs O(stream_size).
    IF p_stream_size IS NOT NULL AND p_stream_size > 0 AND v_offset > p_stream_size THEN
        DELETE FROM cf_map_stream
        WHERE cf_map_stream.channel = p_channel
          AND cf_map_stream.channel_offset <= v_offset - p_stream_size
          AND (
              NOT EXISTS (SELECT 1 FROM cf_map_outbox_cursor)
              OR cf_map_stream.id <= (
                  SELECT c.last_processed_id FROM cf_map_outbox_cursor c
                  WHERE c.shard_id = cf_map_stream.shard_id
              )
          );
    END IF;

    -- 10. Save idempotency key
    IF p_idempotency_key IS NOT NULL THEN
        INSERT INTO cf_map_idempotency (channel, idempotency_key, result_offset, result_id, expires_at)
        VALUES (p_channel, p_idempotency_key, v_offset, v_id, NOW() + COALESCE(p_idempotency_ttl, INTERVAL '5 minutes'))
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN QUERY SELECT v_id, v_offset, v_epoch, FALSE, NULL::TEXT, NULL::JSONB, NULL::BIGINT;
END;
$$ LANGUAGE plpgsql;

-- cf_map_publish_strict: Auto-rollback on suppression
CREATE OR REPLACE FUNCTION cf_map_publish_strict(
    p_channel TEXT,
    p_key TEXT,
    p_data JSONB,
    p_tags JSONB DEFAULT NULL,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_conn_info JSONB DEFAULT NULL,
    p_chan_info JSONB DEFAULT NULL,
    p_subscribed_at TIMESTAMPTZ DEFAULT NULL,
    p_key_mode TEXT DEFAULT NULL,
    p_key_ttl INTERVAL DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_stream_size INT DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL,
    p_expected_offset BIGINT DEFAULT NULL,
    p_score BIGINT DEFAULT NULL,
    p_version BIGINT DEFAULT NULL,
    p_version_epoch TEXT DEFAULT NULL,
    p_key_version BIGINT DEFAULT NULL,
    p_key_version_epoch TEXT DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_refresh_ttl_on_suppress BOOLEAN DEFAULT FALSE,
    p_use_delta BOOLEAN DEFAULT FALSE,
    p_num_shards INTEGER DEFAULT 16,
    p_stream_data JSONB DEFAULT NULL
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT
) AS $$
DECLARE
    v_result RECORD;
BEGIN
    SELECT * INTO v_result
    FROM cf_map_publish(
        p_channel, p_key, p_data, p_tags,
        p_client_id, p_user_id, p_conn_info, p_chan_info, p_subscribed_at,
        p_key_mode, p_key_ttl, p_stream_ttl, p_stream_size, p_meta_ttl,
        p_expected_offset, p_score, p_version, p_version_epoch,
        p_key_version, p_key_version_epoch,
        p_idempotency_key, p_idempotency_ttl, p_refresh_ttl_on_suppress,
        p_use_delta, p_num_shards, p_stream_data
    );

    IF v_result.suppressed THEN
        CASE v_result.suppress_reason
            WHEN 'key_exists' THEN
                RAISE EXCEPTION 'cf_map_publish: key already exists: %.%', p_channel, p_key
                    USING ERRCODE = 'unique_violation';
            WHEN 'key_not_found' THEN
                RAISE EXCEPTION 'cf_map_publish: key not found: %.%', p_channel, p_key
                    USING ERRCODE = 'no_data_found';
            WHEN 'position_mismatch' THEN
                RAISE EXCEPTION 'cf_map_publish: CAS conflict on %.%', p_channel, p_key
                    USING ERRCODE = 'serialization_failure',
                          DETAIL = v_result.current_data::TEXT;
            WHEN 'version' THEN
                RAISE EXCEPTION 'cf_map_publish: version conflict on %.%', p_channel, p_key
                    USING ERRCODE = 'serialization_failure';
            WHEN 'idempotency' THEN
                RAISE EXCEPTION 'cf_map_publish: duplicate idempotency key: %.%', p_channel, p_key
                    USING ERRCODE = 'unique_violation';
            ELSE
                RAISE EXCEPTION 'cf_map_publish: suppressed: %', v_result.suppress_reason
                    USING ERRCODE = 'raise_exception';
        END CASE;
    END IF;

    RETURN QUERY SELECT v_result.result_id, v_result.channel_offset, v_result.epoch;
END;
$$ LANGUAGE plpgsql;

-- cf_map_remove: Remove a key
CREATE OR REPLACE FUNCTION cf_map_remove(
    p_channel TEXT,
    p_key TEXT,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL,
    p_num_shards INTEGER DEFAULT 16,
    p_expected_offset BIGINT DEFAULT NULL
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT,
    suppressed BOOLEAN,
    suppress_reason TEXT,
    current_data JSONB,
    current_offset BIGINT
) AS $$
DECLARE
    v_offset BIGINT;
    v_id BIGINT;
    v_epoch TEXT;
    v_exists BOOLEAN;
    v_shard_id INTEGER;
    v_current_offset BIGINT;
    v_current_data JSONB;
BEGIN
    -- Calculate shard_id from channel hash
    v_shard_id := abs(hashtext(p_channel)) % p_num_shards;

    -- 1. Get stream metadata
    SELECT top_offset, m.epoch INTO v_offset, v_epoch
    FROM cf_map_meta m WHERE m.channel = p_channel;

    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::BIGINT, 0::BIGINT, ''::TEXT, TRUE, 'key_not_found'::TEXT, NULL::JSONB, NULL::BIGINT;
        RETURN;
    END IF;

    -- 2. Check idempotency
    IF p_idempotency_key IS NOT NULL THEN
        SELECT result_offset INTO v_offset
        FROM cf_map_idempotency
        WHERE channel = p_channel AND idempotency_key = p_idempotency_key
          AND expires_at > NOW();
        IF FOUND THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'idempotency'::TEXT, NULL::JSONB, NULL::BIGINT;
            RETURN;
        END IF;
    END IF;

    -- 3. CAS check (ExpectedPosition)
    IF p_expected_offset IS NOT NULL THEN
        SELECT key_offset, sn.data INTO v_current_offset, v_current_data
        FROM cf_map_state sn WHERE sn.channel = p_channel AND sn.key = p_key;
        IF NOT FOUND OR v_current_offset != p_expected_offset THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE,
                'position_mismatch'::TEXT, v_current_data, v_current_offset;
            RETURN;
        END IF;
    END IF;

    -- 4. Check if key exists
    SELECT EXISTS(SELECT 1 FROM cf_map_state WHERE channel = p_channel AND key = p_key) INTO v_exists;
    IF NOT v_exists THEN
        RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'key_not_found'::TEXT, NULL::JSONB, NULL::BIGINT;
        RETURN;
    END IF;

    -- 5. Increment offset
    UPDATE cf_map_meta SET
        top_offset = top_offset + 1,
        expires_at = COALESCE(CASE WHEN p_meta_ttl IS NOT NULL THEN NOW() + p_meta_ttl ELSE NULL END, expires_at),
        updated_at = NOW()
    WHERE channel = p_channel
    RETURNING top_offset INTO v_offset;

    -- 6. Delete from snapshot
    DELETE FROM cf_map_state WHERE channel = p_channel AND key = p_key;

    -- 7. Insert removal into stream (include epoch and shard_id)
    INSERT INTO cf_map_stream (channel, channel_offset, epoch, key, removed, client_id, user_id, expires_at, shard_id)
    VALUES (
        p_channel, v_offset, v_epoch, p_key, TRUE, p_client_id, p_user_id,
        CASE WHEN p_stream_ttl IS NOT NULL THEN NOW() + p_stream_ttl ELSE NULL END,
        v_shard_id
    ) RETURNING cf_map_stream.id INTO v_id;

    -- 8. Save idempotency key
    IF p_idempotency_key IS NOT NULL THEN
        INSERT INTO cf_map_idempotency (channel, idempotency_key, result_offset, result_id, expires_at)
        VALUES (p_channel, p_idempotency_key, v_offset, v_id, NOW() + COALESCE(p_idempotency_ttl, INTERVAL '5 minutes'))
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN QUERY SELECT v_id, v_offset, v_epoch, FALSE, NULL::TEXT, NULL::JSONB, NULL::BIGINT;
END;
$$ LANGUAGE plpgsql;

-- cf_map_remove_strict: Auto-rollback if key not found
CREATE OR REPLACE FUNCTION cf_map_remove_strict(
    p_channel TEXT,
    p_key TEXT,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL,
    p_num_shards INTEGER DEFAULT 16,
    p_expected_offset BIGINT DEFAULT NULL
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT
) AS $$
DECLARE
    v_result RECORD;
BEGIN
    SELECT * INTO v_result FROM cf_map_remove(
        p_channel, p_key, p_client_id, p_user_id,
        p_stream_ttl, p_idempotency_key, p_idempotency_ttl, p_meta_ttl,
        p_num_shards, p_expected_offset
    );

    IF v_result.suppressed THEN
        RAISE EXCEPTION 'cf_map_remove: key not found: %.%', p_channel, p_key
            USING ERRCODE = 'no_data_found';
    END IF;

    RETURN QUERY SELECT v_result.result_id, v_result.channel_offset, v_result.epoch;
END;
$$ LANGUAGE plpgsql;

-- cf_map_expire_keys: Atomically expire keys that have passed their TTL.
-- Lock ordering: meta FOR UPDATE first, then state FOR UPDATE SKIP LOCKED.
-- This matches publish's lock order (meta → state), preventing deadlocks.
CREATE OR REPLACE FUNCTION cf_map_expire_keys(
    p_batch_size INT DEFAULT 1000,
    p_num_shards INTEGER DEFAULT 16,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL,
    p_channel TEXT DEFAULT NULL
) RETURNS TABLE(
    out_channel TEXT,
    out_key TEXT,
    out_offset BIGINT,
    out_epoch TEXT
) AS $$
DECLARE
    v_channel TEXT;
    rec RECORD;
    v_offset BIGINT;
    v_epoch TEXT;
    v_shard_id INTEGER;
    v_processed INT := 0;
BEGIN
    -- Process one channel at a time to maintain meta→state lock ordering.
    FOR v_channel IN
        SELECT DISTINCT s.channel FROM cf_map_state s
        WHERE s.expires_at IS NOT NULL AND s.expires_at <= NOW()
          AND (p_channel IS NULL OR s.channel = p_channel)
    LOOP
        -- 1. Lock meta FIRST (same order as publish: meta → state).
        SELECT top_offset, m.epoch INTO v_offset, v_epoch
        FROM cf_map_meta m WHERE m.channel = v_channel FOR UPDATE;

        IF NOT FOUND THEN
            DELETE FROM cf_map_state WHERE channel = v_channel
              AND expires_at IS NOT NULL AND expires_at <= NOW();
            CONTINUE;
        END IF;

        v_shard_id := abs(hashtext(v_channel)) % p_num_shards;

        -- 2. Now lock and process expired state rows (meta already locked).
        FOR rec IN
            SELECT key FROM cf_map_state
            WHERE channel = v_channel
              AND expires_at IS NOT NULL AND expires_at <= NOW()
            LIMIT p_batch_size - v_processed
            FOR UPDATE SKIP LOCKED
        LOOP
            -- Re-check expiration after lock (key may have been refreshed).
            PERFORM 1 FROM cf_map_state
            WHERE channel = v_channel AND key = rec.key
              AND expires_at IS NOT NULL AND expires_at <= NOW();
            IF NOT FOUND THEN
                CONTINUE;
            END IF;

            -- Increment offset.
            UPDATE cf_map_meta SET
                top_offset = top_offset + 1,
                expires_at = COALESCE(CASE WHEN p_meta_ttl IS NOT NULL THEN NOW() + p_meta_ttl ELSE NULL END, expires_at),
                updated_at = NOW()
            WHERE channel = v_channel
            RETURNING top_offset INTO v_offset;

            -- Delete from state.
            DELETE FROM cf_map_state WHERE channel = v_channel AND key = rec.key;

            -- Insert removal into stream.
            INSERT INTO cf_map_stream (channel, channel_offset, epoch, key, removed, expires_at, shard_id)
            VALUES (
                v_channel, v_offset, v_epoch, rec.key, TRUE,
                CASE WHEN p_stream_ttl IS NOT NULL THEN NOW() + p_stream_ttl ELSE NULL END,
                v_shard_id
            );

            out_channel := v_channel;
            out_key := rec.key;
            out_offset := v_offset;
            out_epoch := v_epoch;
            RETURN NEXT;

            v_processed := v_processed + 1;
            IF v_processed >= p_batch_size THEN
                RETURN;
            END IF;
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
