-- ============================================================================
-- PostgreSQL KeyedEngine Schema and Functions
-- ============================================================================

-- Stream Table (Change History + Fan-out)
CREATE TABLE cf_keyed_stream (
    id              BIGSERIAL PRIMARY KEY,
    channel         TEXT NOT NULL,
    channel_offset  BIGINT NOT NULL,
    key             TEXT NOT NULL,
    data            BYTEA,
    tags            JSONB,
    client_id       TEXT,
    user_id         TEXT,
    conn_info       BYTEA,
    chan_info       BYTEA,
    subscribed_at   TIMESTAMPTZ,
    removed         BOOLEAN DEFAULT FALSE,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX cf_keyed_stream_channel_offset_idx ON cf_keyed_stream (channel, channel_offset);
CREATE INDEX cf_keyed_stream_expires_idx ON cf_keyed_stream (expires_at) WHERE expires_at IS NOT NULL;

-- Snapshot Table (Current State)
CREATE TABLE cf_keyed_snapshot (
    channel             TEXT NOT NULL,
    key                 TEXT NOT NULL,
    data                BYTEA,
    tags                JSONB,
    client_id           TEXT,
    user_id             TEXT,
    conn_info           BYTEA,
    chan_info           BYTEA,
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

CREATE INDEX cf_keyed_snapshot_ordered_idx
    ON cf_keyed_snapshot (channel, score DESC, key)
    WHERE score IS NOT NULL;
CREATE INDEX cf_keyed_snapshot_expires_idx
    ON cf_keyed_snapshot (expires_at)
    WHERE expires_at IS NOT NULL;

-- Stream Metadata Table
CREATE TABLE cf_keyed_stream_meta (
    channel         TEXT PRIMARY KEY,
    top_offset      BIGINT NOT NULL DEFAULT 0,
    epoch           TEXT NOT NULL DEFAULT '',
    version         BIGINT DEFAULT 0,
    version_epoch   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    expires_at      TIMESTAMPTZ
);

CREATE INDEX cf_keyed_stream_meta_expires_idx
    ON cf_keyed_stream_meta (expires_at)
    WHERE expires_at IS NOT NULL;

-- Idempotency Table
CREATE TABLE cf_keyed_idempotency (
    channel         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    result_offset   BIGINT NOT NULL,
    result_id       BIGINT NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (channel, idempotency_key)
);

CREATE INDEX cf_keyed_idempotency_expires_idx ON cf_keyed_idempotency (expires_at);

-- ============================================================================
-- Functions
-- ============================================================================

-- cf_keyed_publish: Main publishing function
CREATE OR REPLACE FUNCTION cf_keyed_publish(
    p_channel TEXT,
    p_key TEXT,
    p_data BYTEA,
    p_tags JSONB DEFAULT NULL,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_conn_info BYTEA DEFAULT NULL,
    p_chan_info BYTEA DEFAULT NULL,
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
    p_refresh_ttl_on_suppress BOOLEAN DEFAULT FALSE
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT,
    suppressed BOOLEAN,
    suppress_reason TEXT,
    current_data BYTEA,
    current_offset BIGINT
) AS $$
DECLARE
    v_offset BIGINT;
    v_id BIGINT;
    v_epoch TEXT;
    v_exists BOOLEAN;
    v_current_offset BIGINT;
    v_current_data BYTEA;
    v_current_version BIGINT;
    v_stream_version BIGINT;
    v_stream_version_epoch TEXT;
BEGIN
    -- 1. Get or create stream metadata
    INSERT INTO cf_keyed_stream_meta (channel, top_offset, epoch, updated_at)
    VALUES (p_channel, 0, substr(md5(random()::text), 1, 4), NOW())
    ON CONFLICT (channel) DO NOTHING;

    SELECT top_offset, m.epoch, COALESCE(version, 0), version_epoch
    INTO v_offset, v_epoch, v_stream_version, v_stream_version_epoch
    FROM cf_keyed_stream_meta m WHERE m.channel = p_channel FOR UPDATE;

    -- 2. Check idempotency
    IF p_idempotency_key IS NOT NULL THEN
        SELECT result_offset INTO v_current_offset
        FROM cf_keyed_idempotency
        WHERE channel = p_channel AND idempotency_key = p_idempotency_key
          AND expires_at > NOW();
        IF FOUND THEN
            RETURN QUERY SELECT NULL::BIGINT, v_current_offset, v_epoch, TRUE,
                'idempotency'::TEXT, NULL::BYTEA, NULL::BIGINT;
            RETURN;
        END IF;
    END IF;

    -- 3. CAS check (ExpectedPosition)
    IF p_expected_offset IS NOT NULL THEN
        SELECT key_offset, sn.data INTO v_current_offset, v_current_data
        FROM cf_keyed_snapshot sn WHERE sn.channel = p_channel AND sn.key = p_key;
        IF NOT FOUND OR v_current_offset != p_expected_offset THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE,
                'position_mismatch'::TEXT, v_current_data, v_current_offset;
            RETURN;
        END IF;
    END IF;

    -- 4. KeyMode check
    IF p_key_mode IS NOT NULL THEN
        SELECT EXISTS(SELECT 1 FROM cf_keyed_snapshot WHERE channel = p_channel AND key = p_key) INTO v_exists;
        IF p_key_mode = 'if_new' AND v_exists THEN
            IF p_refresh_ttl_on_suppress AND p_key_ttl IS NOT NULL THEN
                UPDATE cf_keyed_snapshot SET expires_at = NOW() + p_key_ttl, updated_at = NOW()
                WHERE channel = p_channel AND key = p_key;
            END IF;
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'key_exists'::TEXT, NULL::BYTEA, NULL::BIGINT;
            RETURN;
        END IF;
        IF p_key_mode = 'if_exists' AND NOT v_exists THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'key_not_found'::TEXT, NULL::BYTEA, NULL::BIGINT;
            RETURN;
        END IF;
    END IF;

    -- 5. Stream-level version check
    IF p_version IS NOT NULL THEN
        IF (p_version_epoch IS NULL OR p_version_epoch = v_stream_version_epoch)
           AND p_version <= v_stream_version THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'version'::TEXT, NULL::BYTEA, NULL::BIGINT;
            RETURN;
        END IF;
    END IF;

    -- 6. All checks passed - increment offset and update version
    UPDATE cf_keyed_stream_meta SET
        top_offset = top_offset + 1,
        version = GREATEST(COALESCE(version, 0), COALESCE(p_version, 0)),
        version_epoch = COALESCE(p_version_epoch, version_epoch),
        expires_at = COALESCE(CASE WHEN p_meta_ttl IS NOT NULL THEN NOW() + p_meta_ttl ELSE NULL END, expires_at),
        updated_at = NOW()
    WHERE channel = p_channel
    RETURNING top_offset INTO v_offset;

    -- 7. Update snapshot
    INSERT INTO cf_keyed_snapshot (
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

    -- 8. Insert into stream
    INSERT INTO cf_keyed_stream (
        channel, channel_offset, key, data, tags, client_id, user_id, conn_info, chan_info, subscribed_at, expires_at
    ) VALUES (
        p_channel, v_offset, p_key, p_data, p_tags, p_client_id, p_user_id, p_conn_info, p_chan_info, p_subscribed_at,
        CASE WHEN p_stream_ttl IS NOT NULL THEN NOW() + p_stream_ttl ELSE NULL END
    ) RETURNING cf_keyed_stream.id INTO v_id;

    -- 9. Trim stream if needed
    IF p_stream_size IS NOT NULL AND p_stream_size > 0 THEN
        DELETE FROM cf_keyed_stream
        WHERE cf_keyed_stream.channel = p_channel AND cf_keyed_stream.id <= (
            SELECT s.id FROM cf_keyed_stream s WHERE s.channel = p_channel ORDER BY s.id DESC OFFSET p_stream_size LIMIT 1
        );
    END IF;

    -- 10. Save idempotency key
    IF p_idempotency_key IS NOT NULL THEN
        INSERT INTO cf_keyed_idempotency (channel, idempotency_key, result_offset, result_id, expires_at)
        VALUES (p_channel, p_idempotency_key, v_offset, v_id, NOW() + COALESCE(p_idempotency_ttl, INTERVAL '5 minutes'))
        ON CONFLICT DO NOTHING;
    END IF;

    -- 11. Notify
    PERFORM pg_notify('cf_keyed_stream_changes', '');

    RETURN QUERY SELECT v_id, v_offset, v_epoch, FALSE, NULL::TEXT, NULL::BYTEA, NULL::BIGINT;
END;
$$ LANGUAGE plpgsql;

-- cf_keyed_publish_strict: Auto-rollback on suppression
CREATE OR REPLACE FUNCTION cf_keyed_publish_strict(
    p_channel TEXT,
    p_key TEXT,
    p_data BYTEA,
    p_tags JSONB DEFAULT NULL,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_conn_info BYTEA DEFAULT NULL,
    p_chan_info BYTEA DEFAULT NULL,
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
    p_refresh_ttl_on_suppress BOOLEAN DEFAULT FALSE
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT
) AS $$
DECLARE
    v_result RECORD;
BEGIN
    SELECT * INTO v_result
    FROM cf_keyed_publish(
        p_channel, p_key, p_data, p_tags,
        p_client_id, p_user_id, p_conn_info, p_chan_info, p_subscribed_at,
        p_key_mode, p_key_ttl, p_stream_ttl, p_stream_size, p_meta_ttl,
        p_expected_offset, p_score, p_version, p_version_epoch,
        p_key_version, p_key_version_epoch,
        p_idempotency_key, p_idempotency_ttl, p_refresh_ttl_on_suppress
    );

    IF v_result.suppressed THEN
        CASE v_result.suppress_reason
            WHEN 'key_exists' THEN
                RAISE EXCEPTION 'cf_keyed_publish: key already exists: %.%', p_channel, p_key
                    USING ERRCODE = 'unique_violation';
            WHEN 'key_not_found' THEN
                RAISE EXCEPTION 'cf_keyed_publish: key not found: %.%', p_channel, p_key
                    USING ERRCODE = 'no_data_found';
            WHEN 'position_mismatch' THEN
                RAISE EXCEPTION 'cf_keyed_publish: CAS conflict on %.%', p_channel, p_key
                    USING ERRCODE = 'serialization_failure',
                          DETAIL = encode(v_result.current_data, 'escape');
            WHEN 'version' THEN
                RAISE EXCEPTION 'cf_keyed_publish: version conflict on %.%', p_channel, p_key
                    USING ERRCODE = 'serialization_failure';
            WHEN 'idempotency' THEN
                RAISE EXCEPTION 'cf_keyed_publish: duplicate idempotency key: %.%', p_channel, p_key
                    USING ERRCODE = 'unique_violation';
            ELSE
                RAISE EXCEPTION 'cf_keyed_publish: suppressed: %', v_result.suppress_reason
                    USING ERRCODE = 'raise_exception';
        END CASE;
    END IF;

    RETURN QUERY SELECT v_result.result_id, v_result.channel_offset, v_result.epoch;
END;
$$ LANGUAGE plpgsql;

-- cf_keyed_unpublish: Remove a key
CREATE OR REPLACE FUNCTION cf_keyed_unpublish(
    p_channel TEXT,
    p_key TEXT,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT,
    suppressed BOOLEAN,
    suppress_reason TEXT
) AS $$
DECLARE
    v_offset BIGINT;
    v_id BIGINT;
    v_epoch TEXT;
    v_exists BOOLEAN;
BEGIN
    -- 1. Get stream metadata
    SELECT top_offset, m.epoch INTO v_offset, v_epoch
    FROM cf_keyed_stream_meta m WHERE m.channel = p_channel;

    IF NOT FOUND THEN
        RETURN QUERY SELECT NULL::BIGINT, 0::BIGINT, ''::TEXT, TRUE, 'key_not_found'::TEXT;
        RETURN;
    END IF;

    -- 2. Check idempotency
    IF p_idempotency_key IS NOT NULL THEN
        SELECT result_offset INTO v_offset
        FROM cf_keyed_idempotency
        WHERE channel = p_channel AND idempotency_key = p_idempotency_key
          AND expires_at > NOW();
        IF FOUND THEN
            RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'idempotency'::TEXT;
            RETURN;
        END IF;
    END IF;

    -- 3. Check if key exists
    SELECT EXISTS(SELECT 1 FROM cf_keyed_snapshot WHERE channel = p_channel AND key = p_key) INTO v_exists;
    IF NOT v_exists THEN
        RETURN QUERY SELECT NULL::BIGINT, v_offset, v_epoch, TRUE, 'key_not_found'::TEXT;
        RETURN;
    END IF;

    -- 4. Increment offset
    UPDATE cf_keyed_stream_meta SET
        top_offset = top_offset + 1,
        expires_at = COALESCE(CASE WHEN p_meta_ttl IS NOT NULL THEN NOW() + p_meta_ttl ELSE NULL END, expires_at),
        updated_at = NOW()
    WHERE channel = p_channel
    RETURNING top_offset INTO v_offset;

    -- 5. Delete from snapshot
    DELETE FROM cf_keyed_snapshot WHERE channel = p_channel AND key = p_key;

    -- 6. Insert removal into stream
    INSERT INTO cf_keyed_stream (channel, channel_offset, key, removed, client_id, user_id, expires_at)
    VALUES (
        p_channel, v_offset, p_key, TRUE, p_client_id, p_user_id,
        CASE WHEN p_stream_ttl IS NOT NULL THEN NOW() + p_stream_ttl ELSE NULL END
    ) RETURNING cf_keyed_stream.id INTO v_id;

    -- 7. Save idempotency key
    IF p_idempotency_key IS NOT NULL THEN
        INSERT INTO cf_keyed_idempotency (channel, idempotency_key, result_offset, result_id, expires_at)
        VALUES (p_channel, p_idempotency_key, v_offset, v_id, NOW() + COALESCE(p_idempotency_ttl, INTERVAL '5 minutes'))
        ON CONFLICT DO NOTHING;
    END IF;

    -- 8. Notify
    PERFORM pg_notify('cf_keyed_stream_changes', '');

    RETURN QUERY SELECT v_id, v_offset, v_epoch, FALSE, NULL::TEXT;
END;
$$ LANGUAGE plpgsql;

-- cf_keyed_unpublish_strict: Auto-rollback if key not found
CREATE OR REPLACE FUNCTION cf_keyed_unpublish_strict(
    p_channel TEXT,
    p_key TEXT,
    p_client_id TEXT DEFAULT NULL,
    p_user_id TEXT DEFAULT NULL,
    p_stream_ttl INTERVAL DEFAULT NULL,
    p_idempotency_key TEXT DEFAULT NULL,
    p_idempotency_ttl INTERVAL DEFAULT NULL,
    p_meta_ttl INTERVAL DEFAULT NULL
) RETURNS TABLE(
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT
) AS $$
DECLARE
    v_result RECORD;
BEGIN
    SELECT * INTO v_result FROM cf_keyed_unpublish(
        p_channel, p_key, p_client_id, p_user_id,
        p_stream_ttl, p_idempotency_key, p_idempotency_ttl, p_meta_ttl
    );

    IF v_result.suppressed THEN
        RAISE EXCEPTION 'cf_keyed_unpublish: key not found: %.%', p_channel, p_key
            USING ERRCODE = 'no_data_found';
    END IF;

    RETURN QUERY SELECT v_result.result_id, v_result.channel_offset, v_result.epoch;
END;
$$ LANGUAGE plpgsql;

-- cf_keyed_publish_batch: Publish multiple keys efficiently
CREATE OR REPLACE FUNCTION cf_keyed_publish_batch(
    p_channels TEXT[],
    p_keys TEXT[],
    p_data BYTEA[],
    p_tags JSONB[] DEFAULT NULL
) RETURNS TABLE(
    idx INT,
    result_id BIGINT,
    channel_offset BIGINT,
    epoch TEXT,
    suppressed BOOLEAN,
    suppress_reason TEXT
) AS $$
DECLARE
    v_idx INT;
    v_len INT;
BEGIN
    v_len := array_length(p_channels, 1);

    FOR v_idx IN 1..v_len LOOP
        RETURN QUERY
            SELECT
                v_idx,
                p.result_id,
                p.channel_offset,
                p.epoch,
                p.suppressed,
                p.suppress_reason
            FROM cf_keyed_publish(
                p_channels[v_idx],
                p_keys[v_idx],
                p_data[v_idx],
                CASE WHEN p_tags IS NOT NULL THEN p_tags[v_idx] ELSE NULL END
            ) p;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
