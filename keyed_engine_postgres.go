package centrifuge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresKeyedEngine is KeyedEngine implementation using PostgreSQL for persistent
// keyed subscriptions. It provides durability, CAS operations, and transactional
// publishing from SQL.
//
// Key features:
//   - Dual ID system: global `id` for polling, per-channel `offset` for Centrifuge
//   - Direct polling with NOTIFY for wake-up (no intermediate pub/sub layer)
//   - Full ACID transactions for atomic CAS operations
//   - Optional read replica support for scaling reads
//
// Use cases: collaborative boards, document editing, inventory/booking systems,
// game lobbies with persistent state.
type PostgresKeyedEngine struct {
	node                   *Node
	conf                   PostgresKeyedEngineConfig
	pool                   *pgxpool.Pool // Primary pool for writes
	readPool               *pgxpool.Pool // Replica pool for reads (optional)
	eventHandler           BrokerEventHandler
	lastID                 int64 // Global polling cursor
	lastIDMu               sync.Mutex
	wakeup                 chan struct{} // NOTIFY wake-up channel
	closeCh                chan struct{}
	closeOnce              sync.Once
	running                atomic.Bool
	cancelCtx              context.Context
	cancelFunc             context.CancelFunc
	channelOptionsResolver ChannelOptionsResolver
}

var _ KeyedEngine = (*PostgresKeyedEngine)(nil)

// PostgresKeyedEngineConfig configures the PostgreSQL keyed engine.
type PostgresKeyedEngineConfig struct {
	// ConnString is the primary PostgreSQL connection string for writes.
	// Example: "postgres://user:pass@localhost:5432/dbname?sslmode=disable"
	ConnString string

	// ReplicaConnString is optional connection string for read replica.
	// If empty, reads go to primary. Use for scaling reads across replicas.
	ReplicaConnString string

	// PoolSize sets the maximum number of connections in the pool.
	// Default: 32
	PoolSize int

	// PollInterval is the fallback polling interval when no NOTIFY is received.
	// Default: 100ms
	PollInterval time.Duration

	// PollBatchSize is the maximum number of rows to fetch per poll.
	// Default: 1000
	PollBatchSize int

	// TTLCheckInterval is how often to check for expired keys.
	// Default: 1s
	TTLCheckInterval time.Duration

	// CleanupInterval is how often to clean up expired stream/meta/idempotency entries.
	// Default: 1m
	CleanupInterval time.Duration

	// IdempotentResultTTL is the default TTL for idempotency keys.
	// Default: 5m
	IdempotentResultTTL time.Duration
}

func (c *PostgresKeyedEngineConfig) setDefaults() {
	if c.PoolSize <= 0 {
		c.PoolSize = 32
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 100 * time.Millisecond
	}
	if c.PollBatchSize <= 0 {
		c.PollBatchSize = 1000
	}
	if c.TTLCheckInterval <= 0 {
		c.TTLCheckInterval = time.Second
	}
	if c.CleanupInterval <= 0 {
		c.CleanupInterval = time.Minute
	}
	if c.IdempotentResultTTL <= 0 {
		c.IdempotentResultTTL = 5 * time.Minute
	}
}

// NewPostgresKeyedEngine creates a new PostgreSQL keyed engine.
func NewPostgresKeyedEngine(n *Node, conf PostgresKeyedEngineConfig) (*PostgresKeyedEngine, error) {
	conf.setDefaults()

	if conf.ConnString == "" {
		return nil, errors.New("postgres keyed engine: ConnString is required")
	}

	ctx := context.Background()

	// Configure primary pool
	poolConfig, err := pgxpool.ParseConfig(conf.ConnString)
	if err != nil {
		return nil, fmt.Errorf("postgres keyed engine: parse config: %w", err)
	}
	poolConfig.MaxConns = int32(conf.PoolSize)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres keyed engine: connect primary: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres keyed engine: ping primary: %w", err)
	}

	// Configure read replica pool if specified
	var readPool *pgxpool.Pool
	if conf.ReplicaConnString != "" {
		replicaConfig, err := pgxpool.ParseConfig(conf.ReplicaConnString)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres keyed engine: parse replica config: %w", err)
		}
		replicaConfig.MaxConns = int32(conf.PoolSize)

		readPool, err = pgxpool.NewWithConfig(ctx, replicaConfig)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres keyed engine: connect replica: %w", err)
		}

		if err := readPool.Ping(ctx); err != nil {
			pool.Close()
			readPool.Close()
			return nil, fmt.Errorf("postgres keyed engine: ping replica: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &PostgresKeyedEngine{
		node:       n,
		conf:       conf,
		pool:       pool,
		readPool:   readPool,
		wakeup:     make(chan struct{}, 1),
		closeCh:    make(chan struct{}),
		cancelCtx:  ctx,
		cancelFunc: cancel,
	}

	return e, nil
}

// getReadPool returns the read pool (replica if configured, otherwise primary).
func (e *PostgresKeyedEngine) getReadPool() *pgxpool.Pool {
	if e.readPool != nil {
		return e.readPool
	}
	return e.pool
}

// RegisterBrokerEventHandler registers the event handler and starts background workers.
func (e *PostgresKeyedEngine) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	e.eventHandler = h

	if e.running.Swap(true) {
		return errors.New("postgres keyed engine: already running")
	}

	// Initialize lastID from the database
	var maxID *int64
	err := e.pool.QueryRow(e.cancelCtx, "SELECT MAX(id) FROM cf_keyed_stream").Scan(&maxID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("postgres keyed engine: get max id: %w", err)
	}
	if maxID != nil {
		e.lastID = *maxID
	}

	// Start background workers with cancellable context
	go e.listenForNotify()
	go e.runPollingLoop()
	go e.runTTLExpirationWorker()
	go e.runStreamCleanupWorker()

	return nil
}

// RegisterEventHandler is an alias for RegisterBrokerEventHandler.
func (e *PostgresKeyedEngine) RegisterEventHandler(h BrokerEventHandler) error {
	return e.RegisterBrokerEventHandler(h)
}

// Close shuts down the engine.
func (e *PostgresKeyedEngine) Close(_ context.Context) error {
	e.closeOnce.Do(func() {
		e.cancelFunc() // Cancel context to unblock WaitForNotification
		close(e.closeCh)
		e.pool.Close()
		if e.readPool != nil {
			e.readPool.Close()
		}
	})
	return nil
}

// Subscribe is a no-op for PostgreSQL engine (global polling handles all channels).
func (e *PostgresKeyedEngine) Subscribe(_ string) error {
	return nil
}

// Unsubscribe is a no-op for PostgreSQL engine.
func (e *PostgresKeyedEngine) Unsubscribe(_ string) error {
	return nil
}

// SetChannelOptionsResolver sets the callback for resolving stream options per channel.
func (e *PostgresKeyedEngine) SetChannelOptionsResolver(r ChannelOptionsResolver) {
	e.channelOptionsResolver = r
}

// parseSuppressReason converts SQL suppress_reason string to SuppressReason type.
func parseSuppressReason(reason *string) SuppressReason {
	if reason == nil {
		return SuppressReasonNone
	}
	switch *reason {
	case "idempotency":
		return SuppressReasonIdempotency
	case "position_mismatch":
		return SuppressReasonPositionMismatch
	case "key_exists":
		return SuppressReasonKeyExists
	case "key_not_found":
		return SuppressReasonKeyNotFound
	case "version":
		return SuppressReasonVersion
	default:
		return SuppressReasonNone
	}
}

// Publish publishes data to a keyed channel using the cf_keyed_publish SQL function.
func (e *PostgresKeyedEngine) Publish(ctx context.Context, ch string, key string, opts KeyedPublishOptions) (KeyedPublishResult, error) {
	// Apply channel options defaults from resolver.
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL, opts.KeyTTL = applyChannelOptionsDefaults(
		opts.StreamSize, opts.StreamTTL, opts.MetaTTL, opts.KeyTTL,
		e.channelOptionsResolver, ch,
	)

	// Prepare client info fields
	var clientID, userID *string
	var connInfo, chanInfo []byte
	var subscribedAt *time.Time
	if opts.ClientInfo != nil {
		if opts.ClientInfo.ClientID != "" {
			clientID = &opts.ClientInfo.ClientID
		}
		if opts.ClientInfo.UserID != "" {
			userID = &opts.ClientInfo.UserID
		}
		connInfo = opts.ClientInfo.ConnInfo
		chanInfo = opts.ClientInfo.ChanInfo
		now := time.Now()
		subscribedAt = &now
	}

	// Prepare tags JSON
	var tagsJSON []byte
	if opts.Tags != nil {
		tagsJSON, _ = json.Marshal(opts.Tags)
	}

	// Prepare key mode
	var keyMode *string
	if opts.KeyMode != KeyModeReplace {
		km := string(opts.KeyMode)
		keyMode = &km
	}

	// Prepare TTLs as interval strings
	var keyTTL, streamTTL, metaTTL, idempotencyTTL *string
	if opts.KeyTTL > 0 {
		s := fmt.Sprintf("%d seconds", int(opts.KeyTTL.Seconds()))
		keyTTL = &s
	}
	if opts.StreamTTL > 0 {
		s := fmt.Sprintf("%d seconds", int(opts.StreamTTL.Seconds()))
		streamTTL = &s
	}
	if opts.MetaTTL > 0 {
		s := fmt.Sprintf("%d seconds", int(opts.MetaTTL.Seconds()))
		metaTTL = &s
	}
	idempotentResultTTL := opts.IdempotentResultTTL
	if idempotentResultTTL == 0 {
		idempotentResultTTL = e.conf.IdempotentResultTTL
	}
	if opts.IdempotencyKey != "" && idempotentResultTTL > 0 {
		s := fmt.Sprintf("%d seconds", int(idempotentResultTTL.Seconds()))
		idempotencyTTL = &s
	}

	// Prepare expected offset
	var expectedOffset *int64
	if opts.ExpectedPosition != nil {
		eo := int64(opts.ExpectedPosition.Offset)
		expectedOffset = &eo
	}

	// Prepare score
	var score *int64
	if opts.Ordered || opts.Score != 0 {
		score = &opts.Score
	}

	// Prepare version
	var version *int64
	var versionEpoch *string
	if opts.Version > 0 {
		v := int64(opts.Version)
		version = &v
		if opts.VersionEpoch != "" {
			versionEpoch = &opts.VersionEpoch
		}
	}

	// Prepare key version (stored in snapshot)
	var keyVersion *int64
	var keyVersionEpoch *string
	if opts.Version > 0 {
		v := int64(opts.Version)
		keyVersion = &v
		if opts.VersionEpoch != "" {
			keyVersionEpoch = &opts.VersionEpoch
		}
	}

	// Prepare idempotency key
	var idempotencyKey *string
	if opts.IdempotencyKey != "" {
		idempotencyKey = &opts.IdempotencyKey
	}

	// Prepare stream size
	var streamSize *int
	if opts.StreamSize > 0 {
		streamSize = &opts.StreamSize
	}

	// Call cf_keyed_publish function
	var id *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string
	var currentData []byte
	var currentOffset *int64

	err := e.pool.QueryRow(ctx, `
		SELECT result_id, channel_offset, epoch, suppressed, suppress_reason, current_data, current_offset
		FROM cf_keyed_publish($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::interval, $12::interval, $13, $14::interval, $15, $16, $17, $18, $19, $20, $21, $22::interval, $23)
	`,
		ch, key, opts.Data, tagsJSON,
		clientID, userID, connInfo, chanInfo, subscribedAt,
		keyMode, keyTTL, streamTTL, streamSize, metaTTL,
		expectedOffset, score, version, versionEpoch,
		keyVersion, keyVersionEpoch,
		idempotencyKey, idempotencyTTL, opts.RefreshTTLOnSuppress,
	).Scan(&id, &channelOffset, &epoch, &suppressed, &suppressReason, &currentData, &currentOffset)

	if err != nil {
		return KeyedPublishResult{}, err
	}

	newPos := StreamPosition{Offset: uint64(channelOffset), Epoch: epoch}

	if suppressed {
		result := KeyedPublishResult{
			Position:       newPos,
			Suppressed:     true,
			SuppressReason: parseSuppressReason(suppressReason),
		}
		// For position_mismatch, include current publication data
		if suppressReason != nil && *suppressReason == "position_mismatch" && currentOffset != nil {
			result.CurrentPublication = &Publication{
				Offset: uint64(*currentOffset),
				Key:    key,
				Data:   currentData,
			}
		}
		return result, nil
	}

	// Handle local delivery if configured
	if opts.Publish && e.eventHandler != nil {
		pub := &Publication{
			Offset: uint64(channelOffset),
			Key:    key,
			Data:   opts.Data,
			Tags:   opts.Tags,
			Info:   opts.ClientInfo,
			Time:   time.Now().UnixMilli(),
		}
		_ = e.eventHandler.HandlePublication(ch, pub, newPos, opts.UseDelta, nil)
	}

	return KeyedPublishResult{Position: newPos}, nil
}

// Unpublish removes a key from keyed state using the cf_keyed_unpublish SQL function.
func (e *PostgresKeyedEngine) Unpublish(ctx context.Context, ch string, key string, opts KeyedUnpublishOptions) (KeyedPublishResult, error) {
	// Apply channel options defaults from resolver.
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL, _ = applyChannelOptionsDefaults(
		opts.StreamSize, opts.StreamTTL, opts.MetaTTL, 0,
		e.channelOptionsResolver, ch,
	)

	// Prepare TTLs as interval strings
	var streamTTL, metaTTL, idempotencyTTL *string
	if opts.StreamTTL > 0 {
		s := fmt.Sprintf("%d seconds", int(opts.StreamTTL.Seconds()))
		streamTTL = &s
	}
	if opts.MetaTTL > 0 {
		s := fmt.Sprintf("%d seconds", int(opts.MetaTTL.Seconds()))
		metaTTL = &s
	}
	idempotentResultTTL := opts.IdempotentResultTTL
	if idempotentResultTTL == 0 {
		idempotentResultTTL = e.conf.IdempotentResultTTL
	}
	if opts.IdempotencyKey != "" && idempotentResultTTL > 0 {
		s := fmt.Sprintf("%d seconds", int(idempotentResultTTL.Seconds()))
		idempotencyTTL = &s
	}

	// Prepare idempotency key
	var idempotencyKey *string
	if opts.IdempotencyKey != "" {
		idempotencyKey = &opts.IdempotencyKey
	}

	// Call cf_keyed_unpublish function
	var id *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string

	// Client info is not available in unpublish options
	var clientID, userID *string
	err := e.pool.QueryRow(ctx, `
		SELECT result_id, channel_offset, epoch, suppressed, suppress_reason
		FROM cf_keyed_unpublish($1, $2, $3, $4, $5::interval, $6, $7::interval, $8::interval)
	`,
		ch, key, clientID, userID, streamTTL, idempotencyKey, idempotencyTTL, metaTTL,
	).Scan(&id, &channelOffset, &epoch, &suppressed, &suppressReason)

	if err != nil {
		return KeyedPublishResult{}, err
	}

	newPos := StreamPosition{Offset: uint64(channelOffset), Epoch: epoch}

	if suppressed {
		return KeyedPublishResult{
			Position:       newPos,
			Suppressed:     true,
			SuppressReason: parseSuppressReason(suppressReason),
		}, nil
	}

	// Handle local delivery if configured
	if opts.Publish && e.eventHandler != nil {
		pub := &Publication{
			Key:     key,
			Removed: true,
			Offset:  uint64(channelOffset),
			Time:    time.Now().UnixMilli(),
		}
		_ = e.eventHandler.HandlePublication(ch, pub, newPos, false, nil)
	}

	return KeyedPublishResult{Position: newPos}, nil
}

// ReadSnapshot retrieves keyed snapshot with revisions.
func (e *PostgresKeyedEngine) ReadSnapshot(ctx context.Context, ch string, opts KeyedReadSnapshotOptions) ([]*Publication, StreamPosition, string, error) {
	pool := e.getReadPool()

	// Get current stream position
	var topOffset int64
	var epoch string
	err := pool.QueryRow(ctx, `
		SELECT top_offset, epoch FROM cf_keyed_stream_meta WHERE channel = $1
	`, ch).Scan(&topOffset, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		// Channel doesn't exist
		if opts.Revision != nil && opts.Revision.Epoch != "" {
			// Client sent an epoch but channel is gone - unrecoverable
			return nil, StreamPosition{}, "", ErrorUnrecoverablePosition
		}
		return nil, StreamPosition{}, "", nil
	}
	if err != nil {
		return nil, StreamPosition{}, "", err
	}

	streamPos := StreamPosition{Offset: uint64(topOffset), Epoch: epoch}

	// Check revision epoch
	if opts.Revision != nil && opts.Revision.Epoch != "" {
		if opts.Revision.Epoch != epoch {
			return nil, streamPos, "", ErrorUnrecoverablePosition
		}
	}

	// Single key lookup (for CAS read)
	if opts.Key != "" {
		var p Publication
		var tagsJSON []byte
		var clientID, userID *string
		var connInfo, chanInfo []byte
		err := pool.QueryRow(ctx, `
			SELECT key, data, tags, key_offset, client_id, user_id, conn_info, chan_info
			FROM cf_keyed_snapshot
			WHERE channel = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())
		`, ch, opts.Key).Scan(&p.Key, &p.Data, &tagsJSON, &p.Offset, &clientID, &userID, &connInfo, &chanInfo)

		if errors.Is(err, pgx.ErrNoRows) {
			return []*Publication{}, streamPos, "", nil
		}
		if err != nil {
			return nil, StreamPosition{}, "", err
		}
		if len(tagsJSON) > 0 {
			_ = json.Unmarshal(tagsJSON, &p.Tags)
		}
		if clientID != nil {
			p.Info = &ClientInfo{
				ClientID: *clientID,
				ConnInfo: connInfo,
				ChanInfo: chanInfo,
			}
			if userID != nil {
				p.Info.UserID = *userID
			}
		}
		return []*Publication{&p}, streamPos, "", nil
	}

	// Paginated snapshot read
	var query string
	if opts.Ordered {
		query = `
			SELECT key, data, tags, key_offset, score, client_id, user_id, conn_info, chan_info
			FROM cf_keyed_snapshot
			WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
			ORDER BY score DESC, key
			LIMIT $2 OFFSET $3
		`
	} else {
		query = `
			SELECT key, data, tags, key_offset, score, client_id, user_id, conn_info, chan_info
			FROM cf_keyed_snapshot
			WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
			ORDER BY key
			LIMIT $2 OFFSET $3
		`
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	// Parse cursor as offset
	offset := opts.Offset
	if opts.Cursor != "" {
		parsedOffset, err := strconv.Atoi(opts.Cursor)
		if err == nil {
			offset = parsedOffset
		}
	}

	rows, err := pool.Query(ctx, query, ch, limit+1, offset)
	if err != nil {
		return nil, StreamPosition{}, "", err
	}
	defer rows.Close()

	var pubs []*Publication
	for rows.Next() {
		var p Publication
		var score *int64
		var tagsJSON []byte
		var clientID, userID *string
		var connInfo, chanInfo []byte
		if err := rows.Scan(&p.Key, &p.Data, &tagsJSON, &p.Offset, &score, &clientID, &userID, &connInfo, &chanInfo); err != nil {
			return nil, StreamPosition{}, "", err
		}
		if len(tagsJSON) > 0 {
			_ = json.Unmarshal(tagsJSON, &p.Tags)
		}
		if clientID != nil {
			p.Info = &ClientInfo{
				ClientID: *clientID,
				ConnInfo: connInfo,
				ChanInfo: chanInfo,
			}
			if userID != nil {
				p.Info.UserID = *userID
			}
		}
		pubs = append(pubs, &p)
	}

	var nextCursor string
	if len(pubs) > limit {
		pubs = pubs[:limit]
		nextCursor = strconv.Itoa(offset + limit)
	}

	return pubs, streamPos, nextCursor, nil
}

// ReadStream retrieves publications from stream.
func (e *PostgresKeyedEngine) ReadStream(ctx context.Context, ch string, opts KeyedReadStreamOptions) ([]*Publication, StreamPosition, error) {
	pool := e.getReadPool()

	// Get current meta
	var topOffset int64
	var epoch string
	err := pool.QueryRow(ctx, `
		SELECT top_offset, epoch FROM cf_keyed_stream_meta WHERE channel = $1
	`, ch).Scan(&topOffset, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, StreamPosition{Offset: 0}, nil
	}
	if err != nil {
		return nil, StreamPosition{}, err
	}

	streamPos := StreamPosition{Offset: uint64(topOffset), Epoch: epoch}

	if opts.Filter.Limit == 0 {
		return nil, streamPos, nil
	}

	sinceOffset := int64(0)
	if opts.Filter.Since != nil {
		sinceOffset = int64(opts.Filter.Since.Offset)
	}

	limit := opts.Filter.Limit
	if limit < 0 {
		limit = 10000
	}

	// Query by per-channel offset (not global id)
	var query string
	if opts.Filter.Reverse {
		query = `
			SELECT key, data, tags, channel_offset, removed, client_id, user_id, conn_info, chan_info
			FROM cf_keyed_stream
			WHERE channel = $1 AND channel_offset < $2
			ORDER BY channel_offset DESC
			LIMIT $3
		`
		if opts.Filter.Since == nil {
			sinceOffset = topOffset + 1
		}
	} else {
		query = `
			SELECT key, data, tags, channel_offset, removed, client_id, user_id, conn_info, chan_info
			FROM cf_keyed_stream
			WHERE channel = $1 AND channel_offset > $2
			ORDER BY channel_offset ASC
			LIMIT $3
		`
	}

	rows, err := pool.Query(ctx, query, ch, sinceOffset, limit)
	if err != nil {
		return nil, StreamPosition{}, err
	}
	defer rows.Close()

	var pubs []*Publication
	for rows.Next() {
		var p Publication
		var removed bool
		var tagsJSON []byte
		var clientID, userID *string
		var connInfo, chanInfo []byte
		if err := rows.Scan(&p.Key, &p.Data, &tagsJSON, &p.Offset, &removed, &clientID, &userID, &connInfo, &chanInfo); err != nil {
			return nil, StreamPosition{}, err
		}
		p.Removed = removed
		if len(tagsJSON) > 0 {
			_ = json.Unmarshal(tagsJSON, &p.Tags)
		}
		if clientID != nil {
			p.Info = &ClientInfo{
				ClientID: *clientID,
				ConnInfo: connInfo,
				ChanInfo: chanInfo,
			}
			if userID != nil {
				p.Info.UserID = *userID
			}
		}
		pubs = append(pubs, &p)
	}

	return pubs, streamPos, nil
}

// Stats returns snapshot statistics.
func (e *PostgresKeyedEngine) Stats(ctx context.Context, ch string) (KeyedStats, error) {
	pool := e.getReadPool()

	var count int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM cf_keyed_snapshot
		WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
	`, ch).Scan(&count)
	if err != nil {
		return KeyedStats{}, err
	}

	return KeyedStats{NumKeys: count}, nil
}

// Remove deletes all data for a channel.
func (e *PostgresKeyedEngine) Remove(ctx context.Context, ch string, _ KeyedRemoveOptions) error {
	_, err := e.pool.Exec(ctx, `
		DELETE FROM cf_keyed_stream WHERE channel = $1;
		DELETE FROM cf_keyed_snapshot WHERE channel = $1;
		DELETE FROM cf_keyed_stream_meta WHERE channel = $1;
		DELETE FROM cf_keyed_idempotency WHERE channel = $1;
	`, ch)
	return err
}

// listenForNotify listens for PostgreSQL NOTIFY messages to wake up polling.
func (e *PostgresKeyedEngine) listenForNotify() {
	backoff := time.Second
	maxBackoff := 30 * time.Second
	ctx := e.cancelCtx

	for {
		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		conn, err := e.pool.Acquire(ctx)
		if err != nil {
			select {
			case <-e.closeCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			}
		}

		_, err = conn.Exec(ctx, "LISTEN cf_keyed_stream_changes")
		if err != nil {
			conn.Release()
			select {
			case <-e.closeCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			}
		}

		// Reset backoff on successful connection
		backoff = time.Second

		// Listen loop
		for {
			_, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				conn.Release()
				break // Reconnect (or exit if context cancelled)
			}
			// Wake up the polling loop
			select {
			case e.wakeup <- struct{}{}:
			default:
				// Already pending wake-up
			}
		}
	}
}

// runPollingLoop polls for new stream entries and delivers to subscribers.
func (e *PostgresKeyedEngine) runPollingLoop() {
	ticker := time.NewTicker(e.conf.PollInterval)
	defer ticker.Stop()
	ctx := e.cancelCtx

	for {
		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		case <-e.wakeup:
			// NOTIFY received, poll immediately
		case <-ticker.C:
			// Fallback polling interval
		}

		if err := e.poll(ctx); err != nil {
			// Log error but continue
			if e.node != nil {
				e.node.logger.log(newErrorLogEntry(err, "postgres keyed engine poll error", nil))
			}
		}
	}
}

// poll fetches new rows WHERE id > lastID and delivers to subscribers.
func (e *PostgresKeyedEngine) poll(ctx context.Context) error {
	if e.eventHandler == nil {
		return nil
	}

	pool := e.getReadPool()

	e.lastIDMu.Lock()
	lastID := e.lastID
	e.lastIDMu.Unlock()

	rows, err := pool.Query(ctx, `
		SELECT id, channel, channel_offset, key, data, tags, removed
		FROM cf_keyed_stream
		WHERE id > $1
		ORDER BY id
		LIMIT $2
	`, lastID, e.conf.PollBatchSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id       int64
			channel  string
			offset   uint64
			key      string
			data     []byte
			tagsJSON []byte
			removed  bool
		)
		if err := rows.Scan(&id, &channel, &offset, &key, &data, &tagsJSON, &removed); err != nil {
			return err
		}

		var tags map[string]string
		if len(tagsJSON) > 0 {
			_ = json.Unmarshal(tagsJSON, &tags)
		}

		pub := &Publication{
			Offset:  offset,
			Key:     key,
			Data:    data,
			Tags:    tags,
			Removed: removed,
		}

		// Get epoch for this channel
		var epoch string
		_ = pool.QueryRow(ctx, `
			SELECT epoch FROM cf_keyed_stream_meta WHERE channel = $1
		`, channel).Scan(&epoch)

		// Deliver to subscribers
		_ = e.eventHandler.HandlePublication(
			channel,
			pub,
			StreamPosition{Offset: offset, Epoch: epoch},
			false,
			nil,
		)

		e.lastIDMu.Lock()
		e.lastID = id
		e.lastIDMu.Unlock()
	}

	return rows.Err()
}

// runTTLExpirationWorker expires keys with TTL and emits removal events.
func (e *PostgresKeyedEngine) runTTLExpirationWorker() {
	ticker := time.NewTicker(e.conf.TTLCheckInterval)
	defer ticker.Stop()
	ctx := e.cancelCtx

	for {
		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.expireKeys(ctx)
		}
	}
}

func (e *PostgresKeyedEngine) expireKeys(ctx context.Context) {
	// Find and delete expired keys, returning their info for removal events
	rows, err := e.pool.Query(ctx, `
		DELETE FROM cf_keyed_snapshot
		WHERE expires_at IS NOT NULL AND expires_at <= NOW()
		RETURNING channel, key
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ch, key string
		if err := rows.Scan(&ch, &key); err != nil {
			continue
		}

		// Get channel options for this channel
		opts := DefaultKeyedChannelOptions()
		if e.channelOptionsResolver != nil {
			opts = e.channelOptionsResolver(ch)
		}

		// Emit removal to stream
		_, _ = e.Unpublish(ctx, ch, key, KeyedUnpublishOptions{
			Publish:    true,
			StreamSize: opts.StreamSize,
			StreamTTL:  opts.StreamTTL,
			MetaTTL:    opts.MetaTTL,
		})
	}
}

// runStreamCleanupWorker cleans up expired stream/meta/idempotency entries.
func (e *PostgresKeyedEngine) runStreamCleanupWorker() {
	ticker := time.NewTicker(e.conf.CleanupInterval)
	defer ticker.Stop()
	ctx := e.cancelCtx

	for {
		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.cleanupExpiredEntries(ctx)
		}
	}
}

func (e *PostgresKeyedEngine) cleanupExpiredEntries(ctx context.Context) {
	// Remove expired stream entries
	_, _ = e.pool.Exec(ctx, `
		DELETE FROM cf_keyed_stream
		WHERE expires_at IS NOT NULL AND expires_at < NOW()
	`)

	// Remove expired stream metadata
	_, _ = e.pool.Exec(ctx, `
		DELETE FROM cf_keyed_stream_meta
		WHERE expires_at IS NOT NULL AND expires_at < NOW()
	`)

	// Remove expired idempotency keys
	_, _ = e.pool.Exec(ctx, `
		DELETE FROM cf_keyed_idempotency
		WHERE expires_at < NOW()
	`)
}
