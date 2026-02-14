package centrifuge

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/epoch"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed internal/postgres_sql/schema.sql
var postgresSchemaSQL string

// PostgresMapBroker is MapBroker implementation using PostgreSQL for persistent
// map subscriptions. It provides durability, CAS operations, and transactional
// publishing from SQL.
//
// Key features:
//   - Dual ID system: global `id` for polling, per-channel `offset` for Centrifuge
//   - Two delivery modes: Outbox (default, simpler setup) or WAL (opt-in, uses logical replication)
//   - Full ACID transactions for atomic CAS operations
//   - Optional read replica support for scaling reads
//
// Use cases: collaborative boards, document editing, inventory/booking systems,
// game lobbies with persistent state.
type PostgresMapBroker struct {
	node         *Node
	conf         PostgresMapBrokerConfig
	pool *pgxpool.Pool // Primary pool for writes
	eventHandler BrokerEventHandler
	closeCh      chan struct{}
	closeOnce    sync.Once
	running      atomic.Bool
	cancelCtx    context.Context
	cancelFunc   context.CancelFunc

	// WAL reader state (used when WAL.Enabled = true)
	walReaders        []*walShardReader
	walReaderMu       sync.Mutex
	walClaimedShards  map[int]bool
	walClaimedShardMu sync.RWMutex

	// Outbox worker state (used when WAL.Enabled = false, which is default)
	outboxClaimedShards  map[int]bool
	outboxClaimedShardMu sync.RWMutex
}

var _ MapBroker = (*PostgresMapBroker)(nil)

// OutboxConfig configures the outbox-based delivery mode (default).
// Outbox mode polls cf_map_stream using per-shard cursors tracked in cf_map_outbox_cursor.
// This mode requires no special PostgreSQL setup (no logical replication).
type OutboxConfig struct {
	// PollInterval is how often to poll for new stream entries when idle.
	// Default: 50ms
	PollInterval time.Duration

	// BatchSize is the maximum number of rows to process per batch.
	// Default: 1000
	BatchSize int

	// AdvisoryLockBaseID is the base ID for PostgreSQL advisory locks.
	// Each shard uses AdvisoryLockBaseID + shardID as its lock ID.
	// Default: 72616653 (derived from 'crf' in ASCII)
	AdvisoryLockBaseID int64
}

// WALConfig configures the WAL-based delivery mode (opt-in).
// WAL mode uses PostgreSQL logical replication to stream changes.
// This requires PostgreSQL setup: wal_level=logical, publications, replication slots.
type WALConfig struct {
	// Enabled activates WAL mode instead of the default outbox mode.
	// When true, the broker uses logical replication to stream changes.
	// Default: false (use outbox mode)
	Enabled bool

	// ShardIDs specifies which shard(s) this node should try to claim.
	// Each shard uses advisory locks for leader election - only one node
	// can read each shard at a time.
	// If empty, the node will try to claim all shards (suitable for single-node).
	// For multi-node deployments, distribute shards across nodes or let all
	// nodes compete for all shards (advisory locks ensure only one wins each).
	ShardIDs []int

	// AdvisoryLockBaseID is the base ID for PostgreSQL advisory locks.
	// Each shard uses AdvisoryLockBaseID + shardID as its lock ID.
	// Default: 72616654 (different from outbox to avoid conflicts)
	AdvisoryLockBaseID int64

	// HeartbeatInterval is how often to send standby status updates.
	// Default: 10s
	HeartbeatInterval time.Duration
}

// PostgresMapBrokerConfig configures the PostgreSQL map broker.
type PostgresMapBrokerConfig struct {
	// ConnString is the primary PostgreSQL connection string for writes.
	// Example: "postgres://user:pass@localhost:5432/dbname?sslmode=disable"
	ConnString string

	// PoolSize sets the maximum number of connections in the pool.
	// Default: 32
	PoolSize int

	// NumShards is the total number of shards for parallel delivery workers
	// (both outbox and WAL modes). Channels are distributed across shards
	// using hash(channel) % NumShards.
	// IMPORTANT: In outbox mode, each worker holds a connection for its advisory lock,
	// so NumShards must be less than PoolSize to leave connections for queries.
	// In WAL mode, NumShards must match the number of publications created in PostgreSQL.
	// Default: 16
	NumShards int

	// TTLCheckInterval is how often to check for expired keys.
	// Default: 1s
	TTLCheckInterval time.Duration

	// CleanupInterval is how often to clean up expired stream/meta/idempotency entries.
	// Default: 1m
	CleanupInterval time.Duration

	// IdempotentResultTTL is the default TTL for idempotency keys.
	// Default: 5m
	IdempotentResultTTL time.Duration

	// Broker is used to fan out changes to all nodes via pub/sub (e.g., Redis).
	// Optional. If nil, only local delivery is performed (suitable for single-node).
	// For multi-node deployments, provide a broker.
	Broker Broker

	// Outbox configures the outbox-based delivery mode (default).
	// Used when WAL.Enabled is false.
	Outbox OutboxConfig

	// WAL configures the WAL-based delivery mode (opt-in).
	// Set WAL.Enabled = true to use logical replication instead of outbox.
	WAL WALConfig

	// BinaryData uses BYTEA columns instead of JSONB for data fields.
	// Default: false (JSONB — suitable for JSON payloads, enables JSONB queries).
	// Set to true if data payloads are not valid JSON (binary/protobuf).
	BinaryData bool
}

func (c *PostgresMapBrokerConfig) setDefaults() {
	if c.PoolSize <= 0 {
		c.PoolSize = 32
	}
	if c.NumShards <= 0 {
		c.NumShards = 16
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

	// Outbox config defaults
	if c.Outbox.PollInterval <= 0 {
		c.Outbox.PollInterval = 50 * time.Millisecond
	}
	if c.Outbox.BatchSize <= 0 {
		c.Outbox.BatchSize = 1000
	}
	if c.Outbox.AdvisoryLockBaseID <= 0 {
		c.Outbox.AdvisoryLockBaseID = 72616653 // 'crf' in ASCII
	}

	// WAL config defaults
	if c.WAL.AdvisoryLockBaseID <= 0 {
		c.WAL.AdvisoryLockBaseID = 72616654 // Different from outbox
	}
	if c.WAL.HeartbeatInterval <= 0 {
		c.WAL.HeartbeatInterval = 10 * time.Second
	}
}

// NewPostgresMapBroker creates a new PostgreSQL map broker.
func NewPostgresMapBroker(n *Node, conf PostgresMapBrokerConfig) (*PostgresMapBroker, error) {
	conf.setDefaults()

	if conf.ConnString == "" {
		return nil, errors.New("postgres map broker: ConnString is required")
	}

	// Validate pool size is sufficient for outbox workers.
	// Each outbox worker holds a connection for its advisory lock.
	if !conf.WAL.Enabled && conf.PoolSize <= conf.NumShards {
		return nil, fmt.Errorf("postgres map broker: PoolSize (%d) must be greater than NumShards (%d) to leave connections for queries",
			conf.PoolSize, conf.NumShards)
	}

	ctx := context.Background()

	// Configure primary pool
	poolConfig, err := pgxpool.ParseConfig(conf.ConnString)
	if err != nil {
		return nil, fmt.Errorf("postgres map broker: parse config: %w", err)
	}
	poolConfig.MaxConns = int32(conf.PoolSize)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres map broker: connect primary: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres map broker: ping primary: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &PostgresMapBroker{
		node:                n,
		conf:                conf,
		pool:                pool,
		closeCh:             make(chan struct{}),
		cancelCtx:           ctx,
		cancelFunc:          cancel,
		walClaimedShards:    make(map[int]bool),
		outboxClaimedShards: make(map[int]bool),
	}

	return e, nil
}

// getReadPool returns the pool for read operations.
func (e *PostgresMapBroker) getReadPool() *pgxpool.Pool {
	return e.pool
}

// pgGenerateEpoch creates a random epoch string using the shared epoch package.
func pgGenerateEpoch() string {
	return epoch.Generate()
}

// ensureChannelMeta creates the channel meta if it doesn't exist and returns the stream position.
// This ensures that empty channels have an epoch, matching MemoryMapBroker behavior.
// Uses a single query with CTE for efficiency.
func (e *PostgresMapBroker) ensureChannelMeta(ctx context.Context, ch string) (StreamPosition, error) {
	epoch := pgGenerateEpoch()

	var topOffset int64
	var actualEpoch string
	err := e.pool.QueryRow(ctx, `
		WITH new_row AS (
			INSERT INTO cf_map_meta (channel, top_offset, epoch)
			VALUES ($1, 0, $2)
			ON CONFLICT (channel) DO NOTHING
			RETURNING top_offset, epoch
		)
		SELECT top_offset, epoch FROM new_row
		UNION ALL
		SELECT top_offset, epoch FROM cf_map_meta
		WHERE channel = $1 AND NOT EXISTS (SELECT 1 FROM new_row)
	`, ch, epoch).Scan(&topOffset, &actualEpoch)
	if err != nil {
		return StreamPosition{}, err
	}

	return StreamPosition{Offset: uint64(topOffset), Epoch: actualEpoch}, nil
}

// RegisterEventHandler registers the event handler and starts background workers.
func (e *PostgresMapBroker) RegisterEventHandler(h BrokerEventHandler) error {
	e.eventHandler = h

	if e.running.Swap(true) {
		return errors.New("postgres map broker: already running")
	}

	// Start the inner broker's pub/sub if configured (for multi-node fan-out).
	if e.conf.Broker != nil {
		if err := e.conf.Broker.RegisterBrokerEventHandler(h); err != nil {
			return fmt.Errorf("postgres map broker: register inner broker: %w", err)
		}
	}

	// Start TTL and cleanup workers (always needed)
	go e.runTTLExpirationWorker()
	go e.runStreamCleanupWorker()

	// Mode selection: WAL if explicitly enabled, otherwise outbox (default)
	if e.conf.WAL.Enabled {
		// WAL mode: start WAL readers
		shardIDs := e.conf.WAL.ShardIDs
		if len(shardIDs) == 0 {
			// Default: try to claim all shards
			shardIDs = make([]int, e.conf.NumShards)
			for i := 0; i < e.conf.NumShards; i++ {
				shardIDs[i] = i
			}
		}

		for _, shardID := range shardIDs {
			if shardID < 0 || shardID >= e.conf.NumShards {
				continue
			}
			go e.runWALReaderForShard(shardID)
		}
	} else {
		// Outbox mode (default): start outbox workers
		for shardID := 0; shardID < e.conf.NumShards; shardID++ {
			go e.runOutboxWorkerForShard(shardID)
		}
	}

	return nil
}

// Close shuts down the broker.
func (e *PostgresMapBroker) Close(ctx context.Context) error {
	e.closeOnce.Do(func() {
		e.cancelFunc() // Cancel context to unblock WaitForNotification
		close(e.closeCh)
		if e.conf.Broker != nil {
			if closer, ok := e.conf.Broker.(Closer); ok {
				_ = closer.Close(ctx)
			}
		}
		e.pool.Close()
	})
	return nil
}

// SchemaObject identifies a database object involved in a schema error.
type SchemaObject struct {
	Type string // "table", "index", "function", "publication"
	Name string
}

// SchemaError wraps a schema-related error with object and operation info.
type SchemaError struct {
	Object SchemaObject
	Op     string // "create", "verify"
	Err    error
}

func (e *SchemaError) Error() string {
	return fmt.Sprintf("schema %s %s %q: %v", e.Op, e.Object.Type, e.Object.Name, e.Err)
}

func (e *SchemaError) Unwrap() error {
	return e.Err
}

// EnsureSchema creates all required database objects idempotently.
// It executes the embedded schema SQL (tables, indexes, functions) and,
// when WAL mode is enabled, creates publications for each shard.
//
// Data columns use JSONB by default (suitable for JSON payloads).
// Set BinaryData=true in config to use BYTEA instead.
//
// This method is safe to call multiple times — all DDL uses
// CREATE IF NOT EXISTS / CREATE OR REPLACE. If the schema already exists
// with a different data type (e.g. BYTEA→JSONB), columns are altered and
// functions are dropped/recreated automatically.
func (e *PostgresMapBroker) EnsureSchema(ctx context.Context) error {
	dataType := "JSONB"
	if e.conf.BinaryData {
		dataType = "BYTEA"
	}
	dataTypeLower := strings.ToLower(dataType)

	// Check if schema exists with a different data type and migrate if needed.
	// We probe cf_map_state.data — if the table exists but has a different type,
	// we need to ALTER columns and DROP/recreate functions (CREATE OR REPLACE
	// cannot change parameter/return types).
	if err := e.migrateDataTypeIfNeeded(ctx, dataTypeLower); err != nil {
		return err
	}

	sql := strings.ReplaceAll(postgresSchemaSQL, "__DATA_TYPE__", dataType)

	if _, err := e.pool.Exec(ctx, sql); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return &SchemaError{
				Object: SchemaObject{Type: "schema", Name: pgErr.TableName},
				Op:     "create",
				Err:    err,
			}
		}
		return &SchemaError{
			Object: SchemaObject{Type: "schema", Name: ""},
			Op:     "create",
			Err:    err,
		}
	}

	// Create publications for WAL mode.
	if e.conf.WAL.Enabled {
		if err := e.ensurePublications(ctx); err != nil {
			return err
		}
	}

	return nil
}

// migrateDataTypeIfNeeded checks if existing schema uses a different data type
// and migrates columns + drops functions if needed.
func (e *PostgresMapBroker) migrateDataTypeIfNeeded(ctx context.Context, wantType string) error {
	var currentType string
	err := e.pool.QueryRow(ctx,
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'cf_map_state' AND column_name = 'data'`,
	).Scan(&currentType)
	if err != nil {
		// Table doesn't exist yet — nothing to migrate.
		return nil
	}

	if currentType == wantType {
		return nil // Already the right type.
	}

	// Data type mismatch — need to alter columns and drop functions.
	// Functions must be dropped first because they reference the column types
	// and CREATE OR REPLACE cannot change parameter/return types.
	funcs := []string{
		"cf_map_publish", "cf_map_publish_strict",
		"cf_map_remove", "cf_map_remove_strict",
		"cf_map_expire_keys",
	}
	for _, fn := range funcs {
		if _, err := e.pool.Exec(ctx, fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE", fn)); err != nil {
			return &SchemaError{
				Object: SchemaObject{Type: "function", Name: fn},
				Op:     "drop",
				Err:    err,
			}
		}
	}

	// Alter columns in existing tables.
	pgType := strings.ToUpper(wantType) // JSONB or BYTEA
	columns := []struct {
		table  string
		column string
	}{
		{"cf_map_stream", "data"},
		{"cf_map_stream", "conn_info"},
		{"cf_map_stream", "chan_info"},
		{"cf_map_stream", "previous_data"},
		{"cf_map_state", "data"},
		{"cf_map_state", "conn_info"},
		{"cf_map_state", "chan_info"},
	}
	for _, col := range columns {
		// PostgreSQL can't directly cast between JSONB and BYTEA.
		// JSONB → BYTEA: go through TEXT (JSON text representation as bytes).
		// BYTEA → JSONB: interpret bytes as UTF-8 and parse as JSON.
		var usingExpr string
		if pgType == "BYTEA" {
			usingExpr = fmt.Sprintf("%s::text::bytea", col.column)
		} else {
			usingExpr = fmt.Sprintf("convert_from(%s, 'UTF8')::jsonb", col.column)
		}
		sql := fmt.Sprintf(
			"ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s",
			col.table, col.column, pgType, usingExpr,
		)
		if _, err := e.pool.Exec(ctx, sql); err != nil {
			return &SchemaError{
				Object: SchemaObject{Type: "table", Name: col.table},
				Op:     "alter",
				Err:    fmt.Errorf("alter column %s: %w", col.column, err),
			}
		}
	}

	return nil
}

// ensurePublications creates shard publications and the "all" publication for WAL mode.
// It checks existing publications and only creates missing ones.
func (e *PostgresMapBroker) ensurePublications(ctx context.Context) error {
	numShards := e.conf.NumShards

	// Query existing cf_map_stream publications.
	rows, err := e.pool.Query(ctx,
		`SELECT pubname FROM pg_publication WHERE pubname LIKE 'cf_map_stream_%'`)
	if err != nil {
		return &SchemaError{
			Object: SchemaObject{Type: "publication", Name: ""},
			Op:     "verify",
			Err:    fmt.Errorf("query pg_publication: %w", err),
		}
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return &SchemaError{
				Object: SchemaObject{Type: "publication", Name: ""},
				Op:     "verify",
				Err:    fmt.Errorf("scan pg_publication: %w", err),
			}
		}
		existing[name] = true
	}
	rows.Close()

	// Count shard publications (exclude "all").
	shardCount := 0
	for name := range existing {
		if strings.HasPrefix(name, "cf_map_stream_shard_") {
			shardCount++
		}
	}

	if shardCount > 0 && shardCount != numShards {
		return &SchemaError{
			Object: SchemaObject{Type: "publication", Name: "cf_map_stream_shard_*"},
			Op:     "verify",
			Err: fmt.Errorf(
				"found %d shard publications but NumShards=%d; "+
					"drop existing publications and re-run EnsureSchema, or adjust NumShards to match",
				shardCount, numShards),
		}
	}

	// Create missing shard publications.
	for i := 0; i < numShards; i++ {
		name := fmt.Sprintf("cf_map_stream_shard_%d", i)
		if existing[name] {
			continue
		}
		sql := fmt.Sprintf(
			"CREATE PUBLICATION %s FOR TABLE cf_map_stream WHERE (shard_id = %d)", name, i)
		if _, err := e.pool.Exec(ctx, sql); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42710" {
				// duplicate_object — another node created it concurrently, that's fine.
				continue
			}
			return &SchemaError{
				Object: SchemaObject{Type: "publication", Name: name},
				Op:     "create",
				Err:    err,
			}
		}
	}

	// Create "all" publication if missing.
	if !existing["cf_map_stream_all"] {
		if _, err := e.pool.Exec(ctx,
			"CREATE PUBLICATION cf_map_stream_all FOR TABLE cf_map_stream"); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42710" {
				// duplicate_object — concurrent creation, fine.
			} else {
				return &SchemaError{
					Object: SchemaObject{Type: "publication", Name: "cf_map_stream_all"},
					Op:     "create",
					Err:    err,
				}
			}
		}
	}

	return nil
}

// Subscribe subscribes to broker PUB/SUB when broker is configured.
// For single-node (no broker), this is a no-op since WAL reader delivers locally.
func (e *PostgresMapBroker) Subscribe(ch string) error {
	if e.conf.Broker != nil {
		return e.conf.Broker.Subscribe(ch)
	}
	return nil
}

// Unsubscribe unsubscribes from broker PUB/SUB when broker is configured.
func (e *PostgresMapBroker) Unsubscribe(ch string) error {
	if e.conf.Broker != nil {
		return e.conf.Broker.Unsubscribe(ch)
	}
	return nil
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

// Publish publishes data to a map channel using the cf_map_publish SQL function.
func (e *PostgresMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapPublishResult, error) {
	// Resolve channel options once for this operation.
	resolved := resolveChannelOptions(e.node.ResolveMapChannelOptions, ch)

	// Apply channel options defaults from node config.
	chOpts := applyChannelOptionsDefaults(MapChannelOptions{
		StreamSize: opts.StreamSize, StreamTTL: opts.StreamTTL, MetaTTL: opts.MetaTTL, KeyTTL: opts.KeyTTL,
	}, resolved)
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL, opts.KeyTTL = chOpts.StreamSize, chOpts.StreamTTL, chOpts.MetaTTL, chOpts.KeyTTL

	// Reject CAS and Version in streamless mode.
	if opts.StreamSize <= 0 || opts.StreamTTL <= 0 {
		if opts.ExpectedPosition != nil {
			return MapPublishResult{}, errors.New("CAS (ExpectedPosition) requires stream (StreamSize > 0)")
		}
		if opts.Version > 0 {
			return MapPublishResult{}, errors.New("version-based dedup requires stream (StreamSize > 0)")
		}
	}

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

	// Prepare TTLs as interval strings.
	// Go already applied applyChannelOptionsDefaults, so values are resolved:
	//   > 0: use as-is (send interval string)
	//   = 0: disabled (send NULL — SQL treats NULL as "no TTL")
	var keyTTL, streamTTL, metaTTL, idempotencyTTL *string
	if opts.KeyTTL > 0 {
		s := strconv.Itoa(int(opts.KeyTTL.Seconds())) + " seconds"
		keyTTL = &s
	}
	if opts.StreamTTL > 0 {
		s := strconv.Itoa(int(opts.StreamTTL.Seconds())) + " seconds"
		streamTTL = &s
	}
	if opts.MetaTTL > 0 {
		s := strconv.Itoa(int(opts.MetaTTL.Seconds())) + " seconds"
		metaTTL = &s
	}
	idempotentResultTTL := opts.IdempotentResultTTL
	if idempotentResultTTL == 0 {
		idempotentResultTTL = e.conf.IdempotentResultTTL
	}
	if opts.IdempotencyKey != "" && idempotentResultTTL > 0 {
		s := strconv.Itoa(int(idempotentResultTTL.Seconds())) + " seconds"
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
	ordered := resolved.Ordered
	if ordered || opts.Score != 0 {
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

	// Prepare key version (stored in state)
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

	// Prepare stream size. NULL = no trimming, > 0 = trim to this size.
	var streamSize *int
	if opts.StreamSize > 0 {
		streamSize = &opts.StreamSize
	}

	// Call cf_map_publish function
	numShards := e.conf.NumShards

	var id *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string
	var currentData []byte
	var currentOffset *int64

	useDelta := opts.UseDelta && len(opts.StreamData) == 0

	// StreamData is stored in stream; state always uses Data.
	var streamData []byte
	if len(opts.StreamData) > 0 {
		streamData = opts.StreamData
	}

	err := e.pool.QueryRow(ctx, `
		SELECT result_id, channel_offset, epoch, suppressed, suppress_reason, current_data, current_offset
		FROM cf_map_publish($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::interval, $12::interval, $13, $14::interval, $15, $16, $17, $18, $19, $20, $21, $22::interval, $23, $24, $25, $26)
	`,
		ch, key, opts.Data, tagsJSON,
		clientID, userID, connInfo, chanInfo, subscribedAt,
		keyMode, keyTTL, streamTTL, streamSize, metaTTL,
		expectedOffset, score, version, versionEpoch,
		keyVersion, keyVersionEpoch,
		idempotencyKey, idempotencyTTL, opts.RefreshTTLOnSuppress,
		useDelta, numShards, streamData,
	).Scan(&id, &channelOffset, &epoch, &suppressed, &suppressReason, &currentData, &currentOffset)

	if err != nil {
		return MapPublishResult{}, err
	}

	newPos := StreamPosition{Offset: uint64(channelOffset), Epoch: epoch}

	if suppressed {
		result := MapPublishResult{
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

	return MapPublishResult{Position: newPos}, nil
}

// Remove removes a key from keyed state using the cf_map_remove SQL function.
func (e *PostgresMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapPublishResult, error) {
	if key == "" {
		return MapPublishResult{}, fmt.Errorf("key is required for remove")
	}

	// Resolve channel options once for this operation.
	resolved := resolveChannelOptions(e.node.ResolveMapChannelOptions, ch)

	// Apply channel options defaults from node config.
	chOpts := applyChannelOptionsDefaults(MapChannelOptions{
		StreamSize: opts.StreamSize, StreamTTL: opts.StreamTTL, MetaTTL: opts.MetaTTL,
	}, resolved)
	opts.StreamSize, opts.StreamTTL, opts.MetaTTL = chOpts.StreamSize, chOpts.StreamTTL, chOpts.MetaTTL

	// Reject CAS in streamless mode.
	if opts.StreamSize <= 0 || opts.StreamTTL <= 0 {
		if opts.ExpectedPosition != nil {
			return MapPublishResult{}, errors.New("CAS (ExpectedPosition) requires stream (StreamSize > 0)")
		}
	}

	// Prepare TTLs as interval strings.
	// NULL = no TTL, > 0 = use as-is.
	var streamTTL, metaTTL, idempotencyTTL *string
	if opts.StreamTTL > 0 {
		s := strconv.Itoa(int(opts.StreamTTL.Seconds())) + " seconds"
		streamTTL = &s
	}
	if opts.MetaTTL > 0 {
		s := strconv.Itoa(int(opts.MetaTTL.Seconds())) + " seconds"
		metaTTL = &s
	}
	idempotentResultTTL := opts.IdempotentResultTTL
	if idempotentResultTTL == 0 {
		idempotentResultTTL = e.conf.IdempotentResultTTL
	}
	if opts.IdempotencyKey != "" && idempotentResultTTL > 0 {
		s := strconv.Itoa(int(idempotentResultTTL.Seconds())) + " seconds"
		idempotencyTTL = &s
	}

	// Prepare idempotency key
	var idempotencyKey *string
	if opts.IdempotencyKey != "" {
		idempotencyKey = &opts.IdempotencyKey
	}

	// Prepare expected position for CAS
	var expectedOffset *int64
	if opts.ExpectedPosition != nil {
		eo := int64(opts.ExpectedPosition.Offset)
		expectedOffset = &eo
	}

	// Call cf_map_remove function
	numShards := e.conf.NumShards

	var id *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string
	var currentData []byte
	var currentOffset *int64

	// Client info is not available in remove options
	var clientID, userID *string
	err := e.pool.QueryRow(ctx, `
		SELECT result_id, channel_offset, epoch, suppressed, suppress_reason, current_data, current_offset
		FROM cf_map_remove($1, $2, $3, $4, $5::interval, $6, $7::interval, $8::interval, $9, $10)
	`,
		ch, key, clientID, userID, streamTTL, idempotencyKey, idempotencyTTL, metaTTL,
		numShards, expectedOffset,
	).Scan(&id, &channelOffset, &epoch, &suppressed, &suppressReason, &currentData, &currentOffset)

	if err != nil {
		return MapPublishResult{}, err
	}

	newPos := StreamPosition{Offset: uint64(channelOffset), Epoch: epoch}

	if suppressed {
		result := MapPublishResult{
			Position:       newPos,
			Suppressed:     true,
			SuppressReason: parseSuppressReason(suppressReason),
		}
		if suppressReason != nil && *suppressReason == "position_mismatch" && currentOffset != nil {
			result.CurrentPublication = &Publication{
				Offset: uint64(*currentOffset),
				Key:    key,
				Data:   currentData,
			}
		}
		return result, nil
	}

	return MapPublishResult{Position: newPos}, nil
}

// ReadState retrieves keyed state with revisions.
func (e *PostgresMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	pool := e.getReadPool()

	// Use REPEATABLE READ transaction to ensure atomic read of meta + state.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return MapStateResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Get current stream position.
	var topOffset int64
	var epoch string
	err = tx.QueryRow(ctx, `
		SELECT top_offset, epoch FROM cf_map_meta WHERE channel = $1
	`, ch).Scan(&topOffset, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		// Channel doesn't exist yet
		if opts.Revision != nil && opts.Revision.Epoch != "" {
			// Client sent an epoch but channel is gone - unrecoverable
			return MapStateResult{}, ErrorUnrecoverablePosition
		}
		// Rollback read-only tx, then create channel meta with epoch.
		// This matches MemoryMapBroker behavior where empty channels have an epoch.
		_ = tx.Rollback(ctx)
		pos, err := e.ensureChannelMeta(ctx, ch)
		if err != nil {
			return MapStateResult{}, err
		}
		return MapStateResult{Position: pos}, nil
	}
	if err != nil {
		return MapStateResult{}, err
	}

	streamPos := StreamPosition{Offset: uint64(topOffset), Epoch: epoch}

	// Check revision epoch
	if opts.Revision != nil && opts.Revision.Epoch != "" {
		if opts.Revision.Epoch != epoch {
			return MapStateResult{Position: streamPos}, ErrorUnrecoverablePosition
		}
	}

	// Single key lookup (for CAS read)
	if opts.Key != "" {
		var p Publication
		var tagsJSON []byte
		var clientID, userID *string
		var connInfo, chanInfo []byte
		err := tx.QueryRow(ctx, `
			SELECT key, data, tags, key_offset, client_id, user_id, conn_info, chan_info
			FROM cf_map_state
			WHERE channel = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())
		`, ch, opts.Key).Scan(&p.Key, &p.Data, &tagsJSON, &p.Offset, &clientID, &userID, &connInfo, &chanInfo)

		if errors.Is(err, pgx.ErrNoRows) {
			return MapStateResult{Position: streamPos}, nil
		}
		if err != nil {
			return MapStateResult{}, err
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
		return MapStateResult{Publications: []*Publication{&p}, Position: streamPos}, nil
	}

	// Limit=0: return only stream position (no entries).
	if opts.Limit == 0 {
		return MapStateResult{Position: streamPos}, nil
	}

	// Paginated state read using keyset pagination (no OFFSET).
	ordered := e.node.ResolveMapChannelOptions(ch).Ordered

	limit := opts.Limit
	if limit < 0 {
		// -1 means no client-imposed limit. Use a large SQL limit as a safety
		// bound — if more entries exist, a cursor is returned for pagination.
		limit = 100000
	}

	var rows pgx.Rows
	if ordered {
		// Ordered: sort by (score DESC, key DESC) to match memory/redis.
		if opts.Cursor == "" {
			rows, err = tx.Query(ctx, `
				SELECT key, data, tags, key_offset, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_state
				WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
				ORDER BY score DESC, key DESC
				LIMIT $2
			`, ch, limit+1)
		} else {
			// Parse cursor: "score\x00key"
			cursorScore, cursorKey := parseOrderedCursor(opts.Cursor)
			cursorScoreInt, _ := strconv.ParseInt(cursorScore, 10, 64)
			rows, err = tx.Query(ctx, `
				SELECT key, data, tags, key_offset, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_state
				WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
				  AND (score < $3 OR (score = $3 AND key < $4))
				ORDER BY score DESC, key DESC
				LIMIT $2
			`, ch, limit+1, cursorScoreInt, cursorKey)
		}
	} else {
		// Unordered: sort by key ASC.
		if opts.Cursor == "" {
			rows, err = tx.Query(ctx, `
				SELECT key, data, tags, key_offset, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_state
				WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
				ORDER BY key
				LIMIT $2
			`, ch, limit+1)
		} else {
			rows, err = tx.Query(ctx, `
				SELECT key, data, tags, key_offset, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_state
				WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW()) AND key > $3
				ORDER BY key
				LIMIT $2
			`, ch, limit+1, opts.Cursor)
		}
	}
	if err != nil {
		return MapStateResult{}, err
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
			return MapStateResult{}, err
		}
		if len(tagsJSON) > 0 {
			_ = json.Unmarshal(tagsJSON, &p.Tags)
		}
		if score != nil {
			p.Score = *score
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
		lastPub := pubs[limit-1]
		if ordered {
			nextCursor = makeOrderedCursor(strconv.FormatInt(lastPub.Score, 10), lastPub.Key)
		} else {
			nextCursor = lastPub.Key
		}
	}

	return MapStateResult{Publications: pubs, Position: streamPos, Cursor: nextCursor}, nil
}

// ReadStream retrieves publications from stream.
func (e *PostgresMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	pool := e.getReadPool()

	// Use REPEATABLE READ transaction to ensure atomic read of meta + stream.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return MapStreamResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Get current meta.
	var topOffset int64
	var epoch string
	err = tx.QueryRow(ctx, `
		SELECT top_offset, epoch FROM cf_map_meta WHERE channel = $1
	`, ch).Scan(&topOffset, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		// Channel doesn't exist yet - create it with an epoch.
		// This matches MemoryMapBroker behavior where empty channels have an epoch.
		_ = tx.Rollback(ctx)
		pos, err := e.ensureChannelMeta(ctx, ch)
		if err != nil {
			return MapStreamResult{}, err
		}
		return MapStreamResult{Position: pos}, nil
	}
	if err != nil {
		return MapStreamResult{}, err
	}

	streamPos := StreamPosition{Offset: uint64(topOffset), Epoch: epoch}

	if opts.Filter.Limit == 0 {
		return MapStreamResult{Position: streamPos}, nil
	}

	sinceOffset := int64(0)
	if opts.Filter.Since != nil {
		sinceOffset = int64(opts.Filter.Since.Offset)
		// Validate epoch if provided.
		if opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != epoch {
			return MapStreamResult{}, ErrorUnrecoverablePosition
		}
	}

	limit := opts.Filter.Limit
	unlimited := limit < 0

	// Query by per-channel offset (not global id).
	var query string
	if opts.Filter.Reverse {
		if opts.Filter.Since == nil {
			sinceOffset = topOffset + 1
		}
		if unlimited {
			query = `
				SELECT key, data, tags, channel_offset, removed, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_stream
				WHERE channel = $1 AND channel_offset < $2
				ORDER BY channel_offset DESC
			`
		} else {
			query = `
				SELECT key, data, tags, channel_offset, removed, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_stream
				WHERE channel = $1 AND channel_offset < $2
				ORDER BY channel_offset DESC
				LIMIT $3
			`
		}
	} else {
		if unlimited {
			query = `
				SELECT key, data, tags, channel_offset, removed, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_stream
				WHERE channel = $1 AND channel_offset > $2
				ORDER BY channel_offset ASC
			`
		} else {
			query = `
				SELECT key, data, tags, channel_offset, removed, score, client_id, user_id, conn_info, chan_info
				FROM cf_map_stream
				WHERE channel = $1 AND channel_offset > $2
				ORDER BY channel_offset ASC
				LIMIT $3
			`
		}
	}

	var rows pgx.Rows
	if unlimited {
		rows, err = tx.Query(ctx, query, ch, sinceOffset)
	} else {
		rows, err = tx.Query(ctx, query, ch, sinceOffset, limit)
	}
	if err != nil {
		return MapStreamResult{}, err
	}
	defer rows.Close()

	// Pre-allocate slice with expected capacity.
	allocHint := limit
	if unlimited {
		allocHint = 64
	}
	pubs := make([]*Publication, 0, allocHint)
	for rows.Next() {
		var p Publication
		var removed bool
		var score *int64
		var tagsJSON []byte
		var clientID, userID *string
		var connInfo, chanInfo []byte
		if err := rows.Scan(&p.Key, &p.Data, &tagsJSON, &p.Offset, &removed, &score, &clientID, &userID, &connInfo, &chanInfo); err != nil {
			return MapStreamResult{}, err
		}
		p.Removed = removed
		if score != nil {
			p.Score = *score
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

	return MapStreamResult{Publications: pubs, Position: streamPos}, nil
}

// Stats returns state statistics.
func (e *PostgresMapBroker) Stats(ctx context.Context, ch string) (MapStats, error) {
	pool := e.getReadPool()

	var count int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM cf_map_state
		WHERE channel = $1 AND (expires_at IS NULL OR expires_at > NOW())
	`, ch).Scan(&count)
	if err != nil {
		return MapStats{}, err
	}

	return MapStats{NumKeys: count}, nil
}

// Clear deletes all data for a channel.
func (e *PostgresMapBroker) Clear(ctx context.Context, ch string, _ MapClearOptions) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `DELETE FROM cf_map_stream WHERE channel = $1`, ch)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM cf_map_state WHERE channel = $1`, ch)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM cf_map_meta WHERE channel = $1`, ch)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM cf_map_idempotency WHERE channel = $1`, ch)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ============================================================================
// WAL Reader Implementation
// ============================================================================

// walShardReader represents a reader for a specific shard of the WAL stream.
type walShardReader struct {
	broker      *PostgresMapBroker
	shardID     int
	conn        *pgconn.PgConn
	running     atomic.Bool
	slotName    string
	publication string
}

// runWALReaderForShard attempts to claim and read a shard using advisory locks.
func (e *PostgresMapBroker) runWALReaderForShard(shardID int) {
	ctx := e.cancelCtx
	lockID := e.conf.WAL.AdvisoryLockBaseID + int64(shardID)
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Try to acquire advisory lock for this shard
		conn, err := e.pool.Acquire(ctx)
		if err != nil {
			e.logError("wal reader: acquire connection", err, shardID)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Try non-blocking advisory lock
		var acquired bool
		err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
		if err != nil {
			conn.Release()
			e.logError("wal reader: try advisory lock", err, shardID)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if !acquired {
			// Another node has this shard, wait and retry
			conn.Release()
			time.Sleep(5 * time.Second) // Check every 5 seconds
			continue
		}

		// We got the lock! Mark shard as claimed
		e.walClaimedShardMu.Lock()
		e.walClaimedShards[shardID] = true
		e.walClaimedShardMu.Unlock()

		e.logInfo("wal reader: claimed shard", shardID)
		backoff = time.Second

		// Run the WAL reader until it fails or we're closed
		err = e.runWALReaderLoop(ctx, shardID, conn)
		if err != nil && ctx.Err() == nil {
			e.logError("wal reader: loop error", err, shardID)
		}

		// Release the lock and mark shard as unclaimed
		e.walClaimedShardMu.Lock()
		delete(e.walClaimedShards, shardID)
		e.walClaimedShardMu.Unlock()

		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)
		conn.Release()

		e.logInfo("wal reader: released shard", shardID)

		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		}
	}
}

// runWALReaderLoop runs the logical replication reader for a shard.
func (e *PostgresMapBroker) runWALReaderLoop(ctx context.Context, shardID int, lockConn *pgxpool.Conn) error {
	slotName := fmt.Sprintf("cf_map_shard_%d", shardID)
	publication := fmt.Sprintf("cf_map_stream_shard_%d", shardID)

	// Create replication connection (separate from regular pool)
	replConnStr := e.conf.ConnString
	if replConnStr[len(replConnStr)-1] != '?' && !strings.Contains(replConnStr, "?") {
		replConnStr += "?replication=database"
	} else {
		replConnStr += "&replication=database"
	}

	replConn, err := pgconn.Connect(ctx, replConnStr)
	if err != nil {
		return fmt.Errorf("connect replication: %w", err)
	}
	defer replConn.Close(ctx)

	// Check if slot exists
	var slotExists bool
	result := replConn.Exec(ctx, fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')", slotName))
	result.NextResult()
	if rr := result.ResultReader(); rr != nil && rr.NextRow() {
		existsStr := string(rr.Values()[0])
		slotExists = existsStr == "t"
	}
	result.Close()

	// Determine starting LSN
	var startLSN pglogrepl.LSN

	if !slotExists {
		// Create slot - use the consistent point returned
		res, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			// Ignore "already exists" error (race with another node)
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("create replication slot: %w", err)
			}
			// Slot was created by another node, query its position
			slotExists = true
		} else {
			startLSN, err = pglogrepl.ParseLSN(res.ConsistentPoint)
			if err != nil {
				return fmt.Errorf("parse consistent point: %w", err)
			}
			e.logInfo("wal reader: created replication slot", shardID)
		}
	}

	if slotExists {
		// Query the slot's confirmed_flush_lsn to start from where we left off
		var lsnStr string
		result := replConn.Exec(ctx, fmt.Sprintf(
			"SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text FROM pg_replication_slots WHERE slot_name = '%s'", slotName))
		result.NextResult()
		if rr := result.ResultReader(); rr != nil && rr.NextRow() {
			lsnStr = string(rr.Values()[0])
		}
		result.Close()

		if lsnStr != "" {
			startLSN, err = pglogrepl.ParseLSN(lsnStr)
			if err != nil {
				return fmt.Errorf("parse slot LSN: %w", err)
			}
		}
	}

	// Start replication from slot's position (no gap possible)
	err = pglogrepl.StartReplication(ctx, replConn, slotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", publication),
			},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	e.logInfo("wal reader: started replication", shardID)

	// Message processing loop
	clientXLogPos := startLSN
	standbyDeadline := time.Now().Add(e.conf.WAL.HeartbeatInterval)
	relations := make(map[uint32]*pglogrepl.RelationMessage)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.closeCh:
			return nil
		default:
		}

		// Send standby status if needed
		if time.Now().After(standbyDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			standbyDeadline = time.Now().Add(e.conf.WAL.HeartbeatInterval)
		}

		// Receive message with timeout
		receiveCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		rawMsg, err := replConn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue // Timeout is expected, send heartbeat
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if rawMsg == nil {
			continue
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					standbyDeadline = time.Time{} // Force immediate reply
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse xlog data: %w", err)
				}

				err = e.processWALMessage(ctx, xld.WALData, relations)
				if err != nil {
					e.logError("wal reader: process message", err, shardID)
				}

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		}
	}
}

// processWALMessage processes a single WAL message and publishes to broker.
func (e *PostgresMapBroker) processWALMessage(ctx context.Context, walData []byte, relations map[uint32]*pglogrepl.RelationMessage) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("parse logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[msg.RelationID] = msg

	case *pglogrepl.InsertMessage:
		rel, ok := relations[msg.RelationID]
		if !ok {
			return nil // Unknown relation, skip
		}
		if rel.RelationName != "cf_map_stream" {
			return nil // Not our table
		}

		return e.processInsertMessage(ctx, rel, msg.Tuple)
	}

	return nil
}

// processInsertMessage processes an INSERT into cf_map_stream and publishes to broker.
func (e *PostgresMapBroker) processInsertMessage(ctx context.Context, rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) error {
	if tuple == nil {
		return nil
	}

	// Build a map of column name -> value
	values := make(map[string][]byte)
	for i, col := range tuple.Columns {
		if i < len(rel.Columns) {
			colName := rel.Columns[i].Name
			if col.DataType == 't' { // text
				values[colName] = col.Data
			} else if col.DataType == 'b' { // binary
				values[colName] = col.Data
			}
		}
	}

	// Extract required fields
	channel := string(values["channel"])
	if channel == "" {
		return nil
	}

	key := string(values["key"])
	data := decodePgBytea(values["data"])
	epoch := string(values["epoch"]) // Denormalized in cf_map_stream for efficiency

	var offset uint64
	if offsetBytes, ok := values["channel_offset"]; ok {
		offset, _ = strconv.ParseUint(string(offsetBytes), 10, 64)
	}

	var removed bool
	if removedBytes, ok := values["removed"]; ok {
		removed = string(removedBytes) == "t" || string(removedBytes) == "true"
	}

	var score int64
	if scoreBytes, ok := values["score"]; ok && len(scoreBytes) > 0 {
		score, _ = strconv.ParseInt(string(scoreBytes), 10, 64)
	}

	// Parse tags
	var tags map[string]string
	if tagsBytes, ok := values["tags"]; ok && len(tagsBytes) > 0 {
		_ = json.Unmarshal(tagsBytes, &tags)
	}

	// Parse previous data for key-based delta.
	var previousData []byte
	if prevBytes, ok := values["previous_data"]; ok {
		previousData = decodePgBytea(prevBytes)
	}

	// Create publication
	pub := &Publication{
		Offset:  offset,
		Epoch:   epoch,
		Key:     key,
		Data:    data,
		Tags:    tags,
		Removed: removed,
		Score:   score,
	}

	// Construct prevPub for key-based delta if previous data available.
	var prevPub *Publication
	useDelta := len(previousData) > 0
	if useDelta {
		prevPub = &Publication{Data: previousData}
	}

	if e.conf.Broker != nil {
		// Multi-node: publish to broker, which will deliver to all subscribed nodes (including this one)
		_, err := e.conf.Broker.Publish(channel, data, PublishOptions{
			Key:     key,
			Removed: removed,
			Score:   score,
			Tags:    tags,
			Offset:  offset,
			Epoch:   epoch,
		})
		if err != nil {
			return fmt.Errorf("broker publish: %w", err)
		}
	} else if e.eventHandler != nil {
		// Single-node: deliver locally only
		_ = e.eventHandler.HandlePublication(channel, pub, StreamPosition{Offset: offset, Epoch: epoch}, useDelta, prevPub)
	}

	return nil
}

// decodePgBytea decodes PostgreSQL bytea hex escape format (\x...) to raw bytes.
func decodePgBytea(data []byte) []byte {
	if len(data) < 2 {
		return data
	}
	// Check for hex format: \x followed by hex pairs
	if data[0] == '\\' && data[1] == 'x' {
		hexStr := string(data[2:])
		decoded := make([]byte, len(hexStr)/2)
		for i := 0; i < len(decoded); i++ {
			b, err := strconv.ParseUint(hexStr[i*2:i*2+2], 16, 8)
			if err != nil {
				return data // Return original on decode error
			}
			decoded[i] = byte(b)
		}
		return decoded
	}
	return data
}


func (e *PostgresMapBroker) logError(msg string, err error, shardID int) {
	if e.node != nil {
		e.node.logger.log(newErrorLogEntry(err, msg, map[string]any{"shard": shardID}))
	}
}

func (e *PostgresMapBroker) logInfo(msg string, shardID int) {
	if e.node != nil && e.node.logEnabled(LogLevelInfo) {
		e.node.logger.log(newLogEntry(LogLevelInfo, msg, map[string]any{"shard": shardID}))
	}
}

// IsWALShardClaimed returns whether this node currently holds the WAL reader lock for a shard.
func (e *PostgresMapBroker) IsWALShardClaimed(shardID int) bool {
	e.walClaimedShardMu.RLock()
	defer e.walClaimedShardMu.RUnlock()
	return e.walClaimedShards[shardID]
}

// WALClaimedShards returns a list of currently claimed WAL reader shard IDs.
func (e *PostgresMapBroker) WALClaimedShards() []int {
	e.walClaimedShardMu.RLock()
	defer e.walClaimedShardMu.RUnlock()
	shards := make([]int, 0, len(e.walClaimedShards))
	for id := range e.walClaimedShards {
		shards = append(shards, id)
	}
	return shards
}

// IsOutboxShardClaimed returns whether this node currently holds the outbox worker lock for a shard.
func (e *PostgresMapBroker) IsOutboxShardClaimed(shardID int) bool {
	e.outboxClaimedShardMu.RLock()
	defer e.outboxClaimedShardMu.RUnlock()
	return e.outboxClaimedShards[shardID]
}

// OutboxClaimedShards returns a list of currently claimed outbox worker shard IDs.
func (e *PostgresMapBroker) OutboxClaimedShards() []int {
	e.outboxClaimedShardMu.RLock()
	defer e.outboxClaimedShardMu.RUnlock()
	shards := make([]int, 0, len(e.outboxClaimedShards))
	for id := range e.outboxClaimedShards {
		shards = append(shards, id)
	}
	return shards
}

// ClaimedShards returns a list of currently claimed shard IDs (WAL or outbox depending on mode).
func (e *PostgresMapBroker) ClaimedShards() []int {
	if e.conf.WAL.Enabled {
		return e.WALClaimedShards()
	}
	return e.OutboxClaimedShards()
}

// runTTLExpirationWorker expires keys with TTL and emits removal events.
func (e *PostgresMapBroker) runTTLExpirationWorker() {
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

func (e *PostgresMapBroker) expireKeys(ctx context.Context) {
	numShards := e.conf.NumShards

	// Find distinct channels with expired keys to resolve per-channel options.
	channelRows, err := e.pool.Query(ctx, `
		SELECT DISTINCT channel FROM cf_map_state
		WHERE expires_at IS NOT NULL AND expires_at <= NOW()
		LIMIT 100
	`)
	if err != nil {
		e.node.logger.log(newErrorLogEntry(err, "error querying channels for key expiration", nil))
		return
	}
	var channels []string
	for channelRows.Next() {
		var ch string
		if err := channelRows.Scan(&ch); err != nil {
			continue
		}
		channels = append(channels, ch)
	}
	channelRows.Close()

	// Process each channel with its own resolved options.
	for _, ch := range channels {
		chOpts := resolveChannelOptions(e.node.ResolveMapChannelOptions, ch)
		var streamTTL, metaTTL *string
		if chOpts.StreamTTL > 0 {
			s := strconv.Itoa(int(chOpts.StreamTTL.Seconds())) + " seconds"
			streamTTL = &s
		}
		if chOpts.MetaTTL > 0 {
			s := strconv.Itoa(int(chOpts.MetaTTL.Seconds())) + " seconds"
			metaTTL = &s
		}

		rows, err := e.pool.Query(ctx, `
			SELECT out_channel, out_key, out_offset, out_epoch
			FROM cf_map_expire_keys($1, $2, $3::interval, $4::interval, $5)
		`, 1000, numShards, streamTTL, metaTTL, ch)
		if err != nil {
			e.node.logger.log(newErrorLogEntry(err, "error in batch key expiration", map[string]any{"channel": ch}))
			continue
		}

		for rows.Next() {
			var expCh, key, epochStr string
			var offset int64
			if err := rows.Scan(&expCh, &key, &offset, &epochStr); err != nil {
				continue
			}
			// Emit removal event to subscribers (outbox/WAL handles delivery to other nodes).
			if e.eventHandler != nil {
				pub := &Publication{
					Key:     key,
					Removed: true,
					Offset:  uint64(offset),
					Time:    time.Now().UnixMilli(),
				}
				pos := StreamPosition{Offset: uint64(offset), Epoch: epochStr}
				if err := e.eventHandler.HandlePublication(expCh, pub, pos, false, nil); err != nil {
					e.node.logger.log(newErrorLogEntry(err, "error handling expired key publication", map[string]any{
						"channel": expCh, "key": key,
					}))
				}
			}
		}
		rows.Close()
	}
}

// runStreamCleanupWorker cleans up expired stream/meta/idempotency entries.
func (e *PostgresMapBroker) runStreamCleanupWorker() {
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

func (e *PostgresMapBroker) cleanupExpiredEntries(ctx context.Context) {
	// Remove expired stream entries (cursor-aware: only delete after delivery).
	// Per-shard comparison: each entry checked against its own shard's cursor.
	if _, err := e.pool.Exec(ctx, `
		DELETE FROM cf_map_stream
		WHERE expires_at IS NOT NULL AND expires_at < NOW()
		AND (
			NOT EXISTS (SELECT 1 FROM cf_map_outbox_cursor)
			OR cf_map_stream.id <= (
				SELECT c.last_processed_id FROM cf_map_outbox_cursor c
				WHERE c.shard_id = cf_map_stream.shard_id
			)
		)
	`); err != nil {
		e.node.logger.log(newErrorLogEntry(err, "error cleaning up expired stream entries", map[string]any{}))
	}

	// Clean up delivered stream entries from abandoned channels.
	// Only delete entries where: (a) already delivered (below shard cursor),
	// (b) no explicit TTL, and (c) the channel's meta no longer exists (expired/removed).
	// If meta still exists, entries may be needed for stream_size recovery.
	// Uses USING to let planner drive from the small cursor table (16 rows)
	// via the (shard_id, id) index, avoiding a full table scan on expires_at IS NULL.
	// In WAL mode (no cursor rows), USING produces no joins — nothing deleted (correct).
	if _, err := e.pool.Exec(ctx, `
		DELETE FROM cf_map_stream s
		USING cf_map_outbox_cursor c
		WHERE c.shard_id = s.shard_id
		AND s.id <= c.last_processed_id
		AND s.expires_at IS NULL
		AND NOT EXISTS (
			SELECT 1 FROM cf_map_meta m WHERE m.channel = s.channel
		)
	`); err != nil {
		e.node.logger.log(newErrorLogEntry(err, "error cleaning up delivered stream entries", map[string]any{}))
	}

	// Remove expired stream metadata
	if _, err := e.pool.Exec(ctx, `
		DELETE FROM cf_map_meta
		WHERE expires_at IS NOT NULL AND expires_at < NOW()
	`); err != nil {
		e.node.logger.log(newErrorLogEntry(err, "error cleaning up expired stream metadata", map[string]any{}))
	}

	// Remove expired idempotency keys
	if _, err := e.pool.Exec(ctx, `
		DELETE FROM cf_map_idempotency
		WHERE expires_at < NOW()
	`); err != nil {
		e.node.logger.log(newErrorLogEntry(err, "error cleaning up expired idempotency keys", map[string]any{}))
	}
}

// ============================================================================
// Outbox Worker Implementation (default delivery mode)
// ============================================================================

// runOutboxWorkerForShard attempts to claim and process a shard using advisory locks.
func (e *PostgresMapBroker) runOutboxWorkerForShard(shardID int) {
	ctx := e.cancelCtx
	lockID := e.conf.Outbox.AdvisoryLockBaseID + int64(shardID)
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Try to acquire advisory lock for this shard
		conn, err := e.pool.Acquire(ctx)
		if err != nil {
			e.logError("outbox worker: acquire connection", err, shardID)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Try non-blocking advisory lock
		var acquired bool
		err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
		if err != nil {
			conn.Release()
			e.logError("outbox worker: try advisory lock", err, shardID)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if !acquired {
			// Another node has this shard, wait and retry
			conn.Release()
			time.Sleep(5 * time.Second) // Check every 5 seconds
			continue
		}

		// We got the lock! Mark shard as claimed
		e.outboxClaimedShardMu.Lock()
		e.outboxClaimedShards[shardID] = true
		e.outboxClaimedShardMu.Unlock()

		e.logInfo("outbox worker: claimed shard", shardID)
		backoff = time.Second

		// Initialize cursor: UPSERT cursor row (idempotent)
		_, err = e.pool.Exec(ctx,
			`INSERT INTO cf_map_outbox_cursor (shard_id, last_processed_id) VALUES ($1, 0)
			 ON CONFLICT (shard_id) DO NOTHING`, shardID)
		if err != nil {
			e.logError("outbox worker: init cursor", err, shardID)
			e.outboxClaimedShardMu.Lock()
			delete(e.outboxClaimedShards, shardID)
			e.outboxClaimedShardMu.Unlock()
			_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)
			conn.Release()
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Read initial cursor position
		var lastProcessedID int64
		err = e.pool.QueryRow(ctx,
			`SELECT last_processed_id FROM cf_map_outbox_cursor WHERE shard_id = $1`,
			shardID).Scan(&lastProcessedID)
		if err != nil {
			e.logError("outbox worker: read cursor", err, shardID)
			e.outboxClaimedShardMu.Lock()
			delete(e.outboxClaimedShards, shardID)
			e.outboxClaimedShardMu.Unlock()
			_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)
			conn.Release()
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Run the outbox processor until it fails or we're closed
		err = e.processOutboxLoop(ctx, shardID, conn, lastProcessedID)
		if err != nil && ctx.Err() == nil {
			e.logError("outbox worker: loop error", err, shardID)
		}

		// Release the lock and mark shard as unclaimed
		e.outboxClaimedShardMu.Lock()
		delete(e.outboxClaimedShards, shardID)
		e.outboxClaimedShardMu.Unlock()

		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)
		conn.Release()

		e.logInfo("outbox worker: released shard", shardID)

		select {
		case <-e.closeCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff = min(backoff*2, maxBackoff)
		}
	}
}

// processOutboxLoop is the main processing loop for an outbox worker.
func (e *PostgresMapBroker) processOutboxLoop(ctx context.Context, shardID int, conn *pgxpool.Conn, cursor int64) error {
	pollInterval := e.conf.Outbox.PollInterval

	for {
		select {
		case <-e.closeCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Process a batch of stream entries
		processed, newCursor, err := e.processOutboxBatch(ctx, shardID, cursor)
		if err != nil {
			return fmt.Errorf("process batch: %w", err)
		}
		if newCursor > cursor {
			cursor = newCursor
		}

		// If we processed a full batch, immediately try again (more may be waiting)
		if processed >= e.conf.Outbox.BatchSize {
			continue
		}

		// Otherwise, wait before polling again
		select {
		case <-e.closeCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// streamRow represents a row from cf_map_stream used for outbox delivery.
type streamRow struct {
	id            int64
	shardID       int
	channel       string
	channelOffset int64
	epoch         string
	key           string
	data          []byte
	tags          map[string]string
	removed       bool
	score         *int64 // Nullable
	clientID      *string
	userID        *string
	connInfo      []byte
	chanInfo      []byte
	previousData  []byte
}

// processOutboxBatch fetches and processes a batch of stream entries using cursor-based delivery.
func (e *PostgresMapBroker) processOutboxBatch(ctx context.Context, shardID int, cursor int64) (int, int64, error) {
	batchSize := e.conf.Outbox.BatchSize

	rows, err := e.pool.Query(ctx, `
		SELECT id, shard_id, channel, channel_offset, epoch, key, data, tags, removed, score,
			   client_id, user_id, conn_info, chan_info, previous_data
		FROM cf_map_stream
		WHERE shard_id = $1 AND id > $2
		ORDER BY id
		LIMIT $3
	`, shardID, cursor, batchSize)
	if err != nil {
		return 0, cursor, fmt.Errorf("query stream: %w", err)
	}

	var entries []streamRow
	var maxID int64
	for rows.Next() {
		var row streamRow
		var tagsJSON []byte
		err := rows.Scan(
			&row.id, &row.shardID, &row.channel, &row.channelOffset, &row.epoch,
			&row.key, &row.data, &tagsJSON, &row.removed, &row.score,
			&row.clientID, &row.userID, &row.connInfo, &row.chanInfo, &row.previousData,
		)
		if err != nil {
			rows.Close()
			return 0, cursor, fmt.Errorf("scan row: %w", err)
		}
		if len(tagsJSON) > 0 {
			_ = json.Unmarshal(tagsJSON, &row.tags)
		}
		entries = append(entries, row)
		if row.id > maxID {
			maxID = row.id
		}
	}
	rows.Close()

	if len(entries) == 0 {
		return 0, cursor, nil
	}

	// Process each entry (publish to broker/locally)
	for _, entry := range entries {
		if err := e.handleOutboxEntry(ctx, entry); err != nil {
			e.logError("outbox worker: handle entry", err, shardID)
			// Continue processing other entries
		}
	}

	// Advance cursor
	_, err = e.pool.Exec(ctx,
		`UPDATE cf_map_outbox_cursor SET last_processed_id = $1, updated_at = NOW()
		 WHERE shard_id = $2`, maxID, shardID)
	if err != nil {
		return 0, cursor, fmt.Errorf("advance cursor: %w", err)
	}

	return len(entries), maxID, nil
}

// handleOutboxEntry processes a single stream entry by publishing to broker/locally.
// This shares the same delivery logic as the WAL reader.
func (e *PostgresMapBroker) handleOutboxEntry(_ context.Context, entry streamRow) error {
	// Handle nullable score
	var score int64
	if entry.score != nil {
		score = *entry.score
	}

	// Create publication
	pub := &Publication{
		Offset:  uint64(entry.channelOffset),
		Epoch:   entry.epoch,
		Key:     entry.key,
		Data:    entry.data,
		Tags:    entry.tags,
		Removed: entry.removed,
		Score:   score,
	}

	// Add client info if present
	if entry.clientID != nil || entry.userID != nil {
		pub.Info = &ClientInfo{
			ConnInfo: entry.connInfo,
			ChanInfo: entry.chanInfo,
		}
		if entry.clientID != nil {
			pub.Info.ClientID = *entry.clientID
		}
		if entry.userID != nil {
			pub.Info.UserID = *entry.userID
		}
	}

	streamPos := StreamPosition{Offset: uint64(entry.channelOffset), Epoch: entry.epoch}

	// Construct prevPub for key-based delta if previous data available.
	var prevPub *Publication
	useDelta := len(entry.previousData) > 0
	if useDelta {
		prevPub = &Publication{Data: entry.previousData}
	}

	if e.conf.Broker != nil {
		// Multi-node: publish to broker, which will deliver to all subscribed nodes (including this one)
		_, err := e.conf.Broker.Publish(entry.channel, entry.data, PublishOptions{
			Key:     entry.key,
			Removed: entry.removed,
			Score:   score,
			Tags:    entry.tags,
			Offset:  uint64(entry.channelOffset),
			Epoch:   entry.epoch,
		})
		if err != nil {
			return fmt.Errorf("broker publish: %w", err)
		}
	} else if e.eventHandler != nil {
		// Single-node: deliver locally only
		_ = e.eventHandler.HandlePublication(entry.channel, pub, streamPos, useDelta, prevPub)
	}

	return nil
}
