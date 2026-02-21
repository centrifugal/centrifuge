package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// runAsLeader ensures fn runs on exactly one node using PostgreSQL advisory locks.
// Other nodes block on pg_advisory_lock until the current leader dies or releases.
// If pool is nil, fn runs unconditionally (no leader election, single-node mode).
func runAsLeader(ctx context.Context, pool *pgxpool.Pool, lockID int64, name string, fn func(ctx context.Context)) {
	if pool == nil {
		fn(ctx)
		return
	}

	for ctx.Err() == nil {
		if err := tryLeader(ctx, pool, lockID, name, fn); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] leader session ended: %v", name, err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func tryLeader(ctx context.Context, pool *pgxpool.Pool, lockID int64, name string, fn func(ctx context.Context)) error {
	// Acquire a dedicated connection to hold the advisory lock.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// pg_advisory_lock blocks until the lock is available. The lock is
	// session-level: if the holder's process dies, PG closes the session
	// and automatically releases the lock.
	_, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID)
	if err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}

	log.Printf("[%s] this node is now the leader", name)

	fnCtx, fnCancel := context.WithCancel(ctx)
	defer fnCancel()

	done := make(chan struct{})
	go func() {
		fn(fnCtx)
		close(done)
	}()

	// Monitor connection health — if the PG connection drops, the advisory
	// lock is released and another node may acquire it. We must stop fn.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			fnCancel()
			<-done
			return ctx.Err()
		case <-ticker.C:
			if err := conn.Conn().Ping(ctx); err != nil {
				fnCancel()
				<-done
				return fmt.Errorf("connection lost: %w", err)
			}
		}
	}
}
