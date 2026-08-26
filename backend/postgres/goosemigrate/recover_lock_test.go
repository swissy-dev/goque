package goosemigrate

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/swissy-dev/goque/backend/postgres/pgxv5"
	"github.com/swissy-dev/goque/backend/postgres/postgrestest"
)

func withFastLockRetry(t *testing.T) <-chan struct{} {
	t.Helper()
	prevInterval, prevAttempts, prevHook := lockRetryInterval, lockRetryMaxAttempts, lockRetryHook
	lockRetryInterval = 25 * time.Millisecond
	lockRetryMaxAttempts = 400
	retried := make(chan struct{}, 1024)
	lockRetryHook = func() {
		select {
		case retried <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() {
		lockRetryInterval, lockRetryMaxAttempts, lockRetryHook = prevInterval, prevAttempts, prevHook
	})
	return retried
}

func acquireSimulatedMigrator(ctx context.Context, t *testing.T, db *sql.DB, id int64) *sql.Conn {
	t.Helper()
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var locked bool
	if err := holder.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, id).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("test setup: could not acquire the migration lock to simulate a concurrent migrator")
	}
	t.Cleanup(func() {
		_, _ = holder.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, id)
		_ = holder.Close()
	})
	return holder
}

func TestUpRetriesTheRecoveryLockAndRecoversOnceItIsFree(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, postgrestest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	schema := postgrestest.Schema(ctx, t, pgxv5.New(pool))

	if err := Up(ctx, db, WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = ('"`+schema+`".goque_job_move')::regclass`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM "`+schema+`".goque_migration WHERE version_id = 2`); err != nil {
		t.Fatal(err)
	}

	retried := withFastLockRetry(t)

	id := lockID(schema)
	holder := acquireSimulatedMigrator(ctx, t, db, id)

	done := make(chan error, 1)
	go func() {
		done <- Up(ctx, db, WithSchema(schema))
	}()

	select {
	case <-retried:
	case <-time.After(2 * time.Second):
		t.Fatal("Up never retried the recovery lock while another session held it; it must poll with pg_try_advisory_lock, not give up or block on pg_advisory_lock")
	}

	select {
	case err := <-done:
		t.Fatalf("Up returned before the other session released the lock, got %v", err)
	default:
	}

	if _, err := holder.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, id); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Up must succeed once the lock is free, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Up did not return after the lock was released")
	}

	var valid bool
	if err := db.QueryRowContext(ctx, `SELECT indisvalid FROM pg_index
		WHERE indexrelid = ('"`+schema+`".goque_job_move')::regclass`).Scan(&valid); err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Fatal("goque_job_move did not come back valid after Up recovered from another session's debris; it must not be recorded applied while still invalid")
	}
}

func TestUpAbortsWhenTheContextIsCancelledWhileWaitingForTheLock(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, postgrestest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	schema := postgrestest.Schema(ctx, t, pgxv5.New(pool))

	if err := Up(ctx, db, WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM "`+schema+`".goque_migration WHERE version_id = 2`); err != nil {
		t.Fatal(err)
	}

	id := lockID(schema)
	acquireSimulatedMigrator(ctx, t, db, id)

	boundedCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := Up(boundedCtx, db, WithSchema(schema)); err == nil {
		t.Fatal("Up must fail when the context is cancelled while waiting for the recovery lock")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Up took %s to respect a cancelled context while waiting for the recovery lock; it must abort promptly", elapsed)
	}
}

func TestUpRunsWithASingleOpenConnection(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, postgrestest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	schema := postgrestest.Schema(ctx, t, pgxv5.New(pool))

	if err := Up(ctx, db, WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = ('"`+schema+`".goque_job_move')::regclass`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM "`+schema+`".goque_migration WHERE version_id = 2`); err != nil {
		t.Fatal(err)
	}

	db.SetMaxOpenConns(1)

	boundedCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := Up(boundedCtx, db, WithSchema(schema)); err != nil {
		t.Fatalf("Up must not hang or fail when the pool allows only one open connection, got %v", err)
	}

	var valid bool
	if err := db.QueryRowContext(ctx, `SELECT indisvalid FROM pg_index
		WHERE indexrelid = ('"`+schema+`".goque_job_move')::regclass`).Scan(&valid); err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Fatal("goque_job_move was not recovered under a single-connection pool")
	}
}
