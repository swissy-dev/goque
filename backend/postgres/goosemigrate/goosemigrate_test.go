package goosemigrate_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/swissy-dev/goque/backend/postgres"
	"github.com/swissy-dev/goque/backend/postgres/goosemigrate"
	"github.com/swissy-dev/goque/backend/postgres/pgxv5"
	"github.com/swissy-dev/goque/backend/postgres/postgrestest"
)

func newDB(t *testing.T) (*sql.DB, postgres.Driver) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), postgrestest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	return db, pgxv5.New(pool)
}

func TestUpCreatesTableAndIndexes(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if !tableExists(ctx, t, db, schema, "goque_job") {
		t.Fatalf("%s.goque_job was not created", schema)
	}
	for _, idx := range []string{"goque_job_fetch", "goque_job_move", "goque_job_rescue", "goque_job_clean"} {
		if !indexExists(ctx, t, db, schema, idx) {
			t.Fatalf("index %s was not created; the no-transaction migration did not run", idx)
		}
	}
}

func TestUpIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	for i := 0; i < 3; i++ {
		if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	if n := appliedCount(ctx, t, db, schema); n != 3 {
		t.Fatalf("%d migrations recorded after three runs, want 3 (goose's version-0 initialisation row plus the two real migrations)", n)
	}
}

func TestUpRejectsAnInvalidSchemaBeforeTouchingTheDatabase(t *testing.T) {
	err := goosemigrate.Up(context.Background(), nil, goosemigrate.WithSchema("my-schema"))
	if !errors.Is(err, postgres.ErrInvalidSchema) {
		t.Fatalf("Up must validate the schema before using the handle, got %v", err)
	}
}

func TestUpRejectsAMixedCaseSchema(t *testing.T) {
	err := goosemigrate.Up(context.Background(), nil, goosemigrate.WithSchema("MixedCase"))
	if !errors.Is(err, postgres.ErrInvalidSchema) {
		t.Fatalf("a mixed-case schema must be refused before it can split goque's tables across two schemas, got %v", err)
	}
}

func TestTwoSchemasDoNotObserveEachOther(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	a := postgrestest.Schema(ctx, t, d)
	b := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(a)); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(b)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO "`+a+`".goque_job
		(kind, queue, payload, state, scheduled_at_ns, priority_at_ns, max_attempts, created_at)
		VALUES ('k', 'q', '{}', 'available', 0, 0, 3, now())`); err != nil {
		t.Fatal(err)
	}
	if n := jobCount(ctx, t, db, a); n != 1 {
		t.Fatalf("schema %s holds %d jobs, want 1", a, n)
	}
	if n := jobCount(ctx, t, db, b); n != 0 {
		t.Fatalf("schema %s holds %d jobs, want 0 — one schema must not observe another's rows", b, n)
	}
}

func TestUpDoesNotModifySearchPath(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	var before string
	if err := db.QueryRowContext(ctx, "SHOW search_path").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	var after string
	if err := db.QueryRowContext(ctx, "SHOW search_path").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("search_path changed from %q to %q; it is connection-local and must never be touched", before, after)
	}
}

func tableExists(ctx context.Context, t *testing.T, db *sql.DB, schema, table string) bool {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`,
		schema, table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func indexExists(ctx context.Context, t *testing.T, db *sql.DB, schema, index string) bool {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`,
		schema, index).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func appliedCount(ctx context.Context, t *testing.T, db *sql.DB, schema string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM "`+schema+`".goque_migration`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func jobCount(ctx context.Context, t *testing.T, db *sql.DB, schema string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM "`+schema+`".goque_job`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
