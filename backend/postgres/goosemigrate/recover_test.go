package goosemigrate_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/swissy-dev/goque/backend/postgres/goosemigrate"
	"github.com/swissy-dev/goque/backend/postgres/postgrestest"
)

func TestUpRecoversFromAnInvalidIndex(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = ('"`+schema+`".goque_job_fetch')::regclass`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM "`+schema+`".goque_migration WHERE version_id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatalf("Up must recover from an invalid index left by an interrupted CREATE INDEX CONCURRENTLY, got %v", err)
	}
	if n := invalidIndexCount(ctx, t, db, schema); n != 0 {
		t.Fatalf("%d invalid indexes remain after recovery, want 0", n)
	}
}

func TestUpDoesNotDropAnUnrelatedInvalidIndex(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE "`+schema+`".app_orders (id int)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX app_orders_id_idx ON "`+schema+`".app_orders (id)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = ('"`+schema+`".app_orders_id_idx')::regclass`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM "`+schema+`".goque_migration WHERE version_id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if !indexExists(ctx, t, db, schema, "app_orders_id_idx") {
		t.Fatal("Up dropped an invalid index belonging to the application; recovery must be scoped to goque's own index names")
	}
}

func TestUpDoesNotDropASameNamedInvalidIndexOnAnotherTable(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY "`+schema+`".goque_job_fetch`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE "`+schema+`".app_widgets (id int)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX goque_job_fetch ON "`+schema+`".app_widgets (id)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = ('"`+schema+`".goque_job_fetch')::regclass`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM "`+schema+`".goque_migration WHERE version_id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if !indexExists(ctx, t, db, schema, "goque_job_fetch") {
		t.Fatal("recovery dropped an invalid index named goque_job_fetch that belongs to a different table; sweeping must scope by indrelid, not by name alone")
	}
	var onWidgets bool
	if err := db.QueryRowContext(ctx, `SELECT count(*) > 0 FROM pg_indexes
		WHERE schemaname = $1 AND indexname = 'goque_job_fetch' AND tablename = 'app_widgets'`,
		schema).Scan(&onWidgets); err != nil {
		t.Fatal(err)
	}
	if !onWidgets {
		t.Fatal("goque_job_fetch survived on the wrong table; recovery must have dropped the one on app_widgets and left goque_job without its index")
	}
}

func TestUpLeavesHealthyIndexesInPlaceOnASecondRun(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	for _, idx := range []string{"goque_job_fetch", "goque_job_move", "goque_job_rescue", "goque_job_clean"} {
		if !indexExists(ctx, t, db, schema, idx) {
			t.Fatalf("index %s is missing after a second Up; recovery must never touch an index whose migration goose already applied", idx)
		}
	}
}

func TestUpRecoversWhenLaterStatementsWereNeverReached(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY "`+schema+`".goque_job_rescue`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY "`+schema+`".goque_job_clean`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM "`+schema+`".goque_migration WHERE version_id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatalf("Up must rebuild indexes an interruption never reached, got %v", err)
	}
	for _, idx := range []string{"goque_job_fetch", "goque_job_move", "goque_job_rescue", "goque_job_clean"} {
		if !indexExists(ctx, t, db, schema, idx) {
			t.Fatalf("index %s is missing after recovering from an interruption before statement 3", idx)
		}
	}
}

func TestUpLeavesAnAlreadyAppliedMigrationsInvalidIndexAlone(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = ('"`+schema+`".goque_job_rescue')::regclass`); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if !indexExists(ctx, t, db, schema, "goque_job_rescue") {
		t.Fatal("Up dropped an invalid index belonging to an already-applied migration; goose will never rebuild it, so recovery must leave it for the operator instead of destroying it")
	}
}

func invalidIndexCount(ctx context.Context, t *testing.T, db *sql.DB, schema string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace ns ON ns.oid = c.relnamespace
		WHERE ns.nspname = $1 AND NOT i.indisvalid`, schema).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
