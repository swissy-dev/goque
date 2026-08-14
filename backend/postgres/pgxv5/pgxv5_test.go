package pgxv5_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/postgres"
	"github.com/swissy-dev/goque/backend/postgres/pgxv5"
	"github.com/swissy-dev/goque/backend/postgres/postgrestest"
)

func newDriver(t *testing.T) postgres.Driver {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), postgrestest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pgxv5.New(pool)
}

func TestDriverQueryAndExec(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t)
	schema := postgrestest.Schema(ctx, t, d)

	if _, err := d.Exec(ctx, `CREATE TABLE "`+schema+`".t (n int)`); err != nil {
		t.Fatal(err)
	}
	n, err := d.Exec(ctx, `INSERT INTO "`+schema+`".t (n) VALUES ($1), ($2)`, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("Exec reported %d rows affected, want 2", n)
	}
	rows, err := d.Query(ctx, `SELECT n FROM "`+schema+`".t ORDER BY n`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("scanned %v, want [1 2]", got)
	}
}

func TestDriverTxCommitAndRollback(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t)
	schema := postgrestest.Schema(ctx, t, d)
	if _, err := d.Exec(ctx, `CREATE TABLE "`+schema+`".t (n int)`); err != nil {
		t.Fatal(err)
	}

	tx, err := d.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO "`+schema+`".t (n) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countRows(ctx, t, d, schema); n != 0 {
		t.Fatalf("a rolled-back insert left %d rows, want 0", n)
	}

	tx, err = d.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO "`+schema+`".t (n) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback after Commit must be a no-op so callers can defer it, got %v", err)
	}
	if n := countRows(ctx, t, d, schema); n != 1 {
		t.Fatalf("a committed insert is not visible: found %d rows, want 1", n)
	}
}

func TestDriverConnIsPinned(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t)
	conn, err := d.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	var first, second int
	rows, err := conn.Query(ctx, "SELECT pg_backend_pid()")
	if err != nil {
		t.Fatal(err)
	}
	scanOne(t, rows, &first)
	rows, err = conn.Query(ctx, "SELECT pg_backend_pid()")
	if err != nil {
		t.Fatal(err)
	}
	scanOne(t, rows, &second)
	if first != second {
		t.Fatalf("Conn returned backend pids %d and %d; the online migration phase requires one pinned session", first, second)
	}
}

func TestConnBeginRunsOnTheSameSession(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t)
	conn, err := d.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	var connPID, txPID int
	rows, err := conn.Query(ctx, "SELECT pg_backend_pid()")
	if err != nil {
		t.Fatal(err)
	}
	scanOne(t, rows, &connPID)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	rows, err = tx.Query(ctx, "SELECT pg_backend_pid()")
	if err != nil {
		t.Fatal(err)
	}
	scanOne(t, rows, &txPID)
	if connPID != txPID {
		t.Fatalf("Conn.Begin ran on session %d, not the pinned session %d; a session-scoped advisory lock would not cover it", txPID, connPID)
	}
}

func TestInTxRejectsForeignTransaction(t *testing.T) {
	d := newDriver(t)
	if _, err := d.InTx(context.Background(), "not a transaction"); !errors.Is(err, backend.ErrInvalidTx) {
		t.Fatalf("InTx on a foreign type must return ErrInvalidTx, got %v", err)
	}
}

func TestSQLStateReportsThePostgresCode(t *testing.T) {
	ctx := context.Background()
	d := newDriver(t)
	schema := postgrestest.Schema(ctx, t, d)

	_, err := d.Exec(ctx, `SELECT * FROM "`+schema+`".table_that_does_not_exist`)
	if err == nil {
		t.Fatal("querying a missing table must fail")
	}
	if got := d.SQLState(err); got != "42P01" {
		t.Fatalf("SQLState = %q, want 42P01 for an undefined table", got)
	}

	if _, err := d.Exec(ctx, `CREATE TABLE "`+schema+`".u (id int PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Exec(ctx, `INSERT INTO "`+schema+`".u (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	_, err = d.Exec(ctx, `INSERT INTO "`+schema+`".u (id) VALUES (1)`)
	if err == nil {
		t.Fatal("a duplicate primary key must fail")
	}
	if got := d.SQLState(err); got != "23505" {
		t.Fatalf("SQLState = %q, want 23505 for a unique violation", got)
	}
}

func TestSQLStateIsEmptyForANonDatabaseError(t *testing.T) {
	d := newDriver(t)
	if got := d.SQLState(errors.New("not from the database")); got != "" {
		t.Fatalf("SQLState = %q, want an empty string for a non-database error", got)
	}
	if got := d.SQLState(nil); got != "" {
		t.Fatalf("SQLState(nil) = %q, want an empty string", got)
	}
}

func countRows(ctx context.Context, t *testing.T, d postgres.Driver, schema string) int {
	t.Helper()
	rows, err := d.Query(ctx, `SELECT count(*) FROM "`+schema+`".t`)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	scanOne(t, rows, &n)
	return n
}

func scanOne(t *testing.T, rows postgres.Rows, dest ...any) {
	t.Helper()
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected one row, got none: %v", rows.Err())
	}
	if err := rows.Scan(dest...); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
