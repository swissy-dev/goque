package goosemigrate_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/swissy-dev/goque/backend/postgres/goosemigrate"
	"github.com/swissy-dev/goque/backend/postgres/postgrestest"
)

func TestUpRejectsANewerSchema(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO "`+schema+`".goque_migration (version_id, is_applied, tstamp) VALUES (9999, true, now())`); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); !errors.Is(err, goosemigrate.ErrVersionSkew) {
		t.Fatalf("an old binary must refuse a newer schema, got %v", err)
	}
}

func TestUpAcceptsASchemaAtTheKnownVersion(t *testing.T) {
	ctx := context.Background()
	db, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatalf("a schema at exactly the known version must be accepted, got %v", err)
	}
}

func TestUpRefusesAStatementTimeout(t *testing.T) {
	ctx := context.Background()
	_, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	pool, err := pgxpool.New(ctx, dsnWithStatementTimeout(t, 25000))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	timed := stdlib.OpenDBFromPool(pool)
	defer timed.Close()

	err = goosemigrate.Up(ctx, timed, goosemigrate.WithSchema(schema))
	if !errors.Is(err, goosemigrate.ErrStatementTimeout) {
		t.Fatalf("Up must refuse a handle with a statement_timeout, got %v", err)
	}
	if !strings.Contains(err.Error(), "statement_timeout") {
		t.Fatalf("the error must name the setting the operator has to change, got %q", err)
	}
}

func TestUpAllowsAStatementTimeoutWhenAsked(t *testing.T) {
	ctx := context.Background()
	_, d := newDB(t)
	schema := postgrestest.Schema(ctx, t, d)

	pool, err := pgxpool.New(ctx, dsnWithStatementTimeout(t, 25000))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	timed := stdlib.OpenDBFromPool(pool)
	defer timed.Close()

	if err := goosemigrate.Up(ctx, timed,
		goosemigrate.WithSchema(schema),
		goosemigrate.WithAllowStatementTimeout()); err != nil {
		t.Fatalf("the escape hatch must let a timed handle through, got %v", err)
	}
}

func dsnWithStatementTimeout(t *testing.T, ms int) string {
	t.Helper()
	dsn := postgrestest.DSN(t)
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%sstatement_timeout=%d", dsn, sep, ms)
}
