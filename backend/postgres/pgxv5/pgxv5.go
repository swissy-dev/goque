// Package pgxv5 adapts a pgx v5 pool to goque's PostgreSQL driver seam. It is
// the default and recommended driver: pgx supports LISTEN, so a client built
// on it can be woken by a notification rather than a poll tick.
//
// It carries no SQL. Every statement goque issues lives in the postgres
// package, which is what makes this driver and the database/sql one
// interchangeable.
package pgxv5

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/postgres"
)

// New wraps a pgx pool as a goque PostgreSQL driver.
//
// Size the pool for goque's own concurrent holders — the dispatcher, the
// heartbeat loop, three maintenance loops, and the completer can each hold one
// at a time — with a floor of 6 plus one per configured queue, on top of
// whatever the application itself uses.
func New(pool *pgxpool.Pool) postgres.Driver {
	return &driver{pool: pool}
}

type driver struct {
	pool *pgxpool.Pool
	tx   pgx.Tx
	conn *pgxpool.Conn
}

func (d *driver) Query(ctx context.Context, sql string, args ...any) (postgres.Rows, error) {
	switch {
	case d.tx != nil:
		return wrapRows(d.tx.Query(ctx, sql, args...))
	case d.conn != nil:
		return wrapRows(d.conn.Query(ctx, sql, args...))
	default:
		return wrapRows(d.pool.Query(ctx, sql, args...))
	}
}

func (d *driver) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	switch {
	case d.tx != nil:
		tag, err := d.tx.Exec(ctx, sql, args...)
		return tag.RowsAffected(), err
	case d.conn != nil:
		tag, err := d.conn.Exec(ctx, sql, args...)
		return tag.RowsAffected(), err
	default:
		tag, err := d.pool.Exec(ctx, sql, args...)
		return tag.RowsAffected(), err
	}
}

func (d *driver) Begin(ctx context.Context) (postgres.Tx, error) {
	if d.tx != nil {
		return nil, errors.New("goque/pgxv5: Begin on a transaction-scoped driver")
	}
	if d.conn != nil {
		t, err := d.conn.Begin(ctx)
		if err != nil {
			return nil, err
		}
		return &tx{driver: driver{pool: d.pool, tx: t}, t: t}, nil
	}
	t, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &tx{driver: driver{pool: d.pool, tx: t}, t: t}, nil
}

func (d *driver) Conn(ctx context.Context) (postgres.Conn, error) {
	if d.tx != nil || d.conn != nil {
		return nil, errors.New("goque/pgxv5: Conn on a scoped driver")
	}
	c, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{driver: driver{pool: d.pool, conn: c}, c: c}, nil
}

func (d *driver) InTx(_ context.Context, v any) (postgres.Driver, error) {
	t, ok := v.(pgx.Tx)
	if !ok {
		return nil, fmt.Errorf("%w: pgxv5 needs a pgx.Tx, got %T", backend.ErrInvalidTx, v)
	}
	return &driver{pool: d.pool, tx: t}, nil
}

func (d *driver) SQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

type tx struct {
	driver
	t        pgx.Tx
	finished bool
}

func (t *tx) Commit(ctx context.Context) error {
	if t.finished {
		return nil
	}
	t.finished = true
	return t.t.Commit(ctx)
}

func (t *tx) Rollback(ctx context.Context) error {
	if t.finished {
		return nil
	}
	t.finished = true
	if err := t.t.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return err
	}
	return nil
}

type conn struct {
	driver
	c *pgxpool.Conn
}

func (c *conn) Close(context.Context) error {
	c.c.Release()
	return nil
}

type rows struct {
	r pgx.Rows
}

func wrapRows(r pgx.Rows, err error) (postgres.Rows, error) {
	if err != nil {
		return nil, err
	}
	return &rows{r: r}, nil
}

func (r *rows) Next() bool             { return r.r.Next() }
func (r *rows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *rows) Err() error             { return r.r.Err() }
func (r *rows) Close() error           { r.r.Close(); return nil }
