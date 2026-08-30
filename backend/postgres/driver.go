// Package postgres implements goque's PostgreSQL backend. pgx v5, through the
// sibling pgxv5 package, is its only supported driver.
//
// Every SQL statement the backend issues lives in this package. The Driver
// seam isolates that SQL from pgx's connection mechanics, which keeps this
// package testable against a narrow interface instead of a concrete pgx
// type.
package postgres

import "context"

// Driver is the connection seam between this package and a PostgreSQL client
// library. Implementations adapt a pool; they never carry SQL of their own.
//
// Every method must return once its ctx is done. goque's completer bounds each
// call with a per-attempt timeout and its Stop drains within a deadline, both
// of which depend on cancellation being honoured.
//
// Concurrency: a Driver must be safe for concurrent use by multiple
// goroutines. The dispatcher, the heartbeat loop, three maintenance loops,
// and the completer each hold and use the same Driver at once. A Tx and a
// Conn are scoped to whichever caller obtained them and need not be safe for
// concurrent use.
//
// Parameter portability: every batch parameter this package sends is passed
// as one JSON document bound as text and cast to its real type server-side,
// and every array-typed output column is projected through to_jsonb before it
// is scanned. Array-typed parameters such as bigint[] or jsonb[] are not
// portable, because a Go slice is not a database/sql/driver.Value; the
// attempted_by TEXT[] column is what makes this rule necessary.
type Driver interface {
	// Query runs a statement that returns rows. The caller closes the Rows.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	// Exec runs a statement that returns no rows, reporting how many rows it
	// affected.
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	// Begin starts a transaction this package controls.
	Begin(ctx context.Context) (Tx, error)
	// Conn pins one pooled connection for the caller's exclusive use, for work
	// that must not move between sessions — holding a LISTEN, or a
	// session-scoped advisory lock.
	Conn(ctx context.Context) (Conn, error)
	// InTx adapts a caller-supplied transaction into a Driver scoped to it, so
	// a job can be enqueued or completed inside the transaction that created
	// the reason for it. The shipped pgx v5 adapter accepts a pgx.Tx; it
	// returns backend.ErrInvalidTx when tx is anything else.
	InTx(ctx context.Context, tx any) (Driver, error)
	// SQLState returns the five-character SQLSTATE code a database error
	// carries, or an empty string when err is nil or did not come from the
	// server. It is reserved: no shipped code in this package calls it yet.
	// It stays on Driver anyway because adding a method to this interface
	// later would break every third-party implementation, and this is the
	// seam a caller would need to recognise conditions like a unique
	// violation or a serialization failure without matching on message text,
	// which varies by server version and locale.
	SQLState(err error) string
}

// Tx is a Driver scoped to one transaction.
type Tx interface {
	Driver
	// Commit commits the transaction. Rollback is safe to call after a
	// successful Commit; see Rollback.
	Commit(ctx context.Context) error
	// Rollback is safe to call after Commit and reports no error in that case,
	// so callers may defer it unconditionally.
	Rollback(ctx context.Context) error
}

// Conn is a Driver bound to one pinned connection. Its Begin starts a
// transaction on that same connection rather than reaching back to the pool,
// so work inside the transaction observes any session state the caller has
// set on it.
type Conn interface {
	Driver
	// Close returns the connection to its pool.
	Close(ctx context.Context) error
}

// Rows is the subset of a result set this package reads. It matches both
// *sql.Rows and pgx.Rows.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}
