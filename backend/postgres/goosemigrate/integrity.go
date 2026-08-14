package goosemigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/swissy-dev/goque/backend/postgres"
)

// ErrVersionSkew reports a database migrated beyond what this binary knows.
// Running an old binary against a newer schema would let it write rows it does
// not understand, so it is refused. It is the case a rolling deploy produces
// whenever an old pod restarts after a new one has migrated.
var ErrVersionSkew = errors.New("goque/goosemigrate: database schema is newer than this binary")

// ErrStatementTimeout reports a database handle whose statements are bounded
// by a statement_timeout. One of goque's migrations builds indexes with CREATE
// INDEX CONCURRENTLY, which a timeout cancels partway, leaving an invalid
// index that blocks every later attempt. Migrate with a handle that has no
// timeout, or pass WithAllowStatementTimeout to proceed anyway.
var ErrStatementTimeout = errors.New("goque/goosemigrate: statement_timeout is set on this connection")

// WithAllowStatementTimeout proceeds even when the connection has a
// statement_timeout. Use it only when you know the timeout exceeds the longest
// index build the migration will perform.
func WithAllowStatementTimeout() Option {
	return func(c *config) { c.allowStatementTimeout = true }
}

func checkStatementTimeout(ctx context.Context, db *sql.DB, cfg config) error {
	if cfg.allowStatementTimeout {
		return nil
	}
	var setting string
	if err := db.QueryRowContext(ctx, "SHOW statement_timeout").Scan(&setting); err != nil {
		return err
	}
	if setting == "" || setting == "0" {
		return nil
	}
	return fmt.Errorf("%w (%s); migrate with a connection that has none, or pass WithAllowStatementTimeout", ErrStatementTimeout, setting)
}

func highestVersionIn(fsys fs.FS) (int64, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return 0, err
	}
	highest := int64(0)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		v, err := parseVersion(e.Name())
		if err != nil {
			return 0, err
		}
		if v > highest {
			highest = v
		}
	}
	return highest, nil
}

func highestKnownVersion() (int64, error) {
	return highestVersionIn(postgres.Migrations())
}

func parseVersion(name string) (int64, error) {
	i := strings.IndexByte(name, '_')
	if i <= 0 {
		return 0, fmt.Errorf("goque/goosemigrate: migration %q must be named <version>_<name>.sql", name)
	}
	v, err := strconv.ParseInt(name[:i], 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("goque/goosemigrate: migration %q has no positive integer version", name)
	}
	return v, nil
}

func dbMaxAppliedVersion(ctx context.Context, db *sql.DB, cfg config) (sql.NullInt64, error) {
	table := quoteIdent(cfg.schema) + "." + TableName
	var applied sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT max(version_id) FROM `+table).Scan(&applied)
	if err != nil {
		if isUndefinedTable(err) {
			return sql.NullInt64{}, nil
		}
		return sql.NullInt64{}, err
	}
	return applied, nil
}

func checkVersionSkew(ctx context.Context, db *sql.DB, cfg config) error {
	highest, err := highestKnownVersion()
	if err != nil {
		return err
	}
	applied, err := dbMaxAppliedVersion(ctx, db, cfg)
	if err != nil {
		return err
	}
	if !applied.Valid || applied.Int64 <= highest {
		return nil
	}
	return fmt.Errorf("%w: database has version %d, this binary knows up to %d", ErrVersionSkew, applied.Int64, highest)
}

func migrationsFullyApplied(fsys fs.FS, applied sql.NullInt64) (bool, error) {
	highest, err := highestVersionIn(fsys)
	if err != nil {
		return false, err
	}
	return applied.Valid && applied.Int64 >= highest, nil
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42P01"
	}
	return false
}
