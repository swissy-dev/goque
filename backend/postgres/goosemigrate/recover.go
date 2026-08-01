package goosemigrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"time"

	"github.com/swissy-dev/goque/backend/postgres"
)

var (
	lockRetryInterval    = 5 * time.Second
	lockRetryMaxAttempts = 60
	lockRetryHook        func()
)

var createIndexPattern = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+(?:IF\s+NOT\s+EXISTS\s+)?("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)`)

type queryExecer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type migrationIndexes struct {
	version int64
	names   []string
}

func migrationIndexesByVersion() ([]migrationIndexes, error) {
	fsys := postgres.Migrations()
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("goque/goosemigrate: %w", err)
	}
	var files []migrationIndexes
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		version, err := parseVersion(e.Name())
		if err != nil {
			return nil, err
		}
		raw, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("goque/goosemigrate: %w", err)
		}
		var names []string
		for _, m := range createIndexPattern.FindAllStringSubmatch(string(raw), -1) {
			names = append(names, strings.Trim(m[1], `"`))
		}
		if len(names) > 0 {
			files = append(files, migrationIndexes{version: version, names: names})
		}
	}
	return files, nil
}

func appliedVersions(ctx context.Context, db queryExecer, cfg config) (map[int64]bool, error) {
	table := quoteIdent(cfg.schema) + "." + TableName
	rows, err := db.QueryContext(ctx, `SELECT version_id FROM `+table+` WHERE is_applied`)
	if err != nil {
		if isUndefinedTable(err) {
			return map[int64]bool{}, nil
		}
		return nil, fmt.Errorf("goque/goosemigrate: %w", err)
	}
	defer rows.Close()
	applied := map[int64]bool{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("goque/goosemigrate: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goque/goosemigrate: %w", err)
	}
	return applied, nil
}

func pendingIndexNames(ctx context.Context, db queryExecer, cfg config) ([]string, error) {
	files, err := migrationIndexesByVersion()
	if err != nil {
		return nil, err
	}
	applied, err := appliedVersions(ctx, db, cfg)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, f := range files {
		if applied[f.version] {
			continue
		}
		names = append(names, f.names...)
	}
	return names, nil
}

func atRiskIndexQuery(schema, table string, names []string) (string, []any) {
	args := make([]any, 0, len(names)+2)
	args = append(args, schema, table)
	placeholders := make([]string, len(names))
	for i, name := range names {
		args = append(args, name)
		placeholders[i] = fmt.Sprintf("$%d", i+3)
	}
	query := `SELECT c.relname FROM pg_index i
	JOIN pg_class c ON c.oid = i.indexrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1
	  AND i.indrelid = to_regclass($2)
	  AND NOT i.indisvalid
	  AND c.relname IN (` + strings.Join(placeholders, ", ") + `)
	  AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)`
	return query, args
}

func sweepInvalidIndexes(ctx context.Context, db queryExecer, cfg config) error {
	names, err := pendingIndexNames(ctx, db, cfg)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}
	table := quoteIdent(cfg.schema) + ".goque_job"
	query, args := atRiskIndexQuery(cfg.schema, table, names)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("goque/goosemigrate: %w", err)
	}
	var found []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("goque/goosemigrate: %w", err)
		}
		found = append(found, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return fmt.Errorf("goque/goosemigrate: %w", err)
	}
	for _, name := range found {
		stmt := `DROP INDEX CONCURRENTLY IF EXISTS ` + quoteIdent(cfg.schema) + `.` + quoteIdent(name)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("goque/goosemigrate: dropping invalid index %s: %w", name, err)
		}
	}
	return nil
}

func acquireRecoveryLock(ctx context.Context, conn *sql.Conn, id int64) error {
	for attempt := 0; ; attempt++ {
		var locked bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, id).Scan(&locked); err != nil {
			return fmt.Errorf("goque/goosemigrate: %w", err)
		}
		if locked {
			return nil
		}
		if attempt >= lockRetryMaxAttempts {
			return fmt.Errorf("goque/goosemigrate: recovery lock still held by another session after %d attempts over %s",
				attempt, time.Duration(attempt)*lockRetryInterval)
		}
		if lockRetryHook != nil {
			lockRetryHook()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("goque/goosemigrate: waiting for the recovery lock: %w", ctx.Err())
		case <-time.After(lockRetryInterval):
		}
	}
}

type recoveryLocker struct {
	cfg config
}

func (l recoveryLocker) SessionLock(ctx context.Context, conn *sql.Conn) error {
	id := lockID(l.cfg.schema)
	if err := acquireRecoveryLock(ctx, conn, id); err != nil {
		return fmt.Errorf("goque/goosemigrate: acquiring the recovery lock for schema %q: %w", l.cfg.schema, err)
	}
	if err := sweepInvalidIndexes(ctx, conn, l.cfg); err != nil {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, id)
		return err
	}
	return nil
}

func (l recoveryLocker) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	id := lockID(l.cfg.schema)
	var unlocked bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, id).Scan(&unlocked); err != nil {
		return fmt.Errorf("goque/goosemigrate: releasing the recovery lock for schema %q: %w", l.cfg.schema, err)
	}
	if !unlocked {
		return fmt.Errorf("goque/goosemigrate: recovery lock for schema %q was not held at unlock time", l.cfg.schema)
	}
	return nil
}
