// Package goosemigrate applies goque's PostgreSQL migrations with goose.
//
// The migrations themselves live in goque/backend/postgres, which ships them
// but does not run them. This package is the runner, kept separate so that
// goose's dependencies and the database/sql handle it needs stay out of the
// module holding the backend's SQL.
package goosemigrate

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"strings"

	"github.com/pressly/goose/v3"
	"github.com/swissy-dev/goque/backend/postgres"
)

// TableName is the bookkeeping table goose writes into, qualified with the
// configured schema. goque names it goque_migration rather than goose's
// default so a database shared with an application's own goose migrations
// keeps the two histories apart.
const TableName = "goque_migration"

const schemaPlaceholder = "{{schema}}"

const lockNamespace = 0x676F7175

// Option configures Up.
type Option func(*config)

type config struct {
	schema                string
	allowStatementTimeout bool
}

// WithSchema selects the PostgreSQL schema holding goque's tables. It defaults
// to public. The schema must already exist — Up creates tables within it but
// never creates the schema, so that granting rights stays the operator's
// decision.
//
// The name must be lower case: goose writes its own version table with the
// schema spliced in as an unquoted identifier, which PostgreSQL folds to lower
// case, so anything else would leave goque's tables and goose's version table
// in two different schemas. postgres.ValidateSchema enforces this — Up
// rejects such a name with postgres.ErrInvalidSchema before touching the
// database. A name that is a SQL reserved word, such as order, is refused by
// PostgreSQL itself when goose creates its version table.
func WithSchema(name string) Option {
	return func(c *config) { c.schema = name }
}

func newConfig(opts ...Option) (config, error) {
	c := config{schema: "public"}
	for _, opt := range opts {
		opt(&c)
	}
	if err := postgres.ValidateSchema(c.schema); err != nil {
		return config{}, err
	}
	return c, nil
}

// Up brings the goque tables in the configured schema up to date, creating
// them on first run. It is safe to call concurrently from every instance of an
// application, and safe to call on every boot: each migration is applied at
// most once, guarded by a PostgreSQL advisory lock keyed to the schema. When
// every known migration is already applied — judged by version number alone,
// not by which migrations happen to build an index — Up reports this after a
// few read-only checks, without acquiring the lock or pinning a connection
// for it.
//
// The lock is a session-level PostgreSQL advisory lock, acquired with a
// non-blocking try-lock retried on an interval and held on one connection for
// recovery and the migration run together. It does not survive a
// transaction-pooling proxy: PgBouncer in transaction mode, RDS Proxy, and the
// Supabase pooler can each route the lock and the unlock to different server
// connections, leaking the lock permanently. Point db at a session-mode
// connection, or bypass the pooler for Up, the same requirement goose's own
// locker has always had. Beyond that, Up needs nothing driver-specific: any
// database/sql driver works.
//
// db must be a handle whose statements have no statement_timeout, because one
// of the migrations builds indexes with CREATE INDEX CONCURRENTLY and a
// timeout cancels the build partway, leaving an invalid index behind.
func Up(ctx context.Context, db *sql.DB, opts ...Option) error {
	cfg, err := newConfig(opts...)
	if err != nil {
		return err
	}
	if db == nil {
		return fmt.Errorf("goque/goosemigrate: nil database handle")
	}
	if err := checkStatementTimeout(ctx, db, cfg); err != nil {
		return err
	}
	if err := checkVersionSkew(ctx, db, cfg); err != nil {
		return err
	}
	applied, err := dbMaxAppliedVersion(ctx, db, cfg)
	if err != nil {
		return err
	}
	upToDate, err := migrationsFullyApplied(postgres.Migrations(), applied)
	if err != nil {
		return err
	}
	if upToDate {
		return nil
	}
	provider, err := newProvider(db, cfg)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("goque/goosemigrate: applying migrations: %w", err)
	}
	return nil
}

func newProvider(db *sql.DB, cfg config) (*goose.Provider, error) {
	return goose.NewProvider(
		goose.DialectPostgres,
		db,
		schemaFS{fsys: postgres.Migrations(), quoted: quoteIdent(cfg.schema)},
		goose.WithTableName(cfg.schema+"."+TableName),
		goose.WithSessionLocker(recoveryLocker{cfg: cfg}),
		goose.WithDisableGlobalRegistry(true),
	)
}

func lockID(schema string) int64 {
	h := fnv.New64a()
	_, _ = io.WriteString(h, "goque:")
	_, _ = io.WriteString(h, schema)
	return int64(lockNamespace)<<32 ^ int64(h.Sum64()&0xFFFFFFFF)
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

type schemaFS struct {
	fsys   fs.FS
	quoted string
}

func (s schemaFS) Open(name string) (fs.File, error) {
	f, err := s.fsys.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.IsDir() {
		return f, nil
	}
	body, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, err
	}
	return &substFile{
		Reader: strings.NewReader(strings.ReplaceAll(string(body), schemaPlaceholder, s.quoted)),
		info:   info,
	}, nil
}

func (s schemaFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return fs.ReadDir(s.fsys, name)
}

type substFile struct {
	*strings.Reader
	info fs.FileInfo
}

func (f *substFile) Stat() (fs.FileInfo, error) { return f.info, nil }

func (f *substFile) Close() error { return nil }
