package postgres

import (
	"embed"
	"io/fs"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Migrations returns goque's PostgreSQL migrations as a filesystem rooted at
// the migration files themselves.
//
// They are goose-format SQL. This package ships them but does not apply them:
// running them is the job of goque/backend/postgres/goosemigrate, or of any
// tool you already use — the goose CLI reads this layout directly once the
// files are on disk.
//
// Every object is written against a {{schema}} placeholder rather than a bare
// name, because search_path is connection-local and would leak between
// backends sharing a pool. A runner must substitute the quoted schema
// identifier before handing the SQL to a database.
func Migrations() fs.FS {
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		panic("goque/postgres: embedded migrations are missing: " + err.Error())
	}
	return sub
}

// ValidateSchema reports whether name is a plain SQL identifier goque will
// interpolate into statement text, returning an error wrapping
// ErrInvalidSchema when it is not. A schema name is the only value this
// package ever interpolates, so it is validated rather than trusted.
func ValidateSchema(name string) error {
	_, err := newConfig(WithSchema(name))
	return err
}
