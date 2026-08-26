package postgres

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrInvalidSchema reports a schema name that is not a plain SQL identifier.
// The schema name is the only value this package ever interpolates into
// statement text, so it is validated at construction rather than trusted.
// Everything else travels as a bind parameter.
var ErrInvalidSchema = errors.New("goque/postgres: invalid schema name")

var schemaPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Option configures a Backend.
type Option func(*config)

type config struct {
	schema       string
	quotedSchema string
}

// WithSchema selects the PostgreSQL schema holding goque's tables. It defaults
// to public. The schema must already exist: goque creates tables within it but
// never creates the schema itself, so that granting rights stays the
// operator's decision.
//
// The name must already be lower case. PostgreSQL folds an unquoted
// identifier to lower case, and goque's migration runner writes its
// bookkeeping table with the schema spliced in unquoted, so a schema that
// isn't already lower case could never be migrated even though this package's
// own quoted SQL would accept it. New and ValidateSchema reject such a name
// with ErrInvalidSchema before touching the database.
func WithSchema(name string) Option {
	return func(c *config) { c.schema = name }
}

func newConfig(opts ...Option) (config, error) {
	c := config{schema: "public"}
	for _, opt := range opts {
		opt(&c)
	}
	if !schemaPattern.MatchString(c.schema) {
		return config{}, fmt.Errorf("%w: %q", ErrInvalidSchema, c.schema)
	}
	if c.schema != strings.ToLower(c.schema) {
		return config{}, fmt.Errorf("%w: %q must already be lower case; PostgreSQL folds an unquoted identifier to lower case, and goque's migration runner writes its bookkeeping table with the schema spliced in unquoted, so anything else could never be migrated", ErrInvalidSchema, c.schema)
	}
	c.quotedSchema = quoteIdent(c.schema)
	return c, nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Backend stores goque jobs in PostgreSQL.
type Backend struct {
	d   Driver
	cfg config
}

// New builds a Backend over d. Pass pgxv5.New for the default path, or
// databasesql.New for the compatibility path, which polls rather than listens.
func New(d Driver, opts ...Option) (*Backend, error) {
	if d == nil {
		return nil, errors.New("goque/postgres: nil driver")
	}
	cfg, err := newConfig(opts...)
	if err != nil {
		return nil, err
	}
	return &Backend{d: d, cfg: cfg}, nil
}
