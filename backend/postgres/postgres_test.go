package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSchemaValidation(t *testing.T) {
	cases := []struct {
		name   string
		schema string
		ok     bool
	}{
		{"default is public", "", true},
		{"simple", "goque", true},
		{"leading underscore", "_private", true},
		{"digits after first char", "sched2", true},
		{"single-character name", "x", true},
		{"max length", strings.Repeat("a", 63), true},
		{"too long", strings.Repeat("a", 64), false},
		{"leading digit", "2fast", false},
		{"hyphen", "my-schema", false},
		{"mixed case", "MixedCase", false},
		{"upper case", "PUBLIC", false},
		{"quote injection", `public"; DROP TABLE goque_job; --`, false},
		{"dot qualified", "db.public", false},
		{"space", "my schema", false},
		{"trailing newline", "goque\n", false},
		{"NUL byte", "goque\x00", false},
		{"backslash", `go\que`, false},
		{"non-ASCII character", "schéma", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts []Option
			if tc.schema != "" {
				opts = append(opts, WithSchema(tc.schema))
			}
			cfg, err := newConfig(opts...)
			if tc.ok {
				if err != nil {
					t.Fatalf("schema %q must be accepted, got %v", tc.schema, err)
				}
				want := tc.schema
				if want == "" {
					want = "public"
				}
				if cfg.schema != want {
					t.Fatalf("schema is %q, want %q", cfg.schema, want)
				}
				if cfg.quotedSchema != `"`+want+`"` {
					t.Fatalf("quotedSchema is %q, want %q", cfg.quotedSchema, `"`+want+`"`)
				}
				return
			}
			if !errors.Is(err, ErrInvalidSchema) {
				t.Fatalf("schema %q must be rejected with ErrInvalidSchema, got %v", tc.schema, err)
			}
		})
	}
}

func TestNewRejectsNilDriver(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New must reject a nil driver rather than failing later at the first query")
	}
}

func TestNewAppliesSchema(t *testing.T) {
	b, err := New(stubDriver{}, WithSchema("goque"))
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.quotedSchema != `"goque"` {
		t.Fatalf("backend carries quotedSchema %q, want %q", b.cfg.quotedSchema, `"goque"`)
	}
}

type stubDriver struct{}

func (stubDriver) Query(context.Context, string, ...any) (Rows, error) { return nil, nil }
func (stubDriver) Exec(context.Context, string, ...any) (int64, error) { return 0, nil }
func (stubDriver) Begin(context.Context) (Tx, error)                   { return nil, nil }
func (stubDriver) Conn(context.Context) (Conn, error)                  { return nil, nil }
func (stubDriver) InTx(context.Context, any) (Driver, error)           { return nil, nil }
func (stubDriver) SQLState(error) string                               { return "" }
