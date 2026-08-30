package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
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

type recordingDriver struct {
	stubDriver
	txSeen  any
	tx      Driver
	err     error
	rows    Rows
	queried bool
}

func (d *recordingDriver) Query(context.Context, string, ...any) (Rows, error) {
	d.queried = true
	return d.rows, nil
}

func (d *recordingDriver) InTx(_ context.Context, tx any) (Driver, error) {
	d.txSeen = tx
	return d.tx, d.err
}

type idRows struct {
	ids  []int64
	next int
}

func (r *idRows) Next() bool {
	if r.next >= len(r.ids) {
		return false
	}
	r.next++
	return true
}

func (r *idRows) Scan(dest ...any) error {
	*dest[0].(*int64) = r.ids[r.next-1]
	return nil
}

func (r *idRows) Err() error   { return nil }
func (r *idRows) Close() error { return nil }

func TestTransactionalCapabilitiesUseCallerDriver(t *testing.T) {
	t.Run("EnqueueTx", func(t *testing.T) {
		scoped := &recordingDriver{rows: &idRows{ids: []int64{1}}}
		outer := &recordingDriver{tx: scoped, rows: &idRows{ids: []int64{1}}}
		b, err := New(outer)
		if err != nil {
			t.Fatal(err)
		}
		handle := &struct{ name string }{name: "caller tx"}
		params := backend.EnqueueParams{
			Jobs: []*backend.JobRow{{Kind: "test-kind", Queue: "test-queue"}},
			Now:  time.Now(),
		}

		if err := b.EnqueueTx(context.Background(), handle, params); err != nil {
			t.Fatal(err)
		}
		if outer.txSeen != handle {
			t.Fatalf("EnqueueTx passed %p, want exact handle %p", outer.txSeen, handle)
		}
		if outer.queried {
			t.Fatal("EnqueueTx queried the pool-scoped driver, want it to query only the driver InTx returned")
		}
		if !scoped.queried {
			t.Fatal("EnqueueTx never queried the driver InTx returned")
		}
	})

	t.Run("CompleteTx", func(t *testing.T) {
		scoped := &recordingDriver{rows: &idRows{ids: []int64{1}}}
		outer := &recordingDriver{tx: scoped, rows: &idRows{ids: []int64{1}}}
		b, err := New(outer)
		if err != nil {
			t.Fatal(err)
		}
		handle := &struct{ name string }{name: "caller tx"}
		params := backend.CompleteParams{
			Jobs: []backend.JobFinalize{{ID: 1, Generation: 1}},
			Now:  time.Now(),
		}

		if err := b.CompleteTx(context.Background(), handle, params); err != nil {
			t.Fatal(err)
		}
		if outer.txSeen != handle {
			t.Fatalf("CompleteTx passed %p, want exact handle %p", outer.txSeen, handle)
		}
		if outer.queried {
			t.Fatal("CompleteTx queried the pool-scoped driver, want it to query only the driver InTx returned")
		}
		if !scoped.queried {
			t.Fatal("CompleteTx never queried the driver InTx returned")
		}
	})
}

func TestTransactionalCapabilitiesPropagateInvalidTx(t *testing.T) {
	d := &recordingDriver{err: fmt.Errorf("%w: foreign", backend.ErrInvalidTx)}
	b, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.EnqueueTx(context.Background(), "bad", backend.EnqueueParams{}); !errors.Is(err, backend.ErrInvalidTx) {
		t.Fatalf("EnqueueTx error = %v, want ErrInvalidTx", err)
	}
	if err := b.CompleteTx(context.Background(), "bad", backend.CompleteParams{}); !errors.Is(err, backend.ErrInvalidTx) {
		t.Fatalf("CompleteTx error = %v, want ErrInvalidTx", err)
	}
}
