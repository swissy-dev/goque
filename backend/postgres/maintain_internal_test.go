package postgres

import (
	"context"
	"errors"
	"testing"
)

type queryStubDriver struct {
	stubDriver
	rows Rows
}

func (d queryStubDriver) Query(context.Context, string, ...any) (Rows, error) {
	return d.rows, nil
}

type drainProbeRows struct {
	nexts  int
	closes int
	err    error
}

func (r *drainProbeRows) Next() bool {
	r.nexts++
	return r.nexts == 1
}

func (r *drainProbeRows) Scan(dest ...any) error {
	*dest[0].(*int) = 7
	return nil
}

func (r *drainProbeRows) Err() error {
	if r.nexts >= 2 {
		return r.err
	}
	return nil
}

func (r *drainProbeRows) Close() error {
	r.closes++
	return nil
}

func TestCountingQueryDrainsBeforeCheckingErr(t *testing.T) {
	rows := &drainProbeRows{err: errors.New("connection reset")}
	b, err := New(queryStubDriver{rows: rows})
	if err != nil {
		t.Fatal(err)
	}
	n, err := b.countingQuery(context.Background(), "test", "SELECT 1")
	if err == nil {
		t.Fatal("countingQuery must surface an error that only materialises on the second Next, got nil")
	}
	if !errors.Is(err, rows.err) {
		t.Fatalf("countingQuery error is %v, want it to wrap %v", err, rows.err)
	}
	if n != 0 {
		t.Fatalf("countingQuery returned n=%d alongside a post-row error, want 0", n)
	}
	if rows.nexts < 2 {
		t.Fatalf("countingQuery called Next() %d times, want at least 2 — it must drain past the first row before checking Err", rows.nexts)
	}
	if rows.closes != 1 {
		t.Fatalf("countingQuery closed Rows %d times, want exactly 1", rows.closes)
	}
}
