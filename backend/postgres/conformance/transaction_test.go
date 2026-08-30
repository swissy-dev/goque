package conformance_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/swissy-dev/goque/backend"
)

func assertIndependentConnection(ctx context.Context, t *testing.T, h *harness, tx pgx.Tx) {
	t.Helper()
	if max := h.pool.Config().MaxConns; max < 2 {
		t.Skipf("GOQUE_TEST_POSTGRES caps the pool at %d connection(s); these tests need a second one to read from while a transaction is open", max)
	}
	var txPID int
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&txPID); err != nil {
		t.Fatal(err)
	}
	rows, err := h.d.Query(ctx, `SELECT pg_backend_pid()`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("pg_backend_pid() returned no row")
	}
	var poolPID int
	if err := rows.Scan(&poolPID); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if txPID == poolPID {
		t.Fatalf("the pool-scoped reader landed on backend pid %d, the same session as the open transaction; it cannot prove isolation", poolPID)
	}
}

func countByKindInTx(ctx context.Context, t *testing.T, h *harness, tx pgx.Tx, kind string) int {
	t.Helper()
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM "`+h.schema+`".goque_job WHERE kind = $1`, kind).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func stateInTx(ctx context.Context, t *testing.T, h *harness, tx pgx.Tx, id int64) string {
	t.Helper()
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM "`+h.schema+`".goque_job WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestEnqueueTx(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		ctx := context.Background()
		h := newHarness(t)

		tx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		assertIndependentConnection(ctx, t, h, tx)

		row := &backend.JobRow{Kind: "commit", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0}
		if err := h.EnqueueTx(ctx, tx, backend.EnqueueParams{Jobs: []*backend.JobRow{row}, Now: t0}); err != nil {
			t.Fatal(err)
		}
		if n := countByKindInTx(ctx, t, h, tx, "commit"); n != 1 {
			t.Fatalf("EnqueueTx left %d rows visible inside its own transaction, want 1 — it must have actually written through tx", n)
		}
		if got := h.all(ctx, t); len(got) != 0 {
			t.Fatalf("uncommitted enqueue visible on another connection: %#v", got)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if got := h.all(ctx, t); len(got) != 1 || got[0].Kind != "commit" {
			t.Fatalf("committed enqueue = %#v, want one commit row", got)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		ctx := context.Background()
		h := newHarness(t)

		tx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		assertIndependentConnection(ctx, t, h, tx)

		row := &backend.JobRow{Kind: "rollback", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0}
		if err := h.EnqueueTx(ctx, tx, backend.EnqueueParams{Jobs: []*backend.JobRow{row}, Now: t0}); err != nil {
			t.Fatal(err)
		}
		if n := countByKindInTx(ctx, t, h, tx, "rollback"); n != 1 {
			t.Fatalf("EnqueueTx left %d rows visible inside its own transaction, want 1 — it must have actually written through tx", n)
		}
		if got := h.all(ctx, t); len(got) != 0 {
			t.Fatalf("uncommitted enqueue visible on another connection: %#v", got)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if got := h.all(ctx, t); len(got) != 0 {
			t.Fatalf("rolled back enqueue left rows behind: %#v", got)
		}
	})
}

func TestCompleteTx(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		ctx := context.Background()
		h := newHarness(t)
		claimed := claimOne(ctx, t, h, "k", "q")

		tx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		assertIndependentConnection(ctx, t, h, tx)

		if err := h.CompleteTx(ctx, tx, backend.CompleteParams{
			Jobs: []backend.JobFinalize{{ID: claimed.ID, Generation: claimed.Generation}},
			Now:  t0,
		}); err != nil {
			t.Fatal(err)
		}
		if got := stateInTx(ctx, t, h, tx, claimed.ID); got != string(backend.StateCompleted) {
			t.Fatalf("CompleteTx left state %q inside its own transaction, want completed — it must have actually written through tx", got)
		}
		if got := h.probe(ctx, t, claimed.ID).State; got != string(backend.StateRunning) {
			t.Fatalf("uncommitted completion visible on another connection: state = %q, want running", got)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if got := h.probe(ctx, t, claimed.ID).State; got != string(backend.StateCompleted) {
			t.Fatalf("state after committed CompleteTx = %q, want completed", got)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		ctx := context.Background()
		h := newHarness(t)
		claimed := claimOne(ctx, t, h, "k", "q")

		tx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		assertIndependentConnection(ctx, t, h, tx)

		if err := h.CompleteTx(ctx, tx, backend.CompleteParams{
			Jobs: []backend.JobFinalize{{ID: claimed.ID, Generation: claimed.Generation}},
			Now:  t0,
		}); err != nil {
			t.Fatal(err)
		}
		if got := stateInTx(ctx, t, h, tx, claimed.ID); got != string(backend.StateCompleted) {
			t.Fatalf("CompleteTx left state %q inside its own transaction, want completed — it must have actually written through tx", got)
		}
		if got := h.probe(ctx, t, claimed.ID).State; got != string(backend.StateRunning) {
			t.Fatalf("uncommitted completion visible on another connection: state = %q, want running", got)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if got := h.probe(ctx, t, claimed.ID).State; got != string(backend.StateRunning) {
			t.Fatalf("state after rolled back CompleteTx = %q, want running", got)
		}
	})
}

func TestTransactionalInvalidHandle(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	claimed := claimOne(ctx, t, h, "k", "q")

	cases := []struct {
		name string
		tx   any
	}{
		{"string", "not pgx"},
		{"foreign-sql-tx", &sql.Tx{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := &backend.JobRow{Kind: "invalid-" + tc.name, Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0}
			if err := h.EnqueueTx(ctx, tc.tx, backend.EnqueueParams{Jobs: []*backend.JobRow{row}, Now: t0}); !errors.Is(err, backend.ErrInvalidTx) {
				t.Fatalf("EnqueueTx(%T) error = %v, want ErrInvalidTx", tc.tx, err)
			}
			if got := h.all(ctx, t); len(got) != 1 || got[0].ID != claimed.ID {
				t.Fatalf("EnqueueTx with an invalid handle changed the stored rows: %#v, want only the claimed job %d", got, claimed.ID)
			}

			if err := h.CompleteTx(ctx, tc.tx, backend.CompleteParams{
				Jobs: []backend.JobFinalize{{ID: claimed.ID, Generation: claimed.Generation}},
				Now:  t0,
			}); !errors.Is(err, backend.ErrInvalidTx) {
				t.Fatalf("CompleteTx(%T) error = %v, want ErrInvalidTx", tc.tx, err)
			}
			if got := h.probe(ctx, t, claimed.ID).State; got != string(backend.StateRunning) {
				t.Fatalf("CompleteTx with an invalid handle changed state to %q, want running unchanged", got)
			}
		})
	}
}
