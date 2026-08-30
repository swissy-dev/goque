package conformance_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/swissy-dev/goque"
	"github.com/swissy-dev/goque/backend"
)

type clientArgs struct {
	N int `json:"n"`
}

func (clientArgs) Kind() string { return "conformance.client" }

func newRootClient(t *testing.T, h *harness, cfg *goque.Config) *goque.Client {
	t.Helper()
	c, err := goque.NewClient(h.Backend, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientEnqueueTx(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		ctx := context.Background()
		h := newHarness(t)
		c := newRootClient(t, h, nil)

		tx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		assertIndependentConnection(ctx, t, h, tx)

		res, err := c.EnqueueTx(ctx, tx, clientArgs{N: 1})
		if err != nil {
			t.Fatal(err)
		}
		if res == nil || res.Job == nil || res.Job.ID == 0 {
			t.Fatalf("result = %#v, want a populated *EnqueueResult with an assigned ID", res)
		}
		if got := h.all(ctx, t); len(got) != 0 {
			t.Fatalf("uncommitted Client.EnqueueTx visible on another connection: %#v", got)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if got := h.probe(ctx, t, res.Job.ID); got.State != string(backend.StateAvailable) {
			t.Fatalf("state after committed Client.EnqueueTx = %q, want available", got.State)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		ctx := context.Background()
		h := newHarness(t)
		c := newRootClient(t, h, nil)

		tx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		assertIndependentConnection(ctx, t, h, tx)

		res, err := c.EnqueueTx(ctx, tx, clientArgs{N: 1})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if got := h.all(ctx, t); len(got) != 0 {
			t.Fatalf("rolled back Client.EnqueueTx left rows behind: %#v, want none for job %d", got, res.Job.ID)
		}
	})
}

func TestClientEnqueueManyTxRollback(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := newRootClient(t, h, nil)

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	assertIndependentConnection(ctx, t, h, tx)

	res, err := c.EnqueueManyTx(ctx, tx, []goque.InsertParams{
		{Args: clientArgs{N: 1}},
		{Args: clientArgs{N: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 || res[0].Job.ID == 0 || res[1].Job.ID == 0 {
		t.Fatalf("result = %#v, want two populated *EnqueueResult", res)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if got := h.all(ctx, t); len(got) != 0 {
		t.Fatalf("rolled back Client.EnqueueManyTx left rows behind: %#v", got)
	}
}

func TestClientEnqueueTxRejectsForeignHandle(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := newRootClient(t, h, nil)

	cases := []struct {
		name string
		tx   any
	}{
		{"string", "not pgx"},
		{"foreign-sql-tx", &sql.Tx{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.EnqueueTx(ctx, tc.tx, clientArgs{N: 1})
			if !errors.Is(err, backend.ErrInvalidTx) {
				t.Fatalf("Client.EnqueueTx(%T) error = %v, want ErrInvalidTx", tc.tx, err)
			}
			if got := h.all(ctx, t); len(got) != 0 {
				t.Fatalf("a rejected Client.EnqueueTx stored rows: %#v, want none", got)
			}
		})
	}
}

func TestClientJobCompleteTxRejectsForeignHandle(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	w := goque.NewWorkers()
	var txErr error
	if err := goque.RegisterFunc(w, func(ctx context.Context, job *goque.Job[clientArgs]) error {
		txErr = goque.JobCompleteTx(ctx, &sql.Tx{}, job)
		return txErr
	}); err != nil {
		t.Fatal(err)
	}
	c := newRootClient(t, h, &goque.Config{Workers: w})

	res, err := c.Enqueue(ctx, clientArgs{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ProcessReady(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(txErr, backend.ErrInvalidTx) {
		t.Fatalf("JobCompleteTx(foreign handle) error = %v, want ErrInvalidTx", txErr)
	}
	if got := h.probe(ctx, t, res.Job.ID); got.State != string(backend.StateRetryable) {
		t.Fatalf("state after a rejected JobCompleteTx = %q, want retryable — ordinary finalization must still apply", got.State)
	}
}

func TestClientJobCompleteTxCompletionOutcomeMatrix(t *testing.T) {
	tests := []struct {
		name        string
		commit      bool
		workErr     error
		wantState   backend.State
		wantOutcome goque.Outcome
		wantErr     string
	}{
		{
			name:        "commit succeeds, worker returns nil",
			commit:      true,
			wantState:   backend.StateCompleted,
			wantOutcome: goque.OutcomeCompleted,
		},
		{
			name:        "commit succeeds, worker returns an error",
			commit:      true,
			workErr:     errors.New("side effect outside the transaction failed"),
			wantState:   backend.StateCompleted,
			wantOutcome: goque.OutcomeRetried,
			wantErr:     "side effect outside the transaction failed",
		},
		{
			name:        "rollback, worker returns an error",
			commit:      false,
			workErr:     errors.New("rolled back"),
			wantState:   backend.StateRetryable,
			wantOutcome: goque.OutcomeRetried,
			wantErr:     "rolled back",
		},
		{
			name:        "rollback, worker returns nil",
			commit:      false,
			wantState:   backend.StateCompleted,
			wantOutcome: goque.OutcomeCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t)
			var duringState string
			w := goque.NewWorkers()
			if err := goque.RegisterFunc(w, func(ctx context.Context, job *goque.Job[clientArgs]) error {
				tx, err := h.pool.Begin(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback(ctx)
				assertIndependentConnection(ctx, t, h, tx)

				if err := goque.JobCompleteTx(ctx, tx, job); err != nil {
					return err
				}
				duringState = h.probe(ctx, t, job.ID).State

				if tt.commit {
					if err := tx.Commit(ctx); err != nil {
						return err
					}
				} else if err := tx.Rollback(ctx); err != nil {
					return err
				}
				return tt.workErr
			}); err != nil {
				t.Fatal(err)
			}
			c := newRootClient(t, h, &goque.Config{Workers: w})

			res, err := c.Enqueue(ctx, clientArgs{N: 1}, goque.WithMaxAttempts(5))
			if err != nil {
				t.Fatal(err)
			}
			results, err := c.ProcessReady(ctx, "default")
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 1 {
				t.Fatalf("results = %+v, want 1", results)
			}

			if duringState != string(backend.StateRunning) {
				t.Fatalf("state visible on another connection right after an uncommitted JobCompleteTx = %q, want running — JobCompleteTx must write through tx, not the pool", duringState)
			}
			if results[0].Outcome != tt.wantOutcome {
				t.Fatalf("ProcessReady Outcome = %s, want %s", results[0].Outcome, tt.wantOutcome)
			}
			if results[0].Err != tt.wantErr {
				t.Fatalf("ProcessReady Err = %q, want %q", results[0].Err, tt.wantErr)
			}
			if results[0].Job == nil || results[0].Job.ID != res.Job.ID {
				t.Fatalf("ProcessReady Job = %#v, want the enqueued job %d", results[0].Job, res.Job.ID)
			}
			if got := h.probe(ctx, t, res.Job.ID); got.State != string(tt.wantState) {
				t.Fatalf("stored state = %q, want %q", got.State, tt.wantState)
			}
		})
	}
}
