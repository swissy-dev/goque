package goque

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type txBackend struct {
	*memory.Backend
	txSeen       any
	enqueueSeen  backend.EnqueueParams
	enqueueErr   error
	completeSeen backend.CompleteParams
	completeErr  error
}

func newTxBackend() *txBackend { return &txBackend{Backend: memory.New()} }

func (b *txBackend) EnqueueTx(ctx context.Context, tx any, params backend.EnqueueParams) error {
	b.txSeen = tx
	b.enqueueSeen = params
	if b.enqueueErr != nil {
		return b.enqueueErr
	}
	return b.Backend.Enqueue(ctx, params)
}

func (b *txBackend) CompleteTx(ctx context.Context, tx any, params backend.CompleteParams) error {
	b.txSeen = tx
	b.completeSeen = params
	if b.completeErr != nil {
		return b.completeErr
	}
	return b.Backend.Complete(ctx, params)
}

type tracingTxBackend struct {
	*txBackend
	trace *[]string
}

func (b *tracingTxBackend) EnqueueTx(ctx context.Context, tx any, params backend.EnqueueParams) error {
	*b.trace = append(*b.trace, "backend")
	return b.txBackend.EnqueueTx(ctx, tx, params)
}

type txArgs struct {
	N int `json:"n"`
}

func (txArgs) Kind() string { return "tx" }

func TestEnqueueManyTxUsesExactHandleClockAndOrder(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newTxBackend()
	c, err := NewClient(b, &Config{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	handle := &struct{ id int }{id: 7}
	got, err := c.EnqueueManyTx(context.Background(), handle, []InsertParams{
		{Args: txArgs{N: 1}},
		{Args: txArgs{N: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if b.txSeen != handle || !b.enqueueSeen.Now.Equal(now) {
		t.Fatalf("tx=%v now=%s, want exact handle and %s", b.txSeen, b.enqueueSeen.Now, now)
	}
	if len(got) != 2 || got[0].Job.ID == 0 || got[1].Job.ID == 0 || got[0].Job.ID == got[1].Job.ID {
		t.Fatalf("ordered results were not assigned distinct IDs: %#v", got)
	}
	if string(got[0].Job.Payload) != `{"n":1}` || string(got[1].Job.Payload) != `{"n":2}` {
		t.Fatalf("results lost input order: %s, %s", got[0].Job.Payload, got[1].Job.Payload)
	}
}

func TestTransactionalAPIsRejectUnsupportedBackend(t *testing.T) {
	c, err := NewClient(memory.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.EnqueueTx(context.Background(), struct{}{}, txArgs{})
	if !errors.Is(err, backend.ErrNotSupported) {
		t.Fatalf("EnqueueTx error = %v, want ErrNotSupported", err)
	}
	if got, want := err.Error(), "goque: not supported by backend: EnqueueTx"; got != want {
		t.Fatalf("EnqueueTx error = %q, want %q", got, want)
	}
	_, err = c.EnqueueManyTx(context.Background(), struct{}{}, nil)
	if !errors.Is(err, backend.ErrNotSupported) {
		t.Fatalf("EnqueueManyTx error = %v, want ErrNotSupported", err)
	}
	if got, want := err.Error(), "goque: not supported by backend: EnqueueManyTx"; got != want {
		t.Fatalf("EnqueueManyTx error = %q, want %q", got, want)
	}
}

func TestEnqueueTxPropagatesBackendError(t *testing.T) {
	b := newTxBackend()
	b.enqueueErr = errors.New("insert failed")
	c, err := NewClient(b, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.EnqueueTx(context.Background(), struct{}{}, txArgs{N: 1})
	if err == nil || err.Error() != "insert failed" {
		t.Fatalf("EnqueueTx error = %v, want %q", err, "insert failed")
	}
}

func TestEnqueueTxReturnsStoredResult(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newTxBackend()
	c, err := NewClient(b, &Config{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.EnqueueTx(context.Background(), struct{}{}, txArgs{N: 5}, WithQueue("billing"), WithMaxAttempts(1))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || res.Job == nil {
		t.Fatalf("result = %#v, want a populated *EnqueueResult", res)
	}
	if res.Job.Queue != "billing" || res.Job.MaxAttempts != 1 {
		t.Fatalf("result queue=%q maxAttempts=%d, want billing/1", res.Job.Queue, res.Job.MaxAttempts)
	}
	if res.Job.ID == 0 {
		t.Fatalf("result ID = 0, want a backend-assigned ID")
	}
	if len(b.enqueueSeen.Jobs) != 1 {
		t.Fatalf("backend saw %d jobs, want 1", len(b.enqueueSeen.Jobs))
	}
	stored := b.enqueueSeen.Jobs[0]
	if stored.Queue != "billing" || stored.MaxAttempts != 1 {
		t.Fatalf("stored row queue=%q maxAttempts=%d, want billing/1", stored.Queue, stored.MaxAttempts)
	}
	if stored.ID != res.Job.ID {
		t.Fatalf("stored ID = %d, result ID = %d, want equal", stored.ID, res.Job.ID)
	}
}

func TestEnqueueManyTxMiddlewareOrderAndPreValidation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	var trace []string
	b := &tracingTxBackend{txBackend: newTxBackend(), trace: &trace}
	mk := func(name string) EnqueueMiddleware {
		return func(next EnqueueFunc) EnqueueFunc {
			return func(ctx context.Context, jobs []*JobRow) error {
				trace = append(trace, name+"-before")
				err := next(ctx, jobs)
				trace = append(trace, name+"-after")
				return err
			}
		}
	}
	c, err := NewClient(b, &Config{
		Now:               func() time.Time { return now },
		EnqueueMiddleware: []EnqueueMiddleware{mk("outer"), mk("inner")},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c.EnqueueManyTx(context.Background(), struct{}{}, []InsertParams{{Args: txArgs{N: 1}}}); err != nil {
		t.Fatal(err)
	}
	want := []string{"outer-before", "inner-before", "backend", "inner-after", "outer-after"}
	if !reflect.DeepEqual(trace, want) {
		t.Fatalf("middleware trace = %v, want %v", trace, want)
	}
	if len(b.enqueueSeen.Jobs) != 1 {
		t.Fatalf("backend saw %d jobs, want 1", len(b.enqueueSeen.Jobs))
	}

	trace = nil
	_, err = c.EnqueueManyTx(context.Background(), struct{}{}, []InsertParams{
		{Args: txArgs{N: 2}, Opts: []EnqueueOption{WithDelay(time.Second), WithScheduledAt(now.Add(time.Hour))}},
	})
	if !errors.Is(err, backend.ErrConflictingOptions) {
		t.Fatalf("err = %v, want ErrConflictingOptions", err)
	}
	if len(trace) != 0 {
		t.Fatalf("trace = %v, want empty: middleware must not run before validation", trace)
	}
	if len(b.enqueueSeen.Jobs) != 1 {
		t.Fatalf("enqueueSeen.Jobs changed to %d, want unchanged at 1: backend must not be called on validation failure", len(b.enqueueSeen.Jobs))
	}
}

func TestJobCompleteTxValidatesCallSite(t *testing.T) {
	ctx := context.Background()
	if err := JobCompleteTx[txArgs](ctx, struct{}{}, nil); err == nil || err.Error() != "goque: JobCompleteTx requires a job" {
		t.Fatalf("nil job error = %v", err)
	}
	nilRow := &Job[txArgs]{}
	if err := JobCompleteTx(ctx, struct{}{}, nilRow); err == nil || err.Error() != "goque: JobCompleteTx requires a job" {
		t.Fatalf("nil JobRow error = %v", err)
	}
	notRunning := &Job[txArgs]{JobRow: &JobRow{State: backend.StateAvailable}}
	if err := JobCompleteTx(ctx, struct{}{}, notRunning); err == nil || err.Error() != "goque: JobCompleteTx requires a running job, got available" {
		t.Fatalf("non-running error = %v", err)
	}
	running := &Job[txArgs]{JobRow: &JobRow{State: backend.StateRunning}}
	if err := JobCompleteTx(ctx, struct{}{}, running); err == nil || err.Error() != "goque: JobCompleteTx can only be called from a worker" {
		t.Fatalf("outside-worker error = %v", err)
	}
}

type txRunWorker struct {
	WorkerDefaults[txArgs]
}

func (txRunWorker) Work(ctx context.Context, job *Job[txArgs]) error {
	return JobCompleteTx(ctx, struct{}{}, job)
}

func TestJobCompleteTxFromRunWorkerReportsMissingWorkerContext(t *testing.T) {
	err := RunWorker(context.Background(), txRunWorker{}, txArgs{N: 1})
	want := "goque: JobCompleteTx can only be called from a worker"
	if err == nil || err.Error() != want {
		t.Fatalf("RunWorker error = %v, want %q", err, want)
	}
}

func TestJobCompleteTxRejectsUnsupportedBackendFromWorker(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	var workerErr error
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[txArgs]) error {
		workerErr = JobCompleteTx(ctx, struct{}{}, job)
		return workerErr
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(b, &Config{Workers: w})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), txArgs{N: 1}); err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results=%+v, want 1", results)
	}
	if !errors.Is(workerErr, backend.ErrNotSupported) {
		t.Fatalf("JobCompleteTx error = %v, want ErrNotSupported", workerErr)
	}
	if got, want := workerErr.Error(), "goque: not supported by backend: JobCompleteTx"; got != want {
		t.Fatalf("JobCompleteTx error = %q, want %q", got, want)
	}
	if results[0].Outcome != OutcomeRetried {
		t.Fatalf("outcome=%s, want retried", results[0].Outcome)
	}
}

func TestJobCompleteTxPassesExactHandleAndParams(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newTxBackend()
	handle := &struct{ id int }{id: 42}
	w := NewWorkers()
	var claimed *JobRow
	var txErr error
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[txArgs]) error {
		claimed = job.JobRow
		txErr = JobCompleteTx(ctx, handle, job)
		return txErr
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), txArgs{N: 9}, WithMetadata([]byte(`{"k":"v"}`))); err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results=%+v, want 1", results)
	}
	if txErr != nil {
		t.Fatalf("JobCompleteTx returned %v, want nil", txErr)
	}
	if claimed.Generation == 0 {
		t.Fatalf("claimed.Generation = 0, want the claim's generation")
	}
	if b.txSeen != handle {
		t.Fatalf("txSeen = %v, want exact handle %v", b.txSeen, handle)
	}
	seen := b.completeSeen
	if len(seen.Jobs) != 1 || seen.Jobs[0].ID != claimed.ID ||
		seen.Jobs[0].Generation != claimed.Generation ||
		!seen.Now.Equal(now) {
		t.Fatalf("CompleteTx params = %#v, want current execution at %s", seen, now)
	}
	if string(seen.Jobs[0].Metadata) != string(claimed.Metadata) {
		t.Fatalf("CompleteTx metadata = %s, want %s", seen.Jobs[0].Metadata, claimed.Metadata)
	}
}

func TestJobCompleteTxCommitWinsOverOrdinaryFinalization(t *testing.T) {
	tests := []struct {
		name        string
		workErr     error
		maxAttempts int
	}{
		{name: "nil after commit", workErr: nil, maxAttempts: 5},
		{name: "retryable error after commit", workErr: errors.New("after commit"), maxAttempts: 5},
		{name: "kill-eligible error after commit", workErr: errors.New("after commit"), maxAttempts: 1},
		{name: "cancel after commit", workErr: Cancel(errors.New("after commit")), maxAttempts: 5},
		{name: "snooze after commit", workErr: SnoozeFor(time.Minute), maxAttempts: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0).UTC()
			b := newTxBackend()
			w := NewWorkers()
			if err := RegisterFunc(w, func(ctx context.Context, job *Job[txArgs]) error {
				if err := JobCompleteTx(ctx, struct{}{}, job); err != nil {
					return err
				}
				return tt.workErr
			}); err != nil {
				t.Fatal(err)
			}
			c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			res, err := c.Enqueue(context.Background(), txArgs{N: 1}, WithMaxAttempts(tt.maxAttempts))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.ProcessReady(context.Background(), "default"); err != nil {
				t.Fatal(err)
			}
			if b.txSeen == nil {
				t.Fatalf("txSeen = nil, want JobCompleteTx to have called CompleteTx")
			}
			snap := b.Snapshot(res.Job.ID)
			if snap.State != backend.StateCompleted {
				t.Fatalf("state = %s, want completed", snap.State)
			}
			if len(snap.Errors) != 0 {
				t.Fatalf("errors = %+v, want none", snap.Errors)
			}
		})
	}
}

func TestJobCompleteTxFailurePreservesOrdinaryFinalization(t *testing.T) {
	tests := []struct {
		name        string
		maxAttempts int
		wantState   backend.State
	}{
		{name: "retryable", maxAttempts: 5, wantState: backend.StateRetryable},
		{name: "dead", maxAttempts: 1, wantState: backend.StateDead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0).UTC()
			b := newTxBackend()
			b.completeErr = errors.New("commit failed")
			w := NewWorkers()
			if err := RegisterFunc(w, func(ctx context.Context, job *Job[txArgs]) error {
				return JobCompleteTx(ctx, struct{}{}, job)
			}); err != nil {
				t.Fatal(err)
			}
			c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			res, err := c.Enqueue(context.Background(), txArgs{N: 1}, WithMaxAttempts(tt.maxAttempts))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.ProcessReady(context.Background(), "default"); err != nil {
				t.Fatal(err)
			}
			snap := b.Snapshot(res.Job.ID)
			if snap.State != tt.wantState {
				t.Fatalf("state = %s, want %s", snap.State, tt.wantState)
			}
			if len(snap.Errors) != 1 || snap.Errors[0].Err != "commit failed" {
				t.Fatalf("errors = %+v, want one entry with message %q", snap.Errors, "commit failed")
			}
		})
	}
}

func TestJobCompleteTxRollbackNilReturnDoesNotStrandJob(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	b := newTxBackend()
	b.completeErr = errors.New("rolled back")
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[txArgs]) error {
		_ = JobCompleteTx(ctx, struct{}{}, job)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Enqueue(context.Background(), txArgs{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ProcessReady(context.Background(), "default"); err != nil {
		t.Fatal(err)
	}
	snap := b.Snapshot(res.Job.ID)
	if snap.State != backend.StateCompleted {
		t.Fatalf("state = %s, want completed: a rolled-back JobCompleteTx with a nil worker return must not strand the job", snap.State)
	}
	if len(snap.Errors) != 0 {
		t.Fatalf("errors = %+v, want none", snap.Errors)
	}
}
