package goque

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type countingBackend struct {
	*memory.Backend
	completeCalls atomic.Int64
	completeSizes []int
	mu            sync.Mutex
}

func (cb *countingBackend) Complete(ctx context.Context, params backend.CompleteParams) error {
	cb.completeCalls.Add(1)
	cb.mu.Lock()
	cb.completeSizes = append(cb.completeSizes, len(params.Jobs))
	cb.mu.Unlock()
	return cb.Backend.Complete(ctx, params)
}

func claimN(t *testing.T, b backend.Backend, n int) []*backend.JobRow {
	t.Helper()
	rows := make([]*backend.JobRow, n)
	for i := range rows {
		rows[i] = &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: time.Unix(1_700_000_000, 0)}
	}
	if err := b.Enqueue(context.Background(), backend.EnqueueParams{Jobs: rows, Now: time.Unix(1_700_000_000, 0)}); err != nil {
		t.Fatal(err)
	}
	claimed, err := b.Fetch(context.Background(), backend.FetchParams{Queue: "q", Limit: n, ClientID: "c", Now: time.Unix(1_700_000_000, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != n {
		t.Fatalf("claimed %d want %d", len(claimed), n)
	}
	return claimed
}

func TestCompleterFlushBySize(t *testing.T) {
	cb := &countingBackend{Backend: memory.New()}
	claimed := claimN(t, cb, 3)
	cp := newCompleter(cb, time.Now, 3, time.Hour, slog.Default())
	cp.start()
	for _, r := range claimed {
		cp.push(finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: r.ID, Generation: r.Generation}})
	}
	deadline := time.Now().Add(2 * time.Second)
	for cb.completeCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cp.stop()
	if cb.completeCalls.Load() != 1 {
		t.Fatalf("complete calls=%d want 1 batched call", cb.completeCalls.Load())
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.completeSizes[0] != 3 {
		t.Fatalf("batch size=%d want 3", cb.completeSizes[0])
	}
}

func TestCompleterFlushByInterval(t *testing.T) {
	cb := &countingBackend{Backend: memory.New()}
	claimed := claimN(t, cb, 1)
	cp := newCompleter(cb, time.Now, 100, 20*time.Millisecond, slog.Default())
	cp.start()
	cp.push(finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: claimed[0].ID, Generation: claimed[0].Generation}})
	deadline := time.Now().Add(2 * time.Second)
	for cb.completeCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cp.stop()
	if cb.completeCalls.Load() != 1 {
		t.Fatalf("interval flush did not happen")
	}
}

type transientBackend struct {
	*memory.Backend
	calls atomic.Int64
	failN int64
}

func (tb *transientBackend) Complete(ctx context.Context, params backend.CompleteParams) error {
	if tb.calls.Add(1) <= tb.failN {
		return errors.New("transient complete failure")
	}
	return tb.Backend.Complete(ctx, params)
}

func TestCompleterRetriesTransientFailures(t *testing.T) {
	tb := &transientBackend{Backend: memory.New(), failN: 2}
	claimed := claimN(t, tb, 1)
	cp := newCompleter(tb, time.Now, 1, time.Millisecond, slog.Default())
	cp.start()
	cp.push(finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: claimed[0].ID, Generation: claimed[0].Generation}})
	deadline := time.Now().Add(5 * time.Second)
	for tb.Snapshot(claimed[0].ID).State != backend.StateCompleted && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cp.stop()
	if tb.Snapshot(claimed[0].ID).State != backend.StateCompleted {
		t.Fatalf("job not completed after transient retries: state=%s", tb.Snapshot(claimed[0].ID).State)
	}
	if tb.calls.Load() != 3 {
		t.Fatalf("Complete calls=%d want 3 (1 initial + 2 retries)", tb.calls.Load())
	}
}

func TestCompleterFlushOnStop(t *testing.T) {
	cb := &countingBackend{Backend: memory.New()}
	claimed := claimN(t, cb, 2)
	cp := newCompleter(cb, time.Now, 100, time.Hour, slog.Default())
	cp.start()
	for _, r := range claimed {
		cp.push(finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: r.ID, Generation: r.Generation}})
	}
	cp.stop()
	if cb.completeCalls.Load() != 1 {
		t.Fatalf("stop must flush pending ops")
	}
	if cb.Snapshot(claimed[0].ID).State != backend.StateCompleted {
		t.Fatal("job not completed after stop flush")
	}
}
