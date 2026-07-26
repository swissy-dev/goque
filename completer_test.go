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
	cp.stop(context.Background())
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
	cp.stop(context.Background())
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
	cp.stop(context.Background())
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
	cp.stop(context.Background())
	if cb.completeCalls.Load() != 1 {
		t.Fatalf("stop must flush pending ops")
	}
	if cb.Snapshot(claimed[0].ID).State != backend.StateCompleted {
		t.Fatal("job not completed after stop flush")
	}
}

type wedgedBackend struct {
	*memory.Backend
	release chan struct{}
	calls   atomic.Int64
}

func (wb *wedgedBackend) Complete(ctx context.Context, params backend.CompleteParams) error {
	wb.calls.Add(1)
	select {
	case <-wb.release:
		return wb.Backend.Complete(ctx, params)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type deafBackend struct {
	*memory.Backend
	release chan struct{}
	calls   atomic.Int64
}

func (db *deafBackend) Complete(_ context.Context, params backend.CompleteParams) error {
	db.calls.Add(1)
	<-db.release
	return db.Backend.Complete(context.Background(), params)
}

type strictBackend struct {
	*memory.Backend
}

func (sb *strictBackend) Complete(ctx context.Context, params backend.CompleteParams) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return sb.Backend.Complete(ctx, params)
}

type erroringBackend struct {
	*memory.Backend
	calls atomic.Int64
}

func (eb *erroringBackend) Complete(context.Context, backend.CompleteParams) error {
	eb.calls.Add(1)
	return errors.New("database is down")
}

func shortenCompleterBackoffs(t *testing.T, d ...time.Duration) {
	t.Helper()
	saved := completerBackoffs
	completerBackoffs = d
	t.Cleanup(func() { completerBackoffs = saved })
}

func shortenCompleterDrainTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	saved := completerDrainTimeout
	completerDrainTimeout = d
	t.Cleanup(func() { completerDrainTimeout = saved })
}

func shrinkCompleterFlushSize(t *testing.T, n int) {
	t.Helper()
	saved := completerFlushSize
	completerFlushSize = n
	t.Cleanup(func() { completerFlushSize = saved })
}

func shortenCompleterPushTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	saved := completerPushTimeout
	completerPushTimeout = d
	t.Cleanup(func() { completerPushTimeout = saved })
}

func TestCompleterCallTimeoutBoundsAWedgedBackend(t *testing.T) {
	shortenCompleterBackoffs(t, time.Millisecond, time.Millisecond)
	wb := &wedgedBackend{Backend: memory.New(), release: make(chan struct{})}
	cp := newCompleter(wb, time.Now, 1, time.Hour, slog.New(slog.DiscardHandler))
	cp.callTimeout = 20 * time.Millisecond
	cp.start()
	rows := claimN(t, wb.Backend, 1)
	cp.push(finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: rows[0].ID, Generation: rows[0].Generation}})

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cp.stop(stopCtx)
	}()
	select {
	case <-stopped:
	case <-time.After(4 * time.Second):
		t.Fatal("stop did not return against a wedged backend; every call must be bounded by callTimeout")
	}
	if wb.calls.Load() < 2 {
		t.Fatalf("backend called %d times, want the initial call plus retries", wb.calls.Load())
	}
	if cp.dropped.Load() != 1 {
		t.Fatalf("dropped counter is %d, want 1 abandoned finalization", cp.dropped.Load())
	}
}

func TestCompleterBackoffWakesOnAbort(t *testing.T) {
	shortenCompleterBackoffs(t, time.Hour)

	eb := &erroringBackend{Backend: memory.New()}
	cp := newCompleter(eb, time.Now, 1, time.Hour, slog.New(slog.DiscardHandler))
	cp.start()
	rows := claimN(t, eb.Backend, 1)
	cp.push(finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: rows[0].ID, Generation: rows[0].Generation}})

	waitFor(t, "the first backend call to fail", func() bool { return eb.calls.Load() > 0 })
	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cp.stop(stopCtx)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not return while the completer was in a one-hour backoff; the wait must abort")
	}
}

func TestPushDoesNotBlockAfterAbort(t *testing.T) {
	db := &deafBackend{Backend: memory.New(), release: make(chan struct{})}
	cp := newCompleter(db, time.Now, 1, time.Hour, slog.New(slog.DiscardHandler))
	cp.start()
	rows := claimN(t, db.Backend, 12)

	pushed := make(chan struct{})
	go func() {
		defer close(pushed)
		for _, r := range rows {
			cp.push(finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: r.ID, Generation: r.Generation}})
		}
	}()

	waitFor(t, "the completer to wedge on its first flush", func() bool { return db.calls.Load() > 0 })
	select {
	case <-pushed:
		t.Fatal("push must block while the buffer is full and the completer is wedged")
	case <-time.After(50 * time.Millisecond):
	}

	cp.abortFn()
	select {
	case <-pushed:
	case <-time.After(5 * time.Second):
		t.Fatal("push must return once the completer has been aborted, or a finishing job goroutine is stuck forever")
	}
	if cp.dropped.Load() == 0 {
		t.Fatal("finalizations dropped by an aborted push must be counted")
	}

	close(db.release)

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cp.stop(stopCtx)
	}()
	select {
	case <-stopped:
	case <-time.After(6 * time.Second):
		t.Fatal("stop did not return within its bounded context after abort")
	}
}

func TestStopHonorsItsDeadlineWithAWedgedBackend(t *testing.T) {
	wb := &wedgedBackend{Backend: memory.New(), release: make(chan struct{})}
	w := NewWorkers()
	ran := make(chan struct{}, 1)
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[wedgeArgs]) error {
		ran <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(wb, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), wedgeArgs{}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the job never ran")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = c.Stop(stopCtx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Stop must report its deadline was exceeded when a finalization cannot be delivered")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Stop took %s against a wedged backend, want it bounded by the caller's deadline", elapsed)
	}

	close(wb.release)
	waitFor(t, "the client to finish draining so no goroutine leaks past the test", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return !c.stopping
	})
}

func TestStopUnwedgesJobsBlockedInPush(t *testing.T) {
	shortenCompleterPushTimeout(t, 400*time.Millisecond)
	shortenCompleterDrainTimeout(t, 400*time.Millisecond)
	shrinkCompleterFlushSize(t, 1)

	db := &deafBackend{Backend: memory.New(), release: make(chan struct{})}
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(db.release) }) }
	t.Cleanup(closeRelease)

	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[wedgeArgs]) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 4, PollInterval: 5 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(db, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	var params []InsertParams
	for i := 0; i < 30; i++ {
		params = append(params, InsertParams{Args: wedgeArgs{}})
	}
	if _, err := c.EnqueueMany(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the completer to wedge on its first flush", func() bool { return db.calls.Load() > 0 })
	waitFor(t, "every worker slot to be occupied by a job blocked in push", func() bool {
		return c.globalRunning.Load() >= 4 && len(c.completer.ch) == cap(c.completer.ch)
	})

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- c.Stop(stopCtx) }()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Stop must report an error when it cannot confirm the drain finished by its deadline")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not honor its own deadline against a saturated push buffer")
	}

	waitFor(t, "push's own timeout to release the jobs blocked handing off to a stalled completer", func() bool {
		return c.globalRunning.Load() == 0
	})

	closeRelease()

	waitFor(t, "the client to finish draining once every job blocked in push has been freed", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return !c.stopping
	})
}

func TestStopDoesNotDropAHealthyLongRunningJob(t *testing.T) {
	shortenCompleterPushTimeout(t, 100*time.Millisecond)
	shortenCompleterDrainTimeout(t, 100*time.Millisecond)

	sb := &strictBackend{Backend: memory.New()}
	w := NewWorkers()
	started := make(chan struct{})
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[wedgeArgs]) error {
		close(started)
		time.Sleep(500 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(sb, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Enqueue(context.Background(), wedgeArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the job never started")
	}

	errCh := make(chan error, 1)
	go func() { errCh <- c.Stop(context.Background()) }()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Stop returned an error on a healthy shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return even though the job and backend were healthy")
	}

	if state := sb.Snapshot(res.Job.ID).State; state != backend.StateCompleted {
		t.Fatalf("job state=%s, want completed; a healthy job outliving completerPushTimeout and completerDrainTimeout must not be dropped", state)
	}
	if n := c.completer.dropped.Load(); n != 0 {
		t.Fatalf("dropped=%d, want 0 on a healthy shutdown", n)
	}
}

type wedgeArgs struct{}

func (wedgeArgs) Kind() string { return "test.wedge" }
