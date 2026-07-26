package goque

import (
	"context"
	"errors"
	"runtime/debug"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type ctrlArgs struct {
	Mode string `json:"mode"`
}

func (ctrlArgs) Kind() string { return "test.ctrl" }

func execHarness(t *testing.T, cfg Config) (*Client, *memory.Backend, *completer) {
	t.Helper()
	b := memory.New()
	w := NewWorkers()
	err := RegisterFunc(w, func(ctx context.Context, job *Job[ctrlArgs]) error {
		switch job.Args.Mode {
		case "ok":
			return nil
		case "fail":
			return errors.New("boom")
		case "cancel":
			return Cancel(errors.New("stop it"))
		case "snooze":
			return SnoozeFor(time.Minute)
		case "retryat":
			return RetryAt(time.Unix(1_700_000_500, 0).UTC(), errors.New("later"))
		case "panic":
			panic("kaboom")
		case "slow":
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Workers = w
	cfg.Now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	cp := newCompleter(b, c.now, 1, time.Millisecond, c.cfg.Logger)
	cp.start()
	return c, b, cp
}

func runOne(t *testing.T, c *Client, b *memory.Backend, cp *completer, mode string, opts ...EnqueueOption) *backend.JobRow {
	t.Helper()
	res, err := c.Enqueue(context.Background(), ctrlArgs{Mode: mode}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := b.Fetch(context.Background(), backend.FetchParams{Queue: "default", Limit: 1, ClientID: c.clientID, Now: c.now()})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %d", err, len(claimed))
	}
	c.runJob(context.Background(), claimed[0], cp)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := b.Snapshot(res.Job.ID)
		if snap.State != backend.StateRunning {
			return snap
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("job never finalized")
	return nil
}

func TestRunJobOutcomes(t *testing.T) {
	var errSeen, panicSeen int
	cfg := Config{
		ErrorHandler: func(ctx context.Context, row *JobRow, err error) { errSeen++ },
		PanicHandler: func(ctx context.Context, row *JobRow, recovered any, stack []byte) { panicSeen++ },
	}
	c, b, cp := execHarness(t, cfg)
	defer cp.stop(context.Background())

	if s := runOne(t, c, b, cp, "ok"); s.State != backend.StateCompleted {
		t.Fatalf("ok → %s", s.State)
	}
	if s := runOne(t, c, b, cp, "fail", WithRetryPolicy(Fixed{Interval: 30 * time.Second})); s.State != backend.StateRetryable || !s.ScheduledAt.Equal(c.now().Add(30*time.Second)) {
		t.Fatalf("fail → %s at %v", s.State, s.ScheduledAt)
	}
	if s := runOne(t, c, b, cp, "fail", WithMaxAttempts(1)); s.State != backend.StateDead {
		t.Fatalf("exhausted → %s", s.State)
	}
	if s := runOne(t, c, b, cp, "cancel"); s.State != backend.StateCancelled {
		t.Fatalf("cancel → %s", s.State)
	}
	s := runOne(t, c, b, cp, "snooze")
	if s.State != backend.StateRetryable || s.Attempt != 0 {
		t.Fatalf("snooze → %s attempt=%d", s.State, s.Attempt)
	}
	if _, n := bumpSnoozes(s.Metadata); n != 2 {
		t.Fatalf("snooze count not recorded")
	}
	if s := runOne(t, c, b, cp, "retryat"); !s.ScheduledAt.Equal(time.Unix(1_700_000_500, 0).UTC()) {
		t.Fatalf("retryat → %v", s.ScheduledAt)
	}
	s = runOne(t, c, b, cp, "panic")
	if s.State != backend.StateRetryable || len(s.Errors) != 1 || s.Errors[0].Stack == "" {
		t.Fatalf("panic → %+v", s)
	}
	if errSeen != 2 || panicSeen != 1 {
		t.Fatalf("handlers err=%d panic=%d", errSeen, panicSeen)
	}
}

func TestPanicThroughRecoveryMiddlewarePreservesReporting(t *testing.T) {
	var panicHandlerCalls int
	var handlerRecovered any
	var handlerStack []byte
	var outerCalled bool
	var outerObservedErr error

	recoveryEquivalent := func(next WorkFunc) WorkFunc {
		return func(ctx context.Context, row *JobRow) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = &PanicError{Recovered: r, Stack: debug.Stack()}
				}
			}()
			return next(ctx, row)
		}
	}
	outer := func(next WorkFunc) WorkFunc {
		return func(ctx context.Context, row *JobRow) error {
			err := next(ctx, row)
			outerCalled = true
			outerObservedErr = err
			return err
		}
	}

	cfg := Config{
		WorkerMiddleware: []Middleware{outer, recoveryEquivalent},
		PanicHandler: func(ctx context.Context, row *JobRow, recovered any, stack []byte) {
			panicHandlerCalls++
			handlerRecovered = recovered
			handlerStack = stack
		},
	}
	c, b, cp := execHarness(t, cfg)
	defer cp.stop(context.Background())

	s := runOne(t, c, b, cp, "panic")

	if s.State != backend.StateRetryable || len(s.Errors) != 1 || s.Errors[0].Stack == "" {
		t.Fatalf("panic through middleware → %+v", s)
	}
	if panicHandlerCalls != 1 {
		t.Fatalf("PanicHandler called %d times, want 1", panicHandlerCalls)
	}
	if handlerRecovered != "kaboom" {
		t.Fatalf("PanicHandler recovered=%v want %q", handlerRecovered, "kaboom")
	}
	if len(handlerStack) == 0 {
		t.Fatal("PanicHandler received an empty stack")
	}
	if !outerCalled {
		t.Fatal("outer middleware never observed next() returning; panic must not unwind past it")
	}
	if outerObservedErr == nil {
		t.Fatal("outer middleware observed a nil error; the panic was lost before reaching it")
	}
}

type staleArgs struct{}

func (staleArgs) Kind() string { return "test.stale" }

func TestStaleAttemptDoesNotEvictNewerActiveEntry(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	releases := make(chan chan struct{}, 2)
	rel1 := make(chan struct{})
	rel2 := make(chan struct{})
	releases <- rel1
	releases <- rel2
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[staleArgs]) error {
		r := <-releases
		<-r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0).UTC()
	cfg := Config{Workers: w}
	cfg.Now = func() time.Time { return base }
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	cp := newCompleter(b, c.now, 1, time.Millisecond, c.cfg.Logger)
	cp.start()
	defer cp.stop(context.Background())

	res, err := c.Enqueue(context.Background(), staleArgs{})
	if err != nil {
		t.Fatal(err)
	}
	id := res.Job.ID

	claimed1, err := b.Fetch(context.Background(), backend.FetchParams{Queue: "default", Limit: 1, ClientID: c.clientID, Now: c.now()})
	if err != nil || len(claimed1) != 1 {
		t.Fatalf("claim gen1: %v %d", err, len(claimed1))
	}
	gen1Done := make(chan struct{})
	go func() {
		c.runJob(context.Background(), claimed1[0], cp)
		close(gen1Done)
	}()
	waitFor(t, "gen1 active", func() bool {
		v, ok := c.active.Load(id)
		return ok && v.(*activeJob).generation == 1
	})

	later := base.Add(time.Second)
	if _, err := b.RescueStale(context.Background(), backend.RescueParams{Now: later, TTL: 0, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.MoveDue(context.Background(), backend.MoveDueParams{Now: later, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	claimed2, err := b.Fetch(context.Background(), backend.FetchParams{Queue: "default", Limit: 1, ClientID: c.clientID, Now: later})
	if err != nil || len(claimed2) != 1 {
		t.Fatalf("claim gen2: %v %d", err, len(claimed2))
	}
	gen2Done := make(chan struct{})
	go func() {
		c.runJob(context.Background(), claimed2[0], cp)
		close(gen2Done)
	}()
	waitFor(t, "gen2 active", func() bool {
		v, ok := c.active.Load(id)
		return ok && v.(*activeJob).generation == 2
	})

	close(rel1)
	<-gen1Done

	v, ok := c.active.Load(id)
	if !ok {
		t.Fatal("gen2 active entry evicted by stale gen1 cleanup")
	}
	if v.(*activeJob).generation != 2 {
		t.Fatalf("active entry generation=%d want 2", v.(*activeJob).generation)
	}

	close(rel2)
	<-gen2Done
	waitFor(t, "gen2 completed", func() bool {
		s := b.Snapshot(id)
		return s != nil && s.State == backend.StateCompleted
	})
}

func TestRetryAtRespectsMaxAttempts(t *testing.T) {
	var errSeen int
	cfg := Config{
		ErrorHandler: func(ctx context.Context, row *JobRow, err error) { errSeen++ },
	}
	c, b, cp := execHarness(t, cfg)
	defer cp.stop(context.Background())

	if s := runOne(t, c, b, cp, "retryat", WithMaxAttempts(1)); s.State != backend.StateDead {
		t.Fatalf("retryat exhausted → %s, want dead", s.State)
	}
	if errSeen != 1 {
		t.Fatalf("ErrorHandler invoked %d times on exhausted retryat, want 1", errSeen)
	}
	if s := runOne(t, c, b, cp, "retryat"); s.State != backend.StateRetryable || !s.ScheduledAt.Equal(time.Unix(1_700_000_500, 0).UTC()) {
		t.Fatalf("retryat non-exhausted → %s at %v, want retryable at explicit time", s.State, s.ScheduledAt)
	}
	if errSeen != 1 {
		t.Fatalf("ErrorHandler invoked on non-exhausted retryat: count=%d, want 1", errSeen)
	}
}

func TestRunJobTimeout(t *testing.T) {
	c, b, cp := execHarness(t, Config{})
	defer cp.stop(context.Background())
	s := runOne(t, c, b, cp, "slow", WithTimeout(20*time.Millisecond))
	if s.State != backend.StateRetryable {
		t.Fatalf("timeout → %s", s.State)
	}
}

func TestRunJobUnknownKind(t *testing.T) {
	c, b, cp := execHarness(t, Config{})
	defer cp.stop(context.Background())
	row := &backend.JobRow{Kind: "ghost.kind", Queue: "default", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: c.now()}
	if err := b.Enqueue(context.Background(), backend.EnqueueParams{Jobs: []*backend.JobRow{row}, Now: c.now()}); err != nil {
		t.Fatal(err)
	}
	claimed, _ := b.Fetch(context.Background(), backend.FetchParams{Queue: "default", Limit: 1, ClientID: "c", Now: c.now()})
	c.runJob(context.Background(), claimed[0], cp)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s := b.Snapshot(row.ID); s.State == backend.StateRetryable {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("unknown kind must land in retryable")
}
