package goque

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type prArgs struct {
	Mode string `json:"mode"`
}

func (prArgs) Kind() string { return "test.pr" }

type prFollowArgs struct{}

func (prFollowArgs) Kind() string { return "test.prfollow" }

func processHarness(t *testing.T) (*Client, *memory.Backend, *time.Time) {
	t.Helper()
	b := memory.New()
	w := NewWorkers()
	now := time.Unix(1_700_000_000, 0).UTC()
	cur := &now
	err := RegisterFunc(w, func(ctx context.Context, job *Job[prArgs]) error {
		switch job.Args.Mode {
		case "ok":
			return nil
		case "fail":
			return errors.New("boom")
		case "cancel":
			return Cancel(errors.New("stop"))
		case "snooze":
			return SnoozeFor(time.Minute)
		case "panic":
			panic("kaboom")
		case "chain":
			_, err := clientFromContext(ctx).Enqueue(ctx, prFollowArgs{})
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[prFollowArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return *cur }})
	if err != nil {
		t.Fatal(err)
	}
	return c, b, cur
}

type ctxClientKey struct{}

func clientFromContext(ctx context.Context) *Client {
	return ctx.Value(ctxClientKey{}).(*Client)
}

func TestProcessReadyRunsDueJobsOnly(t *testing.T) {
	c, b, cur := processHarness(t)
	ctx := context.Background()
	nowRes, err := c.Enqueue(ctx, prArgs{Mode: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	laterRes, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Job.ID != nowRes.Job.ID || results[0].Outcome != OutcomeCompleted {
		t.Fatalf("results=%+v", results)
	}
	if b.Snapshot(laterRes.Job.ID).State != backend.StateScheduled {
		t.Fatal("delayed job must remain scheduled")
	}
	*cur = cur.Add(time.Hour)
	results, err = c.ProcessReady(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Job.ID != laterRes.Job.ID {
		t.Fatalf("after advance: %+v", results)
	}
	if b.Snapshot(laterRes.Job.ID).State != backend.StateCompleted {
		t.Fatal("delayed job must complete after advance")
	}
}

func TestProcessReadyOutcomeMapping(t *testing.T) {
	c, b, _ := processHarness(t)
	ctx := context.Background()
	modes := map[string]Outcome{
		"ok":     OutcomeCompleted,
		"fail":   OutcomeRetried,
		"cancel": OutcomeCancelled,
		"snooze": OutcomeSnoozed,
		"panic":  OutcomeRetried,
	}
	for mode, want := range modes {
		res, err := c.Enqueue(ctx, prArgs{Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		results, err := c.ProcessReady(ctx, "default")
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || results[0].Outcome != want {
			t.Fatalf("mode %s: results=%+v want outcome %s", mode, results, want)
		}
		if mode == "fail" && !strings.Contains(results[0].Err, "boom") {
			t.Fatalf("fail Err=%q", results[0].Err)
		}
		if mode == "cancel" && !strings.Contains(results[0].Err, "stop") {
			t.Fatalf("cancel Err=%q", results[0].Err)
		}
		if mode == "panic" {
			if s := b.Snapshot(res.Job.ID); len(s.Errors) == 0 || s.Errors[0].Stack == "" {
				t.Fatal("panic must record a stack")
			}
		}
	}
	killRes, err := c.Enqueue(ctx, prArgs{Mode: "fail"}, WithMaxAttempts(1))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Outcome != OutcomeDead {
		t.Fatalf("exhausted: %+v", results)
	}
	if !strings.Contains(results[0].Err, "boom") {
		t.Fatalf("dead Err=%q", results[0].Err)
	}
	if b.Snapshot(killRes.Job.ID).State != backend.StateDead {
		t.Fatal("exhausted job must be dead")
	}
}

func TestProcessReadyRetryableNotRerunWithinPass(t *testing.T) {
	c, b, cur := processHarness(t)
	ctx := context.Background()
	res, err := c.Enqueue(ctx, prArgs{Mode: "fail"}, WithRetryPolicy(Fixed{Interval: 0}))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("zero-delay retry must not re-run in the same pass: %+v", results)
	}
	if s := b.Snapshot(res.Job.ID); s.State != backend.StateRetryable {
		t.Fatalf("state=%s", s.State)
	}
	*cur = cur.Add(time.Nanosecond)
	results, err = c.ProcessReady(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Job.Attempt != 2 {
		t.Fatalf("second pass must run the retry: %+v", results)
	}
}

func TestProcessReadySameQueueFollowUpRunsInPass(t *testing.T) {
	c, b, _ := processHarness(t)
	ctx := context.WithValue(context.Background(), ctxClientKey{}, c)
	if _, err := c.Enqueue(ctx, prArgs{Mode: "chain"}); err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("due-now follow-up in the same queue must run within the pass: %+v", results)
	}
	kinds := map[string]bool{}
	for _, r := range results {
		kinds[r.Job.Kind] = true
	}
	if !kinds["test.pr"] || !kinds["test.prfollow"] {
		t.Fatalf("kinds=%v", kinds)
	}
	pending := 0
	for _, row := range b.SnapshotAll() {
		if !row.State.Terminal() {
			pending++
		}
	}
	if pending != 0 {
		t.Fatalf("pending=%d", pending)
	}
}

func TestProcessReadyUnknownQueueAndMiddleware(t *testing.T) {
	c, _, _ := processHarness(t)
	ctx := context.Background()
	var mwSaw int
	c.cfg.WorkerMiddleware = []Middleware{func(next WorkFunc) WorkFunc {
		return func(ctx context.Context, job *JobRow) error {
			mwSaw++
			return next(ctx, job)
		}
	}}
	if _, err := c.Enqueue(ctx, prArgs{Mode: "ok"}); err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(ctx, "ghost-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("unknown queue must yield no work: %+v", results)
	}
	results, err = c.ProcessReady(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || mwSaw != 1 {
		t.Fatalf("worker middleware must run inline: results=%d mwSaw=%d", len(results), mwSaw)
	}
}

func TestProcessReadyHonorsQueueOrder(t *testing.T) {
	c, _, _ := processHarness(t)
	ctx := context.Background()
	a1, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithQueue("a"))
	if err != nil {
		t.Fatal(err)
	}
	a2, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithQueue("a"))
	if err != nil {
		t.Fatal(err)
	}
	b1, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithQueue("b"))
	if err != nil {
		t.Fatal(err)
	}
	b2, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithQueue("b"))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.ProcessReady(ctx, "b", "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Fatalf("results=%+v", results)
	}
	want := []int64{b1.Job.ID, b2.Job.ID, a1.Job.ID, a2.Job.ID}
	got := make([]int64, len(results))
	for i, r := range results {
		got[i] = r.Job.ID
	}
	mismatch := len(got) != len(want)
	if !mismatch {
		for i := range want {
			if got[i] != want[i] {
				mismatch = true
				break
			}
		}
	}
	if mismatch {
		t.Fatalf("got order %v, want %v (queues must be processed in call order)", got, want)
	}
}

func TestProcessReadyRequiresWorkers(t *testing.T) {
	c, err := NewClient(memory.New(), &Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ProcessReady(context.Background(), "default"); err == nil {
		t.Fatal("expected error when client has no workers configured")
	}
}

type prCancelArgs struct{}

func (prCancelArgs) Kind() string { return "test.prcancel" }

func TestProcessReadyStopsOnCancelledContext(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	calls := 0
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[prCancelArgs]) error {
		calls++
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for i := 0; i < 5; i++ {
		res, err := c.Enqueue(context.Background(), prCancelArgs{}, WithMaxAttempts(1))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.Job.ID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := c.ProcessReady(ctx, "default")
	if err == nil {
		t.Fatal("ProcessReady must report the cancelled context instead of returning a nil error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
	if calls != 0 {
		t.Fatalf("worker ran %d times under a cancelled context, want 0", calls)
	}
	if len(results) != 0 {
		t.Fatalf("results=%+v, want none", results)
	}
	for _, id := range ids {
		if s := b.Snapshot(id).State; s.Terminal() {
			t.Fatalf("job %d reached terminal state %s under a cancelled context", id, s)
		}
	}
	results, err = c.ProcessReady(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(ids) {
		t.Fatalf("fresh-context pass ran %d jobs, want %d", len(results), len(ids))
	}
	for _, r := range results {
		if r.Outcome != OutcomeCompleted {
			t.Fatalf("fresh-context pass outcome=%s, want completed", r.Outcome)
		}
	}
}

func TestProcessReadyCancelledContextSkipsPromotion(t *testing.T) {
	c, b, cur := processHarness(t)
	ctx := context.Background()
	res, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if s := b.Snapshot(res.Job.ID).State; s != backend.StateScheduled {
		t.Fatalf("setup: job state=%s, want scheduled", s)
	}
	*cur = cur.Add(2 * time.Hour)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = c.ProcessReady(cctx, "default")
	if err == nil {
		t.Fatal("expected ProcessReady to report the already-cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", err)
	}
	if s := b.Snapshot(res.Job.ID).State; s != backend.StateScheduled {
		t.Fatalf("job promoted to %s despite an already-cancelled context; MoveDue must not run before the ctx check", s)
	}
}

func TestProcessReadyPromotesAcrossLargeBacklog(t *testing.T) {
	c, b, cur := processHarness(t)
	ctx := context.Background()
	old := moveDueBatch
	moveDueBatch = 5
	defer func() { moveDueBatch = old }()
	var bulk []int64
	for i := 0; i < 200; i++ {
		res, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithQueue("bulk"), WithDelay(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		bulk = append(bulk, res.Job.ID)
	}
	critical, err := c.Enqueue(ctx, prArgs{Mode: "ok"}, WithQueue("critical"), WithDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	*cur = cur.Add(2 * time.Hour)
	results, err := c.ProcessReady(ctx, "critical")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Job.ID != critical.Job.ID {
		t.Fatalf("critical job did not run: results=%+v, critical state=%s", results, b.Snapshot(critical.Job.ID).State)
	}
	if s := b.Snapshot(critical.Job.ID).State; s != backend.StateCompleted {
		t.Fatalf("critical state=%s want completed", s)
	}
	stranded := 0
	for _, id := range bulk {
		if b.Snapshot(id).State == backend.StateScheduled {
			stranded++
		}
	}
	if stranded != 0 {
		t.Fatalf("%d/%d due rows left scheduled; promotion must drain the backlog, not one batch", stranded, len(bulk))
	}
}

type prLoopArgs struct{}

func (prLoopArgs) Kind() string { return "test.prloop" }

func TestProcessReadyBudgetGuard(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[prLoopArgs]) error {
		_, err := clientFromContext(ctx).Enqueue(ctx, prLoopArgs{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), ctxClientKey{}, c)
	if _, err := c.Enqueue(ctx, prLoopArgs{}); err != nil {
		t.Fatal(err)
	}
	old := perQueueJobBudget
	perQueueJobBudget = 5
	defer func() { perQueueJobBudget = old }()
	results, err := c.ProcessReady(ctx, "default")
	if err == nil {
		t.Fatal("expected error when a queue exceeds the processing budget")
	}
	if len(results) == 0 {
		t.Fatal("expected partial results alongside the budget error")
	}
}
