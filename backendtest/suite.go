// Package backendtest is the conformance suite for goque storage backends.
//
// A backend implementation is only useful if it upholds the guarantees in
// [github.com/swissy-dev/goque/backend.Backend] exactly, and most of them are
// concurrency and fencing rules that are easy to get subtly wrong. Point [Run]
// at your implementation from an ordinary Go test and it exercises those rules
// for you.
package backendtest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

// T0 is the fixed instant the suite treats as "now" at the start of every
// subtest. All scheduling in the suite is expressed relative to it, so a
// backend that reads the wall clock instead of the Now it is given fails.
var T0 = time.Unix(1_700_000_000, 0).UTC()

// Run executes the conformance suite against the backend that factory returns.
// Call it from a Test function in your backend's own package:
//
//	func TestConformance(t *testing.T) {
//		backendtest.Run(t, func(t *testing.T) backend.Backend { return New() })
//	}
//
// factory is called once per subtest and must hand back an empty backend each
// time, cleaning up after itself with t.Cleanup if it owns a resource such as a
// database. Every check runs as a named subtest, so a failure names the rule
// that broke.
//
// The suite covers: what [backend.Backend.Enqueue] fills in; claim exclusivity,
// including a concurrency test that asserts no job is ever delivered twice;
// fetch ordering by effective time, with queue and not-yet-due filtering;
// generation fencing of every finalization, including a replayed snooze and a
// finalization arriving after a rescue; attempt accounting across retry, snooze,
// kill, and cancel; heartbeat renewal restricted to the live execution; mover
// idempotency under concurrent callers; the rescuer's TTL; and the cleaner's
// per-state retention and limit.
func Run(t *testing.T, factory func(t *testing.T) backend.Backend) {
	t.Run("EnqueueFillsRow", func(t *testing.T) { testEnqueueFillsRow(t, factory(t)) })
	t.Run("FetchClaimsExclusively", func(t *testing.T) { testFetchClaimsExclusively(t, factory(t)) })
	t.Run("FetchOrdersByEffectiveTime", func(t *testing.T) { testFetchOrdersByEffectiveTime(t, factory(t)) })
	t.Run("FetchSkipsOtherQueuesAndFuture", func(t *testing.T) { testFetchSkipsOtherQueuesAndFuture(t, factory(t)) })
	t.Run("ConcurrentFetchNoDoubleDelivery", func(t *testing.T) { testConcurrentFetchNoDoubleDelivery(t, factory(t)) })
	t.Run("FinalizationsAreFenced", func(t *testing.T) { testFinalizationsAreFenced(t, factory(t)) })
	t.Run("RetrySchedulesBackoff", func(t *testing.T) { testRetrySchedulesBackoff(t, factory(t)) })
	t.Run("SnoozeDoesNotConsumeAttempt", func(t *testing.T) { testSnoozeDoesNotConsumeAttempt(t, factory(t)) })
	t.Run("KillAndCancelAreTerminal", func(t *testing.T) { testKillAndCancelAreTerminal(t, factory(t)) })
	t.Run("HeartbeatRenewsOnlyRunning", func(t *testing.T) { testHeartbeatRenewsOnlyRunning(t, factory(t)) })
	t.Run("SnoozeReplayIsFenced", func(t *testing.T) { testSnoozeReplayIsFenced(t, factory(t)) })
	t.Run("MoveDueIsIdempotentUnderConcurrency", func(t *testing.T) { testMoveDueIdempotent(t, factory(t)) })
	t.Run("RescueStaleRespectsTTL", func(t *testing.T) { testRescueStaleRespectsTTL(t, factory(t)) })
	t.Run("RescuedJobKeepsAttemptAndFencesOldFinalize", func(t *testing.T) { testRescueFencing(t, factory(t)) })
	t.Run("CleanHonorsRetentionAndLimit", func(t *testing.T) { testCleanHonorsRetention(t, factory(t)) })
	t.Run("StaleHeartbeatDoesNotRenew", func(t *testing.T) { testStaleHeartbeatDoesNotRenew(t, factory(t)) })
}

func enqueue(t *testing.T, b backend.Backend, rows ...*backend.JobRow) []*backend.JobRow {
	t.Helper()
	if err := b.Enqueue(context.Background(), backend.EnqueueParams{Jobs: rows, Now: T0}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func row(kind, queue string) *backend.JobRow {
	return &backend.JobRow{Kind: kind, Queue: queue, Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: T0}
}

func fetch(t *testing.T, b backend.Backend, queue string, limit int, now time.Time) []*backend.JobRow {
	t.Helper()
	rows, err := b.Fetch(context.Background(), backend.FetchParams{Queue: queue, Limit: limit, ClientID: "c1", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func testEnqueueFillsRow(t *testing.T, b backend.Backend) {
	j := row("a", "q")
	j.PriorityBoost = 10 * time.Second
	future := row("b", "q")
	future.ScheduledAt = T0.Add(time.Hour)
	enqueue(t, b, j, future)
	if j.ID == 0 || future.ID == 0 || j.ID == future.ID {
		t.Fatalf("ids not filled uniquely: %d %d", j.ID, future.ID)
	}
	if j.State != backend.StateAvailable {
		t.Fatalf("due job state=%s", j.State)
	}
	if future.State != backend.StateScheduled {
		t.Fatalf("future job state=%s", future.State)
	}
	if !j.CreatedAt.Equal(T0) {
		t.Fatalf("CreatedAt=%v", j.CreatedAt)
	}
	if !j.PriorityAt.Equal(T0.Add(-10 * time.Second)) {
		t.Fatalf("PriorityAt=%v", j.PriorityAt)
	}
}

func testFetchClaimsExclusively(t *testing.T, b backend.Backend) {
	j := enqueue(t, b, row("a", "q"))[0]
	got := fetch(t, b, "q", 10, T0)
	if len(got) != 1 || got[0].ID != j.ID {
		t.Fatalf("fetch got %v", got)
	}
	c := got[0]
	if c.State != backend.StateRunning || c.Attempt != 1 || c.Generation != 1 || !c.AttemptedAt.Equal(T0) || !c.HeartbeatAt.Equal(T0) {
		t.Fatalf("claim fields wrong: %+v", c)
	}
	if len(c.AttemptedBy) != 1 || c.AttemptedBy[0] != "c1" {
		t.Fatalf("AttemptedBy=%v", c.AttemptedBy)
	}
	if again := fetch(t, b, "q", 10, T0); len(again) != 0 {
		t.Fatalf("second fetch must be empty, got %d", len(again))
	}
}

func testFetchOrdersByEffectiveTime(t *testing.T, b backend.Backend) {
	early := row("early", "q")
	boosted := row("boosted", "q")
	boosted.ScheduledAt = T0.Add(5 * time.Second)
	boosted.PriorityBoost = time.Minute
	late := row("late", "q")
	late.ScheduledAt = T0.Add(10 * time.Second)
	enqueue(t, b, early, boosted, late)
	now := T0.Add(15 * time.Second)
	if _, err := b.MoveDue(context.Background(), backend.MoveDueParams{Now: now, Limit: 100}); err != nil {
		t.Fatal(err)
	}
	got := fetch(t, b, "q", 3, now)
	if len(got) != 3 || got[0].Kind != "boosted" || got[1].Kind != "early" || got[2].Kind != "late" {
		kinds := []string{}
		for _, r := range got {
			kinds = append(kinds, r.Kind)
		}
		t.Fatalf("order=%v want [boosted early late]", kinds)
	}
}

func testFetchSkipsOtherQueuesAndFuture(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "other"))
	f := row("b", "q")
	f.ScheduledAt = T0.Add(time.Hour)
	enqueue(t, b, f)
	if got := fetch(t, b, "q", 10, T0); len(got) != 0 {
		t.Fatalf("expected empty fetch, got %d", len(got))
	}
}

func testConcurrentFetchNoDoubleDelivery(t *testing.T, b backend.Backend) {
	const jobs, workers = 200, 10
	rows := make([]*backend.JobRow, jobs)
	for i := range rows {
		rows[i] = row("k", "q")
	}
	enqueue(t, b, rows...)
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := b.Fetch(context.Background(), backend.FetchParams{Queue: "q", Limit: 7, ClientID: "c", Now: T0})
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, r := range got {
					seen[r.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != jobs {
		t.Fatalf("claimed %d unique jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d delivered %d times", id, n)
		}
	}
}

func claimOne(t *testing.T, b backend.Backend, queue string) *backend.JobRow {
	t.Helper()
	got := fetch(t, b, queue, 1, T0)
	if len(got) != 1 {
		t.Fatalf("expected one claim, got %d", len(got))
	}
	return got[0]
}

func refetchAfterRetry(t *testing.T, b backend.Backend, queue string, at time.Time) *backend.JobRow {
	t.Helper()
	if _, err := b.MoveDue(context.Background(), backend.MoveDueParams{Now: at, Limit: 100}); err != nil {
		t.Fatal(err)
	}
	rows, err := b.Fetch(context.Background(), backend.FetchParams{Queue: queue, Limit: 1, ClientID: "c2", Now: at})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected refetch of one job, got %d", len(rows))
	}
	return rows[0]
}

func testFinalizationsAreFenced(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"))
	c := claimOne(t, b, "q")
	stale := backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation + 1}}, Now: T0}
	err := b.Complete(context.Background(), stale)
	var se *backend.StaleError
	if !errors.As(err, &se) || len(se.IDs) != 1 || se.IDs[0] != c.ID {
		t.Fatalf("stale complete must report StaleError, got %v", err)
	}
	ok := backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation, Metadata: []byte(`{"k":1}`)}}, Now: T0.Add(time.Second)}
	if err := b.Complete(context.Background(), ok); err != nil {
		t.Fatal(err)
	}
	if err := b.Complete(context.Background(), ok); !errors.Is(err, backend.ErrStaleAttempt) {
		t.Fatalf("double complete must be stale, got %v", err)
	}
}

func testRetrySchedulesBackoff(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"))
	c := claimOne(t, b, "q")
	at := T0.Add(30 * time.Second)
	aerr := backend.AttemptError{At: T0, Attempt: c.Attempt, Err: "boom"}
	if err := b.Retry(context.Background(), backend.RetryParams{Jobs: []backend.JobRetry{{ID: c.ID, Generation: c.Generation, At: at, Err: aerr}}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	if got := fetch(t, b, "q", 1, T0.Add(time.Second)); len(got) != 0 {
		t.Fatal("retryable job must not be fetchable before backoff")
	}
	r := refetchAfterRetry(t, b, "q", at)
	if r.Attempt != 2 || len(r.Errors) != 1 || r.Errors[0].Err != "boom" {
		t.Fatalf("after retry: attempt=%d errors=%v", r.Attempt, r.Errors)
	}
}

func testSnoozeDoesNotConsumeAttempt(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"))
	c := claimOne(t, b, "q")
	at := T0.Add(time.Minute)
	if err := b.Snooze(context.Background(), backend.SnoozeParams{Jobs: []backend.JobSnooze{{ID: c.ID, Generation: c.Generation, At: at}}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	r := refetchAfterRetry(t, b, "q", at)
	if r.Attempt != 1 {
		t.Fatalf("snooze consumed an attempt: attempt=%d", r.Attempt)
	}
	if len(r.Errors) != 0 {
		t.Fatalf("snooze must not append errors: %v", r.Errors)
	}
}

func testKillAndCancelAreTerminal(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"), row("b", "q"))
	c1 := claimOne(t, b, "q")
	c2 := claimOne(t, b, "q")
	if err := b.Kill(context.Background(), backend.KillParams{Jobs: []backend.JobKill{{ID: c1.ID, Generation: c1.Generation, Err: backend.AttemptError{Err: "dead"}}}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(context.Background(), backend.CancelParams{Jobs: []backend.JobCancel{{ID: c2.ID, Generation: c2.Generation, Err: "stop"}}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.MoveDue(context.Background(), backend.MoveDueParams{Now: T0.Add(time.Hour), Limit: 100}); err != nil {
		t.Fatal(err)
	}
	if got := fetch(t, b, "q", 10, T0.Add(time.Hour)); len(got) != 0 {
		t.Fatal("terminal jobs must never be refetched")
	}
}

func testHeartbeatRenewsOnlyRunning(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"))
	c := claimOne(t, b, "q")
	later := T0.Add(45 * time.Second)
	res, err := b.Heartbeat(context.Background(), backend.HeartbeatParams{ClientID: "c1", Jobs: []backend.JobHeartbeat{{ID: c.ID, Generation: c.Generation}, {ID: 999999, Generation: 1}}, Now: later})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CancelRequested) != 0 {
		t.Fatalf("no cancels requested, got %v", res.CancelRequested)
	}
	rescued, err := b.RescueStale(context.Background(), backend.RescueParams{Now: later.Add(59 * time.Second), TTL: time.Minute, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if rescued != 0 {
		t.Fatal("freshly heartbeated job must not be rescued")
	}
}

func testSnoozeReplayIsFenced(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"))
	first := claimOne(t, b, "q")
	snooze := backend.SnoozeParams{Jobs: []backend.JobSnooze{{ID: first.ID, Generation: first.Generation, At: T0.Add(time.Second)}}, Now: T0}
	if err := b.Snooze(context.Background(), snooze); err != nil {
		t.Fatal(err)
	}
	second := refetchAfterRetry(t, b, "q", T0.Add(2*time.Second))
	if second.Attempt != 1 || second.Generation != 2 {
		t.Fatalf("reclaim after snooze: attempt=%d generation=%d want 1,2", second.Attempt, second.Generation)
	}
	if err := b.Snooze(context.Background(), snooze); !errors.Is(err, backend.ErrStaleAttempt) {
		t.Fatalf("replayed snooze must be fenced, got %v", err)
	}
	if err := b.Complete(context.Background(), backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: second.ID, Generation: second.Generation}}, Now: T0.Add(3 * time.Second)}); err != nil {
		t.Fatalf("live execution must still finalize: %v", err)
	}
}

func testMoveDueIdempotent(t *testing.T, b backend.Backend) {
	rows := make([]*backend.JobRow, 50)
	for i := range rows {
		rows[i] = row("k", "q")
		rows[i].ScheduledAt = T0.Add(time.Second)
	}
	enqueue(t, b, rows...)
	now := T0.Add(time.Minute)
	total := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := b.MoveDue(context.Background(), backend.MoveDueParams{Now: now, Limit: 1000})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	if total != 50 {
		t.Fatalf("concurrent MoveDue moved %d total, want exactly 50", total)
	}
}

func testRescueStaleRespectsTTL(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"), row("b", "q"))
	c1 := claimOne(t, b, "q")
	claimOne(t, b, "q")
	mid := T0.Add(40 * time.Second)
	if _, err := b.Heartbeat(context.Background(), backend.HeartbeatParams{ClientID: "c1", Jobs: []backend.JobHeartbeat{{ID: c1.ID, Generation: c1.Generation}}, Now: mid}); err != nil {
		t.Fatal(err)
	}
	n, err := b.RescueStale(context.Background(), backend.RescueParams{Now: T0.Add(70 * time.Second), TTL: time.Minute, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rescued %d, want 1 (only the silent job)", n)
	}
}

func testRescueFencing(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"))
	c := claimOne(t, b, "q")
	if _, err := b.RescueStale(context.Background(), backend.RescueParams{Now: T0.Add(2 * time.Minute), TTL: time.Minute, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	err := b.Complete(context.Background(), backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation}}, Now: T0.Add(3 * time.Minute)})
	if !errors.Is(err, backend.ErrStaleAttempt) {
		t.Fatalf("finalize after rescue must be stale, got %v", err)
	}
	r := refetchAfterRetry(t, b, "q", T0.Add(4*time.Minute))
	if r.Attempt != 2 || r.Generation != 2 {
		t.Fatalf("re-claim after rescue: attempt=%d generation=%d want 2,2", r.Attempt, r.Generation)
	}
}

func testCleanHonorsRetention(t *testing.T, b backend.Backend) {
	old := row("a", "q")
	fresh := row("b", "q")
	enqueue(t, b, old, fresh)
	c1 := claimOne(t, b, "q")
	c2 := claimOne(t, b, "q")
	if err := b.Complete(context.Background(), backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c1.ID, Generation: c1.Generation}}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	if err := b.Complete(context.Background(), backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c2.ID, Generation: c2.Generation}}, Now: T0.Add(20 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	n, err := b.Clean(context.Background(), backend.CleanParams{Now: T0.Add(25 * time.Hour), CompletedRetention: 24 * time.Hour, CancelledRetention: 7 * 24 * time.Hour, DeadRetention: 7 * 24 * time.Hour, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("cleaned %d, want 1 (only the 25h-old completed job)", n)
	}
}

func testStaleHeartbeatDoesNotRenew(t *testing.T, b backend.Backend) {
	enqueue(t, b, row("a", "q"))
	first := claimOne(t, b, "q")
	if n, err := b.RescueStale(context.Background(), backend.RescueParams{Now: T0.Add(2 * time.Minute), TTL: time.Minute, Limit: 10}); err != nil || n != 1 {
		t.Fatalf("rescue: n=%d err=%v", n, err)
	}
	second := refetchAfterRetry(t, b, "q", T0.Add(3*time.Minute))
	staleHB := backend.HeartbeatParams{ClientID: "old", Jobs: []backend.JobHeartbeat{{ID: first.ID, Generation: first.Generation}}, Now: T0.Add(10 * time.Minute)}
	if _, err := b.Heartbeat(context.Background(), staleHB); err != nil {
		t.Fatal(err)
	}
	rescued, err := b.RescueStale(context.Background(), backend.RescueParams{Now: T0.Add(10 * time.Minute), TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if rescued != 1 {
		t.Fatalf("stale heartbeat must not keep the reclaimed job alive: rescued=%d want 1", rescued)
	}
	if err := b.Complete(context.Background(), backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: second.ID, Generation: second.Generation}}, Now: T0.Add(11 * time.Minute)}); !errors.Is(err, backend.ErrStaleAttempt) {
		t.Fatalf("rescued execution finalize must be fenced, got %v", err)
	}
}
