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
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
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
// kill, and cancel; heartbeat renewal restricted to the live execution; the
// mover's idempotency under concurrent callers and the rescuer's respect for
// its TTL; the cleaner's per-state retention under one global limit, including
// the distinct terminal state each finalizer must record; the range of
// instants a backend must be able to store; partial and duplicated
// finalization batches across all five finalizing operations; per-row values
// inside a batch; concurrent rescue and clean sweeps; metadata surviving the
// full job lifecycle, not only the nil case; and the error history
// accumulating across retries rather than being replaced.
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
	t.Run("RejectsInstantsOutsideRange", func(t *testing.T) { testRejectsInstantsOutsideRange(t, factory(t)) })
	t.Run("AcceptsRangeBoundaries", func(t *testing.T) { testAcceptsRangeBoundaries(t, factory(t)) })
	t.Run("DerivedPriorityClamps", func(t *testing.T) { testDerivedPriorityClamps(t, factory(t)) })
	t.Run("PartialBatchesApplyLiveEntries", func(t *testing.T) { testPartialBatchesApplyLiveEntries(t, factory(t)) })
	t.Run("DuplicateEntriesApplyOnce", func(t *testing.T) { testDuplicateEntriesApplyOnce(t, factory(t)) })
	t.Run("HeartbeatIgnoresStaleEntries", func(t *testing.T) { testHeartbeatIgnoresStaleEntries(t, factory(t)) })
	t.Run("BatchAppliesPerRowValues", func(t *testing.T) { testBatchAppliesPerRowValues(t, factory(t)) })
	t.Run("EnqueueBatchFillsEachRow", func(t *testing.T) { testEnqueueBatchFillsEachRow(t, factory(t)) })
	t.Run("SubMicrosecondBoostsOrder", func(t *testing.T) { testSubMicrosecondBoostsOrder(t, factory(t)) })
	t.Run("NilMetadataRoundTrips", func(t *testing.T) { testNilMetadataRoundTrips(t, factory(t)) })
	t.Run("CleanLimitIsGlobal", func(t *testing.T) { testCleanLimitIsGlobal(t, factory(t)) })
	t.Run("ConcurrentSweepsDoNotDoubleCount", func(t *testing.T) { testConcurrentSweepsDoNotDoubleCount(t, factory(t)) })
	t.Run("RescuedJobKeepsPriorityBoost", func(t *testing.T) { testRescuedJobKeepsPriorityBoost(t, factory(t)) })
	t.Run("CleanDistinguishesTerminalStates", func(t *testing.T) { testCleanDistinguishesTerminalStates(t, factory(t)) })
	t.Run("MetadataRoundTripsThroughLifecycle", func(t *testing.T) { testMetadataRoundTripsThroughLifecycle(t, factory(t)) })
	t.Run("ErrorHistoryAccumulates", func(t *testing.T) { testErrorHistoryAccumulates(t, factory(t)) })
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

func testRejectsInstantsOutsideRange(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	far := backend.MaxInstant.Add(time.Hour)

	beyond := row("beyond", "q")
	beyond.ScheduledAt = far
	if err := b.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{beyond}, Now: T0}); !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("enqueue scheduled beyond MaxInstant: got %v, want ErrTimeOutOfRange", err)
	}

	underflow := row("underflow", "q")
	underflow.ScheduledAt = backend.MinInstant
	underflow.PriorityBoost = time.Hour
	if err := b.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{underflow}, Now: T0}); !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("enqueue whose boost underflows MinInstant: got %v, want ErrTimeOutOfRange", err)
	}

	good, bad := row("good", "q"), row("bad", "q")
	bad.ScheduledAt = far
	if err := b.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{good, bad}, Now: T0}); !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("a batch containing an unstorable row: got %v, want ErrTimeOutOfRange", err)
	}
	if len(fetch(t, b, "q", 10, T0)) != 0 {
		t.Fatal("a rejected batch must store none of its rows")
	}

	enqueue(t, b, row("live", "q"))
	c := claimOne(t, b, "q")
	enqueue(t, b, row("other", "q"))
	other := claimOne(t, b, "q")
	if err := b.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: other.ID, Generation: other.Generation, At: T0.Add(time.Minute)},
		{ID: c.ID, Generation: c.Generation, At: far},
	}, Now: T0}); !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("retry batch containing an unstorable entry: got %v, want ErrTimeOutOfRange", err)
	}
	if err := b.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: other.ID, Generation: other.Generation}}, Now: T0}); err != nil {
		t.Fatalf("a valid entry in a rejected retry batch must remain running and finalizable: %v", err)
	}
	if err := b.Snooze(ctx, backend.SnoozeParams{Jobs: []backend.JobSnooze{{ID: c.ID, Generation: c.Generation, At: far}}, Now: T0}); !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("snooze scheduled beyond MaxInstant: got %v, want ErrTimeOutOfRange", err)
	}
	ok := backend.RetryParams{Jobs: []backend.JobRetry{{ID: c.ID, Generation: c.Generation, At: T0.Add(time.Minute), Err: backend.AttemptError{At: T0, Attempt: 1, Err: "boom"}}}, Now: T0}
	if err := b.Retry(ctx, ok); err != nil {
		t.Fatalf("a rejected finalization must leave the job running and its generation intact: %v", err)
	}
}

func testAcceptsRangeBoundaries(t *testing.T, b backend.Backend) {
	lo, hi := row("lo", "q"), row("hi", "q")
	lo.ScheduledAt = backend.MinInstant
	hi.ScheduledAt = backend.MaxInstant
	enqueue(t, b, lo, hi)

	loGot := fetch(t, b, "q", 10, T0)
	if len(loGot) != 1 || loGot[0].ID != lo.ID {
		t.Fatalf("at T0 only the MinInstant job is due, got %d rows", len(loGot))
	}
	if !loGot[0].PriorityAt.Equal(backend.MinInstant) {
		t.Fatalf("the stored MinInstant job has PriorityAt %s, want MinInstant — the caller's struct is not proof of what was written", loGot[0].PriorityAt)
	}

	hiGot := refetchAfterRetry(t, b, "q", backend.MaxInstant)
	if hiGot.ID != hi.ID {
		t.Fatalf("at MaxInstant the due job is %d, want the MaxInstant job %d", hiGot.ID, hi.ID)
	}
	if !hiGot.PriorityAt.Equal(backend.MaxInstant) {
		t.Fatalf("the stored MaxInstant job has PriorityAt %s, want MaxInstant — the caller's struct is not proof of what was written", hiGot.PriorityAt)
	}
}

func testDerivedPriorityClamps(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	near := backend.MinInstant.Add(time.Hour)

	retried := row("retried", "q")
	retried.PriorityBoost = 2 * time.Hour
	enqueue(t, b, retried)
	c := claimOne(t, b, "q")
	if err := b.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: near, Err: backend.AttemptError{At: T0, Attempt: 1, Err: "boom"}},
	}, Now: T0}); err != nil {
		t.Fatalf("a retry whose stored boost underflows the range must succeed, got %v", err)
	}
	got := refetchAfterRetry(t, b, "q", T0)
	if !got.PriorityAt.Equal(backend.MinInstant) {
		t.Fatalf("retried job's PriorityAt is %s, want it pinned to MinInstant — a boost from a stored row clamps, it must not fail the call", got.PriorityAt)
	}

	snoozed := row("snoozed", "s")
	snoozed.PriorityBoost = 2 * time.Hour
	enqueue(t, b, snoozed)
	s := claimOne(t, b, "s")
	if err := b.Snooze(ctx, backend.SnoozeParams{Jobs: []backend.JobSnooze{
		{ID: s.ID, Generation: s.Generation, At: near},
	}, Now: T0}); err != nil {
		t.Fatalf("a snooze whose stored boost underflows the range must succeed, got %v", err)
	}
	got = refetchAfterRetry(t, b, "s", T0)
	if !got.PriorityAt.Equal(backend.MinInstant) {
		t.Fatalf("snoozed job's PriorityAt is %s, want it pinned to MinInstant — a boost from a stored row clamps, it must not fail the call", got.PriorityAt)
	}

	rescued := row("rescued", "r")
	rescued.PriorityBoost = 2 * time.Hour
	enqueue(t, b, rescued)
	rc := claimOne(t, b, "r")
	if err := b.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: rc.ID, Generation: rc.Generation, At: near, Err: backend.AttemptError{At: T0, Attempt: 1, Err: "boom"}},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	refetchAfterRetry(t, b, "r", near)
	n, err := b.RescueStale(ctx, backend.RescueParams{Now: near.Add(2 * time.Minute), TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatalf("a rescue whose stored boost underflows the range must succeed, got %v", err)
	}
	if n != 1 {
		t.Fatalf("rescued %d jobs, want 1 — the stale job must be recoverable", n)
	}
	got = refetchAfterRetry(t, b, "r", near.Add(2*time.Minute))
	if !got.PriorityAt.Equal(backend.MinInstant) {
		t.Fatalf("rescued job's PriorityAt is %s, want it pinned to MinInstant — a rescue derives priority from a stored boost and must never fail", got.PriorityAt)
	}
}

type finalizeEntry struct {
	id  int64
	gen int
}

type finalizerCall struct {
	name string
	call func(es []finalizeEntry) error
}

func finalizerCalls(b backend.Backend) []finalizerCall {
	ctx := context.Background()
	return []finalizerCall{
		{"complete", func(es []finalizeEntry) error {
			jobs := make([]backend.JobFinalize, len(es))
			for i, e := range es {
				jobs[i] = backend.JobFinalize{ID: e.id, Generation: e.gen}
			}
			return b.Complete(ctx, backend.CompleteParams{Jobs: jobs, Now: T0})
		}},
		{"retry", func(es []finalizeEntry) error {
			jobs := make([]backend.JobRetry, len(es))
			for i, e := range es {
				jobs[i] = backend.JobRetry{ID: e.id, Generation: e.gen, At: T0.Add(time.Minute), Err: backend.AttemptError{At: T0, Attempt: 1, Err: "boom"}}
			}
			return b.Retry(ctx, backend.RetryParams{Jobs: jobs, Now: T0})
		}},
		{"cancel", func(es []finalizeEntry) error {
			jobs := make([]backend.JobCancel, len(es))
			for i, e := range es {
				jobs[i] = backend.JobCancel{ID: e.id, Generation: e.gen, Err: "cancelled"}
			}
			return b.Cancel(ctx, backend.CancelParams{Jobs: jobs, Now: T0})
		}},
		{"kill", func(es []finalizeEntry) error {
			jobs := make([]backend.JobKill, len(es))
			for i, e := range es {
				jobs[i] = backend.JobKill{ID: e.id, Generation: e.gen, Err: backend.AttemptError{At: T0, Attempt: 1, Err: "fatal"}}
			}
			return b.Kill(ctx, backend.KillParams{Jobs: jobs, Now: T0})
		}},
		{"snooze", func(es []finalizeEntry) error {
			jobs := make([]backend.JobSnooze, len(es))
			for i, e := range es {
				jobs[i] = backend.JobSnooze{ID: e.id, Generation: e.gen, At: T0.Add(time.Minute)}
			}
			return b.Snooze(ctx, backend.SnoozeParams{Jobs: jobs, Now: T0})
		}},
	}
}

func testPartialBatchesApplyLiveEntries(t *testing.T, b backend.Backend) {
	for _, f := range finalizerCalls(b) {
		enqueue(t, b, row("live", "q"), row("untouched", "q"))
		live := claimOne(t, b, "q")
		other := claimOne(t, b, "q")

		err := f.call([]finalizeEntry{{live.ID, live.Generation}, {other.ID, other.Generation + 1}})
		var se *backend.StaleError
		if !errors.As(err, &se) {
			t.Fatalf("%s: a batch with one stale entry must return a StaleError, got %v", f.name, err)
		}
		if len(se.IDs) != 1 || se.IDs[0] != other.ID {
			t.Fatalf("%s: StaleError names %v, want exactly [%d]", f.name, se.IDs, other.ID)
		}
		if err := f.call([]finalizeEntry{{live.ID, live.Generation}}); !errors.Is(err, backend.ErrStaleAttempt) {
			t.Fatalf("%s: the live entry in a partial batch was not applied, got %v", f.name, err)
		}
		if err := f.call([]finalizeEntry{{other.ID, other.Generation}}); err != nil {
			t.Fatalf("%s: a stale entry must leave its job untouched, got %v", f.name, err)
		}
	}
}

func testDuplicateEntriesApplyOnce(t *testing.T, b backend.Backend) {
	for _, f := range finalizerCalls(b) {
		for _, staleFirst := range []bool{false, true} {
			enqueue(t, b, row("dup", "q"))
			c := claimOne(t, b, "q")
			live := finalizeEntry{c.ID, c.Generation}
			stale := finalizeEntry{c.ID, c.Generation + 1}
			entries := []finalizeEntry{live, stale}
			if staleFirst {
				entries = []finalizeEntry{stale, live}
			}
			err := f.call(entries)
			var se *backend.StaleError
			if !errors.As(err, &se) || len(se.IDs) != 1 || se.IDs[0] != c.ID {
				t.Fatalf("%s staleFirst=%v: want a StaleError naming job %d, got %v", f.name, staleFirst, c.ID, err)
			}
			if err := f.call([]finalizeEntry{live}); !errors.Is(err, backend.ErrStaleAttempt) {
				t.Fatalf("%s staleFirst=%v: the live entry of a duplicated pair was not applied, got %v", f.name, staleFirst, err)
			}
		}
	}
}

func testHeartbeatIgnoresStaleEntries(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	enqueue(t, b, row("renewed", "q"), row("ignored", "q"))
	renewed := claimOne(t, b, "q")
	ignored := claimOne(t, b, "q")
	if _, err := b.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c1", Now: T0.Add(time.Minute), Jobs: []backend.JobHeartbeat{
		{ID: renewed.ID, Generation: renewed.Generation},
		{ID: ignored.ID, Generation: ignored.Generation + 1},
	}}); err != nil {
		t.Fatal(err)
	}
	n, err := b.RescueStale(ctx, backend.RescueParams{Now: T0.Add(90 * time.Second), TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rescued %d, want 1 — a heartbeat at the wrong generation must renew nothing", n)
	}
}

func testBatchAppliesPerRowValues(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	enqueue(t, b, row("soon", "q"), row("later", "q"))
	soon := claimOne(t, b, "q")
	later := claimOne(t, b, "q")
	if err := b.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: soon.ID, Generation: soon.Generation, At: T0.Add(time.Minute), Err: backend.AttemptError{At: T0, Attempt: 1, Err: "first"}},
		{ID: later.ID, Generation: later.Generation, At: T0.Add(time.Hour), Err: backend.AttemptError{At: T0, Attempt: 1, Err: "second"}},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	got := refetchAfterRetry(t, b, "q", T0.Add(time.Minute))
	if got.ID != soon.ID {
		t.Fatalf("due one minute after retry is job %d, want %d — each row carries its own At", got.ID, soon.ID)
	}
	if len(got.Errors) != 1 || got.Errors[0].Err != "first" {
		t.Fatalf("job %d recorded errors %v, want one entry %q — each row carries its own Err", got.ID, got.Errors, "first")
	}
	if extra := fetch(t, b, "q", 1, T0.Add(time.Minute)); len(extra) != 0 {
		t.Fatalf("job %d is due one minute after retry as well; each row must keep its own At", extra[0].ID)
	}

	enqueue(t, b, row("s1", "s"), row("s2", "s"))
	s1 := claimOne(t, b, "s")
	s2 := claimOne(t, b, "s")
	if err := b.Snooze(ctx, backend.SnoozeParams{Jobs: []backend.JobSnooze{
		{ID: s1.ID, Generation: s1.Generation, At: T0.Add(2 * time.Minute)},
		{ID: s2.ID, Generation: s2.Generation, At: T0.Add(3 * time.Hour)},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	got = refetchAfterRetry(t, b, "s", T0.Add(2*time.Minute))
	if got.ID != s1.ID {
		t.Fatalf("due two minutes after snooze is job %d, want %d — each row carries its own At", got.ID, s1.ID)
	}
	if extra := fetch(t, b, "s", 1, T0.Add(2*time.Minute)); len(extra) != 0 {
		t.Fatalf("job %d is due two minutes after snooze as well; each row must keep its own At", extra[0].ID)
	}
}

func testEnqueueBatchFillsEachRow(t *testing.T, b backend.Backend) {
	rows := []*backend.JobRow{row("k0", "q"), row("k1", "q"), row("k2", "q")}
	enqueue(t, b, rows...)
	seen := make(map[int64]bool, len(rows))
	for i, r := range rows {
		if r.ID == 0 {
			t.Fatalf("row %d was not assigned an ID", i)
		}
		if seen[r.ID] {
			t.Fatalf("row %d reuses ID %d", i, r.ID)
		}
		seen[r.ID] = true
		if want := "k" + strconv.Itoa(i); r.Kind != want {
			t.Fatalf("row %d is kind %q, want %q — Enqueue filled the rows out of order", i, r.Kind, want)
		}
		if r.CreatedAt.IsZero() {
			t.Fatalf("row %d has no CreatedAt", i)
		}
	}
	got := fetch(t, b, "q", len(rows), T0)
	if len(got) != len(rows) {
		t.Fatalf("fetched %d rows, want %d", len(got), len(rows))
	}
	stored := make(map[int64]string, len(got))
	for _, r := range got {
		stored[r.ID] = r.Kind
	}
	for i, r := range rows {
		if stored[r.ID] != r.Kind {
			t.Fatalf("row %d was assigned ID %d, but that ID holds kind %q, want %q — Enqueue mapped IDs onto the wrong rows", i, r.ID, stored[r.ID], r.Kind)
		}
	}
}

func testSubMicrosecondBoostsOrder(t *testing.T, b backend.Backend) {
	early, late := row("early", "q"), row("late", "q")
	early.PriorityBoost = 2 * time.Nanosecond
	late.PriorityBoost = time.Nanosecond
	enqueue(t, b, late, early)
	got := fetch(t, b, "q", 2, T0)
	if len(got) != 2 {
		t.Fatalf("fetched %d rows, want 2", len(got))
	}
	if got[0].ID != early.ID {
		t.Fatalf("first claim is job %d, want %d — a one-nanosecond difference in boost must order the fetch", got[0].ID, early.ID)
	}
}

func testNilMetadataRoundTrips(t *testing.T, b backend.Backend) {
	r := row("meta", "q")
	r.Metadata = nil
	enqueue(t, b, r)
	c := claimOne(t, b, "q")
	if len(c.Metadata) > 0 {
		var m map[string]any
		if err := json.Unmarshal(c.Metadata, &m); err != nil {
			t.Fatalf("nil metadata came back as %q, which is not valid JSON: %v", c.Metadata, err)
		}
		if m == nil {
			t.Fatalf("nil metadata came back as %q, want an absent value or an empty JSON object, not a JSON null", c.Metadata)
		}
		if len(m) != 0 {
			t.Fatalf("nil metadata came back carrying keys %v", m)
		}
	}
	if err := b.Complete(context.Background(), backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation}}, Now: T0}); err != nil {
		t.Fatalf("a job with no metadata must still finalize: %v", err)
	}
}

func testCleanLimitIsGlobal(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		enqueue(t, b, row("sweep", "q"))
	}
	claims := make([]*backend.JobRow, 6)
	for i := range claims {
		claims[i] = claimOne(t, b, "q")
	}
	if err := b.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{
		{ID: claims[0].ID, Generation: claims[0].Generation},
		{ID: claims[1].ID, Generation: claims[1].Generation},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(ctx, backend.CancelParams{Jobs: []backend.JobCancel{
		{ID: claims[2].ID, Generation: claims[2].Generation},
		{ID: claims[3].ID, Generation: claims[3].Generation},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	if err := b.Kill(ctx, backend.KillParams{Jobs: []backend.JobKill{
		{ID: claims[4].ID, Generation: claims[4].Generation, Err: backend.AttemptError{At: T0, Attempt: 3, Err: "fatal"}},
		{ID: claims[5].ID, Generation: claims[5].Generation, Err: backend.AttemptError{At: T0, Attempt: 3, Err: "fatal"}},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	params := backend.CleanParams{
		Now:                T0.Add(48 * time.Hour),
		CompletedRetention: time.Hour,
		CancelledRetention: time.Hour,
		DeadRetention:      time.Hour,
		Limit:              3,
	}
	for i, want := range []int{3, 3, 0} {
		n, err := b.Clean(ctx, params)
		if err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("clean %d removed %d rows, want %d — Limit is one global cap across all terminal states, not a cap per state", i+1, n, want)
		}
	}
}

func testConcurrentSweepsDoNotDoubleCount(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	const total = 20
	for i := 0; i < total; i++ {
		enqueue(t, b, row("sweep", "q"))
	}
	claims := make([]*backend.JobRow, total)
	for i := range claims {
		claims[i] = claimOne(t, b, "q")
	}
	for i := 0; i < total/2; i++ {
		if err := b.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: claims[i].ID, Generation: claims[i].Generation}}, Now: T0}); err != nil {
			t.Fatal(err)
		}
	}

	var rescued, cleaned atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			n, err := b.RescueStale(ctx, backend.RescueParams{Now: T0.Add(time.Hour), TTL: time.Minute, Limit: 100})
			if err != nil {
				t.Error(err)
				return
			}
			rescued.Add(int64(n))
		}()
		go func() {
			defer wg.Done()
			n, err := b.Clean(ctx, backend.CleanParams{
				Now:                T0.Add(48 * time.Hour),
				CompletedRetention: time.Hour,
				CancelledRetention: time.Hour,
				DeadRetention:      time.Hour,
				Limit:              100,
			})
			if err != nil {
				t.Error(err)
				return
			}
			cleaned.Add(int64(n))
		}()
	}
	wg.Wait()

	if got := rescued.Load(); got != total/2 {
		t.Fatalf("concurrent rescuers reported %d rescues in total, want %d — no job may be rescued twice", got, total/2)
	}
	if got := cleaned.Load(); got != total/2 {
		t.Fatalf("concurrent cleaners reported %d deletions in total, want %d — no row may be deleted twice", got, total/2)
	}
}

func testRescuedJobKeepsPriorityBoost(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	boosted := row("boosted", "q")
	boosted.PriorityBoost = time.Hour
	enqueue(t, b, boosted)
	claimOne(t, b, "q")

	rescueAt := T0.Add(2 * time.Hour)
	if _, err := b.RescueStale(ctx, backend.RescueParams{Now: rescueAt, TTL: time.Minute, Limit: 10}); err != nil {
		t.Fatal(err)
	}
	fresh := row("fresh", "q")
	fresh.ScheduledAt = rescueAt.Add(-30 * time.Minute)
	if err := b.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{fresh}, Now: rescueAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.MoveDue(ctx, backend.MoveDueParams{Now: rescueAt, Limit: 100}); err != nil {
		t.Fatal(err)
	}
	got := fetch(t, b, "q", 2, rescueAt)
	if len(got) != 2 {
		t.Fatalf("fetched %d rows, want 2", len(got))
	}
	if got[0].ID != boosted.ID {
		t.Fatalf("first claim after the rescue is job %d, want the boosted job %d — a rescue must re-apply the job's boost to its new priority instant", got[0].ID, boosted.ID)
	}
	if got[0].PriorityBoost != time.Hour {
		t.Fatalf("the rescued job's boost is %s, want 1h — a rescue must not clear it", got[0].PriorityBoost)
	}
}

func testCleanDistinguishesTerminalStates(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	states := []backend.State{backend.StateCompleted, backend.StateCancelled, backend.StateDead}
	for _, short := range states {
		for _, st := range states {
			enqueue(t, b, row("term", "q"))
			c := claimOne(t, b, "q")
			switch st {
			case backend.StateCompleted:
				if err := b.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation}}, Now: T0}); err != nil {
					t.Fatal(err)
				}
			case backend.StateCancelled:
				if err := b.Cancel(ctx, backend.CancelParams{Jobs: []backend.JobCancel{{ID: c.ID, Generation: c.Generation, Err: "stop"}}, Now: T0}); err != nil {
					t.Fatal(err)
				}
			case backend.StateDead:
				if err := b.Kill(ctx, backend.KillParams{Jobs: []backend.JobKill{{ID: c.ID, Generation: c.Generation, Err: backend.AttemptError{At: T0, Attempt: c.Attempt, Err: "fatal"}}}, Now: T0}); err != nil {
					t.Fatal(err)
				}
			}
		}

		retentions := map[backend.State]time.Duration{
			backend.StateCompleted: 100 * time.Hour,
			backend.StateCancelled: 100 * time.Hour,
			backend.StateDead:      100 * time.Hour,
		}
		retentions[short] = time.Hour
		n, err := b.Clean(ctx, backend.CleanParams{
			Now:                T0.Add(2 * time.Hour),
			CompletedRetention: retentions[backend.StateCompleted],
			CancelledRetention: retentions[backend.StateCancelled],
			DeadRetention:      retentions[backend.StateDead],
			Limit:              100,
		})
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("short=%s: clean removed %d rows at a retention only the %s job should clear, want 1 — Complete, Cancel, and Kill must each record their own terminal State, not share one another's", short, n, short)
		}

		n, err = b.Clean(ctx, backend.CleanParams{
			Now:                T0.Add(1000 * time.Hour),
			CompletedRetention: time.Hour,
			CancelledRetention: time.Hour,
			DeadRetention:      time.Hour,
			Limit:              100,
		})
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("short=%s: sweeping the remainder removed %d rows, want 2 — the other two terminal jobs must still exist, gated by their own longer retention", short, n)
		}
	}
}

func testMetadataRoundTripsThroughLifecycle(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	r := row("meta", "q")
	r.Metadata = []byte(`{"a":1}`)
	enqueue(t, b, r)
	c := claimOne(t, b, "q")
	requireMetaEquals(t, "after enqueue", c.Metadata, map[string]any{"a": float64(1)})

	if err := b.Snooze(ctx, backend.SnoozeParams{Jobs: []backend.JobSnooze{
		{ID: c.ID, Generation: c.Generation, At: T0.Add(time.Minute), Metadata: []byte(`{"b":2}`)},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	c2 := refetchAfterRetry(t, b, "q", T0.Add(time.Minute))
	requireMetaEquals(t, "after a snooze that supplied new metadata", c2.Metadata, map[string]any{"b": float64(2)})

	if err := b.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c2.ID, Generation: c2.Generation, At: T0.Add(2 * time.Minute), Err: backend.AttemptError{At: T0.Add(time.Minute), Attempt: c2.Attempt, Err: "boom"}},
	}, Now: T0.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	c3 := refetchAfterRetry(t, b, "q", T0.Add(2*time.Minute))
	requireMetaEquals(t, "after a retry with empty Metadata", c3.Metadata, map[string]any{"b": float64(2)})
}

func requireMetaEquals(t *testing.T, when string, got []byte, want map[string]any) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("%s: metadata %q is not valid JSON: %v", when, got, err)
	}
	if len(m) != len(want) {
		t.Fatalf("%s: metadata is %v, want %v", when, m, want)
	}
	for k, v := range want {
		if m[k] != v {
			t.Fatalf("%s: metadata is %v, want %v", when, m, want)
		}
	}
}

func testErrorHistoryAccumulates(t *testing.T, b backend.Backend) {
	ctx := context.Background()
	enqueue(t, b, row("retried", "q"))
	c := claimOne(t, b, "q")
	if err := b.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: T0.Add(time.Minute), Err: backend.AttemptError{At: T0, Attempt: c.Attempt, Err: "first failure"}},
	}, Now: T0}); err != nil {
		t.Fatal(err)
	}
	c2 := refetchAfterRetry(t, b, "q", T0.Add(time.Minute))
	if err := b.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c2.ID, Generation: c2.Generation, At: T0.Add(2 * time.Minute), Err: backend.AttemptError{At: T0.Add(time.Minute), Attempt: c2.Attempt, Err: "second failure"}},
	}, Now: T0.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	c3 := refetchAfterRetry(t, b, "q", T0.Add(2*time.Minute))
	if len(c3.Errors) != 2 || c3.Errors[0].Err != "first failure" || c3.Errors[1].Err != "second failure" {
		t.Fatalf("errors after two retries = %v, want [%q %q] in order — the history must accumulate, not be replaced", c3.Errors, "first failure", "second failure")
	}
}
