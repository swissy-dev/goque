package conformance_test

import (
	"context"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

func TestRescueStaleKeepsThePriorityBoost(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	boosted := &backend.JobRow{Kind: "boosted", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: time.Hour}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{boosted}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c", Now: t0}); err != nil {
		t.Fatal(err)
	}
	rescueAt := t0.Add(2 * time.Hour)
	n, err := h.RescueStale(ctx, backend.RescueParams{Now: rescueAt, TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rescued %d, want 1", n)
	}
	fresh := &backend.JobRow{Kind: "fresh", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: rescueAt.Add(-30 * time.Minute)}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{fresh}, Now: rescueAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.MoveDue(ctx, backend.MoveDueParams{Now: rescueAt, Limit: 100}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 2, ClientID: "c2", Now: rescueAt})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("claimed %d rows, want 2", len(got))
	}
	if got[0].ID != boosted.ID {
		t.Fatalf("first claim is %d, want the rescued boosted job %d — a rescue must re-apply the stored boost", got[0].ID, boosted.ID)
	}
	if got[0].PriorityBoost != time.Hour {
		t.Fatalf("the rescued job's boost is %s, want 1h", got[0].PriorityBoost)
	}
}

func TestRescueStaleSetsScheduledAtToNow(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	stale := claimOne(ctx, t, h, "k", "q")

	rescueAt := t0.Add(time.Hour)
	n, err := h.RescueStale(ctx, backend.RescueParams{Now: rescueAt, TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rescued %d, want 1", n)
	}
	if got := h.probe(ctx, t, stale.ID).ScheduledAtNS; got != rescueAt.UnixNano() {
		t.Fatalf("ScheduledAtNS after rescue is %d, want %d — RescueStale must set ScheduledAt to Now", got, rescueAt.UnixNano())
	}
}

func TestRescueStaleClampsPriorityAtBothEnds(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	ancient := time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)

	positive := claimOne(ctx, t, h, "k", "q")
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET priority_boost_ns = $1, heartbeat_at = $2 WHERE id = $3`,
		int64(90*time.Second), ancient, positive.ID); err != nil {
		t.Fatal(err)
	}
	n, err := h.RescueStale(ctx, backend.RescueParams{Now: backend.MinInstant, TTL: 0, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rescued %d, want 1", n)
	}
	if got := h.probe(ctx, t, positive.ID).PriorityAtNS; got != math.MinInt64 {
		t.Fatalf("PriorityAtNS is %d after a rescue at MinInstant with a stored positive boost, want clamped to math.MinInt64 (%d)", got, int64(math.MinInt64))
	}

	negative := claimOne(ctx, t, h, "k", "q")
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET priority_boost_ns = $1, heartbeat_at = $2 WHERE id = $3`,
		int64(-90*time.Second), ancient, negative.ID); err != nil {
		t.Fatal(err)
	}
	n, err = h.RescueStale(ctx, backend.RescueParams{Now: backend.MaxInstant, TTL: 0, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rescued %d, want 1", n)
	}
	if got := h.probe(ctx, t, negative.ID).PriorityAtNS; got != math.MaxInt64 {
		t.Fatalf("PriorityAtNS is %d after a rescue at MaxInstant with a stored negative boost, want clamped to math.MaxInt64 (%d)", got, int64(math.MaxInt64))
	}
}

func TestRescueStaleLeavesFreshHeartbeatsAlone(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	fresh := claimOne(ctx, t, h, "fresh", "q")

	rescueAt := t0.Add(time.Hour)
	if _, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: rescueAt.Add(-10 * time.Second), Jobs: []backend.JobHeartbeat{
		{ID: fresh.ID, Generation: fresh.Generation},
	}}); err != nil {
		t.Fatal(err)
	}
	n, err := h.RescueStale(ctx, backend.RescueParams{Now: rescueAt, TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rescued %d rows, want 0 — a heartbeat inside the TTL must not be rescued", n)
	}
	if got := h.probe(ctx, t, fresh.ID).State; got != string(backend.StateRunning) {
		t.Fatalf("job %d is %q after a RescueStale sweep while its heartbeat was still fresh, want running", fresh.ID, got)
	}
}

func TestRescueStaleDoesNotRescueExactlyAtTheTTLCutoff(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	boundary := t0.Add(30 * time.Second)
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET heartbeat_at = $1 WHERE id = $2`, boundary, c.ID); err != nil {
		t.Fatal(err)
	}
	rescueAt := boundary.Add(time.Minute)
	n, err := h.RescueStale(ctx, backend.RescueParams{Now: rescueAt, TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rescued %d rows, want 0 — a heartbeat exactly at the TTL cutoff must not be rescued", n)
	}
	if got := h.probe(ctx, t, c.ID).State; got != string(backend.StateRunning) {
		t.Fatalf("job %d is %q after a RescueStale sweep with its heartbeat exactly at the cutoff, want running", c.ID, got)
	}
}

func TestRescueStaleRespectsLimit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	claims := make([]*backend.JobRow, 5)
	for i := range claims {
		claims[i] = claimOne(ctx, t, h, "s", "q")
	}
	rescueAt := t0.Add(time.Hour)
	n, err := h.RescueStale(ctx, backend.RescueParams{Now: rescueAt, TTL: time.Minute, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rescued %d rows with Limit 2 against 5 stale candidates, want 2", n)
	}
	retryable := 0
	for _, c := range claims {
		if h.probe(ctx, t, c.ID).State == string(backend.StateRetryable) {
			retryable++
		}
	}
	if retryable != 2 {
		t.Fatalf("%d rows are retryable after a Limit-2 RescueStale, want exactly 2 — Limit must bound how many rows are rescued", retryable)
	}
}

func TestCleanLimitIsGlobalAcrossStates(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	var claims []*backend.JobRow
	for i := 0; i < 6; i++ {
		claims = append(claims, claimOne(ctx, t, h, "c", "q"))
	}
	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{
		{ID: claims[0].ID, Generation: claims[0].Generation}, {ID: claims[1].ID, Generation: claims[1].Generation},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Cancel(ctx, backend.CancelParams{Jobs: []backend.JobCancel{
		{ID: claims[2].ID, Generation: claims[2].Generation}, {ID: claims[3].ID, Generation: claims[3].Generation},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Kill(ctx, backend.KillParams{Jobs: []backend.JobKill{
		{ID: claims[4].ID, Generation: claims[4].Generation, Err: backend.AttemptError{At: t0, Attempt: 3, Err: "f"}},
		{ID: claims[5].ID, Generation: claims[5].Generation, Err: backend.AttemptError{At: t0, Attempt: 3, Err: "f"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	p := backend.CleanParams{
		Now: t0.Add(48 * time.Hour), CompletedRetention: time.Hour,
		CancelledRetention: time.Hour, DeadRetention: time.Hour, Limit: 3,
	}
	for i, want := range []int{3, 3, 0} {
		n, err := h.Clean(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Fatalf("clean %d removed %d rows, want %d — Limit is one global cap across all terminal states, not a cap per state", i+1, n, want)
		}
	}
}

func TestCleanDoesNotCleanExactlyAtTheRetentionCutoff(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")
	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation}}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	boundary := t0.Add(30 * time.Second)
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET finalized_at = $1 WHERE id = $2`, boundary, c.ID); err != nil {
		t.Fatal(err)
	}
	cleanAt := boundary.Add(time.Hour)
	n, err := h.Clean(ctx, backend.CleanParams{
		Now: cleanAt, CompletedRetention: time.Hour,
		CancelledRetention: time.Hour, DeadRetention: time.Hour, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("cleaned %d rows, want 0 — a finalized_at exactly at the retention cutoff must not be cleaned", n)
	}
	if got := h.probe(ctx, t, c.ID); got.State != string(backend.StateCompleted) {
		t.Fatalf("job %d is %q after a Clean sweep with FinalizedAt exactly at the cutoff, want it still present and completed", c.ID, got.State)
	}
}

func TestCleanDistinguishesTerminalStates(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	completed := claimOne(ctx, t, h, "completed", "q")
	cancelled := claimOne(ctx, t, h, "cancelled", "q")
	killed := claimOne(ctx, t, h, "killed", "q")
	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: completed.ID, Generation: completed.Generation}}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Cancel(ctx, backend.CancelParams{Jobs: []backend.JobCancel{{ID: cancelled.ID, Generation: cancelled.Generation}}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Kill(ctx, backend.KillParams{Jobs: []backend.JobKill{{ID: killed.ID, Generation: killed.Generation, Err: backend.AttemptError{At: t0, Attempt: 3, Err: "f"}}}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	n, err := h.Clean(ctx, backend.CleanParams{
		Now: t0.Add(48 * time.Hour), CompletedRetention: time.Hour,
		CancelledRetention: 100 * time.Hour, DeadRetention: 100 * time.Hour, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("cleaned %d rows with only CompletedRetention expired, want 1 — the three terminal states must be distinguishable", n)
	}
}

func TestCleanUnderConcurrencyWithLimitBelowCandidateCountDoesNotDoubleCount(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const total = 60
	for i := 0; i < total; i++ {
		c := claimOne(ctx, t, h, "c", "q")
		if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation}}, Now: t0}); err != nil {
			t.Fatal(err)
		}
	}
	p := backend.CleanParams{
		Now: t0.Add(48 * time.Hour), CompletedRetention: time.Hour,
		CancelledRetention: time.Hour, DeadRetention: time.Hour, Limit: 10,
	}
	var cleaned atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := h.Clean(ctx, p)
			if err != nil {
				t.Error(err)
				return
			}
			cleaned.Add(int64(n))
		}()
	}
	wg.Wait()
	if cleaned.Load() != total {
		t.Fatalf("concurrent cleaners with Limit 10 each reported %d in total, want %d — a Limit smaller than the candidate count must not let two cleaners delete the same row", cleaned.Load(), total)
	}
}

func TestMoveDueExcludesNotYetDueJobs(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	future := &backend.JobRow{Kind: "future", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{future}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	n, err := h.MoveDue(ctx, backend.MoveDueParams{Now: t0, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("moved %d rows, want 0 — only the due-time filter can exclude a job scheduled in the future", n)
	}
	if got := h.probe(ctx, t, future.ID).State; got != string(backend.StateScheduled) {
		t.Fatalf("job %d is %q after MoveDue ran before it came due, want scheduled", future.ID, got)
	}
}

func TestMoveDueRespectsLimit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	jobs := make([]*backend.JobRow, 5)
	for i := range jobs {
		jobs[i] = &backend.JobRow{Kind: "m", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)}
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: jobs, Now: t0}); err != nil {
		t.Fatal(err)
	}
	n, err := h.MoveDue(ctx, backend.MoveDueParams{Now: t0.Add(2 * time.Hour), Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("moved %d rows with Limit 2 against 5 due candidates, want 2", n)
	}
	available := 0
	for _, j := range jobs {
		if h.probe(ctx, t, j.ID).State == string(backend.StateAvailable) {
			available++
		}
	}
	if available != 2 {
		t.Fatalf("%d rows are available after a Limit-2 MoveDue, want exactly 2 — Limit must bound how many rows move", available)
	}
}

func TestMoveDueIsIdempotentUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const total = 20
	jobs := make([]*backend.JobRow, total)
	for i := range jobs {
		jobs[i] = &backend.JobRow{Kind: "m", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)}
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: jobs, Now: t0}); err != nil {
		t.Fatal(err)
	}
	var moved atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := h.MoveDue(ctx, backend.MoveDueParams{Now: t0.Add(2 * time.Hour), Limit: 100})
			if err != nil {
				t.Error(err)
				return
			}
			moved.Add(int64(n))
		}()
	}
	wg.Wait()
	if moved.Load() != total {
		t.Fatalf("concurrent movers reported %d promotions in total, want %d — no row may be promoted twice", moved.Load(), total)
	}
}

func TestMoveDueUnderConcurrencyWithLimitBelowCandidateCountDoesNotDoubleCount(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const total = 60
	jobs := make([]*backend.JobRow, total)
	for i := range jobs {
		jobs[i] = &backend.JobRow{Kind: "m", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)}
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: jobs, Now: t0}); err != nil {
		t.Fatal(err)
	}
	var moved atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := h.MoveDue(ctx, backend.MoveDueParams{Now: t0.Add(2 * time.Hour), Limit: 10})
			if err != nil {
				t.Error(err)
				return
			}
			moved.Add(int64(n))
		}()
	}
	wg.Wait()
	if moved.Load() != total {
		t.Fatalf("concurrent movers with Limit 10 each reported %d promotions in total, want %d — a Limit smaller than the candidate count must not let two movers promote the same row", moved.Load(), total)
	}
}

func TestRescueStaleUnderConcurrencyWithLimitBelowCandidateCountDoesNotDoubleCount(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const total = 60
	for i := 0; i < total; i++ {
		claimOne(ctx, t, h, "s", "q")
	}
	rescueAt := t0.Add(time.Hour)
	var rescued atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := h.RescueStale(ctx, backend.RescueParams{Now: rescueAt, TTL: time.Minute, Limit: 10})
			if err != nil {
				t.Error(err)
				return
			}
			rescued.Add(int64(n))
		}()
	}
	wg.Wait()
	if rescued.Load() != total {
		t.Fatalf("concurrent rescuers with Limit 10 each reported %d in total, want %d — a Limit smaller than the candidate count must not let two rescuers claim the same row", rescued.Load(), total)
	}
}

func TestConcurrentSweepsDoNotDoubleCount(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const total = 20
	claims := make([]*backend.JobRow, total)
	for i := range claims {
		claims[i] = claimOne(ctx, t, h, "s", "q")
	}
	for i := 0; i < total/2; i++ {
		if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: claims[i].ID, Generation: claims[i].Generation}}, Now: t0}); err != nil {
			t.Fatal(err)
		}
	}
	var rescued, cleaned atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			n, err := h.RescueStale(ctx, backend.RescueParams{Now: t0.Add(time.Hour), TTL: time.Minute, Limit: 100})
			if err != nil {
				t.Error(err)
				return
			}
			rescued.Add(int64(n))
		}()
		go func() {
			defer wg.Done()
			n, err := h.Clean(ctx, backend.CleanParams{
				Now: t0.Add(48 * time.Hour), CompletedRetention: time.Hour,
				CancelledRetention: time.Hour, DeadRetention: time.Hour, Limit: 100,
			})
			if err != nil {
				t.Error(err)
				return
			}
			cleaned.Add(int64(n))
		}()
	}
	wg.Wait()
	if rescued.Load() != total/2 {
		t.Fatalf("concurrent rescuers reported %d in total, want %d — no job may be rescued twice", rescued.Load(), total/2)
	}
	if cleaned.Load() != total/2 {
		t.Fatalf("concurrent cleaners reported %d in total, want %d — no row may be deleted twice", cleaned.Load(), total/2)
	}
}

func TestMaintenanceSweepsNoOpOnNonPositiveLimit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	due := &backend.JobRow{Kind: "due", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{due}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	claimOne(ctx, t, h, "stale", "q")
	completed := claimOne(ctx, t, h, "completed", "q")
	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: completed.ID, Generation: completed.Generation}}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	before := h.all(ctx, t)

	for _, limit := range []int{0, -1} {
		if n, err := h.MoveDue(ctx, backend.MoveDueParams{Now: t0.Add(2 * time.Hour), Limit: limit}); err != nil || n != 0 {
			t.Fatalf("MoveDue with Limit %d = (%d, %v), want (0, nil)", limit, n, err)
		}
		if n, err := h.RescueStale(ctx, backend.RescueParams{Now: t0.Add(time.Hour), TTL: time.Minute, Limit: limit}); err != nil || n != 0 {
			t.Fatalf("RescueStale with Limit %d = (%d, %v), want (0, nil)", limit, n, err)
		}
		if n, err := h.Clean(ctx, backend.CleanParams{
			Now: t0.Add(48 * time.Hour), CompletedRetention: time.Hour,
			CancelledRetention: time.Hour, DeadRetention: time.Hour, Limit: limit,
		}); err != nil || n != 0 {
			t.Fatalf("Clean with Limit %d = (%d, %v), want (0, nil)", limit, n, err)
		}
	}

	after := h.all(ctx, t)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("state changed after non-positive-Limit sweeps — nothing should have moved, been rescued, or been deleted:\nbefore: %+v\nafter:  %+v", before, after)
	}
}
