package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

func claimOne(ctx context.Context, t *testing.T, h *harness, kind, queue string) *backend.JobRow {
	t.Helper()
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{
		{Kind: kind, Queue: queue, Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: queue, Limit: 1, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one claim, got %d", len(got))
	}
	return got[0]
}

func TestCompleteIsFencedByGeneration(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	stale := backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation + 1}}, Now: t0}
	err := h.Complete(ctx, stale)
	var se *backend.StaleError
	if !errors.As(err, &se) || len(se.IDs) != 1 || se.IDs[0] != c.ID {
		t.Fatalf("a stale generation must report a StaleError naming job %d, got %v", c.ID, err)
	}
	live := backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation}}, Now: t0}
	if err := h.Complete(ctx, live); err != nil {
		t.Fatal(err)
	}
	if err := h.Complete(ctx, live); !errors.Is(err, backend.ErrStaleAttempt) {
		t.Fatalf("completing twice must be stale the second time, got %v", err)
	}
}

func TestPartialBatchAppliesTheLiveEntry(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	live := claimOne(ctx, t, h, "live", "q")
	other := claimOne(ctx, t, h, "other", "q")

	err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{
		{ID: live.ID, Generation: live.Generation},
		{ID: other.ID, Generation: other.Generation + 1},
	}, Now: t0})
	var se *backend.StaleError
	if !errors.As(err, &se) || len(se.IDs) != 1 || se.IDs[0] != other.ID {
		t.Fatalf("StaleError must name exactly the stale entry %d, got %v", other.ID, err)
	}
	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: live.ID, Generation: live.Generation}}, Now: t0}); !errors.Is(err, backend.ErrStaleAttempt) {
		t.Fatal("the live entry in a partial batch was not applied")
	}
	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: other.ID, Generation: other.Generation}}, Now: t0}); err != nil {
		t.Fatalf("a stale entry must leave its job untouched: %v", err)
	}
}

func TestDuplicateEntriesApplyOnceInBothOrders(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	for _, staleFirst := range []bool{false, true} {
		c := claimOne(ctx, t, h, "dup", "q")
		live := backend.JobFinalize{ID: c.ID, Generation: c.Generation}
		stale := backend.JobFinalize{ID: c.ID, Generation: c.Generation + 1}
		jobs := []backend.JobFinalize{live, stale}
		if staleFirst {
			jobs = []backend.JobFinalize{stale, live}
		}
		err := h.Complete(ctx, backend.CompleteParams{Jobs: jobs, Now: t0})
		var se *backend.StaleError
		if !errors.As(err, &se) || len(se.IDs) != 1 || se.IDs[0] != c.ID {
			t.Fatalf("staleFirst=%v: want a StaleError naming job %d, got %v", staleFirst, c.ID, err)
		}
		if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{live}, Now: t0}); !errors.Is(err, backend.ErrStaleAttempt) {
			t.Fatalf("staleFirst=%v: the live entry of a duplicated pair was not applied", staleFirst)
		}
	}
}

func TestRetryAppliesPerRowSchedulesAndErrors(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	soon := claimOne(ctx, t, h, "soon", "q")
	later := claimOne(ctx, t, h, "later", "q")

	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: soon.ID, Generation: soon.Generation, At: t0.Add(time.Minute), Err: backend.AttemptError{At: t0, Attempt: 1, Err: "first"}},
		{ID: later.ID, Generation: later.Generation, At: t0.Add(time.Hour), Err: backend.AttemptError{At: t0, Attempt: 1, Err: "second"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	gotSoon := h.probe(ctx, t, soon.ID)
	gotLater := h.probe(ctx, t, later.ID)
	if gotSoon.ScheduledAtNS != t0.Add(time.Minute).UnixNano() {
		t.Fatalf("job %d rescheduled to %d, want %d — each row carries its own At", soon.ID, gotSoon.ScheduledAtNS, t0.Add(time.Minute).UnixNano())
	}
	if gotLater.ScheduledAtNS != t0.Add(time.Hour).UnixNano() {
		t.Fatalf("job %d rescheduled to %d, want %d — a single At must not be applied to the whole batch", later.ID, gotLater.ScheduledAtNS, t0.Add(time.Hour).UnixNano())
	}
	if !strings.Contains(gotSoon.Errors, "first") || strings.Contains(gotSoon.Errors, "second") {
		t.Fatalf("job %d recorded %s, want only \"first\" — each row carries its own Err", soon.ID, gotSoon.Errors)
	}
	if !strings.Contains(gotLater.Errors, "second") || strings.Contains(gotLater.Errors, "first") {
		t.Fatalf("job %d recorded %s, want only \"second\"", later.ID, gotLater.Errors)
	}
	for _, g := range []stored{gotSoon, gotLater} {
		if g.State != string(backend.StateRetryable) {
			t.Fatalf("job %d is %q after a retry, want retryable", g.ID, g.State)
		}
		if g.FinalizedAt.Valid {
			t.Fatalf("job %d has FinalizedAt set after a retry; a retry must clear it", g.ID)
		}
	}
}

func TestRetryRederivesPriorityFromTheStoredBoost(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: 90 * time.Second}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	claimed, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	at := t0.Add(time.Hour)
	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: claimed[0].ID, Generation: claimed[0].Generation, At: at, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "e"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got := h.probe(ctx, t, r.ID)
	if got.PriorityAtNS != at.Add(-90*time.Second).UnixNano() {
		t.Fatalf("PriorityAt is %d after a retry, want At minus the stored boost (%d)", got.PriorityAtNS, at.Add(-90*time.Second).UnixNano())
	}
}

func TestErrorHistoryAccumulates(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: t0, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "first failure"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	h.makeAvailable(ctx, t, c.ID)
	again, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c2", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 {
		t.Fatalf("re-claimed %d rows, want 1", len(again))
	}
	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: again[0].ID, Generation: again[0].Generation, At: t0, Err: backend.AttemptError{At: t0, Attempt: 2, Err: "second failure"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got := h.probe(ctx, t, c.ID)
	var history []backend.AttemptError
	if err := json.Unmarshal([]byte(got.Errors), &history); err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("error history is %v, want two entries — a retry must append, not replace", history)
	}
	if history[0].Err != "first failure" || history[1].Err != "second failure" {
		t.Fatalf("error history is out of order: %v", history)
	}
	if history[0].Attempt != 1 || history[1].Attempt != 2 {
		t.Fatalf("attempt numbers did not survive the round trip: %v", history)
	}
	if !history[0].At.Equal(t0) {
		t.Fatalf("AttemptError.At came back as %s, want %s", history[0].At, t0)
	}
}

func TestSnoozeDoesNotConsumeAnAttempt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Snooze(ctx, backend.SnoozeParams{Jobs: []backend.JobSnooze{
		{ID: c.ID, Generation: c.Generation, At: t0},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got := h.probe(ctx, t, c.ID)
	if got.Attempt != c.Attempt-1 {
		t.Fatalf("attempt is %d after a snooze, want %d — a snooze must return the attempt the claim consumed", got.Attempt, c.Attempt-1)
	}
	if got.Generation != c.Generation {
		t.Fatalf("generation is %d after a snooze, want %d unchanged — the token only ever rises on a claim", got.Generation, c.Generation)
	}
	if got.Errors != "[]" {
		t.Fatalf("a snooze recorded %s, want no error appended", got.Errors)
	}
	if got.State != string(backend.StateRetryable) {
		t.Fatalf("state is %q after a snooze, want retryable", got.State)
	}
}

func TestKillAndCancelAreTerminal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	killed := claimOne(ctx, t, h, "killed", "q")
	cancelled := claimOne(ctx, t, h, "cancelled", "q")

	if err := h.Kill(ctx, backend.KillParams{Jobs: []backend.JobKill{
		{ID: killed.ID, Generation: killed.Generation, Err: backend.AttemptError{At: t0, Attempt: 3, Err: "fatal"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Cancel(ctx, backend.CancelParams{Jobs: []backend.JobCancel{
		{ID: cancelled.ID, Generation: cancelled.Generation, Err: "stopped"},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	gotKilled := h.probe(ctx, t, killed.ID)
	gotCancelled := h.probe(ctx, t, cancelled.ID)
	if gotKilled.State != string(backend.StateDead) {
		t.Fatalf("a killed job is %q, want dead", gotKilled.State)
	}
	if gotCancelled.State != string(backend.StateCancelled) {
		t.Fatalf("a cancelled job is %q, want cancelled — the two terminal states must be distinguishable", gotCancelled.State)
	}
	if !gotKilled.FinalizedAt.Valid || !gotCancelled.FinalizedAt.Valid {
		t.Fatal("a terminal job must have FinalizedAt set; the cleaner measures retention against it")
	}
	if !strings.Contains(gotKilled.Errors, "fatal") {
		t.Fatalf("a kill recorded %s, want the final error appended", gotKilled.Errors)
	}
	if !strings.Contains(gotCancelled.Errors, "stopped") {
		t.Fatalf("a cancel with a reason recorded %s, want the reason appended", gotCancelled.Errors)
	}
}

func TestCancelWithNoReasonKeepsTheHistory(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: t0, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "earlier failure"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	h.makeAvailable(ctx, t, c.ID)
	again, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c2", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Cancel(ctx, backend.CancelParams{Jobs: []backend.JobCancel{
		{ID: again[0].ID, Generation: again[0].Generation},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got := h.probe(ctx, t, c.ID)
	if !strings.Contains(got.Errors, "earlier failure") {
		t.Fatalf("a cancel with no reason left the history as %s; SQL `errors || NULL` is NULL, which would wipe it", got.Errors)
	}
}

func TestCancelRecordsTheJobsAttempt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: t0, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "first failure"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	h.makeAvailable(ctx, t, c.ID)
	again, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c2", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 {
		t.Fatalf("re-claimed %d rows, want 1", len(again))
	}
	if again[0].Attempt != 2 {
		t.Fatalf("re-claimed attempt is %d, want 2", again[0].Attempt)
	}
	if err := h.Cancel(ctx, backend.CancelParams{Jobs: []backend.JobCancel{
		{ID: again[0].ID, Generation: again[0].Generation, Err: "stopped"},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got := h.probe(ctx, t, c.ID)
	var history []backend.AttemptError
	if err := json.Unmarshal([]byte(got.Errors), &history); err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("error history is %v, want two entries after a retry then a cancel", history)
	}
	if history[1].Attempt != 2 {
		t.Fatalf("cancel recorded attempt %d in the error history, want 2 — the job's attempt when it was cancelled, not 0", history[1].Attempt)
	}
}

func TestRetryDedupTieBreakKeepsTheFirstEntry(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	first := t0.Add(time.Minute)
	second := t0.Add(time.Hour)
	err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: first, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "first"}},
		{ID: c.ID, Generation: c.Generation, At: second, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "second"}},
	}, Now: t0})
	var se *backend.StaleError
	if !errors.As(err, &se) || len(se.IDs) != 1 || se.IDs[0] != c.ID {
		t.Fatalf("an exact (id, generation) duplicate must report the losing ordinal as stale, got %v", err)
	}
	got := h.probe(ctx, t, c.ID)
	if got.ScheduledAtNS != first.UnixNano() {
		t.Fatalf("scheduled_at_ns is %d, want %d (the first entry's At) — an exact duplicate must apply the first entry by ordinality, not the last", got.ScheduledAtNS, first.UnixNano())
	}
}

func TestMetadataRoundTripsThroughAFinalization(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: t0, Metadata: []byte(`{"stage":"one"}`), Err: backend.AttemptError{At: t0, Attempt: 1, Err: "e"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	supplied := h.probe(ctx, t, c.ID)
	if !strings.Contains(supplied.Metadata, `"stage"`) {
		t.Fatalf("metadata is %s after a retry that supplied one, want the supplied value", supplied.Metadata)
	}
	h.makeAvailable(ctx, t, c.ID)
	again, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c2", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: again[0].ID, Generation: again[0].Generation, At: t0, Err: backend.AttemptError{At: t0, Attempt: 2, Err: "e"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, c.ID); got.Metadata != supplied.Metadata {
		t.Fatalf("a retry with no Metadata changed it from %s to %s; an empty Metadata must leave the stored value alone", supplied.Metadata, got.Metadata)
	}
}

func TestCompleteDedupesASupersededGenerationBelowTheLiveOne(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: t0, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "e"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	h.makeAvailable(ctx, t, c.ID)
	again, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c2", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 {
		t.Fatalf("re-claimed %d rows, want 1", len(again))
	}
	live := again[0].Generation

	err = h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{
		{ID: c.ID, Generation: live - 1},
		{ID: c.ID, Generation: live},
	}, Now: t0})
	var se *backend.StaleError
	if !errors.As(err, &se) || len(se.IDs) != 1 || se.IDs[0] != c.ID {
		t.Fatalf("a superseded generation batched with the live one must report a StaleError naming job %d exactly once, got %v", c.ID, err)
	}
	if got := h.probe(ctx, t, c.ID).State; got != string(backend.StateCompleted) {
		t.Fatalf("job %d is %q after completing its live generation alongside a superseded one, want completed — the live entry must not be discarded at the dedup stage", c.ID, got)
	}
}

func TestCompleteStampsFinalizedAt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{{ID: c.ID, Generation: c.Generation}}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got := h.probe(ctx, t, c.ID)
	if !got.FinalizedAt.Valid || !got.FinalizedAt.Time.Equal(t0) {
		t.Fatalf("FinalizedAt is %v after a complete, want %s", got.FinalizedAt, t0)
	}
}

func TestClampNSPinsPriorityAtNSAtBothEnds(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	positive := claimOne(ctx, t, h, "k", "q")
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET priority_boost_ns = $1 WHERE id = $2`,
		int64(90*time.Second), positive.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: positive.ID, Generation: positive.Generation, At: backend.MinInstant, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "e"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, positive.ID).PriorityAtNS; got != math.MinInt64 {
		t.Fatalf("PriorityAtNS is %d after a retry at MinInstant with a stored positive boost, want clamped to math.MinInt64 (%d)", got, int64(math.MinInt64))
	}

	negative := claimOne(ctx, t, h, "k", "q")
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET priority_boost_ns = $1 WHERE id = $2`,
		int64(-90*time.Second), negative.ID); err != nil {
		t.Fatal(err)
	}
	if err := h.Snooze(ctx, backend.SnoozeParams{Jobs: []backend.JobSnooze{
		{ID: negative.ID, Generation: negative.Generation, At: backend.MaxInstant},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, negative.ID).PriorityAtNS; got != math.MaxInt64 {
		t.Fatalf("PriorityAtNS is %d after a snooze at MaxInstant with a stored negative boost, want clamped to math.MaxInt64 (%d)", got, int64(math.MaxInt64))
	}
}

func TestClampNSDoesNotPerturbAnOrdinaryPriorityAt(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET priority_boost_ns = $1 WHERE id = $2`,
		int64(7*time.Second), c.ID); err != nil {
		t.Fatal(err)
	}
	at := t0.Add(time.Hour)
	if err := h.Retry(ctx, backend.RetryParams{Jobs: []backend.JobRetry{
		{ID: c.ID, Generation: c.Generation, At: at, Err: backend.AttemptError{At: t0, Attempt: 1, Err: "e"}},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	want := at.Add(-7 * time.Second).UnixNano()
	if got := h.probe(ctx, t, c.ID).PriorityAtNS; got != want {
		t.Fatalf("PriorityAtNS is %d after an ordinary retry, want %d exactly — clampNS must not perturb an in-range value", got, want)
	}
}
