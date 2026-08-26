package conformance_test

import (
	"context"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

func TestHeartbeatRenewsOnlyTheLiveGeneration(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	renewed := claimOne(ctx, t, h, "renewed", "q")
	ignored := claimOne(ctx, t, h, "ignored", "q")

	at := t0.Add(time.Minute)
	if _, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: at, Jobs: []backend.JobHeartbeat{
		{ID: renewed.ID, Generation: renewed.Generation},
		{ID: ignored.ID, Generation: ignored.Generation + 1},
	}}); err != nil {
		t.Fatal(err)
	}
	gotRenewed := h.probe(ctx, t, renewed.ID)
	gotIgnored := h.probe(ctx, t, ignored.ID)
	if !gotRenewed.HeartbeatAt.Valid || !gotRenewed.HeartbeatAt.Time.UTC().Equal(at) {
		t.Fatalf("job %d has HeartbeatAt %v, want %s — a live entry must renew", renewed.ID, gotRenewed.HeartbeatAt, at)
	}
	if !gotIgnored.HeartbeatAt.Valid || !gotIgnored.HeartbeatAt.Time.UTC().Equal(t0) {
		t.Fatalf("job %d has HeartbeatAt %v, want the claim time %s — a stale generation must renew nothing, or a superseded execution could keep a reclaimed job alive",
			ignored.ID, gotIgnored.HeartbeatAt, t0)
	}
}

func TestHeartbeatDoesNotRenewANonRunningJob(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{
		{ID: c.ID, Generation: c.Generation},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: t0.Add(time.Minute), Jobs: []backend.JobHeartbeat{
		{ID: c.ID, Generation: c.Generation},
	}}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, c.ID); !got.HeartbeatAt.Time.UTC().Equal(t0) {
		t.Fatalf("a completed job's HeartbeatAt moved to %v; only running rows may be renewed", got.HeartbeatAt)
	}
}

func TestHeartbeatDeduplicatesOnIDAndGeneration(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	at := t0.Add(time.Minute)
	if _, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: at, Jobs: []backend.JobHeartbeat{
		{ID: c.ID, Generation: c.Generation + 1},
		{ID: c.ID, Generation: c.Generation},
	}}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, c.ID); !got.HeartbeatAt.Time.UTC().Equal(at) {
		t.Fatalf("HeartbeatAt is %v, want %s — a bogus higher generation alongside the live one must not suppress the renewal, or the job would be rescued and run twice",
			got.HeartbeatAt, at)
	}
}

func TestHeartbeatDedupesASupersededGenerationBelowTheLiveOne(t *testing.T) {
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

	at := t0.Add(time.Minute)
	if _, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c2", Now: at, Jobs: []backend.JobHeartbeat{
		{ID: c.ID, Generation: live - 1},
		{ID: c.ID, Generation: live},
	}}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, c.ID); !got.HeartbeatAt.Valid || !got.HeartbeatAt.Time.UTC().Equal(at) {
		t.Fatalf("HeartbeatAt is %v after heartbeating the live generation alongside a superseded one below it, want %s — the live entry must not be discarded at the dedup stage, or the job would be rescued and run twice",
			got.HeartbeatAt, at)
	}
}

func TestHeartbeatReportsCancelRequested(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	res, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: t0, Jobs: []backend.JobHeartbeat{
		{ID: c.ID, Generation: c.Generation},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CancelRequested) != 0 {
		t.Fatalf("CancelRequested is %v on a job nobody cancelled, want empty", res.CancelRequested)
	}
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET cancel_requested = TRUE WHERE id = $1`, c.ID); err != nil {
		t.Fatal(err)
	}
	res, err = h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: t0, Jobs: []backend.JobHeartbeat{
		{ID: c.ID, Generation: c.Generation},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CancelRequested) != 1 || res.CancelRequested[0] != c.ID {
		t.Fatalf("CancelRequested is %v, want [%d] — nothing else reports a cancellation to a running worker", res.CancelRequested, c.ID)
	}
}

func TestHeartbeatDoesNotReportCancelOnACompletedJob(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if err := h.Complete(ctx, backend.CompleteParams{Jobs: []backend.JobFinalize{
		{ID: c.ID, Generation: c.Generation},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET cancel_requested = TRUE WHERE id = $1`, c.ID); err != nil {
		t.Fatal(err)
	}
	res, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: t0.Add(time.Minute), Jobs: []backend.JobHeartbeat{
		{ID: c.ID, Generation: c.Generation},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CancelRequested) != 0 {
		t.Fatalf("CancelRequested is %v on a completed job, want empty — a job that was never renewed must not be reported as cancel-requested", res.CancelRequested)
	}
}

func TestHeartbeatDoesNotReportCancelOnAStaleGeneration(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	c := claimOne(ctx, t, h, "k", "q")

	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET cancel_requested = TRUE WHERE id = $1`, c.ID); err != nil {
		t.Fatal(err)
	}
	res, err := h.Heartbeat(ctx, backend.HeartbeatParams{ClientID: "c", Now: t0.Add(time.Minute), Jobs: []backend.JobHeartbeat{
		{ID: c.ID, Generation: c.Generation + 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CancelRequested) != 0 {
		t.Fatalf("CancelRequested is %v on a stale-generation entry, want empty — an entry that failed the fence must not be reported as cancel-requested", res.CancelRequested)
	}
}

func TestHeartbeatEmptyBatchIsANoOp(t *testing.T) {
	res, err := newHarness(t).Heartbeat(context.Background(), backend.HeartbeatParams{ClientID: "c", Now: t0})
	if err != nil {
		t.Fatalf("an empty heartbeat must be a no-op, got %v", err)
	}
	if len(res.CancelRequested) != 0 {
		t.Fatalf("CancelRequested is %v, want empty", res.CancelRequested)
	}
}
