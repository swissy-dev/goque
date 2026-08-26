package conformance_test

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

func TestFetchOrdersByEffectiveTimeNotByID(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	late := &backend.JobRow{Kind: "late", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(2 * time.Minute)}
	early := &backend.JobRow{Kind: "early", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Minute)}
	boosted := &backend.JobRow{Kind: "boosted", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(3 * time.Minute), PriorityBoost: 10 * time.Minute}
	fetchNow := t0.Add(time.Hour)
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{late, early, boosted}, Now: fetchNow}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 10, ClientID: "c1", Now: fetchNow})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("claimed %d rows, want 3", len(got))
	}
	want := []string{"boosted", "early", "late"}
	for i, w := range want {
		if got[i].Kind != w {
			t.Fatalf("claim %d is %q, want %q — insertion order deliberately contradicts priority order, so a bare UPDATE ... RETURNING would fail here", i, got[i].Kind, w)
		}
	}
}

func TestFetchClaimsExclusivelyAndStampsTheRow(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{
		{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 10, ClientID: "worker-1", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("claimed %d rows, want 1", len(got))
	}
	j := got[0]
	if j.State != backend.StateRunning {
		t.Fatalf("a claimed job is %q, want running", j.State)
	}
	if j.Attempt != 1 || j.Generation != 1 {
		t.Fatalf("a first claim gives attempt=%d generation=%d, want 1 and 1", j.Attempt, j.Generation)
	}
	if len(j.AttemptedBy) != 1 || j.AttemptedBy[0] != "worker-1" {
		t.Fatalf("AttemptedBy is %v, want [worker-1]", j.AttemptedBy)
	}
	if !j.HeartbeatAt.Equal(t0) || !j.AttemptedAt.Equal(t0) {
		t.Fatalf("AttemptedAt=%s HeartbeatAt=%s, want both %s", j.AttemptedAt, j.HeartbeatAt, t0)
	}
	again, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 10, ClientID: "worker-2", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("a second fetch claimed %d rows; a running job must not be claimable", len(again))
	}
}

func TestFetchSkipsOtherQueuesAndNotYetDue(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{
		{Kind: "other", Queue: "elsewhere", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0},
		{Kind: "future", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)},
		{Kind: "due", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 10, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != "due" {
		t.Fatalf("claimed %d rows %v, want only the due job on queue q", len(got), got)
	}
}

func TestFetchRoundTripsEveryColumn(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{
		Kind: "k", Queue: "q", MaxAttempts: 7, ScheduledAt: t0, PriorityBoost: 90 * time.Second,
		Payload:     []byte(`{"a":1}`),
		Metadata:    []byte(`{"metaMarker":"m"}`),
		RetryPolicy: []byte(`{"retryMarker":"r"}`),
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	j := got[0]
	if j.MaxAttempts != 7 {
		t.Fatalf("MaxAttempts is %d, want 7", j.MaxAttempts)
	}
	if j.PriorityBoost != 90*time.Second {
		t.Fatalf("PriorityBoost is %s, want 90s", j.PriorityBoost)
	}
	if !j.ScheduledAt.Equal(t0) {
		t.Fatalf("ScheduledAt is %s, want %s", j.ScheduledAt, t0)
	}
	if !j.PriorityAt.Equal(t0.Add(-90 * time.Second)) {
		t.Fatalf("PriorityAt is %s, want ScheduledAt minus the boost", j.PriorityAt)
	}
	if !strings.Contains(string(j.RetryPolicy), `"retryMarker"`) || strings.Contains(string(j.RetryPolicy), `"metaMarker"`) {
		t.Fatalf("RetryPolicy is %s, want it to contain retryMarker and not metaMarker — retry_policy and metadata must not cross columns", j.RetryPolicy)
	}
	if !strings.Contains(string(j.Metadata), `"metaMarker"`) || strings.Contains(string(j.Metadata), `"retryMarker"`) {
		t.Fatalf("Metadata is %s, want it to contain metaMarker and not retryMarker — retry_policy and metadata must not cross columns", j.Metadata)
	}
	if !j.FinalizedAt.IsZero() {
		t.Fatalf("FinalizedAt is %s on a running job, want the zero time", j.FinalizedAt)
	}
	if j.CancelRequested {
		t.Fatal("CancelRequested is true on a fresh claim")
	}
	if len(j.Errors) != 0 {
		t.Fatalf("Errors is %v on a fresh claim, want empty", j.Errors)
	}
	if j.Version != 1 {
		t.Fatalf("Version is %d, want the default 1", j.Version)
	}
}

func TestFetchRoundTripsTheReservedColumns(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	deadline := t0.Add(45 * time.Minute)
	r := &backend.JobRow{
		Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0,
		ConcurrencyKey:   "tenant-7",
		ThrottleKey:      "api-writes",
		DebounceKey:      "user-42-save",
		DebounceDeadline: deadline,
		Output:           []byte(`{"result":"ok"}`),
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	j := got[0]
	if j.ConcurrencyKey != "tenant-7" || j.ThrottleKey != "api-writes" || j.DebounceKey != "user-42-save" {
		t.Fatalf("reserved keys came back as %q/%q/%q, want tenant-7/api-writes/user-42-save — these columns exist in the schema and the in-memory backend preserves them",
			j.ConcurrencyKey, j.ThrottleKey, j.DebounceKey)
	}
	if !j.DebounceDeadline.Equal(deadline) {
		t.Fatalf("DebounceDeadline is %s, want %s", j.DebounceDeadline, deadline)
	}
	if len(j.Output) == 0 {
		t.Fatal("Output came back empty")
	}
}

func TestFetchLeavesUnsetReservedColumnsZero(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{
		{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	j := got[0]
	if j.ConcurrencyKey != "" || j.ThrottleKey != "" || j.DebounceKey != "" {
		t.Fatalf("unset reserved keys came back as %q/%q/%q, want empty", j.ConcurrencyKey, j.ThrottleKey, j.DebounceKey)
	}
	if !j.DebounceDeadline.IsZero() {
		t.Fatalf("unset DebounceDeadline is %s, want the zero time", j.DebounceDeadline)
	}
	if len(j.Output) != 0 {
		t.Fatalf("unset Output is %q, want empty", j.Output)
	}
}

func TestFetchSubMicrosecondBoostsOrderCorrectly(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	early := &backend.JobRow{Kind: "early", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: 2 * time.Nanosecond}
	late := &backend.JobRow{Kind: "late", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: time.Nanosecond}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{late, early}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 2, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Kind != "early" {
		t.Fatalf("first claim is %v, want early — a one-nanosecond difference must order the fetch, which TIMESTAMPTZ would quantize away", got)
	}
}

func TestFetchExcludesAnAvailableJobThatIsNotYetDue(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	h.makeAvailable(ctx, t, r.ID)

	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 10, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("claimed %d rows for a job forced available ahead of its ScheduledAt, want 0 — only the due-time filter can exclude it here", len(got))
	}
}

func TestFetchLimitAndInnerOrderingSelectTheHighestPriorityJobs(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	j1 := &backend.JobRow{Kind: "j1", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: 100 * time.Minute}
	j2 := &backend.JobRow{Kind: "j2", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: 10 * time.Minute}
	j3 := &backend.JobRow{Kind: "j3", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: 90 * time.Minute}
	j4 := &backend.JobRow{Kind: "j4", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: 20 * time.Minute}
	j5 := &backend.JobRow{Kind: "j5", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: 0}
	jobs := []*backend.JobRow{j1, j2, j3, j4, j5}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: jobs, Now: t0}); err != nil {
		t.Fatal(err)
	}

	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 2, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("claimed %d rows with Limit 2, want 2", len(got))
	}
	if got[0].Kind != "j1" || got[1].Kind != "j3" {
		t.Fatalf("claimed %q then %q, want j1 then j3 — the two highest-priority jobs, in priority order", got[0].Kind, got[1].Kind)
	}
	for _, r := range []*backend.JobRow{j2, j4, j5} {
		if s := h.probe(ctx, t, r.ID); s.State != string(backend.StateAvailable) {
			t.Fatalf("job %s is %q after a Limit-2 fetch claimed only the top two, want it still available", r.Kind, s.State)
		}
	}
}

func TestFetchUnderConcurrencyClaimsEachJobExactlyOnce(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const numJobs = 60
	const numWorkers = 12

	jobs := make([]*backend.JobRow, numJobs)
	for i := range jobs {
		jobs[i] = &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0}
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: jobs, Now: t0}); err != nil {
		t.Fatal(err)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed []*backend.JobRow
	)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 10, ClientID: "w", Now: t0})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			claimed = append(claimed, got...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(claimed) != numJobs {
		t.Fatalf("%d rows claimed across %d concurrent fetches, want exactly %d — none lost, none duplicated", len(claimed), numWorkers, numJobs)
	}
	seen := make(map[int64]int, numJobs)
	for _, j := range claimed {
		seen[j.ID]++
		if j.Attempt != 1 || j.Generation != 1 {
			t.Fatalf("job %d claimed with attempt=%d generation=%d, want 1 and 1 — a value of 2 means it was claimed twice", j.ID, j.Attempt, j.Generation)
		}
	}
	for _, j := range jobs {
		if seen[j.ID] != 1 {
			t.Fatalf("job %d was claimed %d times, want exactly 1", j.ID, seen[j.ID])
		}
	}
}

func TestFetchNoOpOnNonPositiveLimit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{
		{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0},
	}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	before := h.all(ctx, t)

	for _, limit := range []int{0, -1} {
		got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: limit, ClientID: "c", Now: t0})
		if err != nil || got != nil {
			t.Fatalf("Fetch with Limit %d = (%v, %v), want (nil, nil)", limit, got, err)
		}
	}

	after := h.all(ctx, t)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("state changed after non-positive-Limit fetches — nothing should have been claimed:\nbefore: %+v\nafter:  %+v", before, after)
	}
}

func TestFetchScansAttemptAndGenerationFromTheirOwnColumns(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 20, ScheduledAt: t0}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET attempt = 5, generation = 9 WHERE id = $1`, r.ID); err != nil {
		t.Fatal(err)
	}

	got, err := h.Fetch(ctx, backend.FetchParams{Queue: "q", Limit: 1, ClientID: "c", Now: t0})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("claimed %d rows, want 1", len(got))
	}
	j := got[0]
	if j.Attempt != 6 {
		t.Fatalf("Attempt is %d, want 6 — the stored attempt (5) advanced by one", j.Attempt)
	}
	if j.Generation != 10 {
		t.Fatalf("Generation is %d, want 10 — the stored generation (9) advanced by one", j.Generation)
	}
}
