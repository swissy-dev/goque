package conformance_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

var t0 = time.Unix(1_700_000_000, 0).UTC()

func TestEnqueueAssignsIDsInInputOrder(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	rows := []*backend.JobRow{
		{Kind: "k0", Queue: "q", Payload: []byte(`{"n":0}`), MaxAttempts: 3, ScheduledAt: t0},
		{Kind: "k1", Queue: "q", Payload: []byte(`{"n":1}`), MaxAttempts: 3, ScheduledAt: t0},
		{Kind: "k2", Queue: "q", Payload: []byte(`{"n":2}`), MaxAttempts: 3, ScheduledAt: t0},
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: rows, Now: t0}); err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for i, r := range rows {
		if r.ID == 0 {
			t.Fatalf("row %d was not assigned an ID", i)
		}
		if seen[r.ID] {
			t.Fatalf("row %d reuses ID %d", i, r.ID)
		}
		seen[r.ID] = true
	}
	for i, r := range rows {
		got := h.probe(ctx, t, r.ID)
		if got.Kind != r.Kind {
			t.Fatalf("row %d was assigned ID %d, but that ID holds kind %q, want %q — ids were zipped onto the wrong rows", i, r.ID, got.Kind, r.Kind)
		}
	}
}

func TestEnqueueFillsDerivedFieldsInPlace(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0, PriorityBoost: time.Minute}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if !r.PriorityAt.Equal(t0.Add(-time.Minute)) {
		t.Fatalf("PriorityAt is %s, want ScheduledAt minus the boost", r.PriorityAt)
	}
	if !r.CreatedAt.Equal(t0) {
		t.Fatalf("CreatedAt is %s, want Now", r.CreatedAt)
	}
	if r.State != backend.StateAvailable {
		t.Fatalf("State is %q, want available", r.State)
	}
	got := h.probe(ctx, t, r.ID)
	if got.State != string(backend.StateAvailable) {
		t.Fatalf("stored State is %q, want available", got.State)
	}
	if got.PriorityAtNS != t0.Add(-time.Minute).UnixNano() {
		t.Fatalf("stored PriorityAt is %d, want %d — the caller's struct agreeing proves nothing about what was written",
			got.PriorityAtNS, t0.Add(-time.Minute).UnixNano())
	}
	if got.Attempt != 0 || got.Generation != 0 {
		t.Fatalf("a freshly enqueued row has attempt=%d generation=%d, want 0 and 0", got.Attempt, got.Generation)
	}
}

func TestEnqueueSetsScheduledStateForAFutureJob(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0.Add(time.Hour)}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, r.ID); got.State != string(backend.StateScheduled) {
		t.Fatalf("stored State is %q for a job scheduled an hour out, want scheduled", got.State)
	}
}

func TestEnqueueEmptyBatchIsANoOp(t *testing.T) {
	h := newHarness(t)
	if err := h.Enqueue(context.Background(), backend.EnqueueParams{Now: t0}); err != nil {
		t.Fatalf("an empty batch must be a no-op returning nil, got %v", err)
	}
}

func TestEnqueueRejectsAnUnstorableInstantWithoutWriting(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	good := &backend.JobRow{Kind: "good", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0}
	bad := &backend.JobRow{Kind: "bad", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: backend.MaxInstant.Add(time.Hour)}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{good, bad}, Now: t0}); !errors.Is(err, backend.ErrTimeOutOfRange) {
		t.Fatalf("a batch containing an unstorable instant must fail, got %v", err)
	}
	if rows := h.all(ctx, t); len(rows) != 0 {
		t.Fatalf("a rejected batch stored %d rows; it must store none", len(rows))
	}
}

func TestEnqueueRoundTripsJSONWithoutBase64(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{
		Kind: "k", Queue: "q", MaxAttempts: 3, ScheduledAt: t0,
		Payload:  []byte(`{"user":42,"tags":["a","b"]}`),
		Metadata: []byte(`{"trace":"abc"}`),
	}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	got := h.probe(ctx, t, r.ID)
	for _, want := range []string{`"user"`, `42`, `"tags"`} {
		if !strings.Contains(got.Payload, want) {
			t.Fatalf("stored payload %q is missing %s; base64 or a mangled encoding would look like this", got.Payload, want)
		}
	}
	if !strings.Contains(got.Metadata, `"trace"`) {
		t.Fatalf("stored metadata is %q, want it to contain \"trace\"", got.Metadata)
	}
}

func TestEnqueueNormalizesNilMetadata(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	r := &backend.JobRow{Kind: "k", Queue: "q", Payload: []byte(`{}`), MaxAttempts: 3, ScheduledAt: t0}
	if err := h.Enqueue(ctx, backend.EnqueueParams{Jobs: []*backend.JobRow{r}, Now: t0}); err != nil {
		t.Fatal(err)
	}
	if got := h.probe(ctx, t, r.ID); got.Metadata != "{}" {
		t.Fatalf("stored metadata for a nil Metadata is %q, want {} — the column is NOT NULL and its default does not apply when a value is supplied", got.Metadata)
	}
}
