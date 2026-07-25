package goque

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
)

type optArgs struct{}

func (optArgs) Kind() string { return "test.opt" }

func (optArgs) JobOptions() JobOptions {
	return JobOptions{Queue: "mail", MaxAttempts: 7, Timeout: 9 * time.Second}
}

func (optArgs) Priority() time.Duration { return 30 * time.Second }

func TestResolutionHierarchy(t *testing.T) {
	client := JobOptions{Queue: "default", MaxAttempts: 25, RetryPolicy: Fixed{Interval: time.Second}}
	kind := kindOptions(optArgs{})
	got := resolveOptions(client, kind, []EnqueueOption{WithMaxAttempts(3)})
	if got.Queue != "mail" {
		t.Fatalf("queue=%s want mail (kind beats client)", got.Queue)
	}
	if got.MaxAttempts != 3 {
		t.Fatalf("maxAttempts=%d want 3 (enqueue beats kind)", got.MaxAttempts)
	}
	if got.PriorityBoost != 30*time.Second {
		t.Fatalf("boost=%v want 30s (PriorityProvider)", got.PriorityBoost)
	}
	if got.Timeout != 9*time.Second {
		t.Fatalf("timeout=%v", got.Timeout)
	}
	if _, ok := got.RetryPolicy.(Fixed); !ok {
		t.Fatalf("retry policy must fall through to client default, got %T", got.RetryPolicy)
	}
}

func TestBuildRow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	resolved := resolveOptions(JobOptions{Queue: "default", MaxAttempts: 25}, kindOptions(optArgs{}), []EnqueueOption{WithDelay(time.Minute), WithMetadata([]byte(`{"tenant":"t1"}`))})
	row, err := buildRow(optArgs{}, resolved, now)
	if err != nil {
		t.Fatal(err)
	}
	if row.Kind != "test.opt" || row.Queue != "mail" {
		t.Fatalf("row basics: %+v", row)
	}
	if !row.ScheduledAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("scheduledAt=%v", row.ScheduledAt)
	}
	if row.Version != 1 {
		t.Fatalf("version=%d", row.Version)
	}
	d, ok := envelopeTimeout(row.Metadata)
	if !ok || d != 9*time.Second {
		t.Fatalf("envelope timeout=%v ok=%v", d, ok)
	}
	meta, n := bumpSnoozes(row.Metadata)
	if n != 1 {
		t.Fatalf("first bump=%d", n)
	}
	if _, n2 := bumpSnoozes(meta); n2 != 2 {
		t.Fatalf("second bump=%d", n2)
	}
}

func TestConflictingSchedule(t *testing.T) {
	resolved := resolveOptions(JobOptions{}, JobOptions{}, []EnqueueOption{WithDelay(time.Minute), WithScheduledAt(time.Unix(1, 0))})
	if _, err := buildRow(optArgs{}, resolved, time.Now()); !errors.Is(err, backend.ErrConflictingOptions) {
		t.Fatalf("want ErrConflictingOptions, got %v", err)
	}
}

func TestBuildRowRejectsNonObjectMetadata(t *testing.T) {
	bad := [][]byte{
		[]byte("[1,2,3]"),
		[]byte(`"str"`),
		[]byte("not json"),
		[]byte("{"),
	}
	for _, m := range bad {
		resolved := resolveOptions(JobOptions{}, JobOptions{}, []EnqueueOption{WithMetadata(m)})
		if _, err := buildRow(optArgs{}, resolved, time.Now()); !errors.Is(err, backend.ErrInvalidMetadata) {
			t.Fatalf("metadata %q: want ErrInvalidMetadata, got %v", m, err)
		}
	}

	resolved := resolveOptions(JobOptions{}, JobOptions{}, []EnqueueOption{WithMetadata([]byte(`{"tenant":"t1"}`))})
	row, err := buildRow(optArgs{}, resolved, time.Now())
	if err != nil {
		t.Fatalf("valid metadata must not error, got %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(row.Metadata, &m); err != nil {
		t.Fatalf("row.Metadata not valid JSON: %v", err)
	}
	if _, ok := m["tenant"]; !ok {
		t.Fatalf("row.Metadata missing tenant key: %s", row.Metadata)
	}
}
