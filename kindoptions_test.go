package goque

import (
	"context"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend/memory"
)

type instanceOptsArgs struct {
	Plan string `json:"plan"`
}

func (instanceOptsArgs) Kind() string { return "test.instanceopts" }

func (a instanceOptsArgs) Priority() time.Duration {
	if a.Plan == "enterprise" {
		return 10 * time.Minute
	}
	return 0
}

func (a instanceOptsArgs) JobOptions() JobOptions {
	if a.Plan == "enterprise" {
		return JobOptions{Queue: "priority", MaxAttempts: 50}
	}
	return JobOptions{Queue: "bulk", MaxAttempts: 5}
}

func TestKindDefaultsSeeArgsInstance(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	build := func(registered bool, args instanceOptsArgs) *JobRow {
		t.Helper()
		w := NewWorkers()
		if registered {
			if err := RegisterFunc(w, func(ctx context.Context, job *Job[instanceOptsArgs]) error { return nil }); err != nil {
				t.Fatal(err)
			}
		}
		c, err := NewClient(memory.New(), &Config{Workers: w, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.Enqueue(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		return res.Job
	}

	ent := instanceOptsArgs{Plan: "enterprise"}
	free := instanceOptsArgs{Plan: "free"}

	for _, registered := range []bool{false, true} {
		got := build(registered, ent)
		if got.PriorityBoost != 10*time.Minute {
			t.Fatalf("registered=%v enterprise boost=%v want 10m", registered, got.PriorityBoost)
		}
		if got.Queue != "priority" || got.MaxAttempts != 50 {
			t.Fatalf("registered=%v enterprise queue=%q attempts=%d", registered, got.Queue, got.MaxAttempts)
		}
		got = build(registered, free)
		if got.PriorityBoost != 0 || got.Queue != "bulk" || got.MaxAttempts != 5 {
			t.Fatalf("registered=%v free boost=%v queue=%q attempts=%d", registered, got.PriorityBoost, got.Queue, got.MaxAttempts)
		}
	}
}
