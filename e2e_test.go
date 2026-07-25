package goque

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type e2eArgs struct {
	N int `json:"n"`
}

func (e2eArgs) Kind() string { return "test.e2e" }

type flakyArgs struct{}

func (flakyArgs) Kind() string { return "test.flaky" }

type doomedArgs struct{}

func (doomedArgs) Kind() string { return "test.doomed" }

func TestEndToEndLifecycle(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	var done atomic.Int64
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[e2eArgs]) error {
		done.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var tries atomic.Int64
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[flakyArgs]) error {
		if tries.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[doomedArgs]) error {
		panic("always")
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 4, PollInterval: 10 * time.Millisecond}})
	cfg.Defaults.RetryPolicy = Fixed{Interval: 10 * time.Millisecond}
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := c.Enqueue(context.Background(), e2eArgs{N: i}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Enqueue(context.Background(), e2eArgs{N: 100}, WithDelay(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	flaky, err := c.Enqueue(context.Background(), flakyArgs{})
	if err != nil {
		t.Fatal(err)
	}
	doomed, err := c.Enqueue(context.Background(), doomedArgs{}, WithMaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "all plain jobs done", func() bool { return done.Load() == 21 })
	waitFor(t, "flaky completed on retry", func() bool {
		return b.Snapshot(flaky.Job.ID).State == backend.StateCompleted
	})
	waitFor(t, "doomed dead after 2 panics", func() bool {
		return b.Snapshot(doomed.Job.ID).State == backend.StateDead
	})
	s := b.Snapshot(doomed.Job.ID)
	if len(s.Errors) != 2 || s.Errors[0].Stack == "" {
		t.Fatalf("doomed errors: %+v", s.Errors)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}
