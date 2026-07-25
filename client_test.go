package goque

import (
	"context"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type mailArgs struct {
	To string `json:"to"`
}

func (mailArgs) Kind() string { return "test.mail" }

func newTestClient(t *testing.T, cfg Config) (*Client, *memory.Backend) {
	t.Helper()
	b := memory.New()
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[mailArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cfg.Workers = w
	cfg.Now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, b
}

func TestNewClientValidation(t *testing.T) {
	if _, err := NewClient(memory.New(), &Config{Queues: map[string]QueueConfig{"q": {}}}); err == nil {
		t.Fatal("zero concurrency must fail")
	}
	c, err := NewClient(memory.New(), &Config{})
	if err != nil {
		t.Fatal(err)
	}
	if c.cfg.Defaults.Queue != "default" || c.cfg.Defaults.MaxAttempts != 25 {
		t.Fatalf("defaults not applied: %+v", c.cfg.Defaults)
	}
	if c.clientID == "" {
		t.Fatal("clientID must be set")
	}
}

func TestNewClientDoesNotMutateCallerQueues(t *testing.T) {
	orig := map[string]QueueConfig{"q": {Concurrency: 3}}
	if _, err := NewClient(memory.New(), &Config{Queues: orig}); err != nil {
		t.Fatal(err)
	}
	if orig["q"].Weight != 0 || orig["q"].PollInterval != 0 {
		t.Fatalf("caller's Queues map was mutated: %+v", orig["q"])
	}
}

func TestEnqueueAppliesOptionsAndMiddleware(t *testing.T) {
	var sawKinds []string
	cfg := Config{EnqueueMiddleware: []EnqueueMiddleware{func(next EnqueueFunc) EnqueueFunc {
		return func(ctx context.Context, jobs []*JobRow) error {
			for _, j := range jobs {
				sawKinds = append(sawKinds, j.Kind)
			}
			return next(ctx, jobs)
		}
	}}}
	c, b := newTestClient(t, cfg)
	res, err := c.Enqueue(context.Background(), mailArgs{To: "a@b.c"}, WithQueue("mail"), WithPriority(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if res.Job.ID == 0 || res.Skipped {
		t.Fatalf("result %+v", res)
	}
	if res.Job.Queue != "mail" || res.Job.MaxAttempts != 25 {
		t.Fatalf("row %+v", res.Job)
	}
	if len(sawKinds) != 1 || sawKinds[0] != "test.mail" {
		t.Fatalf("middleware saw %v", sawKinds)
	}
	snap := b.Snapshot(res.Job.ID)
	if snap == nil || snap.State != backend.StateAvailable {
		t.Fatalf("snapshot %+v", snap)
	}
	if !snap.PriorityAt.Equal(snap.ScheduledAt.Add(-10 * time.Second)) {
		t.Fatalf("priorityAt %v", snap.PriorityAt)
	}
}

func TestEnqueueManySingleBackendCall(t *testing.T) {
	c, _ := newTestClient(t, Config{})
	res, err := c.EnqueueMany(context.Background(), []InsertParams{
		{Args: mailArgs{To: "x"}},
		{Args: mailArgs{To: "y"}, Opts: []EnqueueOption{WithDelay(time.Hour)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 || res[0].Job.ID == res[1].Job.ID {
		t.Fatalf("results %+v", res)
	}
	if res[1].Job.State != backend.StateScheduled {
		t.Fatalf("delayed state %s", res[1].Job.State)
	}
}

func TestConfigNowInjection(t *testing.T) {
	fixed := time.Unix(1_700_000_000, 0).UTC()
	c, err := NewClient(memory.New(), &Config{Now: func() time.Time { return fixed }})
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Enqueue(context.Background(), mailArgs{To: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Job.CreatedAt.Equal(fixed) {
		t.Fatalf("CreatedAt=%v want %v", res.Job.CreatedAt, fixed)
	}
	if !res.Job.ScheduledAt.Equal(fixed) {
		t.Fatalf("ScheduledAt=%v want %v", res.Job.ScheduledAt, fixed)
	}
}
