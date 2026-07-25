package goque

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func fastConfig(queues map[string]QueueConfig) Config {
	return Config{
		Queues:            queues,
		HeartbeatInterval: 20 * time.Millisecond,
		RescueTTL:         60 * time.Millisecond,
		MoverInterval:     10 * time.Millisecond,
		CleanInterval:     50 * time.Millisecond,
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type slowArgs struct{}

func (slowArgs) Kind() string { return "test.slow" }

func TestStartRequiresQueues(t *testing.T) {
	c, err := NewClient(memory.New(), &Config{Workers: NewWorkers()})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("Start without queues must error")
	}
}

func TestConcurrencyCapRespected(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	var inFlight, maxInFlight atomic.Int64
	release := make(chan struct{})
	err := RegisterFunc(w, func(ctx context.Context, job *Job[slowArgs]) error {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 3, PollInterval: 10 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := c.Enqueue(context.Background(), slowArgs{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "3 in flight", func() bool { return inFlight.Load() == 3 })
	time.Sleep(50 * time.Millisecond)
	if maxInFlight.Load() != 3 {
		t.Fatalf("max in flight %d, want 3", maxInFlight.Load())
	}
	close(release)
	waitFor(t, "drain", func() bool { return inFlight.Load() == 0 })
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteCancelViaHeartbeat(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	started := make(chan int64, 1)
	err := RegisterFunc(w, func(ctx context.Context, job *Job[slowArgs]) error {
		started <- job.ID
		<-ctx.Done()
		return Cancel(ctx.Err())
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Enqueue(context.Background(), slowArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := <-started
	if id != res.Job.ID {
		t.Fatalf("started %d want %d", id, res.Job.ID)
	}
	b.SetCancelRequested(id)
	waitFor(t, "cancelled state", func() bool {
		s := b.Snapshot(id)
		return s != nil && s.State == backend.StateCancelled
	})
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestStopReturnsAtDeadlineAndRejectsStart(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	release := make(chan struct{})
	err := RegisterFunc(w, func(ctx context.Context, job *Job[slowArgs]) error {
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), slowArgs{}); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "job running", func() bool {
		n := 0
		c.active.Range(func(_, _ any) bool { n++; return true })
		return n == 1
	})
	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = c.Stop(stopCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop must return deadline error, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Stop blocked past its deadline")
	}
	if err := c.Start(context.Background()); err == nil {
		t.Fatal("Start during stopping must be rejected")
	}
	close(release)
	waitFor(t, "drain finished", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return !c.stopping
	})
}

type qaArgs struct{}

func (qaArgs) Kind() string { return "test.qa" }

type qbArgs struct{}

func (qbArgs) Kind() string { return "test.qb" }

func TestWeightedQuotaNoStarvation(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	var runningA, runningB atomic.Int64
	block := make(chan struct{})
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[qaArgs]) error {
		runningA.Add(1)
		<-block
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[qbArgs]) error {
		runningB.Add(1)
		<-block
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{
		"a": {Concurrency: 4, Weight: 3, PollInterval: 10 * time.Millisecond},
		"b": {Concurrency: 4, Weight: 1, PollInterval: 10 * time.Millisecond},
	})
	cfg.MaxWorkers = 4
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := c.Enqueue(context.Background(), qaArgs{}, WithQueue("a")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Enqueue(context.Background(), qbArgs{}, WithQueue("b")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "pool saturated", func() bool { return runningA.Load()+runningB.Load() == 4 })
	if runningB.Load() < 1 {
		t.Fatalf("low-weight queue starved: a=%d b=%d", runningA.Load(), runningB.Load())
	}
	if runningA.Load() < runningB.Load() {
		t.Fatalf("weights not respected: a=%d b=%d", runningA.Load(), runningB.Load())
	}
	close(block)
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

type drainArgs struct{}

func (drainArgs) Kind() string { return "test.drain" }

func TestHeartbeatContinuesDuringDrain(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	release := make(chan struct{})
	started := make(chan int64, 1)
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[drainArgs]) error {
		started <- job.ID
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.HeartbeatInterval = 15 * time.Millisecond
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Enqueue(context.Background(), drainArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	id := <-started
	if id != res.Job.ID {
		t.Fatalf("started %d want %d", id, res.Job.ID)
	}
	initHB := b.Snapshot(id).HeartbeatAt
	waitFor(t, "heartbeat active before stop", func() bool { return b.Snapshot(id).HeartbeatAt.After(initHB) })
	hb0 := b.Snapshot(id).HeartbeatAt
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopErr := make(chan error, 1)
	go func() { stopErr <- c.Stop(stopCtx) }()
	waitFor(t, "heartbeat renewed while draining", func() bool { return b.Snapshot(id).HeartbeatAt.After(hb0) })
	close(release)
	if err := <-stopErr; err != nil {
		t.Fatalf("Stop returned %v", err)
	}
	waitFor(t, "job completed", func() bool {
		s := b.Snapshot(id)
		return s != nil && s.State == backend.StateCompleted
	})
}

func TestWeightedSingleSlotNoPermanentStarvation(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	release := make(chan struct{})
	var doneA, doneB atomic.Int64
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[qaArgs]) error {
		<-release
		doneA.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[qbArgs]) error {
		<-release
		doneB.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{
		"a": {Concurrency: 1, Weight: 3, PollInterval: 5 * time.Millisecond},
		"b": {Concurrency: 1, Weight: 1, PollInterval: 5 * time.Millisecond},
	})
	cfg.MaxWorkers = 1
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := c.Enqueue(context.Background(), qaArgs{}, WithQueue("a")); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Enqueue(context.Background(), qbArgs{}, WithQueue("b")); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		prev := doneA.Load() + doneB.Load()
		release <- struct{}{}
		waitFor(t, "completion advances", func() bool { return doneA.Load()+doneB.Load() > prev })
	}
	gotA := doneA.Load()
	gotB := doneB.Load()
	if gotA+gotB != 8 {
		t.Fatalf("expected 8 completions, got a=%d b=%d", gotA, gotB)
	}
	if gotB < 2 {
		t.Fatalf("low-weight queue permanently starved: a=%d b=%d", gotA, gotB)
	}
	if gotA <= gotB {
		t.Fatalf("weight not respected: a=%d b=%d", gotA, gotB)
	}
	close(release)
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

type startCtxArgs struct{}

func (startCtxArgs) Kind() string { return "test.startctx" }

func TestStartContextCancellationStopsFetching(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	var executed atomic.Int64
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[startCtxArgs]) error {
		executed.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()
	if err := c.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	first, err := c.Enqueue(context.Background(), startCtxArgs{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "first job completed", func() bool {
		s := b.Snapshot(first.Job.ID)
		return s != nil && s.State == backend.StateCompleted
	})
	cancelStart()
	time.Sleep(100 * time.Millisecond)
	second, err := c.Enqueue(context.Background(), startCtxArgs{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := executed.Load(); got != 1 {
		t.Fatalf("executed %d jobs, want 1: fetching continued after Start's context was cancelled", got)
	}
	s := b.Snapshot(second.Job.ID)
	if s == nil {
		t.Fatal("second job missing from backend")
	}
	if s.State.Terminal() {
		t.Fatalf("second job reached terminal state %s after Start's context was cancelled", s.State)
	}
	if s.Attempt != 0 {
		t.Fatalf("second job was claimed %d times, want 0", s.Attempt)
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStop()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestStartContextCancellationKeepsHeartbeatsAlive(t *testing.T) {
	b := memory.New()
	w := NewWorkers()
	release := make(chan struct{})
	started := make(chan int64, 1)
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[startCtxArgs]) error {
		started <- job.ID
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.HeartbeatInterval = 15 * time.Millisecond
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Enqueue(context.Background(), startCtxArgs{})
	if err != nil {
		t.Fatal(err)
	}
	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()
	if err := c.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	id := <-started
	if id != res.Job.ID {
		t.Fatalf("started %d want %d", id, res.Job.ID)
	}
	hb0 := b.Snapshot(id).HeartbeatAt
	cancelStart()
	waitFor(t, "heartbeat renewed after Start's context was cancelled", func() bool {
		return b.Snapshot(id).HeartbeatAt.After(hb0)
	})
	if s := b.Snapshot(id); s.State != backend.StateRunning {
		t.Fatalf("job in state %s while still blocked, want running", s.State)
	}
	close(release)
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatalf("Stop returned %v", err)
	}
	waitFor(t, "job completed", func() bool {
		s := b.Snapshot(id)
		return s != nil && s.State == backend.StateCompleted
	})
}

type failingMaintenanceBackend struct {
	*memory.Backend
}

func (f *failingMaintenanceBackend) MoveDue(ctx context.Context, params backend.MoveDueParams) (int, error) {
	return 0, errors.New("mover exploded")
}

func TestMaintenanceErrorsAreLogged(t *testing.T) {
	var sb syncBuffer
	fb := &failingMaintenanceBackend{Backend: memory.New()}
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[slowArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.Logger = slog.New(slog.NewTextHandler(&sb, nil))
	cfg.Workers = w
	c, err := NewClient(fb, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "maintenance error logged", func() bool {
		return strings.Contains(sb.String(), "maintenance failed") && strings.Contains(sb.String(), "mover")
	})
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}
