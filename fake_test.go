package goque

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

type stubTB struct {
	failed string
}

func (s *stubTB) Helper() {}

func (s *stubTB) Fatalf(format string, args ...any) {
	s.failed = fmt.Sprintf(format, args...)
	panic(stubTBAbort{})
}

type stubTBAbort struct{}

func catchAbort(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(stubTBAbort); !ok {
				panic(r)
			}
		}
	}()
	fn()
}

type fkArgs struct {
	N int `json:"n"`
}

func (fkArgs) Kind() string { return "test.fk" }

func newFakeClient(t *testing.T) (*Client, *memory.Backend) {
	t.Helper()
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	b := memory.New()
	c, err := NewClient(b, &Config{Workers: w, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	return c, b
}

func TestFakeClockTakeoverAndControls(t *testing.T) {
	c, _ := newFakeClient(t)
	f := c.Fake(t)
	epoch := time.Unix(1_700_000_000, 0).UTC()
	if !f.Now().Equal(epoch) {
		t.Fatalf("start instant=%v want Config.Now value %v", f.Now(), epoch)
	}
	f.Advance(time.Hour)
	if !f.Now().Equal(epoch.Add(time.Hour)) {
		t.Fatalf("Advance: %v", f.Now())
	}
	res, err := c.Enqueue(context.Background(), fkArgs{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Job.CreatedAt.Equal(epoch.Add(time.Hour)) {
		t.Fatalf("client must stamp via fake clock: %v", res.Job.CreatedAt)
	}
	f.SetNow(epoch.Add(2 * time.Hour))
	if !f.Now().Equal(epoch.Add(2 * time.Hour)) {
		t.Fatalf("SetNow: %v", f.Now())
	}
}

func TestFakeGuards(t *testing.T) {
	stub := &stubTB{}
	c, _ := newFakeClient(t)
	f := c.Fake(stub)
	catchAbort(func() { f.Advance(-time.Second) })
	if !strings.Contains(stub.failed, "negative") {
		t.Fatalf("negative Advance: %q", stub.failed)
	}
	stub.failed = ""
	catchAbort(func() { f.SetNow(time.Unix(0, 0)) })
	if !strings.Contains(stub.failed, "monotonic") {
		t.Fatalf("rewind SetNow: %q", stub.failed)
	}
	if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "fake-driven") {
		t.Fatalf("Start on fake-driven client: %v", err)
	}
}

type nonMemoryBackend struct {
	backend.Backend
}

func TestFakeRequiresMemoryBackend(t *testing.T) {
	stub := &stubTB{}
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(&nonMemoryBackend{Backend: memory.New()}, &Config{Workers: w})
	if err != nil {
		t.Fatal(err)
	}
	catchAbort(func() { c.Fake(stub) })
	if !strings.Contains(stub.failed, "memory") {
		t.Fatalf("non-memory backend: %q", stub.failed)
	}
}

func TestFakeRejectsStartedClient(t *testing.T) {
	stub := &stubTB{}
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{
		Workers: w,
		Queues:  map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	catchAbort(func() { c.Fake(stub) })
	if !strings.Contains(stub.failed, "started") {
		t.Fatalf("started client: %q", stub.failed)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestFakeQueueSeedingAndRecording(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	b := memory.New()
	c, err := NewClient(b, &Config{
		Workers: w,
		Queues:  map[string]QueueConfig{"configured": {Concurrency: 1}},
		Now:     func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), fkArgs{}, WithQueue("prewrap")); err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if _, err := c.Enqueue(context.Background(), fkArgs{}, WithQueue("postwrap")); err != nil {
		t.Fatal(err)
	}
	f.TrackQueue("manual")
	got := f.trackedQueues()
	want := []string{"configured", "manual", "postwrap", "prewrap"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("tracked=%v want %v", got, want)
	}
}

func TestFakeRebindsTB(t *testing.T) {
	c, _ := newFakeClient(t)
	stub1 := &stubTB{}
	stub2 := &stubTB{}
	f1 := c.Fake(stub1)
	f1.Advance(time.Minute)
	f2 := c.Fake(stub2)
	if !f2.Now().Equal(f1.Now()) {
		t.Fatal("re-wrap must share the persistent clock")
	}
	catchAbort(func() { f2.Advance(-time.Second) })
	if stub2.failed == "" || stub1.failed != "" {
		t.Fatalf("failure must land on the rebinding test: stub1=%q stub2=%q", stub1.failed, stub2.failed)
	}
}

func TestFakeStartInstantStripsMonotonicReading(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{Workers: w})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if strings.Contains(f.Now().String(), "m=") {
		t.Fatalf("captured start instant retains a monotonic reading: %v", f.Now())
	}
}

type reentrantTB struct {
	f      *Fake
	failed string
}

func (r *reentrantTB) Helper() {}

func (r *reentrantTB) Fatalf(format string, args ...any) {
	r.f.Now()
	r.failed = fmt.Sprintf(format, args...)
	panic(stubTBAbort{})
}

func TestFakeSetNowFatalfDoesNotDeadlock(t *testing.T) {
	c, _ := newFakeClient(t)
	stub := &reentrantTB{}
	f := c.Fake(stub)
	stub.f = f
	done := make(chan struct{})
	go func() {
		defer close(done)
		catchAbort(func() { f.SetNow(time.Unix(0, 0)) })
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SetNow deadlocked: Fatalf called back into the fake while st.mu was held")
	}
	if !strings.Contains(stub.failed, "monotonic") {
		t.Fatalf("rewind SetNow via reentrant TB: %q", stub.failed)
	}
}

func TestFakeRejectsStoppingClient(t *testing.T) {
	stub := &stubTB{}
	w := NewWorkers()
	release := make(chan struct{})
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error {
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	b := memory.New()
	cfg := fastConfig(map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}})
	cfg.Workers = w
	c, err := NewClient(b, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Enqueue(context.Background(), fkArgs{N: 1}); err != nil {
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
	if err := c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop must return deadline error while job drains, got %v", err)
	}
	catchAbort(func() { c.Fake(stub) })
	const wantGuard = "goque: Fake requires a client that is not started and not stopping"
	if stub.failed != wantGuard {
		t.Fatalf("Fake on a stopping client must be rejected: got %q want %q", stub.failed, wantGuard)
	}
	close(release)
	waitFor(t, "drain finished", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return !c.stopping
	})
}

type clientTouchingTB struct {
	c      *Client
	failed string
}

func (r *clientTouchingTB) Helper() {}

func (r *clientTouchingTB) Fatalf(format string, args ...any) {
	_ = r.c.Start(context.Background())
	r.failed = fmt.Sprintf(format, args...)
	panic(stubTBAbort{})
}

func runWithDeadline(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		catchAbort(fn)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s deadlocked: Fatalf called back into the client while c.mu was held", what)
	}
}

func TestFakeBackendGuardFatalfDoesNotDeadlock(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(&nonMemoryBackend{Backend: memory.New()}, &Config{Workers: w})
	if err != nil {
		t.Fatal(err)
	}
	stub := &clientTouchingTB{c: c}
	runWithDeadline(t, "Fake backend guard", func() { c.Fake(stub) })
	if !strings.Contains(stub.failed, "memory") {
		t.Fatalf("non-memory backend via reentrant TB: %q", stub.failed)
	}
}

func TestFakeStartedGuardFatalfDoesNotDeadlock(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{
		Workers: w,
		Queues:  map[string]QueueConfig{"default": {Concurrency: 1, PollInterval: 10 * time.Millisecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stub := &clientTouchingTB{c: c}
	runWithDeadline(t, "Fake started guard", func() { c.Fake(stub) })
	const wantGuard = "goque: Fake requires a client that is not started and not stopping"
	if stub.failed != wantGuard {
		t.Fatalf("started client via reentrant TB: got %q want %q", stub.failed, wantGuard)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

type recordingTB struct {
	failed string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.failed = fmt.Sprintf(format, args...)
}

func TestFakeSetNowCheckAndSetIsAtomic(t *testing.T) {
	c, _ := newFakeClient(t)
	f := c.Fake(t)
	epoch := f.Now()
	const writers = 32
	for round := 1; round <= 200; round++ {
		base := epoch.Add(time.Duration(round) * time.Hour)
		var mu sync.Mutex
		var maxAccepted time.Time
		var wg sync.WaitGroup
		for i := 1; i <= writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				target := base.Add(time.Duration(i) * time.Millisecond)
				rec := &recordingTB{}
				c.Fake(rec).SetNow(target)
				if rec.failed != "" {
					return
				}
				mu.Lock()
				if target.After(maxAccepted) {
					maxAccepted = target
				}
				mu.Unlock()
			}(i)
		}
		wg.Wait()
		if got := f.Now(); got.Before(maxAccepted) {
			t.Fatalf("round %d: SetNow lost an update: clock=%v is earlier than accepted %v", round, got, maxAccepted)
		}
	}
}

func TestFakeClockSwapIsRaceFree(t *testing.T) {
	for round := 0; round < 5; round++ {
		w := NewWorkers()
		if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
			t.Fatal(err)
		}
		c, err := NewClient(memory.New(), &Config{
			Workers: w,
			EnqueueMiddleware: []EnqueueMiddleware{func(next EnqueueFunc) EnqueueFunc {
				return func(ctx context.Context, jobs []*JobRow) error { return next(ctx, jobs) }
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		const writers = 8
		errs := make(chan error, writers)
		stop := make(chan struct{})
		var ready, done sync.WaitGroup
		ready.Add(writers)
		done.Add(writers)
		for i := 0; i < writers; i++ {
			go func() {
				defer done.Done()
				first := true
				for {
					if _, err := c.Enqueue(context.Background(), fkArgs{N: round}); err != nil {
						errs <- err
						if first {
							ready.Done()
						}
						return
					}
					if first {
						first = false
						ready.Done()
					}
					select {
					case <-stop:
						return
					default:
					}
				}
			}()
		}
		ready.Wait()
		f := c.Fake(t)
		for i := 0; i < 100; i++ {
			f.Advance(time.Millisecond)
			_ = f.Now()
		}
		close(stop)
		done.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: concurrent Enqueue failed during the fake takeover: %v", round, err)
			}
		}
	}
}

func TestFakePreservesEnqueueMiddleware(t *testing.T) {
	var sawKinds []string
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	b := memory.New()
	c, err := NewClient(b, &Config{
		Workers: w,
		EnqueueMiddleware: []EnqueueMiddleware{func(next EnqueueFunc) EnqueueFunc {
			return func(ctx context.Context, jobs []*JobRow) error {
				for _, j := range jobs {
					sawKinds = append(sawKinds, j.Kind)
				}
				return next(ctx, jobs)
			}
		}},
		Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if _, err := c.Enqueue(context.Background(), fkArgs{N: 1}, WithQueue("mw")); err != nil {
		t.Fatal(err)
	}
	if len(sawKinds) != 1 || sawKinds[0] != "test.fk" {
		t.Fatalf("middleware saw %v, want one test.fk job", sawKinds)
	}
	tracked := f.trackedQueues()
	found := false
	for _, q := range tracked {
		if q == "mw" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tracked queues=%v want to include %q", tracked, "mw")
	}
}

type fkFailArgs struct{}

func (fkFailArgs) Kind() string { return "test.fkfail" }

func TestFakeQuickStartFlow(t *testing.T) {
	w := NewWorkers()
	var sent []int
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error {
		sent = append(sent, job.Args.N)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{Workers: w, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if _, err := c.Enqueue(context.Background(), fkArgs{N: 7}, WithDelay(time.Hour)); err != nil {
		t.Fatal(err)
	}
	f.RunReady(context.Background()).AssertNoneRan()
	f.Advance(time.Hour)
	res := f.RunReady(context.Background())
	res.AssertRan("test.fk")
	res.AssertCompleted(1)
	res.AssertRetried(0)
	if len(sent) != 1 || sent[0] != 7 {
		t.Fatalf("sent=%v", sent)
	}
	if !res.Ran("test.fk") || res.Ran("ghost") {
		t.Fatal("Ran predicate wrong")
	}
	if got := res.Of("test.fk"); len(got) != 1 || got[0].Round != 1 {
		t.Fatalf("Of=%+v", got)
	}
}

func TestFakeRetryThenAdvanceThenSucceed(t *testing.T) {
	w := NewWorkers()
	tries := 0
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkFailArgs]) error {
		tries++
		if tries == 1 {
			return fmt.Errorf("transient")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{
		Workers:  w,
		Now:      func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		Defaults: JobOptions{RetryPolicy: Fixed{Interval: 30 * time.Second}},
	})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if _, err := c.Enqueue(context.Background(), fkFailArgs{}); err != nil {
		t.Fatal(err)
	}
	res := f.RunReady(context.Background())
	res.AssertRetried(1)
	if !strings.Contains(res.Jobs[0].Err, "transient") {
		t.Fatalf("Err=%q", res.Jobs[0].Err)
	}
	f.RunReady(context.Background()).AssertNoneRan()
	f.Advance(30 * time.Second)
	f.RunReady(context.Background()).AssertCompleted(1)
	if tries != 2 {
		t.Fatalf("tries=%d", tries)
	}
}

func TestRunUntilIdleGuardTripsOnZeroDelayLoop(t *testing.T) {
	stub := &stubTB{}
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkFailArgs]) error {
		return fmt.Errorf("always")
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{
		Workers:  w,
		Now:      func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		Defaults: JobOptions{RetryPolicy: Fixed{Interval: 0}, MaxAttempts: 1000},
	})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(stub)
	if _, err := c.Enqueue(context.Background(), fkFailArgs{}); err != nil {
		t.Fatal(err)
	}
	catchAbort(func() { f.RunUntilIdle(context.Background(), 5) })
	if !strings.Contains(stub.failed, "maxRounds") {
		t.Fatalf("guard message: %q", stub.failed)
	}
}

func TestRunUntilIdleDrainsCascadesAndTagsRounds(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkFailArgs]) error {
		return fmt.Errorf("retry me")
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{
		Workers:  w,
		Now:      func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		Defaults: JobOptions{RetryPolicy: Fixed{Interval: 0}, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if _, err := c.Enqueue(context.Background(), fkFailArgs{}); err != nil {
		t.Fatal(err)
	}
	res := f.RunUntilIdle(context.Background(), 10)
	res.AssertRetried(2)
	res.AssertDead(1)
	rounds := []int{res.Jobs[0].Round, res.Jobs[1].Round, res.Jobs[2].Round}
	if rounds[0] != 1 || rounds[1] != 2 || rounds[2] != 3 {
		t.Fatalf("rounds=%v", rounds)
	}
}

type fkCancelArgs struct{}

func (fkCancelArgs) Kind() string { return "test.fkcancel" }

type fkSnoozeArgs struct{}

func (fkSnoozeArgs) Kind() string { return "test.fksnooze" }

func TestFakeAssertsCancelledAndSnoozed(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkCancelArgs]) error {
		return Cancel(errors.New("no longer needed"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkSnoozeArgs]) error {
		return SnoozeFor(time.Minute)
	}); err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(memory.New(), &Config{Workers: w, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	cancelled, err := c.Enqueue(context.Background(), fkCancelArgs{})
	if err != nil {
		t.Fatal(err)
	}
	snoozed, err := c.Enqueue(context.Background(), fkSnoozeArgs{})
	if err != nil {
		t.Fatal(err)
	}
	res := f.RunReady(context.Background())
	res.AssertCancelled(1)
	res.AssertSnoozed(1)
	res.AssertCompleted(0)
	f.AssertState(cancelled.Job.ID, backend.StateCancelled)
	f.AssertState(snoozed.Job.ID, backend.StateRetryable)
	f.Advance(time.Minute)
	f.RunReady(context.Background()).AssertSnoozed(1)
	stub := &stubTB{}
	empty := c.Fake(stub).RunReady(context.Background())
	catchAbort(func() { empty.AssertCancelled(1) })
	if !strings.Contains(stub.failed, "cancelled") {
		t.Fatalf("AssertCancelled message: %q", stub.failed)
	}
	stub.failed = ""
	catchAbort(func() { empty.AssertSnoozed(1) })
	if !strings.Contains(stub.failed, "snoozed") {
		t.Fatalf("AssertSnoozed message: %q", stub.failed)
	}
}

type fkChainArgs struct{}

func (fkChainArgs) Kind() string { return "test.fkchain" }

type fkSideArgs struct{}

func (fkSideArgs) Kind() string { return "test.fkside" }

func TestFakeStateAssertionsAndJob(t *testing.T) {
	c, _ := newFakeClient(t)
	f := c.Fake(t)
	res, err := c.Enqueue(context.Background(), fkArgs{N: 1}, WithDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	f.AssertPending(1)
	f.AssertState(res.Job.ID, backend.StateScheduled)
	if f.Job(res.Job.ID) == nil || f.Job(999999) != nil {
		t.Fatal("Job lookup wrong")
	}
	f.Advance(time.Hour)
	f.RunReady(context.Background())
	f.AssertState(res.Job.ID, backend.StateCompleted)
	f.AssertPending(0)
	stub := &stubTB{}
	f2 := c.Fake(stub)
	catchAbort(func() { f2.AssertState(res.Job.ID, backend.StateScheduled) })
	if !strings.Contains(stub.failed, "completed") {
		t.Fatalf("AssertState mismatch message: %q", stub.failed)
	}
	stub.failed = ""
	catchAbort(func() { f2.AssertPending(5) })
	if !strings.Contains(stub.failed, "pending") {
		t.Fatalf("AssertPending message: %q", stub.failed)
	}
	stub.failed = ""
	catchAbort(func() { f2.AssertState(424242, backend.StateCompleted) })
	if !strings.Contains(stub.failed, "no job") {
		t.Fatalf("AssertState unknown-id message: %q", stub.failed)
	}
}

func TestFakeWorkerEnqueueIntoNewQueueIsTracked(t *testing.T) {
	w := NewWorkers()
	b := memory.New()
	var c *Client
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkChainArgs]) error {
		_, err := c.Enqueue(ctx, fkSideArgs{}, WithQueue("sidequeue"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var sideRan bool
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkSideArgs]) error {
		sideRan = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var err error
	c, err = NewClient(b, &Config{Workers: w, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if _, err := c.Enqueue(context.Background(), fkChainArgs{}); err != nil {
		t.Fatal(err)
	}
	res := f.RunUntilIdle(context.Background(), 5)
	res.AssertCompleted(2)
	if !sideRan {
		t.Fatal("worker-enqueued job in a new queue must run")
	}
	if !res.Ran("test.fkside") {
		t.Fatal("side job missing from results")
	}
}

func TestFakeBootOnceResetAndMonotonicClock(t *testing.T) {
	c, b := newFakeClient(t)
	stub1 := &stubTB{}
	f1 := c.Fake(stub1)
	if _, err := c.Enqueue(context.Background(), fkArgs{N: 1}); err != nil {
		t.Fatal(err)
	}
	f1.Advance(time.Hour)
	f1.RunReady(context.Background()).AssertCompleted(1)
	before := f1.Now()
	stub2 := &stubTB{}
	f2 := c.Fake(stub2)
	f2.Reset()
	if got := len(b.SnapshotAll()); got != 0 {
		t.Fatalf("Reset must wipe jobs, %d remain", got)
	}
	if !f2.Now().Equal(before) {
		t.Fatalf("Reset must not rewind the clock: %v vs %v", f2.Now(), before)
	}
	f2.AssertPending(0)
	if _, err := c.Enqueue(context.Background(), fkArgs{N: 2}); err != nil {
		t.Fatal(err)
	}
	f2.RunReady(context.Background()).AssertCompleted(1)
	if stub1.failed != "" || stub2.failed != "" {
		t.Fatalf("no stub should have failed: %q %q", stub1.failed, stub2.failed)
	}
}

func TestFakeResetReseedsTrackedQueues(t *testing.T) {
	w := NewWorkers()
	if err := RegisterFunc(w, func(ctx context.Context, job *Job[fkArgs]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	b := memory.New()
	c, err := NewClient(b, &Config{
		Workers: w,
		Queues:  map[string]QueueConfig{"configured": {Concurrency: 1}},
		Now:     func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	if _, err := c.Enqueue(context.Background(), fkArgs{}, WithQueue("dynamic")); err != nil {
		t.Fatal(err)
	}
	f.TrackQueue("phantom")
	before := f.trackedQueues()
	wantBefore := []string{"configured", "dynamic", "phantom"}
	if fmt.Sprint(before) != fmt.Sprint(wantBefore) {
		t.Fatalf("tracked before Reset=%v want %v", before, wantBefore)
	}
	f.Reset()
	after := f.trackedQueues()
	wantAfter := []string{"configured"}
	if fmt.Sprint(after) != fmt.Sprint(wantAfter) {
		t.Fatalf("tracked after Reset=%v want %v", after, wantAfter)
	}
}

func TestFakePreWrapJobsAreSeededAndRun(t *testing.T) {
	c, _ := newFakeClient(t)
	if _, err := c.Enqueue(context.Background(), fkArgs{N: 9}, WithQueue("earlybird")); err != nil {
		t.Fatal(err)
	}
	f := c.Fake(t)
	res := f.RunReady(context.Background())
	res.AssertRan("test.fk")
	res.AssertCompleted(1)
}

type rwArgs struct {
	V int `json:"v"`
}

func (rwArgs) Kind() string { return "test.rw" }

type rwWorker struct {
	WorkerDefaults[rwArgs]
	got int
}

func (w *rwWorker) Work(ctx context.Context, job *Job[rwArgs]) error {
	w.got = job.Args.V
	if job.Attempt != 1 || job.Generation != 1 || job.Kind != "test.rw" {
		return fmt.Errorf("row not minimal: %+v", job.JobRow)
	}
	return nil
}

func TestRunWorker(t *testing.T) {
	w := &rwWorker{}
	if err := RunWorker(context.Background(), w, rwArgs{V: 42}); err != nil {
		t.Fatal(err)
	}
	if w.got != 42 {
		t.Fatalf("got=%d", w.got)
	}
}

type rwSecretArgs struct {
	V      int    `json:"v"`
	Secret string `json:"-"`
}

func (rwSecretArgs) Kind() string { return "test.rwsecret" }

type rwSecretWorker struct {
	WorkerDefaults[rwSecretArgs]
	got rwSecretArgs
}

func (w *rwSecretWorker) Work(ctx context.Context, job *Job[rwSecretArgs]) error {
	w.got = job.Args
	return nil
}

func TestRunWorkerRoundTripsPayload(t *testing.T) {
	w := &rwSecretWorker{}
	if err := RunWorker(context.Background(), w, rwSecretArgs{V: 7, Secret: "leaked"}); err != nil {
		t.Fatal(err)
	}
	if w.got.Secret != "" {
		t.Fatalf("RunWorker handed the worker Secret=%q; production dispatch decodes the payload, where a json:\"-\" field is absent", w.got.Secret)
	}
	if w.got.V != 7 {
		t.Fatalf("V=%d want 7", w.got.V)
	}
}

type rwBadArgs struct {
	V int `json:"v"`
}

func (rwBadArgs) Kind() string { return "test.rwbad" }

func (rwBadArgs) MarshalJSON() ([]byte, error) {
	return json.Marshal("not-an-object")
}

type rwBadWorker struct {
	WorkerDefaults[rwBadArgs]
}

func (w *rwBadWorker) Work(ctx context.Context, job *Job[rwBadArgs]) error {
	return nil
}

func TestRunWorkerWrapsDecodeErrorLikeProduction(t *testing.T) {
	w := &rwBadWorker{}
	err := RunWorker(context.Background(), w, rwBadArgs{V: 1})
	if err == nil {
		t.Fatal("expected a decode error")
	}
	if !strings.Contains(err.Error(), "goque: decode") || !strings.Contains(err.Error(), "test.rwbad") {
		t.Fatalf("err=%q, want it to contain %q and the kind %q", err.Error(), "goque: decode", "test.rwbad")
	}
}
