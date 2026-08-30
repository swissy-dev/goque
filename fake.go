package goque

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backend/memory"
)

// TB is the part of testing.TB that the fake needs, satisfied by *testing.T,
// *testing.B, and *testing.F. It exists so this package never imports testing,
// which would drag the flag registration and test harness into production
// builds.
//
// An implementation's Fatalf must abort the calling goroutine the way
// testing.T.Fatalf does, by calling runtime.Goexit. The fake reports every
// misuse through Fatalf and then returns, so an implementation that merely logs
// will let a failed test carry on with a broken invariant.
type TB interface {
	// Helper marks the caller as a test helper so failures are reported at the
	// test's own line rather than inside the fake.
	Helper()
	// Fatalf reports a failure and stops the test.
	Fatalf(format string, args ...any)
}

type fakeState struct {
	mu     sync.Mutex
	now    time.Time
	queues map[string]struct{}
	mem    *memory.Backend
}

// Fake drives a [Client] deterministically in tests. Nothing runs in the
// background: jobs execute only inside [Fake.RunReady] or [Fake.RunUntilIdle],
// on the calling goroutine, and time only moves when the test moves it with
// [Fake.Advance] or [Fake.SetNow]. A test can therefore enqueue work, assert
// that nothing has run, jump forward an hour, and assert that it has.
//
// Get one with [Client.Fake]. A Fake and its client share a single clock, so
// they must not be shared across tests running in parallel; give each parallel
// test its own client and Fake.
type Fake struct {
	c  *Client
	tb TB
	st *fakeState
}

// Fake puts an existing client under test control and returns the handle that
// drives it. It takes over the client's clock, starts recording the queues jobs
// are enqueued to, and marks the client fake-driven so [Client.Start] refuses:
// from here on, jobs run only when the test asks.
//
// The client must use the in-memory backend and must be neither started nor
// stopping; either mistake fails the test through tb. The clock begins at the
// client's current time, and the tracked queues begin as those in
// [Config.Queues] plus any already present in the backend.
//
// Call it once in every test, even on a client built once for the whole
// process: the first call installs the shared state, and later calls hand back
// a fresh handle bound to the calling test's tb, leaving the jobs and the clock
// as they were. That is what makes a boot-once application singleton usable
// across a sequence of tests, with [Fake.Reset] between them.
func (c *Client) Fake(tb TB) *Fake {
	tb.Helper()
	c.mu.Lock()
	if c.started || c.stopping {
		c.mu.Unlock()
		tb.Fatalf("goque: Fake requires a client that is not started and not stopping")
		return nil
	}
	if c.fake != nil {
		st := c.fake
		c.mu.Unlock()
		return &Fake{c: c, tb: tb, st: st}
	}
	mem, ok := c.backend.(*memory.Backend)
	if !ok {
		c.mu.Unlock()
		tb.Fatalf("goque: Fake requires the memory backend, got %T", c.backend)
		return nil
	}
	st := &fakeState{now: c.now().Round(0), queues: map[string]struct{}{}, mem: mem}
	for name := range c.cfg.Queues {
		st.queues[name] = struct{}{}
	}
	for _, row := range mem.SnapshotAll() {
		st.queues[row.Queue] = struct{}{}
	}
	prev := c.enqueueFn()
	c.nowPtr.Store(&clockHolder{fn: func() time.Time {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.now
	}})
	c.enqueuePtr.Store(&enqueueHolder{fn: func(ctx context.Context, jobs []*JobRow) error {
		if err := prev(ctx, jobs); err != nil {
			return err
		}
		st.mu.Lock()
		for _, j := range jobs {
			st.queues[j.Queue] = struct{}{}
		}
		st.mu.Unlock()
		return nil
	}})
	c.fake = st
	c.mu.Unlock()
	return &Fake{c: c, tb: tb, st: st}
}

// Now returns the fake clock's current time, which is what the client stamps on
// jobs and compares schedules against. It moves only when the test moves it.
func (f *Fake) Now() time.Time {
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	return f.st.now
}

// Advance moves the fake clock forward by d, which is how a test reaches work
// scheduled for the future or a retry waiting out its backoff. Advancing does
// not run anything on its own; follow it with [Fake.RunReady] or
// [Fake.RunUntilIdle]. A negative d fails the test.
func (f *Fake) Advance(d time.Duration) {
	f.tb.Helper()
	if d < 0 {
		f.tb.Fatalf("goque: Advance requires a non-negative duration, got %v", d)
		return
	}
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	f.st.now = f.st.now.Add(d)
}

// SetNow moves the fake clock to t, for a test that cares about a specific
// wall-clock instant rather than an offset. The clock is monotonic: a t earlier
// than the current time fails the test, because rewinding would make jobs
// already stamped in the past look scheduled for the future.
func (f *Fake) SetNow(t time.Time) {
	f.tb.Helper()
	f.st.mu.Lock()
	cur := f.st.now
	ok := !t.Before(cur)
	if ok {
		f.st.now = t
	}
	f.st.mu.Unlock()
	if !ok {
		f.tb.Fatalf("goque: the fake clock is monotonic; SetNow(%v) is earlier than %v", t, cur)
		return
	}
}

// TrackQueue adds a queue to the set that [Fake.RunReady] and
// [Fake.RunUntilIdle] work. Queues in [Config.Queues] and any this client
// enqueues to are tracked already, so this is for work that arrives another
// way, such as rows inserted straight into the backend or by another client
// sharing it. Tracking a queue twice is harmless.
func (f *Fake) TrackQueue(name string) {
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	f.st.queues[name] = struct{}{}
}

func (f *Fake) trackedQueues() []string {
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	out := make([]string, 0, len(f.st.queues))
	for name := range f.st.queues {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

// RanJob is one execution recorded by a run, embedding its [JobResult].
type RanJob struct {
	JobResult
	// Round is which pass of [Fake.RunUntilIdle] ran the job, counting from 1.
	// It is always 1 for [Fake.RunReady], and it is how a test tells a job that
	// ran from one enqueued by it and run a round later.
	Round int
}

// RunResult collects the executions of one run, newest round last, and carries
// the assertions a test makes about them.
type RunResult struct {
	// Jobs are the executions the run performed, in the order they ran.
	Jobs []RanJob
	tb   TB
}

// RunReady runs the jobs that are due at the current clock, in a single pass,
// and returns what ran. Work scheduled for later stays pending until the test
// advances time, and so does a job this pass retries or snoozes, however short
// its delay. A job enqueued during the pass runs in it only if it is due at
// once and lands in a queue the pass has not drained yet.
//
// Use it to assert one step at a time, and [Fake.RunUntilIdle] to let a chain
// of jobs play out.
func (f *Fake) RunReady(ctx context.Context) *RunResult {
	f.tb.Helper()
	res := &RunResult{tb: f.tb}
	f.runRound(ctx, res, 1)
	return res
}

// RunUntilIdle runs round after round until a round runs nothing, and returns
// every execution tagged with the round it happened in. It lets a chain settle
// in one call: a job that enqueues another, or that fails and is retried with
// no backoff, is picked up by the following round. The clock does not move, so
// anything scheduled for the future is never reached; it simply stops
// appearing, and the run ends.
//
// maxRounds bounds the loop. Exceeding it fails the test, which is how a
// zero-delay retry loop or a job that re-enqueues itself forever gets caught
// instead of hanging.
func (f *Fake) RunUntilIdle(ctx context.Context, maxRounds int) *RunResult {
	f.tb.Helper()
	res := &RunResult{tb: f.tb}
	for round := 1; ; round++ {
		if round > maxRounds {
			f.tb.Fatalf("goque: RunUntilIdle exceeded maxRounds=%d; likely a zero-delay retry or self-enqueue loop", maxRounds)
			return res
		}
		if f.runRound(ctx, res, round) == 0 {
			return res
		}
	}
}

func (f *Fake) runRound(ctx context.Context, res *RunResult, round int) int {
	f.tb.Helper()
	jrs, err := f.c.ProcessReady(ctx, f.trackedQueues()...)
	if err != nil {
		f.tb.Fatalf("goque: ProcessReady failed: %v", err)
		return 0
	}
	for _, jr := range jrs {
		res.Jobs = append(res.Jobs, RanJob{JobResult: jr, Round: round})
	}
	return len(jrs)
}

// Ran reports whether the run executed at least one job of the given kind,
// whatever its outcome. It is the predicate behind [RunResult.AssertRan], for a
// test that would rather branch than fail.
func (r *RunResult) Ran(kind string) bool {
	for _, j := range r.Jobs {
		if j.Job.Kind == kind {
			return true
		}
	}
	return false
}

// Of returns the executions of the given kind, in the order they ran, or nil if
// there were none. Use it to inspect a job's outcome, error message, or payload
// rather than only counting executions.
func (r *RunResult) Of(kind string) []RanJob {
	var out []RanJob
	for _, j := range r.Jobs {
		if j.Job.Kind == kind {
			out = append(out, j)
		}
	}
	return out
}

// AssertRan fails the test unless the run executed at least one job of the
// given kind.
func (r *RunResult) AssertRan(kind string) {
	r.tb.Helper()
	if !r.Ran(kind) {
		r.tb.Fatalf("goque: expected a %q job to run; ran %d jobs", kind, len(r.Jobs))
	}
}

// AssertNoneRan fails the test unless the run executed nothing at all, the
// usual way to check that work is scheduled for later rather than now.
func (r *RunResult) AssertNoneRan() {
	r.tb.Helper()
	if len(r.Jobs) != 0 {
		r.tb.Fatalf("goque: expected no jobs to run; ran %d", len(r.Jobs))
	}
}

func (r *RunResult) countOutcome(o Outcome) int {
	n := 0
	for _, j := range r.Jobs {
		if j.Outcome == o {
			n++
		}
	}
	return n
}

func (r *RunResult) assertOutcome(o Outcome, want int) {
	r.tb.Helper()
	if got := r.countOutcome(o); got != want {
		r.tb.Fatalf("goque: expected %d %s jobs, got %d", want, o, got)
	}
}

// AssertCompleted fails the test unless exactly n executions in the run
// succeeded.
func (r *RunResult) AssertCompleted(n int) { r.assertOutcome(OutcomeCompleted, n) }

// AssertRetried fails the test unless exactly n executions in the run failed
// and were rescheduled for another attempt.
func (r *RunResult) AssertRetried(n int) { r.assertOutcome(OutcomeRetried, n) }

// AssertDead fails the test unless exactly n executions in the run used up
// their job's last attempt.
func (r *RunResult) AssertDead(n int) { r.assertOutcome(OutcomeDead, n) }

// AssertCancelled fails the test unless exactly n executions in the run
// returned [Cancel].
func (r *RunResult) AssertCancelled(n int) { r.assertOutcome(OutcomeCancelled, n) }

// AssertSnoozed fails the test unless exactly n executions in the run returned
// [SnoozeFor].
func (r *RunResult) AssertSnoozed(n int) { r.assertOutcome(OutcomeSnoozed, n) }

// Job returns a copy of the stored job with the given ID, or nil if there is
// none, so a test can inspect its state, attempt count, error history, or
// schedule. Mutating the copy changes nothing.
func (f *Fake) Job(id int64) *JobRow {
	return f.st.mem.Snapshot(id)
}

// AssertState fails the test unless the job with the given ID is in state s. A
// job that does not exist, because it was never enqueued or has been cleaned
// away, also fails.
func (f *Fake) AssertState(id int64, s State) {
	f.tb.Helper()
	j := f.st.mem.Snapshot(id)
	if j == nil {
		f.tb.Fatalf("goque: no job with id %d", id)
		return
	}
	if j.State != s {
		f.tb.Fatalf("goque: job %d state is %s, want %s", id, j.State, s)
	}
}

// AssertPending fails the test unless exactly n stored jobs are still in a
// non-terminal state, counting scheduled, available, running, and retryable
// jobs across every queue. It is the check for "nothing is left over".
func (f *Fake) AssertPending(n int) {
	f.tb.Helper()
	got := 0
	for _, row := range f.st.mem.SnapshotAll() {
		if !row.State.Terminal() {
			got++
		}
	}
	if got != n {
		f.tb.Fatalf("goque: expected %d pending jobs, got %d", n, got)
	}
}

// Reset deletes every stored job and re-seeds the tracked queues from
// [Config.Queues], dropping the ones discovered from earlier enqueues. It gives
// a client shared by a sequence of tests a clean slate.
//
// It deliberately leaves the clock where it is. Rewinding would make jobs
// enqueued after the reset sort before ones from before it, so a test that
// needs a specific time should set it explicitly.
func (f *Fake) Reset() {
	f.st.mem.Reset()
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	f.st.queues = map[string]struct{}{}
	for name := range f.c.cfg.Queues {
		f.st.queues[name] = struct{}{}
	}
}

// RunWorker calls a worker directly with the given arguments and returns
// whatever its Work method returns. There is no backend, no client, and no
// middleware: nothing is enqueued or stored, control errors such as [Cancel]
// and [SnoozeFor] come back untouched instead of being acted on, and the job is
// never retried. It is the unit test for a worker's own logic.
//
// The arguments still round-trip through JSON exactly as they would in
// production, so a field that does not survive encoding, because it is
// unexported or has no json tag, is absent here too and the test catches it.
// The worker sees a synthetic job row: attempt 1 of 1, with no ID.
func RunWorker[T JobArgs](ctx context.Context, w Worker[T], args T) error {
	payload, err := json.Marshal(args)
	if err != nil {
		return err
	}
	var decoded T
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return fmt.Errorf("goque: decode %s payload: %w", args.Kind(), err)
	}
	row := &JobRow{Kind: args.Kind(), Payload: payload, Attempt: 1, Generation: 1, MaxAttempts: 1, State: backend.StateRunning}
	return w.Work(ctx, &Job[T]{JobRow: row, Args: decoded})
}
