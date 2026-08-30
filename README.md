# goque

A background job and scheduling library for Go: typed jobs with generics, pluggable backends, per-job retry policies, middleware, and a deterministic testing story.

**Zero dependencies.** The library and its test helpers use only the standard library.

> **Status: early.** The core engine, the in-memory backend, and the PostgreSQL backend (on pgx v5) are complete and tested. SQLite, Redis, cron, unique jobs, rate limiting, and LISTEN/NOTIFY wakeups remain future work. See [Roadmap](#roadmap). The in-memory backend keeps jobs in the process that enqueued them, so it fits single-process apps, tests, and anything that runs its workers in the same binary. PostgreSQL is what lets a web tier hand work to a separate worker fleet durably, with transactional enqueue and worker-side transactional completion available today.

## Documentation

Full documentation lives in [`website/`](website/) and is built with [Vocs](https://vocs.dev). To read it locally:

```bash
cd website
npm install
npm run dev
```

This README is the condensed tour; the site covers each topic properly.

## Contents

- [Why goque](#why-goque)
- [Install](#install)
- [Quick start](#quick-start)
- [Defining jobs](#defining-jobs)
- [Enqueueing](#enqueueing)
- [Per-job options](#per-job-options)
- [Retries and error handling](#retries-and-error-handling)
- [Queues, priority, and concurrency](#queues-priority-and-concurrency)
- [Middleware](#middleware)
- [Observability](#observability)
- [Testing](#testing)
- [One-shot processing](#one-shot-processing)
- [Lifecycle and guarantees](#lifecycle-and-guarantees)
- [PostgreSQL](#postgresql)
- [Writing a backend](#writing-a-backend)
- [Roadmap](#roadmap)

## Why goque

**Typed jobs, no reflection.** Job arguments are ordinary structs and workers are generic, so the compiler checks your payloads. There is no `map[string]any`, no `[]byte` to unmarshal by hand, and no reflection in the dispatch path.

**Every knob is per job.** Queue, priority, retry policy, timeout, and error handling resolve per individual job — not per worker, not per client. A single job can override anything.

**Deterministic tests.** `client.Fake(t)` gives you a controllable clock: enqueue a job for next week, advance time, run it synchronously, assert the outcome. No sleeps, no goroutines, no flakes — and jobs run through the real executor, not a mock.

**Correctness first.** Job claiming is exclusive, finalization is fenced by a monotonic generation token so a partitioned worker can never corrupt a live attempt, and crashed workers are rescued via heartbeats.

## Install

```
go get github.com/swissy-dev/goque
```

Requires Go 1.24 or newer.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/swissy-dev/goque"
	"github.com/swissy-dev/goque/backend/memory"
)

type SendEmail struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (SendEmail) Kind() string { return "email.send" }

func main() {
	workers := goque.NewWorkers()
	goque.RegisterFunc(workers, func(ctx context.Context, job *goque.Job[SendEmail]) error {
		fmt.Println("sending to", job.Args.To, "-", job.Args.Subject)
		return nil
	})

	client, err := goque.NewClient(memory.New(), &goque.Config{
		Workers: workers,
		Queues: map[string]goque.QueueConfig{
			"default": {Concurrency: 10},
		},
	})
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		panic(err)
	}

	client.Enqueue(ctx, SendEmail{To: "a@b.c", Subject: "Welcome"})
	client.Enqueue(ctx, SendEmail{To: "d@e.f", Subject: "Reminder"}, goque.WithDelay(time.Hour))

	time.Sleep(time.Second)

	stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client.Stop(stopCtx)
}
```

## Defining jobs

A job is a struct that reports its kind. The kind is the stable identifier stored with the job, so keep it constant once jobs exist in your queue.

```go
type ResizeImage struct {
	ImageID string `json:"image_id"`
	Width   int    `json:"width"`
}

func (ResizeImage) Kind() string { return "image.resize" }
```

Arguments are serialized as JSON, so exported fields with JSON tags are what survive the round trip.

Reach for a struct worker by default — its fields hold the dependencies the work needs, which is what keeps it constructible with a stub in tests:

```go
type ResizeWorker struct {
	goque.WorkerDefaults[ResizeImage]
	Storage *Storage
}

func (w *ResizeWorker) Work(ctx context.Context, job *goque.Job[ResizeImage]) error {
	return w.Storage.Resize(ctx, job.Args.ImageID, job.Args.Width)
}

goque.Register(workers, &ResizeWorker{Storage: storage})
```

For work with no dependencies at all, `RegisterFunc` is a shorthand for the same thing:

```go
goque.RegisterFunc(workers, func(ctx context.Context, job *goque.Job[PurgeSessions]) error {
	return sessions.PurgeExpired(ctx)
})
```

Registration is generic, so `Register` and `RegisterFunc` are package-level functions rather than methods — Go does not allow methods to introduce their own type parameters. Registering the same kind twice returns `backend.ErrDuplicateKind`.

Inside `Work`, `job.Args` is your typed struct and the embedded row gives you the job's metadata:

```go
func (w *ResizeWorker) Work(ctx context.Context, job *goque.Job[ResizeImage]) error {
	log.Printf("job %d, attempt %d of %d", job.ID, job.Attempt, job.MaxAttempts)
	return nil
}
```

Always respect `ctx` — it carries the job's timeout and is cancelled when the job is cancelled remotely.

## Enqueueing

```go
res, err := client.Enqueue(ctx, ResizeImage{ImageID: "img_1", Width: 800})
```

The result carries the stored row, including its assigned `ID`. To insert a batch in one round trip:

```go
_, err := client.EnqueueMany(ctx, []goque.InsertParams{
	{Args: ResizeImage{ImageID: "a", Width: 800}},
	{Args: ResizeImage{ImageID: "b", Width: 1600}, Opts: []goque.EnqueueOption{goque.WithQueue("bulk")}},
})
```

A client with no `Queues` configured is enqueue-only: it can insert jobs but will refuse to `Start`. That is the right shape for a web process that hands work to a separate worker process — once a shared backend exists.

## Per-job options

Options resolve most-specific-first: **per-enqueue → job-kind defaults → client defaults.**

```go
client.Enqueue(ctx, ResizeImage{ImageID: "img_1"},
	goque.WithQueue("images"),
	goque.WithPriority(3*time.Minute),
	goque.WithDelay(30*time.Second),
	goque.WithMaxAttempts(10),
	goque.WithRetryPolicy(goque.Exponential{Base: 2 * time.Second, Max: 10 * time.Minute, Jitter: 0.2}),
	goque.WithTimeout(30*time.Second),
)
```

| Option | Effect |
| --- | --- |
| `WithQueue(name)` | Which queue the job lands in |
| `WithPriority(d)` | Priority boost — see below |
| `WithDelay(d)` | Run no earlier than now + d |
| `WithScheduledAt(t)` | Run no earlier than t (mutually exclusive with `WithDelay`) |
| `WithMaxAttempts(n)` | Attempts before the job is dead |
| `WithRetryPolicy(p)` | Backoff curve for this job |
| `WithTimeout(d)` | Cancels the job's context after d |
| `WithMetadata(b)` | Arbitrary JSON stored on the row |
| `WithVersion(v)` | Args schema version stamp |

**Priority is a time boost, not a rank.** `WithPriority(3*time.Minute)` means "sort as if this had been enqueued three minutes earlier." Two consequences follow: priority is expressed in units you already understand, and starvation is impossible — a waiting job's effective age keeps growing, so it always surfaces eventually.

A job kind can carry its own defaults, used whenever an enqueue does not override them:

```go
func (ResizeImage) JobOptions() goque.JobOptions {
	return goque.JobOptions{Queue: "images", MaxAttempts: 10}
}

func (a ResizeImage) Priority() time.Duration {
	if a.Width > 2000 {
		return 0
	}
	return time.Minute
}
```

Client-wide defaults live in `Config.Defaults`:

```go
&goque.Config{
	Defaults: goque.JobOptions{
		MaxAttempts: 25,
		RetryPolicy: goque.Exponential{Base: 2 * time.Second, Max: 15 * time.Minute, Jitter: 0.1},
	},
}
```

## Retries and error handling

Returning an error retries the job using its own retry policy. When attempts run out, the job becomes **dead** — retained with its full error history rather than discarded.

Four built-in policies, all serialized with the job so any process applies the right backoff:

```go
goque.Exponential{Base: 2 * time.Second, Max: 15 * time.Minute, Jitter: 0.1}
goque.Linear{Step: 30 * time.Second, Max: 10 * time.Minute}
goque.Fixed{Interval: time.Minute}
goque.Intervals{time.Second, 10 * time.Second, time.Minute, time.Hour}
```

For a custom curve, register it by name so it stays serializable:

```go
goque.RegisterRetryPolicy("stepped", func(attempt int) time.Duration {
	return time.Duration(attempt*attempt) * time.Second
})

client.Enqueue(ctx, args, goque.WithRetryPolicy(goque.Named{Name: "stepped"}))
```

A worker can also steer its own fate by returning a control error:

```go
func (w *Worker) Work(ctx context.Context, job *goque.Job[Args]) error {
	if !job.Args.Valid() {
		return goque.Cancel(errors.New("invalid input"))
	}
	if !dependencyReady() {
		return goque.SnoozeFor(5 * time.Minute)
	}
	if rateLimited() {
		return goque.RetryAt(time.Now().Add(time.Hour), errors.New("rate limited"))
	}
	return doWork()
}
```

- `Cancel(err)` stops the job permanently. It will not retry.
- `SnoozeFor(d)` reschedules **without consuming an attempt** — for waiting on something, not for failure.
- `RetryAt(t, err)` overrides the backoff for this attempt. The attempt budget still applies: an exhausted job goes dead rather than retrying forever.

Panics never take down the process. A panicking job is recorded with its stack trace and retried under its normal policy.

## Queues, priority, and concurrency

```go
&goque.Config{
	Queues: map[string]goque.QueueConfig{
		"critical": {Concurrency: 10, Weight: 6},
		"default":  {Concurrency: 5, Weight: 3},
		"bulk":     {Concurrency: 2, Weight: 1},
	},
	MaxWorkers: 15,
}
```

`Concurrency` caps in-flight jobs per queue; `MaxWorkers` caps the client overall (defaulting to the sum of queue concurrencies).

`Weight` divides contended capacity using smooth weighted round-robin: with the weights above, `critical` wins roughly six selections for every one that `bulk` wins, but **every queue with work always makes progress** — even when slots free up one at a time. If you genuinely want strict draining instead, set `StrictQueuePriority: true`, which empties higher-weight queues before touching lower ones and can starve them.

Queue weight arbitrates *between* queues; per-job priority boosts order jobs *within* a queue.

## Middleware

Two chains, both shaped like `net/http` middleware. The first entry is outermost.

```go
&goque.Config{
	WorkerMiddleware: []goque.Middleware{
		middleware.Logger(slog.Default()),
		middleware.Recovery(),
	},
	EnqueueMiddleware: []goque.EnqueueMiddleware{tenantTagger},
}
```

Order matters: **the first entry is outermost.** `Recovery()` goes last so it sits innermost, closest to your worker — that way a panic is converted to an error *before* it unwinds past `Logger`, which would otherwise skip its error log.

Shipped in `github.com/swissy-dev/goque/middleware`:

- `Recovery()` converts panics into job errors — see the note below.
- `Logger(l)` logs start, completion, and failure with kind, queue, id, attempt, and duration.
- `Hooks(HookFuncs{Before, After})` gives you before/after callbacks for metrics or tracing.
- `Timeout()` applies the job's `WithTimeout` deadline. You do not need it in a normal client: the executor already applies that deadline before the middleware chain runs, so installing this simply nests a second identical one. It exists for chains you drive yourself.

Insert-side middleware runs when jobs are enqueued, which is where you propagate context onto the row:

```go
func tenantTagger(next goque.EnqueueFunc) goque.EnqueueFunc {
	return func(ctx context.Context, jobs []*goque.JobRow) error {
		for _, j := range jobs {
			j.Metadata = tagWithTenant(ctx, j.Metadata)
		}
		return next(ctx, jobs)
	}
}
```

Worker-side middleware can then read that metadata and enrich the context before your typed `Work` runs.

Writing your own worker middleware:

```go
func metrics(next goque.WorkFunc) goque.WorkFunc {
	return func(ctx context.Context, job *goque.JobRow) error {
		start := time.Now()
		err := next(ctx, job)
		observe(job.Kind, time.Since(start), err)
		return err
	}
}
```

**On panics and `Recovery()`.** The executor always recovers panics, so `middleware.Recovery()` is never required to keep your process alive. What it adds is *position*: without it, a panic unwinds straight past your middleware, so anything doing work after `next` returns — the shipped `Logger`'s error log, a `Hooks.After` callback, your own metrics — is skipped. Installing `Recovery()` innermost converts the panic into an error inside the chain, so outer middleware still run and observe the failure.

Either way you keep full panic reporting: `Recovery()` returns a `*goque.PanicError` carrying the recovered value and stack, which the executor recognizes, so `PanicHandler` still fires and the stack is still recorded on the attempt. If you write your own recovery middleware, return that same type:

```go
func myRecovery(next goque.WorkFunc) goque.WorkFunc {
	return func(ctx context.Context, job *goque.JobRow) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &goque.PanicError{Recovered: r, Stack: debug.Stack()}
			}
		}()
		return next(ctx, job)
	}
}
```

## Observability

```go
&goque.Config{
	Logger: slog.Default(),
	ErrorHandler: func(ctx context.Context, row *goque.JobRow, err error) {
		sentry.CaptureException(err)
	},
	PanicHandler: func(ctx context.Context, row *goque.JobRow, recovered any, stack []byte) {
		sentry.CaptureMessage(fmt.Sprintf("panic in %s: %v\n%s", row.Kind, recovered, stack))
	},
}
```

For metrics, `middleware.Hooks` is the intended integration point:

```go
middleware.Hooks(middleware.HookFuncs{
	Before: func(ctx context.Context, row *goque.JobRow) {
		inFlight.WithLabelValues(row.Queue).Inc()
	},
	After: func(ctx context.Context, row *goque.JobRow, d time.Duration, err error) {
		inFlight.WithLabelValues(row.Queue).Dec()
		duration.WithLabelValues(row.Kind, status(err)).Observe(d.Seconds())
	},
})
```

## Testing

This is the part goque is built around. `client.Fake(t)` takes over the client's clock and lets you drive execution synchronously — no background goroutines, no sleeps, no polling.

```go
func TestWelcomeEmailIsScheduled(t *testing.T) {
	workers := goque.NewWorkers()
	var sent []string
	goque.RegisterFunc(workers, func(ctx context.Context, job *goque.Job[SendEmail]) error {
		sent = append(sent, job.Args.To)
		return nil
	})

	client, err := goque.NewClient(memory.New(), &goque.Config{Workers: workers})
	if err != nil {
		t.Fatal(err)
	}
	f := client.Fake(t)

	ctx := context.Background()
	client.Enqueue(ctx, SendEmail{To: "a@b.c"}, goque.WithDelay(24*time.Hour))

	f.RunReady(ctx).AssertNoneRan()

	f.Advance(24 * time.Hour)
	res := f.RunReady(ctx)
	res.AssertRan("email.send")
	res.AssertCompleted(1)

	if len(sent) != 1 {
		t.Fatalf("sent = %v", sent)
	}
}
```

The fake wraps a client you built yourself, so your test exercises the same construction path as production. Enqueueing still goes through `client.Enqueue` — the fake only adds time control, execution, and assertions.

**Testing an HTTP handler** works exactly as you would hope, because the handler enqueues through the client it was injected with:

```go
func TestSignupSchedulesReminder(t *testing.T) {
	client, workers := newTestClient(t)
	f := client.Fake(t)
	app := NewApp(client)

	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, httptest.NewRequest("POST", "/signup", body))

	f.AssertPending(1)

	f.Advance(24 * time.Hour)
	f.RunReady(context.Background()).AssertRan("trial.reminder")
}
```

That two-phase shape is the point: assert the handler's contract (job enqueued, request returned fast) separately from the worker's contract (job ran, side effect happened).

**Time control**

- `Advance(d)` moves the clock forward. Negative durations fail the test.
- `SetNow(t)` jumps to an instant. The clock is monotonic — rewinding fails the test.
- `Now()` reads it.

**Running**

- `RunReady(ctx)` runs exactly the jobs due at the current clock, once. Retries scheduled for later stay pending until you advance again.
- `RunUntilIdle(ctx, maxRounds)` drains cascades that are due *now* — a job that enqueues another, or a zero-delay retry — without advancing time. It fails the test past `maxRounds`, which catches runaway retry loops.

**Assertions** on the result: `AssertRan(kind)`, `AssertNoneRan()`, `AssertCompleted(n)`, `AssertRetried(n)`, `AssertDead(n)`, `AssertCancelled(n)`, `AssertSnoozed(n)`. For arbitrary checks, `Ran(kind)`, `Of(kind)`, and the `Jobs` slice are exported.

**Assertions** on stored state: `AssertState(id, state)`, `AssertPending(n)`, and `Job(id)` for the raw row.

Retries are straightforward to test end to end:

```go
res := f.RunReady(ctx)
res.AssertRetried(1)

f.RunReady(ctx).AssertNoneRan()

f.Advance(30 * time.Second)
f.RunReady(ctx).AssertCompleted(1)
```

**Booting your app once.** If your suite builds the whole app in `TestMain`, call `client.Fake(t)` at the top of each test — it rebinds to the current test so failures land on the right one — and `f.Reset()` between tests to wipe jobs. The clock is deliberately not rewound by `Reset`; it stays monotonic for the whole run. Tests sharing one client must run sequentially: they share a clock, so `t.Parallel()` is unsafe. For parallel tests, build a client per test — construction costs microseconds.

**Unit-testing a worker** with no client or backend at all:

```go
err := goque.RunWorker(context.Background(), &ResizeWorker{Storage: storage}, ResizeImage{ImageID: "img_1"})
```

The arguments round-trip through JSON first, exactly as in production, so a field that would not survive serialization fails here too.

A few boundaries worth knowing: `WithTimeout` deadlines and middleware durations are real wall-clock, since Go contexts cannot run on a simulated clock. The fake requires the in-memory backend, and a faked client refuses to `Start` — the synchronous driver and the background dispatcher are mutually exclusive by design.

## One-shot processing

`ProcessReady` is what the fake drives, and it is a supported production API in its own right — useful when a scheduler owns the cadence, as in a cron container or a serverless function:

```go
results, err := client.ProcessReady(ctx, "default", "bulk")
```

It promotes due jobs, then runs everything currently available in the named queues on the calling goroutine, returning one `JobResult` per execution with its `Outcome` (`OutcomeCompleted`, `OutcomeRetried`, `OutcomeCancelled`, `OutcomeDead`, `OutcomeSnoozed`). When a worker finalized a job with `JobCompleteTx` and that transaction committed, `Outcome` reports what the executor submitted rather than what was actually stored, so a `retried` or `cancelled` result here can belong to a job that is in fact completed. It honors context cancellation, returning the results so far alongside the error, so an invocation deadline stops the pass cleanly instead of failing the remaining backlog. Use it on a client you have not started.

## Lifecycle and guarantees

A job moves through `scheduled → available → running → completed | retryable | dead | cancelled`. Retryable jobs return to available when their backoff elapses; dead jobs are retained with their error history.

**Execution is at-least-once.** Claiming is exclusive — exactly one worker gets any given job — but a crash after side effects and before finalization means the job runs again. **Write idempotent workers.**

Crashed workers are handled by heartbeats: a running job whose heartbeat goes stale is rescued back to retryable and re-claimed. Every finalization is fenced by a monotonic generation token, so an execution that was rescued out from under you cannot corrupt the attempt that replaced it.

`Start(ctx)` launches fetchers, the completer, and the mover/rescuer/cleaner maintenance loops; cancelling `ctx` stops fetching new work and the maintenance loops, but in-flight jobs keep running and keep heartbeating, so you still call `Stop` to drain. `Stop(ctx)` stops fetching and drains in-flight jobs, continuing to heartbeat throughout so the cluster does not mistake draining work for dead work. If the drain outlasts your deadline, it cancels the stragglers' contexts, allows a brief grace period, and returns the context error while a background finisher completes the drain. `StopAndCancel(ctx)` cancels running jobs' contexts immediately instead.

Finalized jobs are cleaned up on a retention schedule: completed after 24h, cancelled and dead after 7 days, all configurable.

## PostgreSQL

`backend/postgres` is goque's durable backend. It is production-facing today, with [pgx v5](https://github.com/jackc/pgx) — via the `backend/postgres/pgxv5` adapter — as its only supported driver. Jobs written to PostgreSQL survive a restart and are visible to every process pointed at the same database, so a web tier can enqueue work that a separate worker fleet claims.

pgx lives in its own module. The root `goque` module and the `backend/postgres` module that holds all of the backend's SQL stay dependency-free; only `backend/postgres/pgxv5` imports pgx.

```go
pool, err := pgxpool.New(ctx, dsn)
if err != nil {
	log.Fatal(err)
}

driver := pgxv5.New(pool)
store, err := postgres.New(driver)
if err != nil {
	log.Fatal(err)
}
client, err := goque.NewClient(store, &goque.Config{Workers: workers})
if err != nil {
	log.Fatal(err)
}
```

Run the migrations in `backend/postgres/goosemigrate` against a fresh database before starting a client against it.

### Transactional enqueue

`Client.EnqueueTx` and `Client.EnqueueManyTx` take a transaction the caller already opened, so a job can be created atomically with the business data that justifies it. goque never calls `Begin`, `Commit`, or `Rollback` on that transaction — the caller alone owns its lifecycle, and with the PostgreSQL backend the transaction must be a pgx v5 `pgx.Tx`.

```go
tx, err := pool.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)

if err := createUser(ctx, tx, user); err != nil {
	return err
}
if _, err := client.EnqueueTx(ctx, tx, SendWelcomeEmail{UserID: user.ID}); err != nil {
	return err
}

return tx.Commit(ctx)
```

Roll the transaction back and the job was never created. Commit it and both the user and the job are there — no outbox table, no reconciliation sweep.

### Transactional completion

`goque.JobCompleteTx` couples a worker's own database writes to the job's completion, inside the worker's own transaction:

```go
func (w *ChargeWorker) Work(ctx context.Context, job *goque.Job[ChargeCard]) error {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := recordCharge(ctx, tx, job.Args); err != nil {
		return err
	}
	if err := goque.JobCompleteTx(ctx, tx, job); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
```

When that commit succeeds, the executor's own ordinary finalization still runs after `Work` returns, finds the job already completed, and drops itself harmlessly. When the transaction rolls back instead, ordinary finalization applies out of band, so the job retries rather than being stranded.

This is not global exactly-once execution: a crash between `Work` returning and that transaction committing can still cause the job to run again, and nothing here helps a side effect outside the transaction, such as an outbound email or HTTP call. `JobCompleteTx` closes the gap only for writes made inside the same transaction. Ordinary processing is still **at-least-once** — write idempotent workers regardless. See [Lifecycle and guarantees](#lifecycle-and-guarantees).

## Writing a backend

A backend implements `backend.Backend` — eleven methods covering enqueue, an atomic exclusive fetch, five fenced finalization transitions, heartbeats, and three maintenance operations. Each method's contract is documented on the interface itself; run `go doc github.com/swissy-dev/goque/backend Backend` for the full text.

`backendtest` is a reusable conformance suite that any implementation can run against itself:

```go
func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Backend { return myBackend(t) })
}
```

It covers claim exclusivity under concurrency, effective-time ordering, generation fencing (including a replayed snooze after reclaim and stale heartbeats), retry and snooze attempt accounting, rescue TTL, mover idempotency, and cleaner retention.

## Roadmap

Implemented today: the core engine, the in-memory backend, the PostgreSQL backend on pgx v5 — including transactional enqueue (`EnqueueTx`/`EnqueueManyTx`) and transactional completion (`JobCompleteTx`) — the conformance suite, and the testing story.

Designed and specced, not yet built:

- **SQLite and Redis backends**
- **LISTEN/NOTIFY wakeups for PostgreSQL** — the client still discovers new work by polling; pgx supports LISTEN, but nothing here uses it yet
- **Enqueue-only clients** — a dedicated `Enqueuer` type for API processes that produce work but never run it, without the registry, the dispatcher, or a `Start` to call by mistake
- **Enqueueing from other languages** — the wire contract written down and versioned, plus a TypeScript package, so a Node service can hand work to a Go worker
- **Cron and periodic jobs**, with leader election and no-overlap scheduling
- **Unique jobs** — declarative deduplication by args, queue, period, and state
- **Debounce** — last-wins coalescing for bursts
- **Concurrency limits and throttling** — cluster-wide ceilings and GCRA rate smoothing, keyed per job
- **Remote cancellation, recorded outputs, queue pause/resume, and a dead-letter browser**

Several fields already on `JobRow` — `ConcurrencyKey`, `ThrottleKey`, `DebounceKey`, `DebounceDeadline`, `Output` — are reserved for these and carry no behavior yet.

## License

Not yet licensed.
