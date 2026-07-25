package goque

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swissy-dev/goque/backend"
)

// QueueConfig describes how a single queue is worked.
type QueueConfig struct {
	// Concurrency caps the jobs this queue may have in flight at once. It must
	// be greater than zero; [NewClient] rejects a queue without it.
	Concurrency int
	// Weight is this queue's share of capacity when queues compete for it,
	// divided by smooth weighted round-robin: a queue with weight 6 wins about
	// six selections for every one won by a queue with weight 1, but every
	// queue with work still makes progress. Set [Config.StrictQueuePriority] to
	// drain by weight instead. Defaults to 1.
	Weight int
	// PollInterval is how often the client looks for new work. All queues share
	// one dispatch loop, so the smallest PollInterval among the configured
	// queues sets the pace for all of them; the loop also wakes early whenever
	// a job finishes. Defaults to 500ms.
	PollInterval time.Duration
}

// Config configures a [Client]. Every field is optional: the zero Config
// produces an enqueue-only client with library defaults.
type Config struct {
	// Workers is the registry of job kinds this client can run. It is required
	// to run jobs and unused by a client that only enqueues.
	Workers *Workers
	// Queues maps queue name to its settings and selects which queues this
	// client works. Leaving it empty makes the client enqueue-only:
	// [Client.Start] refuses to run.
	Queues map[string]QueueConfig
	// MaxWorkers caps jobs in flight across all queues. Defaults to the sum of
	// the queues' Concurrency values.
	MaxWorkers int
	// StrictQueuePriority gives heavier queues first claim on all free
	// capacity, reaching a lighter queue only once the heavier ones are out of
	// work or at their own Concurrency. This can starve low-weight queues;
	// leave it false for weighted round-robin.
	StrictQueuePriority bool
	// Defaults are the client-wide fallback job options, used for any knob that
	// neither the enqueue nor the job kind set. [NewClient] fills in the
	// unset ones: queue "default", 25 attempts, and an Exponential retry policy
	// with Base 2s, Max 15m, and Jitter 0.1.
	Defaults JobOptions
	// HeartbeatInterval is how often running jobs report liveness, which is
	// also how often stale jobs are rescued. Defaults to 30s.
	HeartbeatInterval time.Duration
	// RescueTTL is how long a running job may go without a heartbeat before it
	// is assumed lost and made retryable again. Defaults to three
	// HeartbeatIntervals.
	RescueTTL time.Duration
	// MoverInterval is how often scheduled and retryable jobs that have come
	// due are made available. Defaults to 1s.
	MoverInterval time.Duration
	// CleanInterval is how often finished jobs past their retention are
	// deleted. Defaults to 1m.
	CleanInterval time.Duration
	// CompletedRetention is how long completed jobs are kept after they
	// finish. Defaults to 24h.
	CompletedRetention time.Duration
	// CancelledRetention is how long cancelled jobs are kept after they
	// finish. Defaults to 7 days.
	CancelledRetention time.Duration
	// DeadRetention is how long dead jobs are kept after they finish, with
	// their full error history. Defaults to 7 days.
	DeadRetention time.Duration
	// WorkerMiddleware wraps every job execution. The first entry is outermost.
	WorkerMiddleware []Middleware
	// EnqueueMiddleware wraps every insert, including batches. The first entry
	// is outermost.
	EnqueueMiddleware []EnqueueMiddleware
	// ErrorHandler, if set, is called when an attempt fails, whether the job is
	// retried or declared dead. It is not called for a job that panics (see
	// PanicHandler), for [Cancel] or [SnoozeFor], or for a [RetryAt] failure
	// unless that attempt was the job's last.
	ErrorHandler func(ctx context.Context, row *JobRow, err error)
	// PanicHandler, if set, is called when a job panics, with the recovered
	// value and the stack captured at the panic. Either way the attempt is
	// recorded with its stack trace, and the job then retries under its normal
	// policy or becomes dead if its attempts are used up.
	PanicHandler func(ctx context.Context, row *JobRow, recovered any, stack []byte)
	// Logger receives the client's operational logging. Defaults to
	// [slog.Default].
	Logger *slog.Logger
	// Now, if set, replaces [time.Now] as the client's clock for job timestamps
	// and scheduling decisions. It does not drive wall-clock machinery such as
	// tickers or context deadlines. See [Client.Fake] for the testing clock
	// built on it.
	Now func() time.Time
}

type clockHolder struct {
	fn func() time.Time
}

type enqueueHolder struct {
	fn EnqueueFunc
}

// Client enqueues jobs and, when [Config.Queues] is non-empty, works them.
// Create one with [NewClient], then use [Client.Enqueue] and start the workers
// with [Client.Start]. A Client is safe for concurrent use.
type Client struct {
	backend    backend.Backend
	workers    *Workers
	cfg        Config
	clientID   string
	nowPtr     atomic.Pointer[clockHolder]
	enqueuePtr atomic.Pointer[enqueueHolder]

	mu          sync.Mutex
	started     bool
	stopping    bool
	fetchCancel context.CancelFunc
	liveCancel  context.CancelFunc
	fetchWg     sync.WaitGroup
	liveWg      sync.WaitGroup
	jobWg       sync.WaitGroup
	active      sync.Map
	wake        chan struct{}

	completer     *completer
	queues        []*queueState
	globalRunning *atomic.Int64
	fake          *fakeState
}

// NewClient returns a client backed by b, which is required. cfgPtr may be nil;
// the Config it points to is copied, along with its Queues map, so later edits
// by the caller do not reach the client. Unset fields are filled in with the
// defaults documented on [Config] and [QueueConfig].
//
// A client whose config has no Queues is enqueue-only: it inserts jobs but
// [Client.Start] refuses to run, which is the right shape for a process that
// hands work to a separate worker process. NewClient returns an error if b is
// nil or if a configured queue has no Concurrency.
func NewClient(b backend.Backend, cfgPtr *Config) (*Client, error) {
	if b == nil {
		return nil, fmt.Errorf("goque: backend is required")
	}
	var cfg Config
	if cfgPtr != nil {
		cfg = *cfgPtr
	}
	if cfg.Queues != nil {
		cloned := make(map[string]QueueConfig, len(cfg.Queues))
		for k, v := range cfg.Queues {
			cloned[k] = v
		}
		cfg.Queues = cloned
	}
	for name, q := range cfg.Queues {
		if q.Concurrency <= 0 {
			return nil, fmt.Errorf("goque: queue %q needs Concurrency > 0", name)
		}
		if q.Weight <= 0 {
			q.Weight = 1
		}
		if q.PollInterval <= 0 {
			q.PollInterval = 500 * time.Millisecond
		}
		cfg.Queues[name] = q
	}
	if cfg.Defaults.Queue == "" {
		cfg.Defaults.Queue = "default"
	}
	if cfg.Defaults.MaxAttempts == 0 {
		cfg.Defaults.MaxAttempts = 25
	}
	if cfg.Defaults.RetryPolicy == nil {
		cfg.Defaults.RetryPolicy = Exponential{Base: 2 * time.Second, Max: 15 * time.Minute, Jitter: 0.1}
	}
	if cfg.MaxWorkers == 0 {
		for _, q := range cfg.Queues {
			cfg.MaxWorkers += q.Concurrency
		}
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if cfg.RescueTTL == 0 {
		cfg.RescueTTL = 3 * cfg.HeartbeatInterval
	}
	if cfg.MoverInterval == 0 {
		cfg.MoverInterval = time.Second
	}
	if cfg.CleanInterval == 0 {
		cfg.CleanInterval = time.Minute
	}
	if cfg.CompletedRetention == 0 {
		cfg.CompletedRetention = 24 * time.Hour
	}
	if cfg.CancelledRetention == 0 {
		cfg.CancelledRetention = 7 * 24 * time.Hour
	}
	if cfg.DeadRetention == 0 {
		cfg.DeadRetention = 7 * 24 * time.Hour
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	host, _ := os.Hostname()
	suffix := make([]byte, 4)
	rand.Read(suffix)
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	c := &Client{
		backend:  b,
		workers:  cfg.Workers,
		cfg:      cfg,
		clientID: fmt.Sprintf("%s-%s", host, hex.EncodeToString(suffix)),
		wake:     make(chan struct{}, 1),
	}
	c.nowPtr.Store(&clockHolder{fn: nowFn})
	c.enqueuePtr.Store(&enqueueHolder{fn: chainEnqueue(cfg.EnqueueMiddleware, func(ctx context.Context, jobs []*JobRow) error {
		return c.backend.Enqueue(ctx, backend.EnqueueParams{Jobs: jobs, Now: c.now()})
	})})
	return c, nil
}

func (c *Client) now() time.Time {
	return c.nowPtr.Load().fn()
}

func (c *Client) enqueueFn() EnqueueFunc {
	return c.enqueuePtr.Load().fn
}

// EnqueueResult reports the outcome of inserting one job.
type EnqueueResult struct {
	// Job is the stored row, including the ID the backend assigned it and the
	// resolved queue, schedule, and retry settings.
	Job *JobRow
	// Skipped reports that the backend deduplicated the insert instead of
	// storing a new job. No backend deduplicates yet, so it is always false.
	Skipped bool
}

// InsertParams is one job in a batch passed to [Client.EnqueueMany].
type InsertParams struct {
	// Args are the job's arguments, and their Kind selects the worker.
	Args JobArgs
	// Opts override this job's options, exactly as the variadic options of
	// [Client.Enqueue] do.
	Opts []EnqueueOption
}

func (c *Client) buildRows(params []InsertParams) ([]*JobRow, error) {
	rows := make([]*JobRow, len(params))
	for i, p := range params {
		resolved := resolveOptions(c.cfg.Defaults, kindOptions(p.Args), p.Opts)
		row, err := buildRow(p.Args, resolved, c.now())
		if err != nil {
			return nil, err
		}
		rows[i] = row
	}
	return rows, nil
}

// EnqueueMany inserts a batch of jobs in a single backend round trip and
// returns one result per entry, in order. Options are resolved per job, and the
// whole batch is built before anything is sent, so one job's invalid options
// fail the call without storing any of them.
func (c *Client) EnqueueMany(ctx context.Context, params []InsertParams) ([]*EnqueueResult, error) {
	rows, err := c.buildRows(params)
	if err != nil {
		return nil, err
	}
	if err := c.enqueueFn()(ctx, rows); err != nil {
		return nil, err
	}
	out := make([]*EnqueueResult, len(rows))
	for i, r := range rows {
		out[i] = &EnqueueResult{Job: r}
	}
	return out, nil
}

// Enqueue inserts a single job. Its kind, and so the worker that will run it,
// comes from args; opts override the job kind's defaults and the client's.
//
// The returned result carries the stored row with its assigned ID. Enqueue
// returns an error wrapping [backend.ErrConflictingOptions] if the resolved
// options combine [WithDelay] with [WithScheduledAt], and one wrapping
// [backend.ErrInvalidMetadata] if [WithMetadata] is not a JSON object.
func (c *Client) Enqueue(ctx context.Context, args JobArgs, opts ...EnqueueOption) (*EnqueueResult, error) {
	res, err := c.EnqueueMany(ctx, []InsertParams{{Args: args, Opts: opts}})
	if err != nil {
		return nil, err
	}
	return res[0], nil
}
