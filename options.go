package goque

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/swissy-dev/goque/backend"
)

// JobOptions is the full set of per-job knobs. It is used both as a job kind's
// own defaults (see [JobOptionsProvider]) and as the client-wide fallback in
// [Config.Defaults].
//
// Options resolve most-specific-first: per-enqueue [EnqueueOption] values win
// over job-kind defaults, which win over client defaults. A field left at its
// zero value counts as unset and falls through to the next layer, so a job kind
// cannot use JobOptions to force a knob back to zero.
type JobOptions struct {
	// Queue is the queue the job is stored in. Defaults to "default".
	Queue string
	// PriorityBoost sorts the job as if it had been enqueued this much earlier.
	// See [WithPriority].
	PriorityBoost time.Duration
	// MaxAttempts is the total number of attempts, including the first, before
	// the job is declared dead. Defaults to 25.
	MaxAttempts int
	// RetryPolicy is the backoff curve applied between attempts. It is stored
	// with the job, so any process retrying it applies the same curve.
	RetryPolicy RetryPolicy
	// Timeout bounds a single attempt by cancelling the job's context. Zero
	// means the attempt is not time-bounded.
	Timeout time.Duration
	// ScheduledAt is the earliest time the job may run. Mutually exclusive
	// with Delay.
	ScheduledAt time.Time
	// Delay schedules the job that far after the enqueue time. Mutually
	// exclusive with ScheduledAt.
	Delay time.Duration
	// Metadata is arbitrary JSON stored on the job row. It must encode a JSON
	// object; the "goque" key is reserved for the library's own bookkeeping.
	Metadata []byte
	// Version stamps the args schema version on the row. Defaults to 1.
	Version int
}

// EnqueueOption overrides a [JobOptions] field for a single enqueue. Options
// are applied in order and take precedence over both job-kind and client
// defaults.
type EnqueueOption func(*JobOptions)

// WithQueue puts the job in queue q. The queue does not have to be one the
// enqueueing client works: an enqueue-only client can feed a queue that only a
// worker process consumes.
func WithQueue(q string) EnqueueOption {
	return func(o *JobOptions) { o.Queue = q }
}

// WithPriority boosts the job's priority by d. Priority is a time boost rather
// than a discrete rank: the job sorts as if it had been enqueued d earlier, so
// WithPriority(3*time.Minute) beats everything queued in the last three
// minutes. Because a waiting job's effective age keeps growing, boosts cannot
// starve unboosted work — every job surfaces eventually.
//
// Boosts order jobs within a queue; [QueueConfig.Weight] arbitrates between
// queues.
func WithPriority(d time.Duration) EnqueueOption {
	return func(o *JobOptions) { o.PriorityBoost = d }
}

// WithDelay schedules the job to run no earlier than d after it is enqueued.
// Combining it with [WithScheduledAt] fails the enqueue with an error wrapping
// [backend.ErrConflictingOptions].
func WithDelay(d time.Duration) EnqueueOption {
	return func(o *JobOptions) { o.Delay = d }
}

// WithScheduledAt schedules the job to run no earlier than t. Combining it with
// [WithDelay] fails the enqueue with an error wrapping
// [backend.ErrConflictingOptions].
func WithScheduledAt(t time.Time) EnqueueOption {
	return func(o *JobOptions) { o.ScheduledAt = t }
}

// WithMaxAttempts allows the job n attempts in total, counting the first run.
// Once they are used up a failing job becomes dead rather than retrying.
func WithMaxAttempts(n int) EnqueueOption {
	return func(o *JobOptions) { o.MaxAttempts = n }
}

// WithRetryPolicy sets the backoff curve used between this job's attempts. The
// policy is serialized onto the job row, so it survives a restart and applies
// in whichever process retries the job.
func WithRetryPolicy(p RetryPolicy) EnqueueOption {
	return func(o *JobOptions) { o.RetryPolicy = p }
}

// WithTimeout bounds each attempt to d by cancelling the job's context when it
// elapses. The context is cancelled, not the goroutine: the executor still
// waits for Work to return, so a worker that ignores ctx runs to completion.
func WithTimeout(d time.Duration) EnqueueOption {
	return func(o *JobOptions) { o.Timeout = d }
}

// WithMetadata attaches arbitrary JSON to the job row, where enqueue and worker
// middleware can read and extend it. m must encode a JSON object; anything else
// fails the enqueue with an error wrapping [backend.ErrInvalidMetadata]. The
// "goque" key is reserved for the library.
func WithMetadata(m []byte) EnqueueOption {
	return func(o *JobOptions) { o.Metadata = m }
}

// WithVersion stamps v on the job row as the version of the args schema, so a
// worker can tell which shape of payload it is decoding. Jobs are stamped
// version 1 by default.
func WithVersion(v int) EnqueueOption {
	return func(o *JobOptions) { o.Version = v }
}

// JobOptionsProvider is implemented by a [JobArgs] type that carries its own
// default options. The returned options apply to every job of that kind unless
// the enqueue overrides them, and they themselves override [Config.Defaults].
type JobOptionsProvider interface {
	JobOptions() JobOptions
}

// PriorityProvider is implemented by a [JobArgs] type that computes its
// priority boost from the arguments themselves, which [JobOptionsProvider]
// cannot do. The returned boost has the meaning described by [WithPriority] and
// is ignored if the kind's JobOptions already set a non-zero PriorityBoost.
type PriorityProvider interface {
	Priority() time.Duration
}

func kindOptions(args JobArgs) JobOptions {
	var o JobOptions
	if p, ok := args.(JobOptionsProvider); ok {
		o = p.JobOptions()
	}
	if p, ok := args.(PriorityProvider); ok && o.PriorityBoost == 0 {
		o.PriorityBoost = p.Priority()
	}
	return o
}

func overlay(base, over JobOptions) JobOptions {
	if over.Queue != "" {
		base.Queue = over.Queue
	}
	if over.PriorityBoost != 0 {
		base.PriorityBoost = over.PriorityBoost
	}
	if over.MaxAttempts != 0 {
		base.MaxAttempts = over.MaxAttempts
	}
	if over.RetryPolicy != nil {
		base.RetryPolicy = over.RetryPolicy
	}
	if over.Timeout != 0 {
		base.Timeout = over.Timeout
	}
	if !over.ScheduledAt.IsZero() {
		base.ScheduledAt = over.ScheduledAt
	}
	if over.Delay != 0 {
		base.Delay = over.Delay
	}
	if over.Metadata != nil {
		base.Metadata = over.Metadata
	}
	if over.Version != 0 {
		base.Version = over.Version
	}
	return base
}

func resolveOptions(clientDefaults, kindDefaults JobOptions, opts []EnqueueOption) JobOptions {
	var enq JobOptions
	for _, o := range opts {
		o(&enq)
	}
	return overlay(overlay(clientDefaults, kindDefaults), enq)
}

type envelope struct {
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
	Snoozes   int   `json:"snoozes,omitempty"`
}

func decodeMeta(meta []byte) (map[string]json.RawMessage, envelope) {
	m := map[string]json.RawMessage{}
	if len(meta) > 0 {
		json.Unmarshal(meta, &m)
	}
	var env envelope
	if raw, ok := m["goque"]; ok {
		json.Unmarshal(raw, &env)
	}
	return m, env
}

func encodeMeta(m map[string]json.RawMessage, env envelope) []byte {
	raw, _ := json.Marshal(env)
	m["goque"] = raw
	out, _ := json.Marshal(m)
	return out
}

func setEnvelope(meta []byte, timeout time.Duration) ([]byte, error) {
	m, env := decodeMeta(meta)
	env.TimeoutMS = timeout.Milliseconds()
	return encodeMeta(m, env), nil
}

func envelopeTimeout(meta []byte) (time.Duration, bool) {
	_, env := decodeMeta(meta)
	if env.TimeoutMS == 0 {
		return 0, false
	}
	return time.Duration(env.TimeoutMS) * time.Millisecond, true
}

func bumpSnoozes(meta []byte) ([]byte, int) {
	m, env := decodeMeta(meta)
	env.Snoozes++
	return encodeMeta(m, env), env.Snoozes
}

func buildRow(args JobArgs, resolved JobOptions, now time.Time) (*backend.JobRow, error) {
	if resolved.Delay != 0 && !resolved.ScheduledAt.IsZero() {
		return nil, fmt.Errorf("%w: WithDelay and WithScheduledAt are mutually exclusive", backend.ErrConflictingOptions)
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("goque: marshal %s args: %w", args.Kind(), err)
	}
	policy, err := EncodeRetryPolicy(resolved.RetryPolicy)
	if err != nil {
		return nil, err
	}
	scheduledAt := now
	if resolved.Delay != 0 {
		scheduledAt = now.Add(resolved.Delay)
	} else if !resolved.ScheduledAt.IsZero() {
		scheduledAt = resolved.ScheduledAt
	}
	version := resolved.Version
	if version == 0 {
		version = 1
	}
	if len(resolved.Metadata) > 0 {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(resolved.Metadata, &obj); err != nil {
			return nil, fmt.Errorf("%w: %v", backend.ErrInvalidMetadata, err)
		}
	}
	meta, err := setEnvelope(resolved.Metadata, resolved.Timeout)
	if err != nil {
		return nil, err
	}
	return &backend.JobRow{
		Kind:          args.Kind(),
		Queue:         resolved.Queue,
		Payload:       payload,
		PriorityBoost: resolved.PriorityBoost,
		MaxAttempts:   resolved.MaxAttempts,
		ScheduledAt:   scheduledAt,
		RetryPolicy:   policy,
		Metadata:      meta,
		Version:       version,
	}, nil
}
