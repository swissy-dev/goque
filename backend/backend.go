// Package backend defines the storage contract that goque drives.
//
// A [Backend] is the whole persistence layer: it stores job rows, hands them
// out as exclusive claims, records how each attempt ended, and performs the
// periodic maintenance that makes due jobs available, rescues abandoned ones,
// and deletes finished ones. Implement it to run goque on a new store, and
// verify the implementation with the conformance suite in
// [github.com/swissy-dev/goque/backendtest].
//
// Every method takes the caller's clock as a Now field, so a backend never
// reads the wall clock itself and a test can drive time deterministically.
package backend

import (
	"context"
	"time"
)

// State is a job's position in its lifecycle. A job starts scheduled or
// available, is running while an attempt executes, returns to retryable between
// attempts, and finishes in one of the three terminal states.
type State string

const (
	// StateScheduled is a job whose ScheduledAt has not arrived yet. The mover
	// makes it available once it comes due.
	StateScheduled State = "scheduled"
	// StateAvailable is a job that is due and waiting for the next claim.
	StateAvailable State = "available"
	// StateRunning is a job that has been claimed and whose attempt is
	// executing. Only running jobs can be finalized or heartbeated.
	StateRunning State = "running"
	// StateRetryable is a job whose attempt failed or snoozed and which waits
	// for its new ScheduledAt before it can be claimed again.
	StateRetryable State = "retryable"
	// StateCompleted is a job that finished successfully. Terminal.
	StateCompleted State = "completed"
	// StateCancelled is a job stopped deliberately rather than by failure, and
	// never retried however many attempts remain. Terminal.
	StateCancelled State = "cancelled"
	// StateDead is a job that used up its attempts without succeeding. Its
	// error history is kept for inspection. Terminal.
	StateDead State = "dead"
)

// Terminal reports whether s is a state a job never leaves: completed,
// cancelled, or dead. A job in any other state is still in flight and may be
// claimed again.
func (s State) Terminal() bool {
	return s == StateCompleted || s == StateCancelled || s == StateDead
}

// AttemptError records one failed attempt. Backends append it to the job's
// Errors on [Backend.Retry] and [Backend.Kill], and on a [Backend.Cancel] that
// carries a message.
type AttemptError struct {
	// At is when the attempt failed.
	At time.Time `json:"at"`
	// Attempt is the number of the attempt that failed, counting from 1.
	Attempt int `json:"attempt"`
	// Err is the error's message. Failures are stored as text, not as Go
	// values, so they survive a round trip through the store.
	Err string `json:"err"`
	// Stack is the stack trace captured where the panic was recovered, empty
	// unless the attempt panicked.
	Stack string `json:"stack,omitempty"`
}

// JobRow is a job as the backend stores it: identity, state, timing, attempt
// history, and the encoded arguments. [Backend.Fetch] hands out copies, so a
// worker can read and mutate its own row without touching stored state.
type JobRow struct {
	// ID is the backend-assigned identifier, filled in by [Backend.Enqueue].
	ID int64
	// Kind is the job type's stable identifier and selects the worker.
	Kind string
	// Queue is the queue the job is worked from.
	Queue string
	// Payload is the job's arguments, encoded as JSON.
	Payload []byte
	// State is the job's lifecycle state.
	State State
	// PriorityBoost sorts the job as if it had been enqueued this much earlier.
	// It is subtracted from ScheduledAt to give PriorityAt.
	PriorityBoost time.Duration
	// PriorityAt is the effective time the job sorts by, ScheduledAt minus
	// PriorityBoost. Fetch orders by it, then by ID.
	PriorityAt time.Time
	// Attempt is how many times the job has been claimed, counting from 1 for
	// the attempt in flight. It is the retry budget spent against MaxAttempts:
	// every claim raises it and a snooze rolls it back.
	Attempt int
	// Generation is the fencing token. It rises on every claim and never
	// regresses, which is what lets a backend recognize and reject work sent by
	// an execution that has already been superseded.
	Generation int
	// MaxAttempts is how many attempts the job gets before it is declared dead.
	MaxAttempts int
	// CreatedAt is when the job was enqueued.
	CreatedAt time.Time
	// ScheduledAt is the earliest time the job may run. A retry, snooze, or
	// rescue moves it.
	ScheduledAt time.Time
	// AttemptedAt is when the attempt in flight was claimed.
	AttemptedAt time.Time
	// FinalizedAt is when the job reached a terminal state, and is what the
	// cleaner measures retention against. A retry or snooze clears it.
	FinalizedAt time.Time
	// HeartbeatAt is when the running attempt last reported liveness, and is
	// what the rescuer measures its TTL against.
	HeartbeatAt time.Time
	// AttemptedBy accumulates the ClientID of every process that has claimed
	// the job, one entry per attempt.
	AttemptedBy []string
	// CancelRequested asks whichever process is running the job to stop:
	// [Backend.Heartbeat] reports it, and the client cancels that job's
	// context. No exported API requests a cancellation yet; the memory backend
	// exposes SetCancelRequested so tests can exercise the path.
	CancelRequested bool
	// ConcurrencyKey is reserved for per-key concurrency limits, which are not
	// implemented yet. A backend stores it, but nothing reads it.
	ConcurrencyKey string
	// ThrottleKey is reserved for rate limiting, which is not implemented yet.
	ThrottleKey string
	// DebounceKey is reserved for debounced enqueues, which are not implemented
	// yet.
	DebounceKey string
	// DebounceDeadline is reserved for debounced enqueues, which are not
	// implemented yet.
	DebounceDeadline time.Time
	// RetryPolicy is the job's encoded retry policy, or nil to fall back to the
	// client's default.
	RetryPolicy []byte
	// Metadata is arbitrary JSON travelling with the job. goque reserves the
	// "goque" key of that object for its own fields, such as the job's timeout.
	Metadata []byte
	// Output is reserved for a recorded job result, which is not implemented
	// yet. Nothing writes it.
	Output []byte
	// Version is the args schema version stamped at enqueue, defaulting to 1.
	// It is stored for a worker to branch on and is never validated.
	Version int
	// Errors is the history of failed attempts, oldest first.
	Errors []AttemptError
}

// EnqueueParams is the input to [Backend.Enqueue].
type EnqueueParams struct {
	// Jobs are the rows to store. Enqueue fills in each one's assigned fields
	// in place.
	Jobs []*JobRow
	// Now is the caller's clock, stamped as CreatedAt and used to decide
	// whether a job is already due.
	Now time.Time
}

// FetchParams selects the jobs a [Backend.Fetch] call may claim.
type FetchParams struct {
	// Queue is the only queue this call claims from.
	Queue string
	// Limit caps how many jobs the call may claim.
	Limit int
	// ClientID identifies the claiming process and is appended to each claimed
	// job's AttemptedBy.
	ClientID string
	// Now is the caller's clock. Jobs scheduled after it are not yet due, and
	// it is stamped as AttemptedAt and HeartbeatAt on every claim.
	Now time.Time
}

// JobFinalize identifies one execution to complete.
type JobFinalize struct {
	// ID is the job to finalize.
	ID int64
	// Generation must equal the job's current generation or the entry is
	// rejected as stale.
	Generation int
	// Metadata replaces the job's metadata when non-empty, and leaves it alone
	// otherwise.
	Metadata []byte
}

// CompleteParams is the input to [Backend.Complete].
type CompleteParams struct {
	// Jobs are the executions to mark completed.
	Jobs []JobFinalize
	// Now is the caller's clock, stamped as FinalizedAt.
	Now time.Time
}

// JobRetry identifies one execution to fail and reschedule.
type JobRetry struct {
	// ID is the job to retry.
	ID int64
	// Generation must equal the job's current generation or the entry is
	// rejected as stale.
	Generation int
	// At is the job's new ScheduledAt, the time the next attempt becomes due.
	At time.Time
	// Err is the failure to append to the job's error history.
	Err AttemptError
	// Metadata replaces the job's metadata when non-empty.
	Metadata []byte
}

// RetryParams is the input to [Backend.Retry].
type RetryParams struct {
	// Jobs are the executions to fail and reschedule.
	Jobs []JobRetry
	// Now is the caller's clock.
	Now time.Time
}

// JobCancel identifies one execution to cancel.
type JobCancel struct {
	// ID is the job to cancel.
	ID int64
	// Generation must equal the job's current generation or the entry is
	// rejected as stale.
	Generation int
	// Err is the reason, recorded as a final attempt error when non-empty.
	Err string
	// Metadata replaces the job's metadata when non-empty.
	Metadata []byte
}

// CancelParams is the input to [Backend.Cancel].
type CancelParams struct {
	// Jobs are the executions to cancel.
	Jobs []JobCancel
	// Now is the caller's clock, stamped as FinalizedAt.
	Now time.Time
}

// JobKill identifies one execution to declare dead.
type JobKill struct {
	// ID is the job to kill.
	ID int64
	// Generation must equal the job's current generation or the entry is
	// rejected as stale.
	Generation int
	// Err is the final failure, appended to the job's error history.
	Err AttemptError
	// Metadata replaces the job's metadata when non-empty.
	Metadata []byte
}

// KillParams is the input to [Backend.Kill].
type KillParams struct {
	// Jobs are the executions to declare dead.
	Jobs []JobKill
	// Now is the caller's clock, stamped as FinalizedAt.
	Now time.Time
}

// JobSnooze identifies one execution to reschedule without failing it.
type JobSnooze struct {
	// ID is the job to snooze.
	ID int64
	// Generation must equal the job's current generation or the entry is
	// rejected as stale.
	Generation int
	// At is the job's new ScheduledAt, the time it becomes due again.
	At time.Time
	// Metadata replaces the job's metadata when non-empty.
	Metadata []byte
}

// SnoozeParams is the input to [Backend.Snooze].
type SnoozeParams struct {
	// Jobs are the executions to reschedule.
	Jobs []JobSnooze
	// Now is the caller's clock.
	Now time.Time
}

// JobHeartbeat identifies one execution whose lease is being renewed.
type JobHeartbeat struct {
	// ID is the running job.
	ID int64
	// Generation must equal the job's current generation or the entry is
	// ignored.
	Generation int
}

// HeartbeatParams is the input to [Backend.Heartbeat].
type HeartbeatParams struct {
	// ClientID identifies the process reporting liveness.
	ClientID string
	// Jobs are the executions to renew.
	Jobs []JobHeartbeat
	// Now is the caller's clock, stamped as HeartbeatAt on every renewed row.
	Now time.Time
}

// HeartbeatResult reports what [Backend.Heartbeat] found.
type HeartbeatResult struct {
	// CancelRequested lists the IDs, among the renewed jobs, that have been
	// asked to stop. It is nil when none have.
	CancelRequested []int64
}

// MoveDueParams is the input to [Backend.MoveDue].
type MoveDueParams struct {
	// Now is the caller's clock; jobs scheduled at or before it have come due.
	Now time.Time
	// Limit caps how many jobs one call may move, keeping the work bounded.
	Limit int
}

// RescueParams is the input to [Backend.RescueStale].
type RescueParams struct {
	// Now is the caller's clock.
	Now time.Time
	// TTL is how long a running job may go without a heartbeat before it is
	// assumed lost.
	TTL time.Duration
	// Limit caps how many jobs one call may rescue.
	Limit int
}

// CleanParams is the input to [Backend.Clean]. Each retention is measured from
// a job's FinalizedAt.
type CleanParams struct {
	// Now is the caller's clock.
	Now time.Time
	// CompletedRetention is how long completed jobs are kept.
	CompletedRetention time.Duration
	// CancelledRetention is how long cancelled jobs are kept.
	CancelledRetention time.Duration
	// DeadRetention is how long dead jobs are kept.
	DeadRetention time.Duration
	// Limit caps how many jobs one call may delete.
	Limit int
}

// Backend is the storage contract a goque client drives. Implementations must
// be safe for concurrent use, both from many goroutines in one process and from
// many processes sharing one store; the conformance suite in
// [github.com/swissy-dev/goque/backendtest] checks the guarantees described below.
type Backend interface {
	// Enqueue stores jobs. For each row it assigns ID, sets CreatedAt to Now,
	// defaults an unset ScheduledAt to Now, sets State to available or
	// scheduled depending on whether ScheduledAt has arrived, and derives
	// PriorityAt from ScheduledAt and PriorityBoost. The passed rows are
	// updated in place so the caller can read the assigned IDs back.
	Enqueue(ctx context.Context, params EnqueueParams) error

	// Fetch atomically claims up to Limit due jobs from one queue, so a job is
	// delivered to exactly one worker no matter how many processes fetch at
	// once. A claim sets State to running, increments both Attempt and
	// Generation, sets AttemptedAt and HeartbeatAt to Now, and appends ClientID
	// to AttemptedBy. Jobs are returned as copies, ordered by PriorityAt and
	// then ID; an empty result means there is no work, not an error.
	Fetch(ctx context.Context, params FetchParams) ([]*JobRow, error)

	// Complete marks executions as completed. Like every finalization it is
	// fenced: an entry applies only to a row matching its ID and Generation and
	// still running, so a superseded execution cannot overwrite the outcome of
	// the one that replaced it. Unmatched entries do not stop the rest from
	// applying and are reported together in a [*StaleError].
	Complete(ctx context.Context, params CompleteParams) error

	// Retry fails executions and reschedules them: State becomes retryable,
	// ScheduledAt and PriorityAt move to At, FinalizedAt is cleared, and Err is
	// appended to the error history. The attempt already spent by the claim is
	// not refunded. It is fenced like [Backend.Complete].
	Retry(ctx context.Context, params RetryParams) error

	// Cancel stops executions permanently: State becomes cancelled, FinalizedAt
	// is set, and a non-empty Err is recorded as a final attempt error. A
	// cancelled job is never retried, however many attempts it had left. It is
	// fenced like [Backend.Complete].
	Cancel(ctx context.Context, params CancelParams) error

	// Kill declares executions dead, the outcome for a job that has used up its
	// attempts: State becomes dead, FinalizedAt is set, and Err is appended to
	// the error history, which is retained for inspection. It is fenced like
	// [Backend.Complete].
	Kill(ctx context.Context, params KillParams) error

	// Snooze reschedules executions without failing them: State becomes
	// retryable and ScheduledAt moves to At, but the attempt spent by the claim
	// is given back and no error is recorded, so snoozing never moves a job
	// closer to dead. Generation is untouched, so the token still only ever
	// rises. It is fenced like [Backend.Complete].
	Snooze(ctx context.Context, params SnoozeParams) error

	// Heartbeat renews the lease on running executions, setting HeartbeatAt to
	// Now only on rows matching an entry's ID and Generation and still running.
	// A superseded execution therefore cannot keep a reclaimed job alive or
	// mask the real owner's death, and unmatched entries are skipped rather
	// than reported as an error. The result names which of the renewed jobs
	// have been asked to cancel.
	Heartbeat(ctx context.Context, params HeartbeatParams) (HeartbeatResult, error)

	// MoveDue makes scheduled and retryable jobs whose ScheduledAt has arrived
	// available, up to Limit, and returns how many it moved. Like the other
	// maintenance operations it must be idempotent and safe to run
	// concurrently from several instances: each job counts for exactly one
	// caller.
	MoveDue(ctx context.Context, params MoveDueParams) (int, error)

	// RescueStale returns running jobs whose HeartbeatAt is older than TTL to
	// the retryable state with ScheduledAt set to Now, up to Limit, and returns
	// how many it rescued. The attempt is not consumed again, and because the
	// row stops being running the abandoned execution's own finalization is
	// fenced out if it ever arrives. It must be idempotent and
	// concurrency-safe.
	RescueStale(ctx context.Context, params RescueParams) (int, error)

	// Clean deletes terminal jobs whose FinalizedAt is older than the retention
	// for their state, up to Limit, and returns how many it deleted. It must be
	// idempotent and concurrency-safe.
	Clean(ctx context.Context, params CleanParams) (int, error)
}
