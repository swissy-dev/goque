// Package goque is a background job and scheduling library for Go.
//
// Job arguments are ordinary structs that implement [JobArgs], and workers are
// generic, so payloads are checked by the compiler rather than unmarshalled by
// hand. A [Client] enqueues jobs and, when queues are configured, also runs
// them; queue, priority, retry policy, and timeout all resolve per job.
//
// Storage is pluggable: a Client drives any implementation of
// [github.com/swissy-dev/goque/backend.Backend].
package goque

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/swissy-dev/goque/backend"
)

// JobRow is the stored form of a job: its identity, state, timing, attempt
// history, and encoded arguments. It is an alias for [backend.JobRow].
type JobRow = backend.JobRow

// State is a job's lifecycle state, such as available, running, or dead.
// It is an alias for [backend.State].
type State = backend.State

// AttemptError records one failed attempt: when it failed, which attempt it
// was, the error message, and a stack trace if the attempt panicked. It is an
// alias for [backend.AttemptError].
type AttemptError = backend.AttemptError

// JobArgs is implemented by job argument types. Args are stored as JSON, so
// their exported fields should carry json tags.
type JobArgs interface {
	// Kind returns the stable identifier persisted with every job of this type
	// and used to route it to a worker, for example "email.welcome". Changing
	// it orphans jobs already stored under the old name.
	Kind() string
}

// Job is a single job handed to a [Worker]. It embeds the stored [JobRow], so a
// worker can read the job's metadata (ID, Attempt, MaxAttempts, Queue, and so
// on), and carries the decoded arguments in Args.
type Job[T JobArgs] struct {
	*JobRow
	Args T
}

// Worker processes jobs whose arguments are of type T. Register an
// implementation with [Register].
type Worker[T JobArgs] interface {
	// Work runs one attempt of a job. Returning nil completes the job and
	// returning an error retries it under its retry policy until its attempts
	// run out, at which point it becomes dead. Returning [Cancel], [SnoozeFor],
	// or [RetryAt] chooses the outcome directly, and a panic is recovered and
	// recorded as a failed attempt.
	//
	// Work should respect ctx: it carries the job's [WithTimeout] deadline and
	// is cancelled when the job is cancelled remotely or the client is stopped
	// with [Client.StopAndCancel].
	Work(ctx context.Context, job *Job[T]) error
}

// WorkerDefaults is an empty struct that a worker implementation may embed to
// pick up default implementations of a worker's optional hooks. It holds no
// state; the embedding type must still implement Work.
type WorkerDefaults[T JobArgs] struct{}

type cancelErr struct {
	err error
}

func (e *cancelErr) Error() string {
	return fmt.Sprintf("goque cancel: %v", e.err)
}

func (e *cancelErr) Unwrap() error {
	return e.err
}

// Cancel returns a control error that a [Worker] returns to stop a job
// permanently. The job is moved to the cancelled state with err's message
// recorded, and is never retried no matter how many attempts remain. err must
// not be nil.
func Cancel(err error) error {
	return &cancelErr{err: err}
}

type snoozeErr struct {
	d time.Duration
}

func (e *snoozeErr) Error() string {
	return fmt.Sprintf("goque snooze: %v", e.d)
}

// SnoozeFor returns a control error that a [Worker] returns to reschedule a job
// for d from now without consuming an attempt: the attempt counter is rolled
// back, so snoozing never moves a job closer to dead. Use it to wait for
// something, not to report failure.
func SnoozeFor(d time.Duration) error {
	return &snoozeErr{d: d}
}

type retryAtErr struct {
	at  time.Time
	err error
}

func (e *retryAtErr) Error() string {
	return fmt.Sprintf("goque retry at %v: %v", e.at, e.err)
}

func (e *retryAtErr) Unwrap() error {
	return e.err
}

// RetryAt returns a control error that a [Worker] returns to fail an attempt
// with err but retry it at t instead of at the time the job's retry policy
// would pick. The attempt is consumed and the attempt budget still applies: a
// job that has used its last attempt becomes dead rather than retrying at t.
// err must not be nil.
func RetryAt(t time.Time, err error) error {
	return &retryAtErr{at: t, err: err}
}

func asCancel(err error) (*cancelErr, bool) {
	var e *cancelErr
	ok := errors.As(err, &e)
	return e, ok
}

func asSnooze(err error) (*snoozeErr, bool) {
	var e *snoozeErr
	ok := errors.As(err, &e)
	return e, ok
}

func asRetryAt(err error) (*retryAtErr, bool) {
	var e *retryAtErr
	ok := errors.As(err, &e)
	return e, ok
}
