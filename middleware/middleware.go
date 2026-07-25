// Package middleware provides ready-made worker middleware for goque.
//
// Each function returns a [goque.Middleware] to install in
// [goque.Config.WorkerMiddleware], where the first entry is the outermost
// wrapper and so runs first. They compose freely with middleware of your own,
// which is nothing more than a function wrapping a [goque.WorkFunc].
package middleware

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/swissy-dev/goque"
)

// Recovery returns middleware that turns a panic from further down the chain
// into a [*goque.PanicError] carrying the recovered value and the stack.
//
// A client already recovers panics on its own, so this is not what keeps the
// process alive; it is about position. Converting the panic here means the
// middleware wrapped around this one see a returned error instead of being
// unwound past, so their logging, metrics, and cleanup still run. Because the
// error is a [*goque.PanicError], the attempt is still recorded with its stack
// and [goque.Config.PanicHandler] is still called. Install it inside anything
// that must observe failures, that is, later in [goque.Config.WorkerMiddleware].
func Recovery() goque.Middleware {
	return func(next goque.WorkFunc) goque.WorkFunc {
		return func(ctx context.Context, job *goque.JobRow) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = &goque.PanicError{Recovered: r, Stack: debug.Stack()}
				}
			}()
			return next(ctx, job)
		}
	}
}

// Timeout returns middleware that applies the job's own timeout, set with
// [goque.WithTimeout], as a deadline on the context passed further down the
// chain. A job with no timeout configured passes through untouched.
//
// A client already applies the same deadline before the chain starts, measured
// from when the job begins. Add this when you assemble a [goque.WorkFunc]
// chain yourself, or to restart the clock at this point in the chain so time
// spent in outer middleware does not count against the worker.
func Timeout() goque.Middleware {
	return func(next goque.WorkFunc) goque.WorkFunc {
		return func(ctx context.Context, job *goque.JobRow) error {
			d, ok := goque.JobTimeout(job)
			if !ok {
				return next(ctx, job)
			}
			tctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(tctx, job)
		}
	}
}

// Logger returns middleware that logs each execution to l: one message when the
// job starts and one when it finishes, at info level, or at error level with
// the error when it fails. Every message carries the job's kind, queue, ID, and
// attempt, and the finishing ones its duration. l must not be nil.
func Logger(l *slog.Logger) goque.Middleware {
	return func(next goque.WorkFunc) goque.WorkFunc {
		return func(ctx context.Context, job *goque.JobRow) error {
			start := time.Now()
			l.InfoContext(ctx, "job start", "kind", job.Kind, "queue", job.Queue, "id", job.ID, "attempt", job.Attempt)
			err := next(ctx, job)
			if err != nil {
				l.ErrorContext(ctx, "job error", "kind", job.Kind, "queue", job.Queue, "id", job.ID, "attempt", job.Attempt, "duration", time.Since(start), "err", err)
			} else {
				l.InfoContext(ctx, "job done", "kind", job.Kind, "queue", job.Queue, "id", job.ID, "attempt", job.Attempt, "duration", time.Since(start))
			}
			return err
		}
	}
}

// HookFuncs are the callbacks [Hooks] invokes. Either field may be nil, in
// which case that end is skipped.
type HookFuncs struct {
	// Before is called just before the job runs, with the row about to be
	// worked.
	Before func(ctx context.Context, row *goque.JobRow)
	// After is called once the job has run, with how long it took and the error
	// it returned, which is nil on success. Control errors such as
	// [goque.SnoozeFor] arrive here as errors too, since the outcome has not
	// been decided yet.
	After func(ctx context.Context, row *goque.JobRow, d time.Duration, err error)
}

// Hooks returns middleware that calls h.Before and h.After around each
// execution, the seam for metrics, tracing spans, or anything else that needs
// to bracket a job. The callbacks run on the job's own goroutine, so keep them
// quick; a panic in either is recovered by the client and recorded as a failed
// attempt, which is rarely what a metrics callback should cause.
func Hooks(h HookFuncs) goque.Middleware {
	return func(next goque.WorkFunc) goque.WorkFunc {
		return func(ctx context.Context, job *goque.JobRow) error {
			if h.Before != nil {
				h.Before(ctx, job)
			}
			start := time.Now()
			err := next(ctx, job)
			if h.After != nil {
				h.After(ctx, job, time.Since(start), err)
			}
			return err
		}
	}
}
