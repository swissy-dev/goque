package goque

import (
	"context"
	"time"
)

// WorkFunc runs one attempt of a job. It is the untyped, row-level view of the
// work that [Middleware] wraps; decoding into the worker's argument type
// happens at the end of the chain.
type WorkFunc func(ctx context.Context, job *JobRow) error

// Middleware wraps job execution, in the style of net/http middleware: it
// receives the next step and returns a replacement. Install it with
// [Config.WorkerMiddleware], where the first entry is the outermost wrapper and
// so runs first.
type Middleware func(next WorkFunc) WorkFunc

// EnqueueFunc inserts a batch of already-built job rows. It is the step that
// [EnqueueMiddleware] wraps, and it is called once per [Client.Enqueue],
// [Client.EnqueueMany], [Client.EnqueueTx], or [Client.EnqueueManyTx] call.
// The transaction handle passed to the Tx variants is not visible to
// middleware.
type EnqueueFunc func(ctx context.Context, jobs []*JobRow) error

// EnqueueMiddleware wraps job insertion, the natural place to stamp
// request-scoped context onto rows before they are stored. Install it with
// [Config.EnqueueMiddleware], where the first entry is the outermost wrapper
// and so runs first.
type EnqueueMiddleware func(next EnqueueFunc) EnqueueFunc

func chainWork(mws []Middleware, final WorkFunc) WorkFunc {
	wf := final
	for i := len(mws) - 1; i >= 0; i-- {
		wf = mws[i](wf)
	}
	return wf
}

func chainEnqueue(mws []EnqueueMiddleware, final EnqueueFunc) EnqueueFunc {
	ef := final
	for i := len(mws) - 1; i >= 0; i-- {
		ef = mws[i](ef)
	}
	return ef
}

// JobTimeout returns the timeout configured for row with [WithTimeout], and
// whether one was set at all. It lets middleware read a job's own deadline
// without knowing how the option is stored on the row.
func JobTimeout(row *JobRow) (time.Duration, bool) {
	return envelopeTimeout(row.Metadata)
}
