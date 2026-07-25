package goque

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/swissy-dev/goque/backend"
)

type activeJob struct {
	cancel     context.CancelFunc
	generation int
	fenced     *atomic.Bool
}

func (c *Client) pushFenced(fin finalizer, fenced *atomic.Bool, op finalizeOp) {
	if fenced.Load() {
		c.cfg.Logger.Warn("goque: dropping finalization from self-fenced execution")
		return
	}
	fin.push(op)
}

func policyDelay(row *JobRow, fallback RetryPolicy) time.Duration {
	p, err := DecodeRetryPolicy(row.RetryPolicy)
	if err != nil || p == nil {
		p = fallback
	}
	if p == nil {
		p = Exponential{Base: 2 * time.Second, Max: 15 * time.Minute}
	}
	return p.Delay(row.Attempt)
}

// PanicError reports a job that panicked. The executor produces one when a
// worker panics and recognizes it in a returned error with [errors.As], which
// is what records the stack on the attempt and invokes [Config.PanicHandler];
// custom recovery middleware should therefore return this type rather than an
// error of its own.
type PanicError struct {
	// Recovered is the value passed to panic.
	Recovered any
	// Stack is the stack trace captured where the panic was recovered.
	Stack []byte
}

// Error returns the panic value formatted as a message.
func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Recovered)
}

func (c *Client) workOnce(ctx context.Context, row *JobRow) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Recovered: r, Stack: debug.Stack()}
		}
	}()
	dispatch, ok := c.workers.dispatch(row.Kind)
	if !ok {
		return fmt.Errorf("no worker registered for kind %s", row.Kind)
	}
	wf := chainWork(c.cfg.WorkerMiddleware, func(ctx context.Context, row *JobRow) error {
		return dispatch(ctx, row)
	})
	return wf(ctx, row)
}

func (c *Client) runJob(ctx context.Context, row *JobRow, fin finalizer) {
	var jctx context.Context
	var cancel context.CancelFunc
	if d, ok := envelopeTimeout(row.Metadata); ok {
		jctx, cancel = context.WithTimeout(ctx, d)
	} else {
		jctx, cancel = context.WithCancel(ctx)
	}
	fenced := &atomic.Bool{}
	aj := &activeJob{cancel: cancel, generation: row.Generation, fenced: fenced}
	c.active.Store(row.ID, aj)
	defer func() {
		cancel()
		c.active.CompareAndDelete(row.ID, aj)
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}()
	err := c.workOnce(jctx, row)
	now := c.now()
	switch {
	case err == nil:
		c.pushFenced(fin, fenced, finalizeOp{op: opComplete, complete: backend.JobFinalize{ID: row.ID, Generation: row.Generation, Metadata: row.Metadata}})
	default:
		if ce, ok := asCancel(err); ok {
			c.pushFenced(fin, fenced, finalizeOp{op: opCancel, cancel: backend.JobCancel{ID: row.ID, Generation: row.Generation, Err: ce.err.Error(), Metadata: row.Metadata}})
			return
		}
		if se, ok := asSnooze(err); ok {
			meta, _ := bumpSnoozes(row.Metadata)
			c.pushFenced(fin, fenced, finalizeOp{op: opSnooze, snooze: backend.JobSnooze{ID: row.ID, Generation: row.Generation, At: now.Add(se.d), Metadata: meta}})
			return
		}
		if re, ok := asRetryAt(err); ok {
			ae := backend.AttemptError{At: now, Attempt: row.Attempt, Err: re.err.Error()}
			if row.Attempt >= row.MaxAttempts {
				if c.cfg.ErrorHandler != nil {
					c.cfg.ErrorHandler(ctx, row, re.err)
				}
				c.pushFenced(fin, fenced, finalizeOp{op: opKill, kill: backend.JobKill{ID: row.ID, Generation: row.Generation, Err: ae, Metadata: row.Metadata}})
				return
			}
			c.pushFenced(fin, fenced, finalizeOp{op: opRetry, retry: backend.JobRetry{ID: row.ID, Generation: row.Generation, At: re.at, Err: ae, Metadata: row.Metadata}})
			return
		}
		ae := backend.AttemptError{At: now, Attempt: row.Attempt, Err: err.Error()}
		var pe *PanicError
		if errors.As(err, &pe) {
			ae.Stack = string(pe.Stack)
			if c.cfg.PanicHandler != nil {
				c.cfg.PanicHandler(ctx, row, pe.Recovered, pe.Stack)
			}
		} else if c.cfg.ErrorHandler != nil {
			c.cfg.ErrorHandler(ctx, row, err)
		}
		if row.Attempt >= row.MaxAttempts {
			c.pushFenced(fin, fenced, finalizeOp{op: opKill, kill: backend.JobKill{ID: row.ID, Generation: row.Generation, Err: ae, Metadata: row.Metadata}})
			return
		}
		c.pushFenced(fin, fenced, finalizeOp{op: opRetry, retry: backend.JobRetry{ID: row.ID, Generation: row.Generation, At: now.Add(policyDelay(row, c.cfg.Defaults.RetryPolicy)), Err: ae, Metadata: row.Metadata}})
	}
}
