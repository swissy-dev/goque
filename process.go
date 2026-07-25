package goque

import (
	"context"
	"fmt"
	"time"

	"github.com/swissy-dev/goque/backend"
)

// Outcome is how a single job execution ended, as reported by
// [Client.ProcessReady].
type Outcome string

const (
	// OutcomeCompleted means the worker returned nil and the job is done.
	OutcomeCompleted Outcome = "completed"
	// OutcomeRetried means the attempt failed and the job was rescheduled for
	// another one. It is now retryable, with the failure recorded.
	OutcomeRetried Outcome = "retried"
	// OutcomeCancelled means the worker returned [Cancel]. The job is
	// cancelled and will not run again, whatever attempts it had left.
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeDead means the attempt failed and used up the job's last one, so
	// the job is dead and will not run again.
	OutcomeDead Outcome = "dead"
	// OutcomeSnoozed means the worker returned [SnoozeFor]. The job is
	// rescheduled without the attempt counting against it and without a
	// failure recorded.
	OutcomeSnoozed Outcome = "snoozed"
)

// JobResult reports one execution performed by [Client.ProcessReady].
type JobResult struct {
	// Job is the row as it was handed to the worker, so its Attempt is the
	// attempt that just ran. It is not re-read after finalization.
	Job *JobRow
	// Outcome is how the execution ended.
	Outcome Outcome
	// Err is the failure message for a retried, dead, or cancelled job, and
	// empty for a completed or snoozed one.
	Err string
}

type syncFinalizer struct {
	cp   *completer
	last *finalizeOp
}

func (s *syncFinalizer) push(op finalizeOp) {
	s.cp.flush([]finalizeOp{op})
	s.last = &op
}

func (s *syncFinalizer) take(row *JobRow) (JobResult, bool) {
	op := s.last
	s.last = nil
	if op == nil {
		return JobResult{}, false
	}
	res := JobResult{Job: row}
	switch op.op {
	case opComplete:
		res.Outcome = OutcomeCompleted
	case opRetry:
		res.Outcome = OutcomeRetried
		res.Err = op.retry.Err.Err
	case opCancel:
		res.Outcome = OutcomeCancelled
		res.Err = op.cancel.Err
	case opKill:
		res.Outcome = OutcomeDead
		res.Err = op.kill.Err.Err
	case opSnooze:
		res.Outcome = OutcomeSnoozed
	}
	return res, true
}

const moveDueMaxBatches = 1000

var (
	moveDueBatch      = 1000
	perQueueJobBudget = 10000
)

// ProcessReady makes one synchronous pass over the named queues on the calling
// goroutine. It first promotes every scheduled and retryable job that has come
// due, then works the named queues in the order given, draining each one before
// moving to the next, and returns one [JobResult] per execution in the order
// they ran. A job enqueued while the pass runs is picked up by it if it is due
// at once and lands in a queue the pass has not drained yet; a job it retries
// or snoozes is not promoted again and waits for the next call. Nothing runs in
// the background, and nothing is left in flight when it returns.
//
// It is the engine behind [Client.Fake], but it is equally a production API for
// a deployment where something else owns the cadence: a cron container, a
// serverless function, or any scheduler that would rather call a function than
// supervise a long-lived worker pool. Use it on a client that has not been
// started, so the two are not racing for the same jobs.
//
// ProcessReady requires [Config.Workers] and returns an error without running
// anything if the registry is missing or ctx is already done. It honors
// cancellation between jobs, returning the results collected so far alongside
// ctx's error, and does the same for a backend failure. It also gives up on a
// queue that yields more than ten thousand executions in one pass, which means
// a job is enqueueing itself in a loop.
func (c *Client) ProcessReady(ctx context.Context, queues ...string) ([]JobResult, error) {
	if c.workers == nil {
		return nil, fmt.Errorf("goque: ProcessReady requires workers")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i := 0; i < moveDueMaxBatches; i++ {
		moved, err := c.backend.MoveDue(ctx, backend.MoveDueParams{Now: c.now(), Limit: moveDueBatch})
		if err != nil {
			return nil, err
		}
		if moved < moveDueBatch {
			break
		}
	}
	fin := &syncFinalizer{cp: newCompleter(c.backend, c.now, 1, time.Hour, c.cfg.Logger)}
	var results []JobResult
	for _, q := range queues {
		processed := 0
		for {
			if err := ctx.Err(); err != nil {
				return results, err
			}
			rows, err := c.backend.Fetch(ctx, backend.FetchParams{Queue: q, Limit: 100, ClientID: c.clientID, Now: c.now()})
			if err != nil {
				return results, err
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				c.runJob(ctx, row, fin)
				if res, ok := fin.take(row); ok {
					results = append(results, res)
				}
				processed++
			}
			if processed > perQueueJobBudget {
				return results, fmt.Errorf("goque: ProcessReady exceeded %d jobs in queue %q; likely a job enqueueing itself in a loop", perQueueJobBudget, q)
			}
		}
	}
	return results, nil
}
