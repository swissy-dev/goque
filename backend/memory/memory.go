// Package memory provides an in-process implementation of
// [github.com/swissy-dev/goque/backend.Backend].
//
// Jobs live in a map guarded by a mutex, so the backend is safe for concurrent
// use but nothing more: its state does not outlive the process and is not
// shared between processes. That makes it the right choice for tests, for
// [github.com/swissy-dev/goque.Client.Fake], and for a single-process application
// that can afford to lose queued work on restart, and the wrong choice for
// anything that needs durability or more than one worker process.
package memory

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/swissy-dev/goque/backend"
)

// Backend is an in-memory [backend.Backend]. Create one with [New]. It is safe
// for concurrent use, and every stored job is discarded when the process exits.
type Backend struct {
	mu     sync.Mutex
	nextID int64
	jobs   map[int64]*backend.JobRow
}

// New returns an empty in-memory backend. Job IDs start at 1 and increase by
// one per job, per Backend.
func New() *Backend {
	return &Backend{jobs: map[int64]*backend.JobRow{}}
}

func copyRow(r *backend.JobRow) *backend.JobRow {
	c := *r
	c.AttemptedBy = slices.Clone(r.AttemptedBy)
	c.Errors = slices.Clone(r.Errors)
	c.Payload = slices.Clone(r.Payload)
	c.Metadata = slices.Clone(r.Metadata)
	c.Output = slices.Clone(r.Output)
	c.RetryPolicy = slices.Clone(r.RetryPolicy)
	return &c
}

// Enqueue implements [backend.Backend]. It assigns each row an ID and stores a
// copy, filling in CreatedAt, State, and PriorityAt on the caller's rows as
// well. If any row's ScheduledAt, or its ScheduledAt minus its PriorityBoost,
// falls outside the storable range, it rejects the whole batch with an error
// wrapping [backend.ErrTimeOutOfRange] and stores nothing.
func (b *Backend) Enqueue(_ context.Context, params backend.EnqueueParams) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	prio := make([]time.Time, len(params.Jobs))
	for i, j := range params.Jobs {
		at := j.ScheduledAt
		if at.IsZero() {
			at = params.Now
		}
		p, err := backend.SubDuration(at, j.PriorityBoost)
		if err != nil {
			return err
		}
		prio[i] = p
	}
	for i, j := range params.Jobs {
		b.nextID++
		j.ID = b.nextID
		j.CreatedAt = params.Now
		if j.ScheduledAt.IsZero() {
			j.ScheduledAt = params.Now
		}
		if j.ScheduledAt.After(params.Now) {
			j.State = backend.StateScheduled
		} else {
			j.State = backend.StateAvailable
		}
		j.PriorityAt = prio[i]
		b.jobs[j.ID] = copyRow(j)
	}
	return nil
}

// Fetch implements [backend.Backend]. The claim is exclusive because the whole
// scan and update happen under one mutex, so concurrent callers never receive
// the same job. It never fails.
func (b *Backend) Fetch(_ context.Context, params backend.FetchParams) ([]*backend.JobRow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var candidates []*backend.JobRow
	for _, j := range b.jobs {
		if j.State == backend.StateAvailable && j.Queue == params.Queue && !j.ScheduledAt.After(params.Now) {
			candidates = append(candidates, j)
		}
	}
	slices.SortFunc(candidates, func(a, c *backend.JobRow) int {
		if v := a.PriorityAt.Compare(c.PriorityAt); v != 0 {
			return v
		}
		return int(a.ID - c.ID)
	})
	if len(candidates) > params.Limit {
		candidates = candidates[:params.Limit]
	}
	out := make([]*backend.JobRow, 0, len(candidates))
	for _, j := range candidates {
		j.State = backend.StateRunning
		j.Attempt++
		j.Generation++
		j.AttemptedAt = params.Now
		j.HeartbeatAt = params.Now
		j.AttemptedBy = append(j.AttemptedBy, params.ClientID)
		out = append(out, copyRow(j))
	}
	return out, nil
}

func (b *Backend) finalize(ids []int64, gens []int, now time.Time, apply func(j *backend.JobRow, i int)) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var stale []int64
	for i, id := range ids {
		j, ok := b.jobs[id]
		if !ok || j.State != backend.StateRunning || j.Generation != gens[i] {
			stale = append(stale, id)
			continue
		}
		j.FinalizedAt = now
		apply(j, i)
	}
	if len(stale) > 0 {
		return &backend.StaleError{IDs: stale}
	}
	return nil
}

func mergeMeta(j *backend.JobRow, m []byte) {
	if len(m) > 0 {
		j.Metadata = slices.Clone(m)
	}
}

// Complete implements [backend.Backend], returning a [backend.StaleError] that
// names any job whose ID, generation, and running state did not match.
func (b *Backend) Complete(_ context.Context, params backend.CompleteParams) error {
	ids := make([]int64, len(params.Jobs))
	gens := make([]int, len(params.Jobs))
	for i, f := range params.Jobs {
		ids[i], gens[i] = f.ID, f.Generation
	}
	return b.finalize(ids, gens, params.Now, func(j *backend.JobRow, i int) {
		j.State = backend.StateCompleted
		mergeMeta(j, params.Jobs[i].Metadata)
	})
}

// Retry implements [backend.Backend], returning a [backend.StaleError] that
// names any job whose ID, generation, and running state did not match. If any
// entry's At falls outside the representable range, it returns an error
// wrapping [backend.ErrTimeOutOfRange] and leaves every job in the batch
// untouched.
func (b *Backend) Retry(_ context.Context, params backend.RetryParams) error {
	ids := make([]int64, len(params.Jobs))
	gens := make([]int, len(params.Jobs))
	for i, f := range params.Jobs {
		if err := backend.ValidateInstant(f.At); err != nil {
			return err
		}
		ids[i], gens[i] = f.ID, f.Generation
	}
	return b.finalize(ids, gens, params.Now, func(j *backend.JobRow, i int) {
		f := params.Jobs[i]
		j.State = backend.StateRetryable
		j.ScheduledAt = f.At
		j.PriorityAt = backend.SubDurationClamped(f.At, j.PriorityBoost)
		j.FinalizedAt = time.Time{}
		j.Errors = append(j.Errors, f.Err)
		mergeMeta(j, f.Metadata)
	})
}

// Cancel implements [backend.Backend], returning a [backend.StaleError] that
// names any job whose ID, generation, and running state did not match.
func (b *Backend) Cancel(_ context.Context, params backend.CancelParams) error {
	ids := make([]int64, len(params.Jobs))
	gens := make([]int, len(params.Jobs))
	for i, f := range params.Jobs {
		ids[i], gens[i] = f.ID, f.Generation
	}
	return b.finalize(ids, gens, params.Now, func(j *backend.JobRow, i int) {
		f := params.Jobs[i]
		j.State = backend.StateCancelled
		if f.Err != "" {
			j.Errors = append(j.Errors, backend.AttemptError{At: params.Now, Attempt: j.Attempt, Err: f.Err})
		}
		mergeMeta(j, f.Metadata)
	})
}

// Kill implements [backend.Backend], returning a [backend.StaleError] that
// names any job whose ID, generation, and running state did not match.
func (b *Backend) Kill(_ context.Context, params backend.KillParams) error {
	ids := make([]int64, len(params.Jobs))
	gens := make([]int, len(params.Jobs))
	for i, f := range params.Jobs {
		ids[i], gens[i] = f.ID, f.Generation
	}
	return b.finalize(ids, gens, params.Now, func(j *backend.JobRow, i int) {
		f := params.Jobs[i]
		j.State = backend.StateDead
		j.Errors = append(j.Errors, f.Err)
		mergeMeta(j, f.Metadata)
	})
}

// Snooze implements [backend.Backend], giving back the attempt the claim spent
// and returning a [backend.StaleError] that names any job whose ID,
// generation, and running state did not match. If any entry's At falls
// outside the representable range, it returns an error wrapping
// [backend.ErrTimeOutOfRange] and leaves every job in the batch untouched.
func (b *Backend) Snooze(_ context.Context, params backend.SnoozeParams) error {
	ids := make([]int64, len(params.Jobs))
	gens := make([]int, len(params.Jobs))
	for i, f := range params.Jobs {
		if err := backend.ValidateInstant(f.At); err != nil {
			return err
		}
		ids[i], gens[i] = f.ID, f.Generation
	}
	return b.finalize(ids, gens, params.Now, func(j *backend.JobRow, i int) {
		f := params.Jobs[i]
		j.State = backend.StateRetryable
		j.Attempt--
		j.ScheduledAt = f.At
		j.PriorityAt = backend.SubDurationClamped(f.At, j.PriorityBoost)
		j.FinalizedAt = time.Time{}
		mergeMeta(j, f.Metadata)
	})
}

// Heartbeat implements [backend.Backend]. Entries that do not match a running
// job at the given generation are skipped silently. It never fails.
func (b *Backend) Heartbeat(_ context.Context, params backend.HeartbeatParams) (backend.HeartbeatResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var res backend.HeartbeatResult
	for _, hb := range params.Jobs {
		j, ok := b.jobs[hb.ID]
		if !ok || j.State != backend.StateRunning || j.Generation != hb.Generation {
			continue
		}
		j.HeartbeatAt = params.Now
		if j.CancelRequested {
			res.CancelRequested = append(res.CancelRequested, hb.ID)
		}
	}
	return res, nil
}

// MoveDue implements [backend.Backend], returning how many jobs it made
// available. It never fails.
func (b *Backend) MoveDue(_ context.Context, params backend.MoveDueParams) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	moved := 0
	for _, j := range b.jobs {
		if moved >= params.Limit {
			break
		}
		if (j.State == backend.StateScheduled || j.State == backend.StateRetryable) && !j.ScheduledAt.After(params.Now) {
			j.State = backend.StateAvailable
			moved++
		}
	}
	return moved, nil
}

// RescueStale implements [backend.Backend], returning how many jobs it
// returned to the retryable state. It never fails.
func (b *Backend) RescueStale(_ context.Context, params backend.RescueParams) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := params.Now.Add(-params.TTL)
	n := 0
	for _, j := range b.jobs {
		if n >= params.Limit {
			break
		}
		if j.State == backend.StateRunning && j.HeartbeatAt.Before(cutoff) {
			j.State = backend.StateRetryable
			j.ScheduledAt = params.Now
			j.PriorityAt = backend.SubDurationClamped(params.Now, j.PriorityBoost)
			n++
		}
	}
	return n, nil
}

// SetCancelRequested marks the job as asked to stop, which the next
// [Backend.Heartbeat] covering it reports so the client can cancel the running
// job's context. It is a test helper standing in for the out-of-band
// cancellation API that does not exist yet, and does nothing for an unknown ID.
func (b *Backend) SetCancelRequested(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if j, ok := b.jobs[id]; ok {
		j.CancelRequested = true
	}
}

// Snapshot returns a copy of the stored job, or nil if no job has that ID. It
// is an inspection helper for tests: the copy is safe to read while the backend
// keeps running, and mutating it changes nothing.
func (b *Backend) Snapshot(id int64) *backend.JobRow {
	b.mu.Lock()
	defer b.mu.Unlock()
	j, ok := b.jobs[id]
	if !ok {
		return nil
	}
	return copyRow(j)
}

// Clean implements [backend.Backend], returning how many jobs it deleted. When
// more jobs are eligible than Limit allows, which ones are deleted first is
// unspecified. It never fails.
func (b *Backend) Clean(_ context.Context, params backend.CleanParams) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	retention := map[backend.State]time.Duration{
		backend.StateCompleted: params.CompletedRetention,
		backend.StateCancelled: params.CancelledRetention,
		backend.StateDead:      params.DeadRetention,
	}
	n := 0
	for id, j := range b.jobs {
		if n >= params.Limit {
			break
		}
		r, ok := retention[j.State]
		if !ok {
			continue
		}
		if j.FinalizedAt.Before(params.Now.Add(-r)) {
			delete(b.jobs, id)
			n++
		}
	}
	return n, nil
}

// SnapshotAll returns copies of every stored job, ordered by ID, including
// finished ones the cleaner has not deleted yet. Like [Backend.Snapshot] it is
// an inspection helper for tests.
func (b *Backend) SnapshotAll() []*backend.JobRow {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*backend.JobRow, 0, len(b.jobs))
	for _, j := range b.jobs {
		out = append(out, copyRow(j))
	}
	slices.SortFunc(out, func(a, c *backend.JobRow) int { return cmp.Compare(a.ID, c.ID) })
	return out
}

// Reset deletes every stored job so a shared backend can be reused between
// tests. The ID counter keeps going, so IDs are never reused. Do not call it
// while jobs are running: their finalizations will simply find nothing.
func (b *Backend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.jobs)
}
