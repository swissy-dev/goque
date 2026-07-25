package goque

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/swissy-dev/goque/backend"
)

type queueState struct {
	name    string
	cfg     QueueConfig
	running atomic.Int64
	credit  int
}

// Start begins working jobs. It launches the fetchers that claim work from the
// configured queues, the completer that batches finalizations back to the
// backend, the heartbeat loop, and the three maintenance loops: the mover that
// makes due jobs available, the rescuer that reclaims jobs whose process died,
// and the cleaner that deletes finished jobs past their retention. Start
// returns as soon as they are running; the work happens in the background until
// [Client.Stop].
//
// Cancelling ctx stops the client claiming new work and stops the maintenance
// loops. It is not a shutdown: the jobs already in flight run to completion and
// their heartbeats keep renewing, so no other process rescues them, and the
// completer stays up to record their outcomes. Call Stop to drain those jobs
// and release the client's resources.
//
// Start returns an error if the client is already started, is still draining
// from a previous Stop, is driven by [Client.Fake], has no [Config.Queues], or
// has no [Config.Workers]. A client with no queues is enqueue-only by design,
// so this is how that mistake surfaces.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return fmt.Errorf("goque: client already started")
	}
	if c.stopping {
		return fmt.Errorf("goque: client is stopping; wait for Stop to finish draining")
	}
	if c.fake != nil {
		return fmt.Errorf("goque: client is fake-driven")
	}
	if len(c.cfg.Queues) == 0 {
		return fmt.Errorf("goque: no queues configured; this is an enqueue-only client")
	}
	if c.workers == nil {
		return fmt.Errorf("goque: no workers registered")
	}
	c.started = true
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	liveCtx, liveCancel := context.WithCancel(context.Background())
	c.fetchCancel = fetchCancel
	c.liveCancel = liveCancel
	c.completer = newCompleter(c.backend, c.now, 128, 50*time.Millisecond, c.cfg.Logger)
	c.completer.start()
	var queues []*queueState
	for name, qc := range c.cfg.Queues {
		queues = append(queues, &queueState{name: name, cfg: qc})
	}
	slices.SortFunc(queues, func(a, b *queueState) int { return b.cfg.Weight - a.cfg.Weight })
	c.queues = queues
	c.globalRunning = &atomic.Int64{}
	c.fetchWg.Add(1)
	go c.dispatchLoop(fetchCtx)
	c.liveWg.Add(1)
	go c.heartbeatLoop(liveCtx)
	c.fetchWg.Add(1)
	go c.maintenanceLoop(fetchCtx, "mover", c.cfg.MoverInterval, func() (int, error) {
		return c.backend.MoveDue(fetchCtx, backend.MoveDueParams{Now: c.now(), Limit: 1000})
	})
	c.fetchWg.Add(1)
	go c.maintenanceLoop(fetchCtx, "rescuer", c.cfg.HeartbeatInterval, func() (int, error) {
		return c.backend.RescueStale(fetchCtx, backend.RescueParams{Now: c.now(), TTL: c.cfg.RescueTTL, Limit: 1000})
	})
	c.fetchWg.Add(1)
	go c.maintenanceLoop(fetchCtx, "cleaner", c.cfg.CleanInterval, func() (int, error) {
		return c.backend.Clean(fetchCtx, backend.CleanParams{
			Now:                c.now(),
			CompletedRetention: c.cfg.CompletedRetention,
			CancelledRetention: c.cfg.CancelledRetention,
			DeadRetention:      c.cfg.DeadRetention,
			Limit:              1000,
		})
	})
	return nil
}

func (c *Client) dispatchLoop(ctx context.Context) {
	defer c.fetchWg.Done()
	base := c.queues[0].cfg.PollInterval
	for _, q := range c.queues {
		if q.cfg.PollInterval < base {
			base = q.cfg.PollInterval
		}
	}
	for {
		c.dispatchOnce(ctx)
		jitter := time.Duration(float64(base) * (0.8 + 0.4*rand.Float64()))
		timer := time.NewTimer(jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-c.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (c *Client) dispatchOnce(ctx context.Context) {
	globalFree := c.cfg.MaxWorkers - int(c.globalRunning.Load())
	if globalFree <= 0 {
		return
	}
	type demand struct {
		q    *queueState
		free int
	}
	var demands []demand
	totalWeight := 0
	for _, q := range c.queues {
		free := q.cfg.Concurrency - int(q.running.Load())
		if free > 0 {
			demands = append(demands, demand{q: q, free: free})
			totalWeight += q.cfg.Weight
		}
	}
	if !c.cfg.StrictQueuePriority && len(demands) > 0 {
		for i := range demands {
			demands[i].q.credit += demands[i].q.cfg.Weight
		}
		slices.SortFunc(demands, func(a, b demand) int {
			if a.q.credit != b.q.credit {
				return b.q.credit - a.q.credit
			}
			return strings.Compare(a.q.name, b.q.name)
		})
		demands[0].q.credit -= totalWeight
	}
	for _, d := range demands {
		quota := globalFree
		if !c.cfg.StrictQueuePriority {
			quota = (globalFree*d.q.cfg.Weight + totalWeight - 1) / totalWeight
		}
		limit := min(quota, d.free, globalFree)
		if limit <= 0 {
			continue
		}
		rows, err := c.backend.Fetch(ctx, backend.FetchParams{Queue: d.q.name, Limit: limit, ClientID: c.clientID, Now: c.now()})
		if err != nil {
			if ctx.Err() == nil {
				c.cfg.Logger.Error("goque: fetch failed", "queue", d.q.name, "err", err)
			}
			continue
		}
		globalFree -= len(rows)
		for _, row := range rows {
			d.q.running.Add(1)
			c.globalRunning.Add(1)
			c.jobWg.Add(1)
			go func(row *JobRow, q *queueState) {
				defer c.jobWg.Done()
				defer q.running.Add(-1)
				defer c.globalRunning.Add(-1)
				c.runJob(context.WithoutCancel(ctx), row, c.completer)
			}(row, d.q)
		}
		if globalFree <= 0 {
			return
		}
	}
}

func (c *Client) heartbeatLoop(ctx context.Context) {
	defer c.liveWg.Done()
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var jobs []backend.JobHeartbeat
			c.active.Range(func(k, v any) bool {
				jobs = append(jobs, backend.JobHeartbeat{ID: k.(int64), Generation: v.(*activeJob).generation})
				return true
			})
			if len(jobs) == 0 {
				failures = 0
				continue
			}
			hbCtx, hbCancel := context.WithTimeout(ctx, c.cfg.HeartbeatInterval)
			res, err := c.backend.Heartbeat(hbCtx, backend.HeartbeatParams{ClientID: c.clientID, Jobs: jobs, Now: c.now()})
			hbCancel()
			if err != nil {
				failures++
				c.cfg.Logger.Error("goque: heartbeat failed", "failures", failures, "err", err)
				if failures >= 3 {
					c.cfg.Logger.Error("goque: heartbeat lost, self-fencing active executions")
					c.fenceActive()
					failures = 0
				}
				continue
			}
			failures = 0
			for _, id := range res.CancelRequested {
				if v, ok := c.active.Load(id); ok {
					v.(*activeJob).cancel()
				}
			}
		}
	}
}

func (c *Client) cancelActive() {
	c.active.Range(func(_, v any) bool {
		v.(*activeJob).cancel()
		return true
	})
}

func (c *Client) fenceActive() {
	c.active.Range(func(_, v any) bool {
		aj := v.(*activeJob)
		aj.fenced.Store(true)
		aj.cancel()
		return true
	})
}

func (c *Client) maintenanceLoop(ctx context.Context, name string, every time.Duration, fn func() (int, error)) {
	defer c.fetchWg.Done()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := fn(); err != nil && ctx.Err() == nil {
				c.cfg.Logger.Error("goque: maintenance failed", "service", name, "err", err)
			}
		}
	}
}

// Stop shuts the client down gracefully: it stops claiming new work, then waits
// for the jobs already in flight to finish and for their outcomes to reach the
// backend. Heartbeats keep going for the whole drain, so no other process
// mistakes a job that is finishing normally for one whose owner died and
// rescues it out from under this client. Stop returns nil once the drain is
// done, and immediately if the client was never started.
//
// ctx bounds how long the drain may take. If it is cancelled first, Stop
// cancels the contexts of the jobs still running so they can return early,
// gives them a brief grace period, and then returns ctx's error while a
// background goroutine finishes the drain. Losing patience therefore interrupts
// jobs rather than abandoning them, but the client may still be settling when
// Stop returns.
func (c *Client) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = false
	c.stopping = true
	fetchCancel := c.fetchCancel
	liveCancel := c.liveCancel
	c.fetchCancel = nil
	c.liveCancel = nil
	c.mu.Unlock()
	fetchCancel()
	done := make(chan struct{})
	go func() {
		c.fetchWg.Wait()
		c.jobWg.Wait()
		liveCancel()
		c.liveWg.Wait()
		c.completer.stop()
		c.mu.Lock()
		c.stopping = false
		c.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		c.cancelActive()
		grace := time.NewTimer(100 * time.Millisecond)
		defer grace.Stop()
		select {
		case <-done:
			return nil
		case <-grace.C:
			return ctx.Err()
		}
	}
}

// StopAndCancel shuts the client down without waiting for running jobs to
// finish on their own: it cancels their contexts first and then stops as
// [Client.Stop] does. A worker that respects its context returns promptly, and
// the attempt is recorded as a failure and retried under the job's policy.
// Reach for it when the process must exit now; prefer Stop otherwise.
func (c *Client) StopAndCancel(ctx context.Context) error {
	c.cancelActive()
	return c.Stop(ctx)
}
