package goque

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swissy-dev/goque/backend"
)

type opKind int

const (
	opComplete opKind = iota
	opRetry
	opCancel
	opKill
	opSnooze
)

type finalizeOp struct {
	op       opKind
	complete backend.JobFinalize
	retry    backend.JobRetry
	cancel   backend.JobCancel
	kill     backend.JobKill
	snooze   backend.JobSnooze
}

type finalizer interface {
	push(op finalizeOp)
}

type completer struct {
	b           backend.Backend
	now         func() time.Time
	flushSize   int
	flushEvery  time.Duration
	logger      *slog.Logger
	ch          chan finalizeOp
	done        chan struct{}
	stopOnce    sync.Once
	reportOnce  sync.Once
	lifetime    context.Context
	abortFn     context.CancelFunc
	callTimeout time.Duration
	dropped     atomic.Int64
}

func newCompleter(b backend.Backend, now func() time.Time, flushSize int, flushEvery time.Duration, logger *slog.Logger) *completer {
	lifetime, abortFn := context.WithCancel(context.Background())
	return &completer{
		b:           b,
		now:         now,
		flushSize:   flushSize,
		flushEvery:  flushEvery,
		logger:      logger,
		ch:          make(chan finalizeOp, flushSize*4),
		done:        make(chan struct{}),
		lifetime:    lifetime,
		abortFn:     abortFn,
		callTimeout: completerCallTimeout,
	}
}

func (cp *completer) start() {
	go cp.loop()
}

func (cp *completer) push(op finalizeOp) {
	select {
	case cp.ch <- op:
		return
	default:
	}
	timer := time.NewTimer(completerPushTimeout)
	defer timer.Stop()
	select {
	case cp.ch <- op:
	case <-cp.lifetime.Done():
		cp.dropped.Add(1)
		cp.logger.Warn("goque: finalization dropped, completer is shutting down")
	case <-timer.C:
		cp.dropped.Add(1)
		cp.logger.Warn("goque: finalization dropped, completer is not draining")
	}
}

func (cp *completer) stop(ctx context.Context) {
	cp.stopOnce.Do(func() { close(cp.ch) })
	select {
	case <-cp.done:
	case <-ctx.Done():
		cp.abortFn()
		<-cp.done
	}
	cp.abortFn()
	cp.reportOnce.Do(func() {
		if n := cp.dropped.Load(); n > 0 {
			cp.logger.Warn("goque: finalizations dropped during shutdown", "count", n)
		}
	})
}

func (cp *completer) loop() {
	defer close(cp.done)
	buf := make([]finalizeOp, 0, cp.flushSize)
	timer := time.NewTimer(cp.flushEvery)
	defer timer.Stop()
	for {
		select {
		case op, ok := <-cp.ch:
			if !ok {
				cp.flush(buf)
				return
			}
			buf = append(buf, op)
			if len(buf) >= cp.flushSize {
				cp.flush(buf)
				buf = buf[:0]
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(cp.flushEvery)
			}
		case <-timer.C:
			if len(buf) > 0 {
				cp.flush(buf)
				buf = buf[:0]
			}
			timer.Reset(cp.flushEvery)
		}
	}
}

func (cp *completer) flush(ops []finalizeOp) {
	if len(ops) == 0 {
		return
	}
	now := cp.now()
	var completes []backend.JobFinalize
	var retries []backend.JobRetry
	var cancels []backend.JobCancel
	var kills []backend.JobKill
	var snoozes []backend.JobSnooze
	for _, op := range ops {
		switch op.op {
		case opComplete:
			completes = append(completes, op.complete)
		case opRetry:
			retries = append(retries, op.retry)
		case opCancel:
			cancels = append(cancels, op.cancel)
		case opKill:
			kills = append(kills, op.kill)
		case opSnooze:
			snoozes = append(snoozes, op.snooze)
		}
	}
	if len(completes) > 0 {
		cp.call(len(completes), func(ctx context.Context) error {
			return cp.b.Complete(ctx, backend.CompleteParams{Jobs: completes, Now: now})
		})
	}
	if len(retries) > 0 {
		cp.call(len(retries), func(ctx context.Context) error {
			return cp.b.Retry(ctx, backend.RetryParams{Jobs: retries, Now: now})
		})
	}
	if len(cancels) > 0 {
		cp.call(len(cancels), func(ctx context.Context) error {
			return cp.b.Cancel(ctx, backend.CancelParams{Jobs: cancels, Now: now})
		})
	}
	if len(kills) > 0 {
		cp.call(len(kills), func(ctx context.Context) error {
			return cp.b.Kill(ctx, backend.KillParams{Jobs: kills, Now: now})
		})
	}
	if len(snoozes) > 0 {
		cp.call(len(snoozes), func(ctx context.Context) error {
			return cp.b.Snooze(ctx, backend.SnoozeParams{Jobs: snoozes, Now: now})
		})
	}
}

const completerCallTimeout = 30 * time.Second

var completerDrainTimeout = 30 * time.Second

var completerPushTimeout = completerCallTimeout

var completerBackoffs = []time.Duration{
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
}

func (cp *completer) call(n int, fn func(ctx context.Context) error) {
	if err := cp.attempt(fn); err != nil {
		cp.dropped.Add(int64(n))
		cp.logger.Error("goque: finalizations dropped", "count", n, "err", err)
	}
}

func (cp *completer) attempt(fn func(ctx context.Context) error) error {
	for i := 0; ; i++ {
		ctx, cancel := context.WithTimeout(cp.lifetime, cp.callTimeout)
		err := fn(ctx)
		cancel()
		if err == nil {
			return nil
		}
		if errors.Is(err, backend.ErrStaleAttempt) {
			cp.logger.Debug("goque: stale finalization dropped", "err", err)
			return nil
		}
		if cp.lifetime.Err() != nil || i >= len(completerBackoffs) {
			return err
		}
		timer := time.NewTimer(completerBackoffs[i])
		select {
		case <-timer.C:
		case <-cp.lifetime.Done():
			timer.Stop()
			return err
		}
	}
}
