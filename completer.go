package goque

import (
	"context"
	"errors"
	"log/slog"
	"sync"
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
	b          backend.Backend
	now        func() time.Time
	flushSize  int
	flushEvery time.Duration
	logger     *slog.Logger
	ch         chan finalizeOp
	done       chan struct{}
	stopOnce   sync.Once
}

func newCompleter(b backend.Backend, now func() time.Time, flushSize int, flushEvery time.Duration, logger *slog.Logger) *completer {
	return &completer{
		b:          b,
		now:        now,
		flushSize:  flushSize,
		flushEvery: flushEvery,
		logger:     logger,
		ch:         make(chan finalizeOp, flushSize*4),
		done:       make(chan struct{}),
	}
}

func (cp *completer) start() {
	go cp.loop()
}

func (cp *completer) push(op finalizeOp) {
	cp.ch <- op
}

func (cp *completer) stop() {
	cp.stopOnce.Do(func() { close(cp.ch) })
	<-cp.done
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
	ctx := context.Background()
	if len(completes) > 0 {
		cp.attempt(func() error { return cp.b.Complete(ctx, backend.CompleteParams{Jobs: completes, Now: now}) })
	}
	if len(retries) > 0 {
		cp.attempt(func() error { return cp.b.Retry(ctx, backend.RetryParams{Jobs: retries, Now: now}) })
	}
	if len(cancels) > 0 {
		cp.attempt(func() error { return cp.b.Cancel(ctx, backend.CancelParams{Jobs: cancels, Now: now}) })
	}
	if len(kills) > 0 {
		cp.attempt(func() error { return cp.b.Kill(ctx, backend.KillParams{Jobs: kills, Now: now}) })
	}
	if len(snoozes) > 0 {
		cp.attempt(func() error { return cp.b.Snooze(ctx, backend.SnoozeParams{Jobs: snoozes, Now: now}) })
	}
}

var completerBackoffs = []time.Duration{
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
}

func (cp *completer) attempt(call func() error) {
	err := call()
	for i := 0; ; i++ {
		if err == nil {
			return
		}
		if errors.Is(err, backend.ErrStaleAttempt) {
			cp.logger.Debug("goque: stale finalization dropped", "err", err)
			return
		}
		if i >= len(completerBackoffs) {
			cp.logger.Error("goque: finalization flush failed after retries", "err", err)
			return
		}
		time.Sleep(completerBackoffs[i])
		err = call()
	}
}
