package goque

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/swissy-dev/goque/backend"
)

type binding struct {
	kind     string
	dispatch func(ctx context.Context, row *JobRow) error
}

// Workers is the registry mapping job kinds to the workers that run them. Build
// one with [NewWorkers], fill it with [Register] and [RegisterFunc], and pass it
// as [Config.Workers]. A Workers is not safe for concurrent registration and is
// expected to be fully populated before [Client.Start].
type Workers struct {
	bindings map[string]*binding
}

// NewWorkers returns an empty worker registry.
func NewWorkers() *Workers {
	return &Workers{bindings: map[string]*binding{}}
}

// Register binds worker to the kind reported by T, so jobs of that kind are
// decoded into T and dispatched to worker. It returns an error wrapping
// [backend.ErrDuplicateKind] if the kind is already registered.
//
// Register is a package-level function rather than a method on [Workers]
// because Go does not allow methods to declare their own type parameters.
func Register[T JobArgs](w *Workers, worker Worker[T]) error {
	var zero T
	kind := zero.Kind()
	if _, exists := w.bindings[kind]; exists {
		return fmt.Errorf("%w: %s", backend.ErrDuplicateKind, kind)
	}
	w.bindings[kind] = &binding{
		kind: kind,
		dispatch: func(ctx context.Context, row *JobRow) error {
			var args T
			if err := json.Unmarshal(row.Payload, &args); err != nil {
				return fmt.Errorf("goque: decode %s payload: %w", kind, err)
			}
			return worker.Work(ctx, &Job[T]{JobRow: row, Args: args})
		},
	}
	return nil
}

type workFunc[T JobArgs] struct {
	fn func(ctx context.Context, job *Job[T]) error
}

func (w *workFunc[T]) Work(ctx context.Context, job *Job[T]) error {
	return w.fn(ctx, job)
}

// RegisterFunc registers fn as the worker for the kind reported by T, for jobs
// that need no worker struct. It behaves exactly like [Register], including
// returning an error wrapping [backend.ErrDuplicateKind] on a duplicate kind.
func RegisterFunc[T JobArgs](w *Workers, fn func(ctx context.Context, job *Job[T]) error) error {
	return Register(w, &workFunc[T]{fn: fn})
}

func (w *Workers) dispatch(kind string) (func(ctx context.Context, row *JobRow) error, bool) {
	b, ok := w.bindings[kind]
	if !ok {
		return nil, false
	}
	return b.dispatch, true
}
