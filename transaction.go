package goque

import (
	"context"
	"errors"
	"fmt"

	"github.com/swissy-dev/goque/backend"
)

type clientContextKey struct{}

func withClient(ctx context.Context, client *Client) context.Context {
	return context.WithValue(ctx, clientContextKey{}, client)
}

func clientFromWorkerContext(ctx context.Context) *Client {
	client, _ := ctx.Value(clientContextKey{}).(*Client)
	return client
}

// JobCompleteTx marks job completed as part of a transaction tx that the
// caller alone owns: JobCompleteTx never calls Begin, Commit, or Rollback. Call
// it from within [Worker.Work] after tx's other side effects succeed, so the
// job's completion commits atomically with them; with the PostgreSQL backend,
// tx must be a pgx v5 pgx.Tx.
//
// It returns an error wrapping [backend.ErrNotSupported] if the configured
// backend does not implement [backend.TxCompleter], one wrapping
// [backend.ErrInvalidTx] if tx is not a handle the backend recognizes, and an
// error if job is nil, not currently running, or the call is outside a
// worker.
//
// JobCompleteTx does not suppress the executor's ordinary finalization after
// Work returns: if tx later commits, that finalization is stale and is
// dropped harmlessly; if tx is rolled back instead, the ordinary finalization
// applies out of band, so a job is never stranded by caller misuse. After a
// committed transaction, [Client.ProcessReady] still reports the outcome the
// executor submitted rather than the one that applied, so its JobResult can
// show a retried or cancelled outcome for a job that is in fact completed.
func JobCompleteTx[T JobArgs](ctx context.Context, tx any, job *Job[T]) error {
	if job == nil || job.JobRow == nil {
		return errors.New("goque: JobCompleteTx requires a job")
	}
	if job.State != backend.StateRunning {
		return fmt.Errorf("goque: JobCompleteTx requires a running job, got %s", job.State)
	}
	client := clientFromWorkerContext(ctx)
	if client == nil {
		return errors.New("goque: JobCompleteTx can only be called from a worker")
	}
	b, ok := client.backend.(backend.TxCompleter)
	if !ok {
		return fmt.Errorf("%w: JobCompleteTx", backend.ErrNotSupported)
	}
	return b.CompleteTx(ctx, tx, backend.CompleteParams{
		Jobs: []backend.JobFinalize{{
			ID:         job.ID,
			Generation: job.Generation,
			Metadata:   job.Metadata,
		}},
		Now: client.now(),
	})
}
