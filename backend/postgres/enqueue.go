package postgres

import (
	"context"
	"fmt"

	"github.com/swissy-dev/goque/backend"
)

func (b *Backend) enqueueSQL() string {
	return `WITH input AS (
    SELECT * FROM ROWS FROM (
        jsonb_to_recordset($1::jsonb) AS (
            kind text, queue text, payload jsonb, state text,
            priority_boost_ns bigint, scheduled_at_ns bigint, priority_at_ns bigint,
            max_attempts int, created_at timestamptz,
            concurrency_key text, throttle_key text, debounce_key text,
            debounce_deadline timestamptz, retry_policy jsonb,
            metadata jsonb, output jsonb, version int)
    ) WITH ORDINALITY AS v(kind, queue, payload, state, priority_boost_ns,
        scheduled_at_ns, priority_at_ns, max_attempts, created_at,
        concurrency_key, throttle_key, debounce_key, debounce_deadline,
        retry_policy, metadata, output, version, ord)
),
ins AS (
    INSERT INTO ` + b.cfg.quotedSchema + `.goque_job (kind, queue, payload, state, priority_boost_ns,
        scheduled_at_ns, priority_at_ns, max_attempts, created_at,
        concurrency_key, throttle_key, debounce_key, debounce_deadline,
        retry_policy, metadata, output, version)
    SELECT kind, queue, payload, state, priority_boost_ns, scheduled_at_ns, priority_at_ns,
           max_attempts, created_at, concurrency_key, throttle_key, debounce_key,
           debounce_deadline, retry_policy, metadata, output, version
    FROM input ORDER BY ord
    RETURNING id
)
SELECT id FROM ins ORDER BY id`
}

// Enqueue implements [backend.Backend]. It assigns each row an ID and stores a
// copy, filling in CreatedAt, State, and PriorityAt on the caller's rows as
// well, and defaulting a zero ScheduledAt to Now and a zero Version to 1. The
// batch is one statement, so all rows land or none do. An empty batch is a
// no-op with no round trip.
//
// If any row's ScheduledAt, or its ScheduledAt minus its PriorityBoost, falls
// outside the storable range, it rejects the whole batch with an error wrapping
// [backend.ErrTimeOutOfRange] before issuing any statement.
func (b *Backend) Enqueue(ctx context.Context, params backend.EnqueueParams) error {
	return b.enqueueOn(ctx, b.d, params)
}

// EnqueueTx stores jobs on a caller-owned transaction. It never commits or
// rolls back tx.
func (b *Backend) EnqueueTx(ctx context.Context, tx any, params backend.EnqueueParams) error {
	d, err := b.d.InTx(ctx, tx)
	if err != nil {
		return err
	}
	return b.enqueueOn(ctx, d, params)
}

func (b *Backend) enqueueOn(ctx context.Context, d Driver, params backend.EnqueueParams) error {
	if len(params.Jobs) == 0 {
		return nil
	}
	rows, err := newEnqueueRows(params.Jobs, params.Now)
	if err != nil {
		return err
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		return fmt.Errorf("goque/postgres: encoding enqueue batch: %w", err)
	}
	result, err := d.Query(ctx, b.enqueueSQL(), doc)
	if err != nil {
		return fmt.Errorf("goque/postgres: enqueue: %w", err)
	}
	defer result.Close()
	ids := make([]int64, 0, len(rows))
	for result.Next() {
		var id int64
		if err := result.Scan(&id); err != nil {
			return fmt.Errorf("goque/postgres: enqueue: %w", err)
		}
		ids = append(ids, id)
	}
	if err := result.Err(); err != nil {
		return fmt.Errorf("goque/postgres: enqueue: %w", err)
	}
	if len(ids) != len(params.Jobs) {
		return fmt.Errorf("goque/postgres: enqueue inserted %d rows, expected %d", len(ids), len(params.Jobs))
	}
	for i, j := range params.Jobs {
		j.ID = ids[i]
		j.CreatedAt = params.Now
		if j.ScheduledAt.IsZero() {
			j.ScheduledAt = params.Now
		}
		j.PriorityAt = fromNanos(rows[i].PriorityAtNS)
		j.State = backend.State(rows[i].State)
		if j.Version == 0 {
			j.Version = rows[i].Version
		}
	}
	return nil
}
