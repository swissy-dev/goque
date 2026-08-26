package postgres

import (
	"context"
	"fmt"

	"github.com/swissy-dev/goque/backend"
)

func (b *Backend) moveDueSQL() string {
	return `WITH candidate AS (
    SELECT id FROM ` + b.cfg.quotedSchema + `.goque_job
    WHERE state IN ('scheduled', 'retryable') AND scheduled_at_ns <= $1
    ORDER BY scheduled_at_ns
    LIMIT $2
    FOR UPDATE SKIP LOCKED
),
moved AS (
    UPDATE ` + b.cfg.quotedSchema + `.goque_job j SET state = 'available'
    FROM candidate c WHERE j.id = c.id
    RETURNING j.id
)
SELECT count(*) FROM moved`
}

func (b *Backend) rescueSQL() string {
	return `WITH candidate AS (
    SELECT id FROM ` + b.cfg.quotedSchema + `.goque_job
    WHERE state = 'running' AND heartbeat_at < $1
    ORDER BY heartbeat_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
),
rescued AS (
    UPDATE ` + b.cfg.quotedSchema + `.goque_job j SET
        state = 'retryable',
        scheduled_at_ns = $3::bigint,
        priority_at_ns = ` + clampNS(`$3::bigint::numeric - j.priority_boost_ns::numeric`) + `,
        finalized_at = NULL
    FROM candidate c WHERE j.id = c.id
    RETURNING j.id
)
SELECT count(*) FROM rescued`
}

func (b *Backend) cleanSQL() string {
	return `WITH candidate AS (
    SELECT id FROM ` + b.cfg.quotedSchema + `.goque_job
    WHERE (state = 'completed' AND finalized_at < $1)
       OR (state = 'cancelled' AND finalized_at < $2)
       OR (state = 'dead'      AND finalized_at < $3)
    ORDER BY finalized_at
    LIMIT $4
    FOR UPDATE SKIP LOCKED
),
deleted AS (
    DELETE FROM ` + b.cfg.quotedSchema + `.goque_job
    WHERE id IN (SELECT id FROM candidate)
    RETURNING id
)
SELECT count(*) FROM deleted`
}

// MoveDue implements [backend.Backend]. It promotes scheduled and retryable
// jobs whose instant has arrived to available, up to Limit per call. Candidates
// are locked with FOR UPDATE SKIP LOCKED, so every instance may run it
// concurrently without promoting a row twice.
func (b *Backend) MoveDue(ctx context.Context, params backend.MoveDueParams) (int, error) {
	if params.Limit <= 0 {
		return 0, nil
	}
	nowNS, err := nanos(params.Now)
	if err != nil {
		return 0, err
	}
	return b.countingQuery(ctx, "move due", b.moveDueSQL(), nowNS, params.Limit)
}

// RescueStale implements [backend.Backend]. A running job whose heartbeat is
// older than TTL is assumed lost and returned to retryable with ScheduledAt set
// to Now, keeping its attempt and re-deriving PriorityAt from Now and its
// stored PriorityBoost so it keeps its place in line.
func (b *Backend) RescueStale(ctx context.Context, params backend.RescueParams) (int, error) {
	if params.Limit <= 0 {
		return 0, nil
	}
	nowNS, err := nanos(params.Now)
	if err != nil {
		return 0, err
	}
	cutoff := params.Now.Add(-params.TTL).UTC()
	return b.countingQuery(ctx, "rescue stale", b.rescueSQL(), cutoff, params.Limit, nowNS)
}

// Clean implements [backend.Backend]. It deletes terminal jobs past their
// per-state retention. Limit is one global cap across completed, cancelled, and
// dead — not a cap per state — so a call can never delete more rows, or take
// more locks, than the caller budgeted for.
func (b *Backend) Clean(ctx context.Context, params backend.CleanParams) (int, error) {
	if params.Limit <= 0 {
		return 0, nil
	}
	return b.countingQuery(ctx, "clean", b.cleanSQL(),
		params.Now.Add(-params.CompletedRetention).UTC(),
		params.Now.Add(-params.CancelledRetention).UTC(),
		params.Now.Add(-params.DeadRetention).UTC(),
		params.Limit)
}

func (b *Backend) countingQuery(ctx context.Context, what, sql string, args ...any) (int, error) {
	rows, err := b.d.Query(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("goque/postgres: %s: %w", what, err)
	}
	defer rows.Close()
	var n int
	found := false
	for rows.Next() {
		if !found {
			if err := rows.Scan(&n); err != nil {
				return 0, fmt.Errorf("goque/postgres: %s: %w", what, err)
			}
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("goque/postgres: %s: %w", what, err)
	}
	if !found {
		return 0, fmt.Errorf("goque/postgres: %s returned no count", what)
	}
	return n, nil
}
