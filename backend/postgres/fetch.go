package postgres

import (
	"context"
	"fmt"

	"github.com/swissy-dev/goque/backend"
)

func (b *Backend) fetchSQL() string {
	return `WITH candidate AS (
    SELECT id FROM ` + b.cfg.quotedSchema + `.goque_job
    WHERE state = 'available' AND queue = $1 AND scheduled_at_ns <= $2
    ORDER BY priority_at_ns, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE ` + b.cfg.quotedSchema + `.goque_job j SET
        state = 'running',
        attempt = attempt + 1,
        generation = generation + 1,
        attempted_at = $4,
        heartbeat_at = $4,
        attempted_by = array_append(attempted_by, $5)
    FROM candidate c
    WHERE j.id = c.id
    RETURNING j.*
)
SELECT ` + jobColumns + `
FROM claimed
ORDER BY priority_at_ns, id`
}

// Fetch implements [backend.Backend]. It claims up to Limit available jobs on
// the given queue whose ScheduledAt has arrived, in effective-time order, and
// returns them stamped as running with their attempt and generation advanced.
// A Limit of zero or less is a no-op with no round trip.
//
// The claim is exclusive: candidates are locked with FOR UPDATE SKIP LOCKED, so
// concurrent callers step over each other's rows rather than blocking or
// double-delivering.
func (b *Backend) Fetch(ctx context.Context, params backend.FetchParams) ([]*backend.JobRow, error) {
	if params.Limit <= 0 {
		return nil, nil
	}
	nowNS, err := nanos(params.Now)
	if err != nil {
		return nil, err
	}
	rows, err := b.d.Query(ctx, b.fetchSQL(), params.Queue, nowNS, params.Limit, params.Now.UTC(), params.ClientID)
	if err != nil {
		return nil, fmt.Errorf("goque/postgres: fetch: %w", err)
	}
	defer rows.Close()
	var out []*backend.JobRow
	for rows.Next() {
		j, err := scanJobRow(rows)
		if err != nil {
			return nil, fmt.Errorf("goque/postgres: fetch: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("goque/postgres: fetch: %w", err)
	}
	return out, nil
}
