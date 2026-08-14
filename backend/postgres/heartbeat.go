package postgres

import (
	"context"
	"fmt"

	"github.com/swissy-dev/goque/backend"
)

func (b *Backend) heartbeatSQL() string {
	return `WITH input AS (
    SELECT * FROM ROWS FROM (
        jsonb_to_recordset($1::jsonb) AS (id bigint, generation int)
    ) WITH ORDINALITY AS v(id, generation, ord)
),
winner AS (SELECT DISTINCT ON (id, generation) * FROM input ORDER BY id, generation, ord),
updated AS (
    UPDATE ` + b.cfg.quotedSchema + `.goque_job j SET heartbeat_at = $2
    FROM winner w
    WHERE j.id = w.id AND j.generation = w.generation AND j.state = 'running'
    RETURNING j.id, j.cancel_requested
)
SELECT id FROM updated WHERE cancel_requested`
}

// Heartbeat implements [backend.Backend]. It renews the liveness stamp of the
// running executions it is given, ignoring entries whose generation does not
// match — a stale token must never suppress a live renewal, because a job whose
// heartbeat lapses is rescued and run again.
//
// The returned CancelRequested names those renewed jobs that have been asked to
// stop, and is nil when none have.
func (b *Backend) Heartbeat(ctx context.Context, params backend.HeartbeatParams) (backend.HeartbeatResult, error) {
	if len(params.Jobs) == 0 {
		return backend.HeartbeatResult{}, nil
	}
	rows := make([]heartbeatRow, len(params.Jobs))
	for i, h := range params.Jobs {
		rows[i] = heartbeatRow{ID: h.ID, Generation: h.Generation}
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		return backend.HeartbeatResult{}, fmt.Errorf("goque/postgres: encoding heartbeat batch: %w", err)
	}
	result, err := b.d.Query(ctx, b.heartbeatSQL(), doc, params.Now.UTC())
	if err != nil {
		return backend.HeartbeatResult{}, fmt.Errorf("goque/postgres: heartbeat: %w", err)
	}
	defer result.Close()
	var cancelled []int64
	for result.Next() {
		var id int64
		if err := result.Scan(&id); err != nil {
			return backend.HeartbeatResult{}, fmt.Errorf("goque/postgres: heartbeat: %w", err)
		}
		cancelled = append(cancelled, id)
	}
	if err := result.Err(); err != nil {
		return backend.HeartbeatResult{}, fmt.Errorf("goque/postgres: heartbeat: %w", err)
	}
	return backend.HeartbeatResult{CancelRequested: cancelled}, nil
}
