package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/swissy-dev/goque/backend"
)

func (b *Backend) finalizeSQL(set string) string {
	return `WITH input AS (
    SELECT * FROM ROWS FROM (
        jsonb_to_recordset($1::jsonb) AS (id bigint, generation int, metadata jsonb, at_ns bigint, err jsonb)
    ) WITH ORDINALITY AS v(id, generation, metadata, at_ns, err, ord)
),
winner AS (
    SELECT DISTINCT ON (id, generation) * FROM input
    ORDER BY id, generation, ord
),
updated AS (
    UPDATE ` + b.cfg.quotedSchema + `.goque_job j SET ` + set + `
    FROM winner w
    WHERE j.id = w.id AND j.generation = w.generation AND j.state = 'running'
    RETURNING w.ord
)
SELECT ord FROM updated`
}

func clampNS(expr string) string {
	return `GREATEST(LEAST(` + expr + `, 9223372036854775807::numeric), '-9223372036854775808'::numeric)::bigint`
}

var (
	completeSet = `state = 'completed', finalized_at = $2, metadata = COALESCE(w.metadata, j.metadata)`
	retrySet    = `state = 'retryable', scheduled_at_ns = w.at_ns,
        priority_at_ns = ` + clampNS(`w.at_ns::numeric - j.priority_boost_ns::numeric`) + `,
        finalized_at = NULL, errors = j.errors || w.err,
        metadata = COALESCE(w.metadata, j.metadata)`
	snoozeSet = `state = 'retryable', attempt = j.attempt - 1, scheduled_at_ns = w.at_ns,
        priority_at_ns = ` + clampNS(`w.at_ns::numeric - j.priority_boost_ns::numeric`) + `,
        finalized_at = NULL, metadata = COALESCE(w.metadata, j.metadata)`
	killSet = `state = 'dead', finalized_at = $2, errors = j.errors || w.err,
        metadata = COALESCE(w.metadata, j.metadata)`
	cancelSet = `state = 'cancelled', finalized_at = $2,
        errors = CASE WHEN w.err IS NULL THEN j.errors
            ELSE j.errors || jsonb_set(w.err, '{0,attempt}', to_jsonb(j.attempt))
        END,
        metadata = COALESCE(w.metadata, j.metadata)`
)

// Complete implements [backend.Backend]. An entry applies only to a row
// matching its id and generation and still running; unmatched entries are
// reported together in a [backend.StaleError] while the rest still apply.
func (b *Backend) Complete(ctx context.Context, params backend.CompleteParams) error {
	return b.completeOn(ctx, b.d, params)
}

func (b *Backend) completeOn(ctx context.Context, d Driver, params backend.CompleteParams) error {
	rows := make([]finalizeRow, len(params.Jobs))
	ids := make([]int64, len(params.Jobs))
	for i, f := range params.Jobs {
		rows[i] = finalizeRow{ID: f.ID, Generation: f.Generation, Metadata: rawOrNull(f.Metadata)}
		ids[i] = f.ID
	}
	return b.runFinalize(ctx, d, "complete", completeSet, rows, ids, params.Now.UTC())
}

// Retry implements [backend.Backend]. Each entry carries its own At and Err, so
// one call may reschedule different jobs to different instants. PriorityAt is
// re-derived from At and the job's stored PriorityBoost. It is fenced like
// [Backend.Complete].
func (b *Backend) Retry(ctx context.Context, params backend.RetryParams) error {
	rows := make([]finalizeRow, len(params.Jobs))
	ids := make([]int64, len(params.Jobs))
	for i, f := range params.Jobs {
		atNS, err := nanos(f.At)
		if err != nil {
			return err
		}
		errJSON, err := json.Marshal(f.Err)
		if err != nil {
			return fmt.Errorf("goque/postgres: encoding attempt error: %w", err)
		}
		at := atNS
		rows[i] = finalizeRow{
			ID: f.ID, Generation: f.Generation,
			Metadata: rawOrNull(f.Metadata), AtNS: &at,
			Err: json.RawMessage(`[` + string(errJSON) + `]`),
		}
		ids[i] = f.ID
	}
	return b.runFinalize(ctx, b.d, "retry", retrySet, rows, ids)
}

// Snooze implements [backend.Backend]. It reschedules without consuming an
// attempt and without appending an error, and leaves the generation alone so an
// in-flight finalization for the same attempt is still fenced. The call itself
// is fenced like [Backend.Complete].
func (b *Backend) Snooze(ctx context.Context, params backend.SnoozeParams) error {
	rows := make([]finalizeRow, len(params.Jobs))
	ids := make([]int64, len(params.Jobs))
	for i, f := range params.Jobs {
		atNS, err := nanos(f.At)
		if err != nil {
			return err
		}
		at := atNS
		rows[i] = finalizeRow{ID: f.ID, Generation: f.Generation, Metadata: rawOrNull(f.Metadata), AtNS: &at}
		ids[i] = f.ID
	}
	return b.runFinalize(ctx, b.d, "snooze", snoozeSet, rows, ids)
}

// Kill implements [backend.Backend]. The job moves to dead and its final error
// is appended, so the history survives for inspection. It is fenced like
// [Backend.Complete].
func (b *Backend) Kill(ctx context.Context, params backend.KillParams) error {
	rows := make([]finalizeRow, len(params.Jobs))
	ids := make([]int64, len(params.Jobs))
	for i, f := range params.Jobs {
		errJSON, err := json.Marshal(f.Err)
		if err != nil {
			return fmt.Errorf("goque/postgres: encoding attempt error: %w", err)
		}
		rows[i] = finalizeRow{
			ID: f.ID, Generation: f.Generation,
			Metadata: rawOrNull(f.Metadata),
			Err:      json.RawMessage(`[` + string(errJSON) + `]`),
		}
		ids[i] = f.ID
	}
	return b.runFinalize(ctx, b.d, "kill", killSet, rows, ids, params.Now.UTC())
}

// Cancel implements [backend.Backend]. The reason is recorded as a final
// attempt error when non-empty. It is fenced like [Backend.Complete].
func (b *Backend) Cancel(ctx context.Context, params backend.CancelParams) error {
	rows := make([]finalizeRow, len(params.Jobs))
	ids := make([]int64, len(params.Jobs))
	for i, f := range params.Jobs {
		var raw json.RawMessage
		if f.Err != "" {
			errJSON, err := json.Marshal(backend.AttemptError{At: params.Now, Err: f.Err})
			if err != nil {
				return fmt.Errorf("goque/postgres: encoding cancel reason: %w", err)
			}
			raw = json.RawMessage(`[` + string(errJSON) + `]`)
		}
		rows[i] = finalizeRow{ID: f.ID, Generation: f.Generation, Metadata: rawOrNull(f.Metadata), Err: raw}
		ids[i] = f.ID
	}
	return b.runFinalize(ctx, b.d, "cancel", cancelSet, rows, ids, params.Now.UTC())
}

func (b *Backend) runFinalize(ctx context.Context, d Driver, what, set string, rows []finalizeRow, ids []int64, extra ...any) error {
	if len(rows) == 0 {
		return nil
	}
	doc, err := encodeBatch(rows)
	if err != nil {
		return fmt.Errorf("goque/postgres: encoding %s batch: %w", what, err)
	}
	args := append([]any{doc}, extra...)
	result, err := d.Query(ctx, b.finalizeSQL(set), args...)
	if err != nil {
		return fmt.Errorf("goque/postgres: %s: %w", what, err)
	}
	defer result.Close()
	won := make(map[int64]bool, len(rows))
	for result.Next() {
		var ord int64
		if err := result.Scan(&ord); err != nil {
			return fmt.Errorf("goque/postgres: %s: %w", what, err)
		}
		won[ord] = true
	}
	if err := result.Err(); err != nil {
		return fmt.Errorf("goque/postgres: %s: %w", what, err)
	}
	var stale []int64
	for i := range rows {
		if !won[int64(i+1)] {
			stale = append(stale, ids[i])
		}
	}
	if len(stale) > 0 {
		return &backend.StaleError{IDs: stale}
	}
	return nil
}
