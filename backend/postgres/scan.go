package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/swissy-dev/goque/backend"
)

const jobColumns = `id, kind, queue, payload, state, priority_boost_ns, scheduled_at_ns, priority_at_ns,
       attempt, generation, max_attempts, created_at, attempted_at, finalized_at, heartbeat_at,
       to_jsonb(attempted_by) AS attempted_by, cancel_requested,
       concurrency_key, throttle_key, debounce_key, debounce_deadline,
       retry_policy, metadata, output, version, errors`

func scanJobRow(rows Rows) (*backend.JobRow, error) {
	var (
		j                                            backend.JobRow
		state                                        string
		priorityBoostNS, scheduledAtNS, priorityAtNS int64
		attemptedAt, finalizedAt, heartbeatAt        sql.NullTime
		debounceDeadline                             sql.NullTime
		concurrencyKey, throttleKey, debounceKey     sql.NullString
		attemptedBy                                  []byte
		retryPolicy, output, errorsRaw               []byte
	)
	if err := rows.Scan(
		&j.ID, &j.Kind, &j.Queue, &j.Payload, &state, &priorityBoostNS, &scheduledAtNS, &priorityAtNS,
		&j.Attempt, &j.Generation, &j.MaxAttempts, &j.CreatedAt, &attemptedAt, &finalizedAt, &heartbeatAt,
		&attemptedBy, &j.CancelRequested,
		&concurrencyKey, &throttleKey, &debounceKey, &debounceDeadline,
		&retryPolicy, &j.Metadata, &output, &j.Version, &errorsRaw,
	); err != nil {
		return nil, err
	}
	j.ConcurrencyKey = concurrencyKey.String
	j.ThrottleKey = throttleKey.String
	j.DebounceKey = debounceKey.String
	if debounceDeadline.Valid {
		j.DebounceDeadline = debounceDeadline.Time.UTC()
	}
	j.State = backend.State(state)
	j.PriorityBoost = time.Duration(priorityBoostNS)
	j.ScheduledAt = fromNanos(scheduledAtNS)
	j.PriorityAt = fromNanos(priorityAtNS)
	if attemptedAt.Valid {
		j.AttemptedAt = attemptedAt.Time.UTC()
	}
	if finalizedAt.Valid {
		j.FinalizedAt = finalizedAt.Time.UTC()
	}
	if heartbeatAt.Valid {
		j.HeartbeatAt = heartbeatAt.Time.UTC()
	}
	j.CreatedAt = j.CreatedAt.UTC()
	if len(attemptedBy) > 0 {
		if err := json.Unmarshal(attemptedBy, &j.AttemptedBy); err != nil {
			return nil, fmt.Errorf("decoding attempted_by: %w", err)
		}
	}
	j.RetryPolicy = retryPolicy
	j.Output = output
	if len(errorsRaw) > 0 {
		if err := json.Unmarshal(errorsRaw, &j.Errors); err != nil {
			return nil, fmt.Errorf("decoding errors: %w", err)
		}
	}
	return &j, nil
}
