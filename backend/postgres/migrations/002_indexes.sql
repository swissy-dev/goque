-- +goose no transaction

-- +goose up
CREATE INDEX CONCURRENTLY IF NOT EXISTS goque_job_fetch ON {{schema}}.goque_job (queue, priority_at_ns, id)
    WHERE state = 'available';

CREATE INDEX CONCURRENTLY IF NOT EXISTS goque_job_move ON {{schema}}.goque_job (scheduled_at_ns)
    WHERE state IN ('scheduled', 'retryable');

CREATE INDEX CONCURRENTLY IF NOT EXISTS goque_job_rescue ON {{schema}}.goque_job (heartbeat_at)
    WHERE state = 'running';

CREATE INDEX CONCURRENTLY IF NOT EXISTS goque_job_clean ON {{schema}}.goque_job (state, finalized_at)
    WHERE state IN ('completed', 'cancelled', 'dead');

-- +goose down
DROP INDEX CONCURRENTLY IF EXISTS {{schema}}.goque_job_clean;
DROP INDEX CONCURRENTLY IF EXISTS {{schema}}.goque_job_rescue;
DROP INDEX CONCURRENTLY IF EXISTS {{schema}}.goque_job_move;
DROP INDEX CONCURRENTLY IF EXISTS {{schema}}.goque_job_fetch;
