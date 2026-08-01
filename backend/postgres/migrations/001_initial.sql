-- +goose up
CREATE TABLE {{schema}}.goque_job (
    id                BIGSERIAL PRIMARY KEY,
    kind              TEXT        NOT NULL,
    queue             TEXT        NOT NULL,
    payload           JSONB       NOT NULL,
    state             TEXT        NOT NULL,
    priority_boost_ns BIGINT      NOT NULL DEFAULT 0,
    scheduled_at_ns   BIGINT      NOT NULL,
    priority_at_ns    BIGINT      NOT NULL,
    attempt           INT         NOT NULL DEFAULT 0,
    generation        INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL,
    attempted_at      TIMESTAMPTZ,
    finalized_at      TIMESTAMPTZ,
    heartbeat_at      TIMESTAMPTZ,
    attempted_by      TEXT[]      NOT NULL DEFAULT '{}',
    cancel_requested  BOOLEAN     NOT NULL DEFAULT FALSE,
    concurrency_key   TEXT,
    throttle_key      TEXT,
    debounce_key      TEXT,
    debounce_deadline TIMESTAMPTZ,
    retry_policy      JSONB,
    metadata          JSONB       NOT NULL DEFAULT '{}',
    output            JSONB,
    version           INT         NOT NULL DEFAULT 1,
    errors            JSONB       NOT NULL DEFAULT '[]'
);

-- +goose down
DROP TABLE {{schema}}.goque_job;
