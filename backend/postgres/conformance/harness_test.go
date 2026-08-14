package conformance_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/swissy-dev/goque/backend/postgres"
	"github.com/swissy-dev/goque/backend/postgres/goosemigrate"
	"github.com/swissy-dev/goque/backend/postgres/pgxv5"
	"github.com/swissy-dev/goque/backend/postgres/postgrestest"
)

type harness struct {
	*postgres.Backend
	d      postgres.Driver
	schema string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, postgrestest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	d := pgxv5.New(pool)
	schema := postgrestest.Schema(ctx, t, d)

	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = db.Close() })
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(schema)); err != nil {
		t.Fatal(err)
	}
	b, err := postgres.New(d, postgres.WithSchema(schema))
	if err != nil {
		t.Fatal(err)
	}
	return &harness{Backend: b, d: d, schema: schema}
}

type stored struct {
	ID            int64
	Kind          string
	State         string
	Attempt       int
	Generation    int
	ScheduledAtNS int64
	PriorityAtNS  int64
	PriorityBoost time.Duration
	AttemptedAt   sql.NullTime
	HeartbeatAt   sql.NullTime
	FinalizedAt   sql.NullTime
	AttemptedBy   string
	Payload       string
	Metadata      string
	Errors        string
}

func (h *harness) all(ctx context.Context, t *testing.T) []stored {
	t.Helper()
	rows, err := h.d.Query(ctx, `SELECT id, kind, state, attempt, generation, scheduled_at_ns,
        priority_at_ns, priority_boost_ns, attempted_at, heartbeat_at, finalized_at,
        to_jsonb(attempted_by)::text, payload::text, metadata::text, errors::text
        FROM "`+h.schema+`".goque_job ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []stored
	for rows.Next() {
		var s stored
		var boost int64
		if err := rows.Scan(&s.ID, &s.Kind, &s.State, &s.Attempt, &s.Generation, &s.ScheduledAtNS,
			&s.PriorityAtNS, &boost, &s.AttemptedAt, &s.HeartbeatAt, &s.FinalizedAt,
			&s.AttemptedBy, &s.Payload, &s.Metadata, &s.Errors); err != nil {
			t.Fatal(err)
		}
		s.PriorityBoost = time.Duration(boost)
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (h *harness) makeAvailable(ctx context.Context, t *testing.T, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		if _, err := h.d.Exec(ctx, `UPDATE "`+h.schema+`".goque_job SET state = 'available' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *harness) probe(ctx context.Context, t *testing.T, id int64) stored {
	t.Helper()
	for _, s := range h.all(ctx, t) {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("no stored row with id %d", id)
	return stored{}
}
