// Package postgrestest gates goque's PostgreSQL integration tests and gives
// each one a throwaway schema.
//
// It is a test-support package: importing it pulls in testing, so nothing in
// the library's own dependency graph may import it.
package postgrestest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/swissy-dev/goque/backend/postgres"
)

// EnvVar names the environment variable holding the connection string the
// integration tests use.
const EnvVar = "GOQUE_TEST_POSTGRES"

// DSN returns the connection string from GOQUE_TEST_POSTGRES, skipping the
// calling test when it is unset. That is what lets go test ./... stay green on
// a machine with no database, while CI — which sets the variable — fails if a
// PostgreSQL test is skipped.
func DSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(EnvVar)
	if dsn == "" {
		t.Skipf("%s is unset; skipping PostgreSQL integration test", EnvVar)
	}
	return dsn
}

// Schema creates a randomly named schema through d and registers a cleanup
// that drops it, returning the name. Every test gets its own, so tests sharing
// one database never collide and a failing run leaves nothing behind.
func Schema(ctx context.Context, t *testing.T, d postgres.Driver) string {
	t.Helper()
	name := fmt.Sprintf("goque_test_%d", rand.Uint64())
	if _, err := d.Exec(ctx, `CREATE SCHEMA "`+name+`"`); err != nil {
		t.Fatalf("creating test schema %s: %v", name, err)
	}
	t.Cleanup(func() {
		if _, err := d.Exec(context.WithoutCancel(ctx), `DROP SCHEMA "`+name+`" CASCADE`); err != nil {
			t.Errorf("dropping test schema %s: %v", name, err)
		}
	})
	return name
}
