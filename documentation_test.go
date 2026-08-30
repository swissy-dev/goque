package goque

import (
	"os"
	"strings"
	"testing"
)

func flattenDoc(s string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(s, "//", " ")), " ")
}

func TestDocumentationHasNoStalePostgresClaims(t *testing.T) {
	stale := map[string][]string{
		"README.md": {
			"Postgres, SQLite, and Redis backends — plus",
			"designed but **not yet implemented**",
		},
		"backend/postgres/driver.go": {
			"pgx and database/sql paths provably interchangeable",
			"a *sql.Tx or a pgx.Tx",
		},
		"backend/postgres/postgres.go": {
			"database/sql-based driver",
		},
		"backend/postgres/pgxv5/pgxv5.go": {
			"database/sql one interchangeable",
		},
		"website/src/pages/index.mdx": {
			"Redis backends — along with cron, unique jobs, and rate limiting — are designed but",
		},
		"website/src/pages/installation.mdx": {
			"they are specified but not yet built",
		},
		"website/src/pages/basics/client.mdx": {
			"are designed and specced but not yet built",
		},
		"website/src/pages/basics/enqueueing.mdx": {
			"is designed but arrives with the database backends",
		},
		"website/src/pages/running/queues.mdx": {
			"today this shape is for one binary that both enqueues and works",
		},
		"website/src/pages/reference/backends.mdx": {
			"Durable backends are [on the roadmap](/reference/roadmap)",
		},
		"website/src/pages/reference/roadmap.mdx": {
			"**Postgres, SQLite, and Redis.** The obvious gap",
		},
		"website/src/pages/reference/guarantees.mdx": {
			"which requires a transactional backend. Planned, not built",
		},
	}

	for path, phrases := range stale {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		flat := flattenDoc(string(data))
		for _, phrase := range phrases {
			if strings.Contains(flat, flattenDoc(phrase)) {
				t.Errorf("%s still contains stale claim %q", path, phrase)
			}
		}
	}
}
