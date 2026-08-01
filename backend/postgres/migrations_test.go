package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"strings"
	"testing"
)

func TestMigrationsExposesTheEmbeddedFiles(t *testing.T) {
	entries, err := fs.ReadDir(Migrations(), ".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 2 {
		t.Fatalf("Migrations() exposes %v, want exactly the two migration files", names)
	}
	if names[0] != "001_initial.sql" || names[1] != "002_indexes.sql" {
		t.Fatalf("Migrations() exposes %v, want [001_initial.sql 002_indexes.sql]", names)
	}
}

func TestMigrationsAreGooseFormatted(t *testing.T) {
	initial := readMigration(t, "001_initial.sql")
	if !strings.Contains(initial, "-- +goose up") {
		t.Fatal("001_initial.sql has no -- +goose up annotation; goose requires it")
	}
	if !strings.Contains(initial, "-- +goose down") {
		t.Fatal("001_initial.sql has no -- +goose down annotation")
	}
	if strings.Contains(initial, "-- +goose no transaction") {
		t.Fatal("001_initial.sql must run transactionally")
	}

	indexes := readMigration(t, "002_indexes.sql")
	if !strings.Contains(indexes, "-- +goose no transaction") {
		t.Fatal("002_indexes.sql builds indexes concurrently and must declare -- +goose no transaction")
	}
	if i := strings.Index(indexes, "-- +goose no transaction"); i > strings.Index(indexes, "-- +goose up") {
		t.Fatal("-- +goose no transaction must come before -- +goose up")
	}
	if strings.Contains(indexes, "-- goque:concurrent") {
		t.Fatal("the bespoke -- goque:concurrent marker must be gone")
	}
}

func TestMigrationsAreSchemaTemplated(t *testing.T) {
	for _, name := range []string{"001_initial.sql", "002_indexes.sql"} {
		body := readMigration(t, name)
		if !strings.Contains(body, "{{schema}}") {
			t.Fatalf("%s has no {{schema}} placeholder; every object must be schema-qualified", name)
		}
		if strings.Contains(body, "search_path") {
			t.Fatalf("%s sets search_path, which is connection-local and leaks between backends sharing a pool", name)
		}
	}
}

func TestValidateSchema(t *testing.T) {
	if err := ValidateSchema("goque"); err != nil {
		t.Fatalf("a plain identifier must be accepted, got %v", err)
	}
	for _, bad := range []string{"my-schema", "2fast", `pub"lic`, "a\nb", strings.Repeat("a", 64), "MixedCase"} {
		if err := ValidateSchema(bad); !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("%q must be rejected with ErrInvalidSchema, got %v", bad, err)
		}
	}
}

func TestMigrationChecksumsAreStable(t *testing.T) {
	want := map[string]string{
		"001_initial.sql": "6c04ac34737d0b2fe2472ab62393eea36e3aeb3d9a44889cfd5e9e3c4e09d026",
		"002_indexes.sql": "1333f48890122852bcbfea4986e81961c61386ad93c4ac567a1504da4a282aea",
	}
	for name, expect := range want {
		sum := sha256.Sum256([]byte(readMigration(t, name)))
		got := hex.EncodeToString(sum[:])
		if expect == "" {
			t.Fatalf("%s has no recorded checksum; paste %q into the want map above", name, got)
		}
		if got != expect {
			t.Fatalf("%s has changed (recorded %s, found %s).\n"+
				"A shipped migration must never be edited — every deployment that already applied it "+
				"would silently disagree with this binary about the schema. Add a new migration instead. "+
				"If the edit is genuinely safe, update the recorded checksum deliberately.", name, expect, got)
		}
	}
}

func readMigration(t *testing.T, name string) string {
	t.Helper()
	b, err := fs.ReadFile(Migrations(), name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
