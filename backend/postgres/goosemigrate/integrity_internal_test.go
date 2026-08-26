package goosemigrate

import (
	"database/sql"
	"testing"
	"testing/fstest"
)

func syntheticMigrationsWithATrailingNonIndexVersion() fstest.MapFS {
	return fstest.MapFS{
		"001_initial.sql": &fstest.MapFile{
			Data: []byte("-- +goose up\nCREATE TABLE {{schema}}.goque_job (id int);\n\n-- +goose down\nDROP TABLE {{schema}}.goque_job;\n"),
		},
		"002_indexes.sql": &fstest.MapFile{
			Data: []byte("-- +goose no transaction\n\n-- +goose up\nCREATE INDEX CONCURRENTLY IF NOT EXISTS goque_job_fetch ON {{schema}}.goque_job (id);\n\n-- +goose down\nDROP INDEX CONCURRENTLY IF EXISTS {{schema}}.goque_job_fetch;\n"),
		},
		"003_reserved.sql": &fstest.MapFile{
			Data: []byte("-- +goose up\nALTER TABLE {{schema}}.goque_job ADD COLUMN zz_probe_col int;\n\n-- +goose down\nALTER TABLE {{schema}}.goque_job DROP COLUMN zz_probe_col;\n"),
		},
	}
}

func TestHighestVersionInCountsAMigrationWithNoIndexStatement(t *testing.T) {
	got, err := highestVersionIn(syntheticMigrationsWithATrailingNonIndexVersion())
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("highestVersionIn saw the highest known version as %d, want 3; a migration with no CREATE INDEX CONCURRENTLY statement must still count", got)
	}
}

func TestMigrationsFullyAppliedSeesAPendingNonIndexMigration(t *testing.T) {
	fsys := syntheticMigrationsWithATrailingNonIndexVersion()
	upToDate, err := migrationsFullyApplied(fsys, sql.NullInt64{Valid: true, Int64: 2})
	if err != nil {
		t.Fatal(err)
	}
	if upToDate {
		t.Fatal("migrationsFullyApplied reported up to date with version 2 applied and a pending non-index migration 003 on disk; Up would return nil and never call provider.Up, silently skipping 003")
	}
}

func TestMigrationsFullyAppliedTrueOnceTheNonIndexMigrationIsApplied(t *testing.T) {
	fsys := syntheticMigrationsWithATrailingNonIndexVersion()
	upToDate, err := migrationsFullyApplied(fsys, sql.NullInt64{Valid: true, Int64: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !upToDate {
		t.Fatal("migrationsFullyApplied reported pending work with every known migration, including the non-index one, already recorded applied")
	}
}

func TestMigrationsFullyAppliedFalseWhenNothingIsRecordedApplied(t *testing.T) {
	fsys := syntheticMigrationsWithATrailingNonIndexVersion()
	upToDate, err := migrationsFullyApplied(fsys, sql.NullInt64{})
	if err != nil {
		t.Fatal(err)
	}
	if upToDate {
		t.Fatal("migrationsFullyApplied reported up to date against a database with nothing recorded applied")
	}
}
