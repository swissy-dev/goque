package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/swissy-dev/goque/backend/postgres"
)

func TestRunRequiresDSN(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"goque-migrate"}, &out, func(string) string { return "" })
	if err == nil {
		t.Fatal("run must fail when no connection string is supplied")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("the error must name the environment variable it looked for, got %q", err)
	}
}

func TestRunRejectsAnInvalidSchema(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"goque-migrate", "-schema", "my-schema"}, &out, func(string) string {
		return "this is not a dsn"
	})
	if !errors.Is(err, postgres.ErrInvalidSchema) {
		t.Fatalf("run must surface ErrInvalidSchema for an invalid schema, got %v", err)
	}
}

func TestRunPrefersTheDSNFlagOverTheEnvironment(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"goque-migrate", "-dsn", "flag is not a dsn"}, &out, func(string) string {
		return "environment is not a dsn"
	})
	if err == nil {
		t.Fatal("run must attempt to open the connection string it was given")
	}
	if !strings.Contains(err.Error(), "flag is not a dsn") {
		t.Fatalf("run used DATABASE_URL instead of -dsn: %v", err)
	}
}

func TestRunReportsUnknownFlag(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"goque-migrate", "-nope"}, &out, func(string) string { return "" }); err == nil {
		t.Fatal("an unknown flag must be an error")
	}
}
