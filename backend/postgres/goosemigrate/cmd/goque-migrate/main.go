// Command goque-migrate creates or updates goque's PostgreSQL tables.
//
// Usage:
//
//	goque-migrate [-schema NAME] [-dsn URL]
//
// The connection string comes from -dsn, or from DATABASE_URL when the flag is
// not given. The schema must already exist; goque-migrate creates tables
// within it.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/swissy-dev/goque/backend/postgres/goosemigrate"
)

func main() {
	if err := run(context.Background(), os.Args, os.Stdout, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "goque-migrate:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(out)
	schema := fs.String("schema", "public", "schema holding goque's tables")
	dsn := fs.String("dsn", "", "PostgreSQL connection string (defaults to DATABASE_URL)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *dsn == "" {
		*dsn = getenv("DATABASE_URL")
	}
	if *dsn == "" {
		return errors.New("no connection string: pass -dsn or set DATABASE_URL")
	}
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := goosemigrate.Up(ctx, db, goosemigrate.WithSchema(*schema)); err != nil {
		return err
	}
	fmt.Fprintf(out, "goque schema %q is up to date\n", *schema)
	return nil
}
