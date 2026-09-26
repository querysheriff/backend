package pgmigrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed *.sql
var fsys embed.FS

// Up applies every pending postgres migration.
func Up(ctx context.Context, db *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys)
	if err != nil {
		return fmt.Errorf("load postgres migrations: %w", err)
	}

	if _, err = provider.Up(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}

	return nil
}
