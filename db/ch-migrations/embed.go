package chmigrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed *.sql
var fsys embed.FS

// Up applies every pending clickhouse migration.
func Up(ctx context.Context, db *sql.DB) error {
	provider, err := goose.NewProvider(goose.DialectClickHouse, db, fsys)
	if err != nil {
		return fmt.Errorf("load clickhouse migrations: %w", err)
	}

	if _, err = provider.Up(ctx); err != nil {
		return fmt.Errorf("migrate clickhouse: %w", err)
	}

	return nil
}
