package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/jackc/pgx/v5/stdlib"

	chmigrations "github.com/querysheriff/backend/db/ch-migrations"
	pgmigrations "github.com/querysheriff/backend/db/pg-migrations"
	"github.com/querysheriff/backend/internal/config"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(context.Background(), logger); err != nil {
		logger.Error("migrate failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	if err = migrate(ctx, "pgx", cfg.DatabaseURL, pgmigrations.Up); err != nil {
		return err
	}

	if err = migrate(ctx, "clickhouse", cfg.ClickHouseURL, chmigrations.Up); err != nil {
		return err
	}

	logger.InfoContext(ctx, "migrations applied")

	return nil
}

func migrate(ctx context.Context, driver, url string, up func(context.Context, *sql.DB) error) error {
	db, err := sql.Open(driver, url)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return up(ctx, db)
}
