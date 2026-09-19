package main

import (
	"database/sql"
	"embed"
	"errors"
	"log/slog"
	"os"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	chmigrations "github.com/querysheriff/backend/db/ch-migrations"
	pgmigrations "github.com/querysheriff/backend/db/pg-migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("migrate failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	databaseURL := os.Getenv("POSTGRES_URL")
	if databaseURL == "" {
		return errors.New("POSTGRES_URL is not set")
	}

	clickhouseURL := os.Getenv("CLICKHOUSE_URL")
	if clickhouseURL == "" {
		return errors.New("CLICKHOUSE_URL is not set")
	}

	if err := migrate(logger, "pgx", databaseURL, "postgres", pgmigrations.FS); err != nil {
		return err
	}

	return migrate(logger, "clickhouse", clickhouseURL, "clickhouse", chmigrations.FS)
}

func migrate(logger *slog.Logger, driver, url, dialect string, files embed.FS) error {
	sqlDB, err := sql.Open(driver, url)
	if err != nil {
		return err
	}
	defer func() { _ = sqlDB.Close() }()

	goose.SetBaseFS(files)

	if err = goose.SetDialect(dialect); err != nil {
		return err
	}

	if err = goose.Up(sqlDB, "."); err != nil {
		return err
	}

	logger.Info("migrations applied", "dialect", dialect)

	return nil
}
