package main

import (
	"context"
	_ "embed"
	"errors"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
)

//go:embed seed.sql
var seedSQL string

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(context.Background(), logger); err != nil {
		logger.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	databaseURL := os.Getenv("POSTGRES_URL")
	if databaseURL == "" {
		return errors.New("POSTGRES_URL is not set")
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	if _, err = conn.Exec(ctx, seedSQL); err != nil {
		return err
	}

	logger.InfoContext(ctx, "dev data seeded")

	return nil
}
