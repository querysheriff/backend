package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/querysheriff/backend/internal/alerts"
	chstats "github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/config"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/postgres"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("jobs failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	conn, err := chstats.Connect(ctx, cfg.ClickHouseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	stats := chstats.New(conn)

	queries := db.New(pool)
	notifier := alerts.NewNotifier(queries, logger)

	logger.InfoContext(ctx, "querysheriff jobs started")

	alerts.RunScheduler(ctx, queries, stats, notifier, cfg.DashboardURL, logger)
	notifier.Wait()

	logger.InfoContext(ctx, "querysheriff jobs stopped")

	return nil
}
