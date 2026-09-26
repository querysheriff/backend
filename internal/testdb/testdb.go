package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for the migrations
	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"

	chmigrations "github.com/querysheriff/backend/db/ch-migrations"
	pgmigrations "github.com/querysheriff/backend/db/pg-migrations"
	"github.com/querysheriff/backend/internal/postgres"
)

const (
	user     = "querysheriff_backend"
	database = "querysheriff"
)

//nolint:gochecknoglobals // One container per package/store: internal/clickhouse tests share 1 ClickHouse, internal/server tests share 1 ClickHouse + 1 Postgres.
var (
	clickHouse = sync.OnceValues(startClickHouse)
	pg         = sync.OnceValues(startPostgres)
)

func ClickHouse(t *testing.T) driver.Conn {
	t.Helper()

	conn, err := clickHouse()
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}

	return conn
}

func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pg()
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	return pool
}

func startClickHouse() (driver.Conn, error) {
	ctx := context.Background()

	container, err := chmodule.Run(ctx, "clickhouse/clickhouse-server:25.12",
		chmodule.WithUsername(user), chmodule.WithPassword(user), chmodule.WithDatabase(database))
	if err != nil {
		return nil, fmt.Errorf("run container: %w", err)
	}

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		return nil, fmt.Errorf("connection host: %w", err)
	}

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s/%s", user, user, host, database)
	if err = migrate(ctx, "clickhouse", dsn, chmigrations.Up); err != nil {
		return nil, err
	}

	options, err := ch.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	return ch.Open(options)
}

func startPostgres() (*pgxpool.Pool, error) {
	ctx := context.Background()

	container, err := pgmodule.Run(ctx, "postgres:17",
		pgmodule.WithUsername(user), pgmodule.WithPassword(user), pgmodule.WithDatabase(database),
		pgmodule.BasicWaitStrategies())
	if err != nil {
		return nil, fmt.Errorf("run container: %w", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("connection string: %w", err)
	}

	if err = migrate(ctx, "pgx", dsn, pgmigrations.Up); err != nil {
		return nil, err
	}

	return postgres.Connect(ctx, dsn)
}

func migrate(ctx context.Context, driverName, dsn string, up func(context.Context, *sql.DB) error) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("open %s: %w", driverName, err)
	}
	defer func() { _ = db.Close() }()

	return up(ctx, db)
}
