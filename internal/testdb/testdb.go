package testdb

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sync"
	"testing"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"
	chmodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"

	chmigrations "github.com/querysheriff/backend/db/ch-migrations"
	pgmigrations "github.com/querysheriff/backend/db/pg-migrations"
)

const (
	user     = "querysheriff_backend"
	database = "querysheriff"
)

type chHandle struct {
	conn driver.Conn
	err  error
}

type pgHandle struct {
	pool *pgxpool.Pool
	err  error
}

//nolint:gochecknoglobals // One container per package/store: internal/clickhouse tests share 1 ClickHouse, internal/server tests share 1 ClickHouse + 1 Postgres.
var (
	chOnce sync.Once
	chOut  chHandle
	pgOnce sync.Once
	pgOut  pgHandle
)

func ClickHouse(t *testing.T) driver.Conn {
	t.Helper()
	chOnce.Do(startClickHouse)

	if chOut.err != nil {
		t.Fatalf("start clickhouse: %v", chOut.err)
	}

	return chOut.conn
}

func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pgOnce.Do(startPostgres)

	if pgOut.err != nil {
		t.Fatalf("start postgres: %v", pgOut.err)
	}

	return pgOut.pool
}

func startClickHouse() {
	ctx := context.Background()

	container, err := chmodule.Run(ctx, "clickhouse/clickhouse-server:25.12",
		chmodule.WithUsername(user), chmodule.WithPassword(user), chmodule.WithDatabase(database))
	if err != nil {
		chOut.err = fmt.Errorf("run container: %w", err)

		return
	}

	host, err := container.ConnectionHost(ctx)
	if err != nil {
		chOut.err = fmt.Errorf("connection host: %w", err)

		return
	}

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s/%s", user, user, host, database)
	if err = migrate("clickhouse", dsn, "clickhouse", chmigrations.FS); err != nil {
		chOut.err = err

		return
	}

	options, err := ch.ParseDSN(dsn)
	if err != nil {
		chOut.err = fmt.Errorf("parse dsn: %w", err)

		return
	}

	conn, err := ch.Open(options)
	if err != nil {
		chOut.err = fmt.Errorf("connect: %w", err)

		return
	}

	chOut.conn = conn
}

func startPostgres() {
	ctx := context.Background()

	container, err := pgmodule.Run(ctx, "postgres:17",
		pgmodule.WithUsername(user), pgmodule.WithPassword(user), pgmodule.WithDatabase(database),
		pgmodule.BasicWaitStrategies())
	if err != nil {
		pgOut.err = fmt.Errorf("run container: %w", err)

		return
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		pgOut.err = fmt.Errorf("connection string: %w", err)

		return
	}

	if err = migrate("pgx", dsn, "postgres", pgmigrations.FS); err != nil {
		pgOut.err = err

		return
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		pgOut.err = fmt.Errorf("connect: %w", err)

		return
	}

	pgOut.pool = pool
}

func migrate(driverName, dsn, dialect string, files embed.FS) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("open %s: %w", dialect, err)
	}
	defer func() { _ = db.Close() }()

	goose.SetBaseFS(files)
	goose.SetLogger(goose.NopLogger())

	if err = goose.SetDialect(dialect); err != nil {
		return fmt.Errorf("goose dialect %s: %w", dialect, err)
	}

	if err = goose.Up(db, "."); err != nil {
		return fmt.Errorf("migrate %s: %w", dialect, err)
	}

	return nil
}
