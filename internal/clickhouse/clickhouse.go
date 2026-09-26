package clickhouse

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	connectTimeout   = 10 * time.Second
	dateTimeLayout   = "2006-01-02 15:04:05"
	dateTime64Layout = "2006-01-02 15:04:05.000"
	ascending        = " ASC"
	descending       = " DESC"
)

func direction(desc bool) string {
	if desc {
		return descending
	}

	return ascending
}

// Connect opens and verifies a ClickHouse connection.
// Example: Connect(ctx, "clickhouse://localhost:9000/default") -> conn.
func Connect(ctx context.Context, dsn string) (driver.Conn, error) {
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if err = conn.Ping(pingCtx); err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return conn, nil
}

// bucketEndSQL assigns collected_at to a right-closed chart bucket.
// Example: start=12:00, bucket=60s -> 12:00:00 belongs to 12:00, while 12:00:01..12:01:00 belong to 12:01.
const bucketEndSQL = "toStartOfInterval(collected_at - toIntervalSecond(1), INTERVAL {bucket:UInt32} SECOND, " +
	"{range_start:DateTime('UTC')}) + INTERVAL {bucket:UInt32} SECOND"

type Client struct {
	conn driver.Conn
}

func New(conn driver.Conn) *Client {
	return &Client{conn: conn}
}

func param(name string, value any) any {
	return clickhouse.Named(name, value)
}

func timeParam(name string, value time.Time) any {
	return clickhouse.Named(name, value.UTC().Format(dateTimeLayout))
}

func flagParam(name string, on bool) any {
	if on {
		return clickhouse.Named(name, uint8(1))
	}

	return clickhouse.Named(name, uint8(0))
}

func listParam[T any](name string, values []T) any {
	if values == nil {
		return clickhouse.Named(name, []T{})
	}

	return clickhouse.Named(name, values)
}

func countParam(name string, value int32) any {
	return clickhouse.Named(name, countOf(int64(value)))
}

func countOf(value int64) uint32 {
	switch {
	case value < 0:
		return 0
	case value > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(value)
	}
}

func secondsParam(name string, value time.Duration) any {
	seconds := value / time.Second

	switch {
	case seconds < 1:
		return clickhouse.Named(name, uint32(1))
	case seconds > math.MaxUint32:
		return clickhouse.Named(name, uint32(math.MaxUint32))
	default:
		return clickhouse.Named(name, uint32(seconds))
	}
}

func enumOf(value int32) uint8 {
	if value < 0 || value > math.MaxUint8 {
		return 0
	}

	return uint8(value)
}

func insertBatch[T any](ctx context.Context, c *Client, table string, rows []T, values func(T) []any) error {
	if len(rows) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+table)
	if err != nil {
		return fmt.Errorf("prepare %s batch: %w", table, err)
	}
	defer batch.Close()

	for _, row := range rows {
		if err = batch.Append(values(row)...); err != nil {
			return fmt.Errorf("append %s row: %w", table, err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("send %s batch: %w", table, err)
	}

	return nil
}
