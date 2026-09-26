package clickhouse

import (
	"context"
	"fmt"
	"time"
)

type StatementDelta struct {
	StatementID   uint64
	CollectedAt   time.Time
	ServerName    string
	DatabaseName  string
	Calls         int64
	Rows          int64
	TotalExecTime float64
	TotalIoTime   float64
}

// InsertStatementDeltas inserts statement metric deltas in one ClickHouse batch.
// Example: [{StatementID:42, Calls:10, Rows:100}] -> one row in statement_deltas.
func (c *Client) InsertStatementDeltas(ctx context.Context, deltas []StatementDelta) error {
	return insertBatch(ctx, c, "statement_deltas", deltas, func(delta StatementDelta) []any {
		return []any{
			delta.StatementID, delta.CollectedAt.UTC(), delta.ServerName, delta.DatabaseName,
			countOf(delta.Calls), countOf(delta.Rows), delta.TotalExecTime, delta.TotalIoTime,
		}
	})
}

type MetricSeriesParams struct {
	RangeStart, RangeEnd time.Time
	Bucket               time.Duration

	ServerName   string
	DatabaseName string
	StatementID  uint64
}

type MetricBucket struct {
	BucketEnd     time.Time `ch:"bucket_end"`
	TotalExecTime float64   `ch:"total_exec_time"`
	TotalIoTime   float64   `ch:"total_io_time"`
	Calls         int64     `ch:"calls"`
}

const metricSeriesSQL = `
WITH bucketed AS (
    SELECT ` + bucketEndSQL + ` AS bucket_end,
           total_exec_time,
           total_io_time,
           calls,
           ({statement_id:UInt64} = 0 OR statement_id = {statement_id:UInt64}) AS matched
    FROM statement_deltas
    WHERE server_name = {server_name:String}
      AND database_name = {database_name:String}
      AND collected_at >  {range_start:DateTime('UTC')}
      AND collected_at <= {range_end:DateTime('UTC')}
)
SELECT bucket_end,
       sum(if(matched, total_exec_time, 0)) AS total_exec_time,
       sum(if(matched, total_io_time, 0))   AS total_io_time,
       toInt64(sum(if(matched, calls, 0)))  AS calls
FROM bucketed
GROUP BY bucket_end
ORDER BY bucket_end`

// StatementMetricSeries returns execution time, I/O time, and calls grouped by time bucket.
// Example: 1m bucket -> {12:01, Exec:120ms, IO:30ms, Calls:50}.
func (c *Client) StatementMetricSeries(
	ctx context.Context,
	params MetricSeriesParams,
) ([]MetricBucket, error) {
	var out []MetricBucket
	if err := c.conn.Select(ctx, &out, metricSeriesSQL,
		timeParam("range_start", params.RangeStart),
		timeParam("range_end", params.RangeEnd),
		secondsParam("bucket", params.Bucket),
		param("server_name", params.ServerName),
		param("database_name", params.DatabaseName),
		param("statement_id", params.StatementID),
	); err != nil {
		return nil, fmt.Errorf("query metric series: %w", err)
	}

	return out, nil
}

type LatencyBucket struct {
	BucketEnd time.Time `ch:"bucket_end"`
	P90       float64   `ch:"p90"`
	P95       float64   `ch:"p95"`
	P99       float64   `ch:"p99"`
}

type LatencySeriesParams struct {
	RangeStart, RangeEnd time.Time
	Bucket               time.Duration

	ServerName   string
	DatabaseName string
	UtilityKind  int32
}

// latencyBinSQL groups average statement latency into ~1%-wide logarithmic buckets.
// Example: 100 ms and 100.5 ms land in the same bucket; the bucket midpoint converts back to an approximate latency.
const latencyBinSQL = "toInt16(floor(log(greatest(total_exec_time / calls, 0.001)) / log(1.01)))"

const latencySeriesSQL = `
WITH
    dimension AS (
        SELECT id, query_kind
        FROM statements FINAL
        WHERE server_name = {server_name:String}
          AND database_name = {database_name:String}
    ),
    percentiles AS (
        SELECT ` + bucketEndSQL + ` AS bucket_end,
               quantilesExactWeighted(0.90, 0.95, 0.99)(` + latencyBinSQL + `, calls) AS bins
        FROM statement_deltas AS d
        INNER JOIN dimension AS s ON s.id = d.statement_id
        WHERE server_name = {server_name:String}
          AND database_name = {database_name:String}
          AND collected_at >  {range_start:DateTime('UTC')}
          AND collected_at <= {range_end:DateTime('UTC')}
          AND calls > 0
          AND s.query_kind != {utility_kind:Int32}
        GROUP BY bucket_end
    )
SELECT bucket_end,
       exp((bins[1] + 0.5) * log(1.01)) AS p90,
       exp((bins[2] + 0.5) * log(1.01)) AS p95,
       exp((bins[3] + 0.5) * log(1.01)) AS p99
FROM percentiles
ORDER BY bucket_end`

// StatementLatencySeries returns call-weighted P90/P95/P99 latency per time bucket.
// Example: 12:01 -> P90=20ms, P95=25ms, P99=50ms.
func (c *Client) StatementLatencySeries(
	ctx context.Context,
	params LatencySeriesParams,
) ([]LatencyBucket, error) {
	var out []LatencyBucket
	if err := c.conn.Select(ctx, &out, latencySeriesSQL,
		timeParam("range_start", params.RangeStart),
		timeParam("range_end", params.RangeEnd),
		secondsParam("bucket", params.Bucket),
		param("utility_kind", params.UtilityKind),
		param("server_name", params.ServerName),
		param("database_name", params.DatabaseName),
	); err != nil {
		return nil, fmt.Errorf("query latency series: %w", err)
	}

	return out, nil
}

type TopStatement struct {
	ID      uint64            `ch:"id"`
	Calls   int64             `ch:"calls"`
	AvgMs   float64           `ch:"avg_ms"`
	TotalMs float64           `ch:"total_ms"`
	Tags    map[string]string `ch:"tags"`
}

type TopStatementsParams struct {
	ServerName string
	From       time.Time
	MinCalls   int64
	MinAvgMs   float64
	MaxRows    int32
}

const topStatementsSQL = `
SELECT d.statement_id AS id, d.calls AS calls, d.total_ms AS total_ms, d.avg_ms AS avg_ms, s.tags AS tags
FROM (
    SELECT statement_id,
           toInt64(sum(calls))                    AS calls,
           sum(total_exec_time)                   AS total_ms,
           ifNull(total_ms / nullIf(calls, 0), 0) AS avg_ms
    FROM statement_deltas
    WHERE server_name = {server_name:String}
      AND collected_at >= {from:DateTime('UTC')}
    GROUP BY statement_id
    HAVING calls >= {min_calls:Int64}
       AND avg_ms >= {min_avg_ms:Float64}
    ORDER BY total_ms DESC
    LIMIT {max_rows:UInt32}
) AS d
LEFT JOIN (
    SELECT id, tags FROM statements FINAL WHERE server_name = {server_name:String}
) AS s ON s.id = d.statement_id
ORDER BY d.total_ms DESC`

// TopStatements returns the statements with the most execution time that pass the call and average thresholds.
// Example: MinCalls=10, MinAvgMs=1000 -> statements called 10+ times averaging 1s+, busiest first.
func (c *Client) TopStatements(ctx context.Context, params TopStatementsParams) ([]TopStatement, error) {
	var out []TopStatement
	if err := c.conn.Select(ctx, &out, topStatementsSQL,
		param("server_name", params.ServerName),
		timeParam("from", params.From),
		param("min_calls", params.MinCalls),
		param("min_avg_ms", params.MinAvgMs),
		countParam("max_rows", params.MaxRows),
	); err != nil {
		return nil, fmt.Errorf("query top statements: %w", err)
	}

	return out, nil
}

const callsBetweenSQL = `
SELECT toInt64(sum(calls))
FROM statement_deltas
WHERE server_name = {server_name:String}
  AND collected_at >= {from:DateTime('UTC')}
  AND collected_at <  {to:DateTime('UTC')}`

func (c *Client) CallsBetween(ctx context.Context, serverName string, from, to time.Time) (int64, error) {
	var calls int64
	if err := c.conn.QueryRow(ctx, callsBetweenSQL,
		param("server_name", serverName),
		timeParam("from", from),
		timeParam("to", to),
	).Scan(&calls); err != nil {
		return 0, fmt.Errorf("query calls total: %w", err)
	}

	return calls, nil
}

const latencyQuantileSQL = `
WITH
    dimension AS (
        SELECT id, query_kind
        FROM statements FINAL
        WHERE server_name = {server_name:String}
    ),
    bins AS (
        SELECT count() AS samples,
               quantileExactWeighted({quantile:Float64})(` + latencyBinSQL + `, calls) AS bin
        FROM statement_deltas AS d
        INNER JOIN dimension AS s ON s.id = d.statement_id
        WHERE server_name = {server_name:String}
          AND collected_at >= {from:DateTime('UTC')}
          AND collected_at <  {to:DateTime('UTC')}
          AND calls > 0
          AND s.query_kind != {utility_kind:Int32}
    )
SELECT if(samples = 0, 0, exp((bin + 0.5) * log(1.01)))
FROM bins`

func (c *Client) LatencyQuantile(
	ctx context.Context,
	serverName string,
	from, to time.Time,
	quantile float64,
	utilityKind int32,
) (float64, error) {
	var ms float64
	if err := c.conn.QueryRow(ctx, latencyQuantileSQL,
		param("server_name", serverName),
		timeParam("from", from),
		timeParam("to", to),
		param("quantile", quantile),
		param("utility_kind", utilityKind),
	).Scan(&ms); err != nil {
		return 0, fmt.Errorf("query latency quantile: %w", err)
	}

	return ms, nil
}
