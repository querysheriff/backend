package clickhouse

import (
	"context"
	"fmt"
	"math"
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
	if len(deltas) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO statement_deltas")
	if err != nil {
		return fmt.Errorf("prepare statement delta batch: %w", err)
	}
	defer batch.Close()

	for _, delta := range deltas {
		if err = batch.Append(
			delta.StatementID, delta.CollectedAt.UTC(), delta.ServerName, delta.DatabaseName,
			countOf(delta.Calls), countOf(delta.Rows), delta.TotalExecTime, delta.TotalIoTime,
		); err != nil {
			return fmt.Errorf("append statement delta: %w", err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("send statement deltas: %w", err)
	}

	return nil
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

type MetricSeriesParams struct {
	RangeStart, RangeEnd time.Time
	Bucket               time.Duration

	ServerName   string
	DatabaseName string
	StatementID  uint64
}

type MetricBucket struct {
	BucketEnd     time.Time
	TotalExecTime float64
	TotalIoTime   float64
	Calls         int64
}

const metricSeriesSQL = `
WITH bucketed AS (
    SELECT toStartOfInterval(collected_at - toIntervalSecond(1),
                             INTERVAL {bucket:UInt32} SECOND,
                             {range_start:DateTime('UTC')})
               + INTERVAL {bucket:UInt32} SECOND AS bucket_end,
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
	rows, err := c.conn.Query(ctx, metricSeriesSQL,
		timeParam("range_start", params.RangeStart),
		timeParam("range_end", params.RangeEnd),
		secondsParam("bucket", params.Bucket),
		param("server_name", params.ServerName),
		param("database_name", params.DatabaseName),
		param("statement_id", params.StatementID),
	)
	if err != nil {
		return nil, fmt.Errorf("query metric series: %w", err)
	}
	defer rows.Close()

	var out []MetricBucket

	for rows.Next() {
		var bucket MetricBucket
		if err = rows.Scan(
			&bucket.BucketEnd, &bucket.TotalExecTime, &bucket.TotalIoTime, &bucket.Calls,
		); err != nil {
			return nil, fmt.Errorf("scan metric bucket: %w", err)
		}

		out = append(out, bucket)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read metric buckets: %w", err)
	}

	return out, nil
}

type LatencyBucket struct {
	BucketEnd time.Time
	Bins      []int16
	Weights   []int32
}

type LatencySeriesParams struct {
	RangeStart, RangeEnd time.Time
	Bucket               time.Duration

	ServerName   string
	DatabaseName string
	UtilityKind  int32
}

const latencySeriesSQL = `
WITH
    dimension AS (
        SELECT id, query_kind FROM statements FINAL
    ),
    binned AS (
        SELECT toStartOfInterval(collected_at - toIntervalSecond(1),
                                 INTERVAL {bucket:UInt32} SECOND,
                                 {range_start:DateTime('UTC')})
                   + INTERVAL {bucket:UInt32} SECOND AS bucket_end,
               toInt16(floor(log(greatest(total_exec_time / calls, 0.001)) / log(1.01))) AS bin,
               toInt32(sum(calls)) AS weight
        FROM statement_deltas AS d
        INNER JOIN dimension AS s ON s.id = d.statement_id
        WHERE server_name = {server_name:String}
          AND database_name = {database_name:String}
          AND collected_at >  {range_start:DateTime('UTC')}
          AND collected_at <= {range_end:DateTime('UTC')}
          AND calls > 0
          AND s.query_kind != {utility_kind:Int32}
        GROUP BY bucket_end, bin
        ORDER BY bucket_end, bin
    )
SELECT bucket_end, groupArray(bin) AS bins, groupArray(weight) AS weights
FROM binned
GROUP BY bucket_end`

// StatementLatencySeries returns latency histograms grouped by time bucket.
// Example: 12:01 -> Bins=[100,200], Weights=[90,10].
func (c *Client) StatementLatencySeries(
	ctx context.Context,
	params LatencySeriesParams,
) ([]LatencyBucket, error) {
	rows, err := c.conn.Query(ctx, latencySeriesSQL,
		timeParam("range_start", params.RangeStart),
		timeParam("range_end", params.RangeEnd),
		secondsParam("bucket", params.Bucket),
		param("utility_kind", params.UtilityKind),
		param("server_name", params.ServerName),
		param("database_name", params.DatabaseName),
	)
	if err != nil {
		return nil, fmt.Errorf("query latency series: %w", err)
	}
	defer rows.Close()

	var out []LatencyBucket

	for rows.Next() {
		var bucket LatencyBucket
		if err = rows.Scan(&bucket.BucketEnd, &bucket.Bins, &bucket.Weights); err != nil {
			return nil, fmt.Errorf("scan latency bucket: %w", err)
		}

		out = append(out, bucket)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read latency buckets: %w", err)
	}

	return out, nil
}
