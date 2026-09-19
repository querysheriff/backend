package clickhouse

import (
	"context"
	"fmt"
	"time"
)

type SlowStatement struct {
	ID      uint64
	Calls   int64
	AvgMs   float64
	TotalMs float64
	Tags    map[string]string
}

type SlowStatementsParams struct {
	ServerName string
	From       time.Time
	MinCalls   int64
	MinAvgMs   float64
	MaxRows    int32
}

const slowStatementsSQL = `
SELECT statement_id,
       toInt64(sum(calls))               AS total_calls,
       sum(total_exec_time)              AS total_ms,
       total_ms / nullIf(total_calls, 0) AS avg_ms
FROM statement_deltas
WHERE server_name = {server_name:String}
  AND collected_at >= {from:DateTime('UTC')}
GROUP BY statement_id
HAVING total_calls >= {min_calls:Int64}
   AND avg_ms >= {min_avg_ms:Float64}
ORDER BY total_ms DESC
LIMIT {max_rows:UInt32}`

func (c *Client) ListSlowStatements(
	ctx context.Context,
	params SlowStatementsParams,
) ([]SlowStatement, error) {
	rows, err := c.conn.Query(ctx, slowStatementsSQL,
		param("server_name", params.ServerName),
		timeParam("from", params.From),
		param("min_calls", params.MinCalls),
		param("min_avg_ms", params.MinAvgMs),
		countParam("max_rows", params.MaxRows),
	)
	if err != nil {
		return nil, fmt.Errorf("query slow statements: %w", err)
	}
	defer rows.Close()

	var out []SlowStatement

	for rows.Next() {
		var row SlowStatement
		if err = rows.Scan(&row.ID, &row.Calls, &row.TotalMs, &row.AvgMs); err != nil {
			return nil, fmt.Errorf("scan slow statement: %w", err)
		}

		out = append(out, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read slow statements: %w", err)
	}

	return c.attachTags(ctx, out)
}

func (c *Client) attachTags(ctx context.Context, statements []SlowStatement) ([]SlowStatement, error) {
	ids := make([]uint64, len(statements))
	for i, statement := range statements {
		ids[i] = statement.ID
	}

	tags, err := c.StatementTags(ctx, ids)
	if err != nil {
		return nil, err
	}

	for i := range statements {
		statements[i].Tags = tags[statements[i].ID]
	}

	return statements, nil
}

func (c *Client) TopStatementsByExecTime(
	ctx context.Context,
	serverName string,
	from time.Time,
	maxRows int32,
) ([]SlowStatement, error) {
	return c.ListSlowStatements(ctx, SlowStatementsParams{
		ServerName: serverName,
		From:       from,
		MaxRows:    maxRows,
	})
}

const callsBetweenSQL = `
SELECT toInt64(sum(calls))
FROM statement_deltas
WHERE server_name = {server_name:String}
  AND collected_at >= {from:DateTime('UTC')}
  AND collected_at <  {to:DateTime('UTC')}`

func (c *Client) CallsBetween(
	ctx context.Context,
	serverName string,
	from, to time.Time,
) (int64, error) {
	var calls int64

	err := c.conn.QueryRow(ctx, callsBetweenSQL,
		param("server_name", serverName),
		timeParam("from", from),
		timeParam("to", to),
	).Scan(&calls)
	if err != nil {
		return 0, fmt.Errorf("query calls total: %w", err)
	}

	return calls, nil
}

const latencyQuantileSQL = `
WITH
    dimension AS (
        SELECT id, query_kind FROM statements FINAL
    ),
    histogram AS (
        SELECT toInt16(floor(log(greatest(total_exec_time / calls, 0.001)) / log(1.01))) AS bin,
               sum(calls) AS weight
        FROM statement_deltas AS d
        INNER JOIN dimension AS s ON s.id = d.statement_id
        WHERE server_name = {server_name:String}
          AND collected_at >= {from:DateTime('UTC')}
          AND collected_at <  {to:DateTime('UTC')}
          AND calls > 0
          AND s.query_kind != {utility_kind:Int32}
        GROUP BY bin
    ),
    running AS (
        SELECT bin,
               sum(weight) OVER (ORDER BY bin) AS below,
               sum(weight) OVER ()             AS total
        FROM histogram
    )
SELECT if(count() = 0, 0, exp((min(bin) + 0.5) * log(1.01)))
FROM running
WHERE below >= {quantile:Float64} * total`

func (c *Client) LatencyQuantile(
	ctx context.Context,
	serverName string,
	from, to time.Time,
	quantile float64,
	utilityKind int32,
) (float64, error) {
	var ms float64

	err := c.conn.QueryRow(ctx, latencyQuantileSQL,
		param("server_name", serverName),
		timeParam("from", from),
		timeParam("to", to),
		param("quantile", quantile),
		param("utility_kind", utilityKind),
	).Scan(&ms)
	if err != nil {
		return 0, fmt.Errorf("query latency quantile: %w", err)
	}

	return ms, nil
}
