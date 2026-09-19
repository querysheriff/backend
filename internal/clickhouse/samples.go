package clickhouse

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
)

type Sample struct {
	ID              uint64
	CollectedAt     time.Time
	ServerName      string
	DatabaseName    string
	OccurredAt      time.Time
	StatementID     uint64
	Query           string
	DurationMs      float64
	Parameters      []string
	ExplainPlanJSON string
	Tags            map[string]string
}

// InsertSamples inserts statement samples in one ClickHouse batch.
// Example: [sample1, sample2] -> 2 rows in statement_samples.
func (c *Client) InsertSamples(ctx context.Context, samples []Sample) error {
	if len(samples) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO statement_samples")
	if err != nil {
		return fmt.Errorf("prepare sample batch: %w", err)
	}
	defer batch.Close()

	for _, sample := range samples {
		tags := sample.Tags
		if tags == nil {
			tags = map[string]string{}
		}

		occurred := sample.OccurredAt
		if occurred.IsZero() {
			occurred = sample.CollectedAt
		}

		if err = batch.Append(
			sample.ID, sample.StatementID, sample.CollectedAt.UTC(), occurred.UTC(),
			sample.ServerName, sample.DatabaseName, sample.Query, sample.DurationMs, sample.Parameters,
			sample.ExplainPlanJSON, tags,
		); err != nil {
			return fmt.Errorf("append sample: %w", err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("send samples: %w", err)
	}

	return nil
}

type ListSamplesParams struct {
	StatementID uint64
	From, To    time.Time

	SortKey    string
	SortDesc   bool
	OffsetRows int32
	RowLimit   int32
}

const listSamplesSQL = `
WITH scoped AS (
    SELECT id, occurred_at, query, duration_ms, parameters, explain_plan_json, tags,
           mapSort(tags) AS sorted_tags
    FROM statement_samples
    WHERE statement_id = {statement_id:UInt64}
      AND collected_at >= {from:DateTime('UTC')}
      AND collected_at <= {to:DateTime('UTC')}
),
explained AS (
    SELECT query, parameters, sorted_tags, occurred_at, duration_ms,
           arrayJoin([toStartOfSecond(occurred_at) - toIntervalSecond(1),
                      toStartOfSecond(occurred_at),
                      toStartOfSecond(occurred_at) + toIntervalSecond(1)]) AS second_bucket
    FROM scoped
    WHERE explain_plan_json != ''
),
superseded AS (
    SELECT DISTINCT s.id AS id
    FROM scoped AS s
    INNER JOIN explained AS e
        ON e.query = s.query
       AND e.parameters = s.parameters
       AND toString(e.sorted_tags) = toString(s.sorted_tags)
       AND e.second_bucket = toStartOfSecond(s.occurred_at)
    WHERE s.explain_plan_json = ''
      AND e.occurred_at BETWEEN s.occurred_at - toIntervalSecond(1)
                            AND s.occurred_at + toIntervalSecond(1)
      AND s.duration_ms >= e.duration_ms - 50
)
SELECT s.id, s.occurred_at, s.query, s.duration_ms, s.parameters, s.explain_plan_json, s.tags
FROM scoped AS s
WHERE s.explain_plan_json != '' OR s.id NOT IN (SELECT id FROM superseded)
ORDER BY ` + orderByPlaceholder + `, s.id DESC
LIMIT {row_limit:UInt32} OFFSET {offset_rows:UInt32}`

// ListStatementSamples returns filtered, sorted, paginated samples for one statement.
// Example: StatementID=42, sort=duration DESC, limit=50 -> 50 slowest samples.
func (c *Client) ListStatementSamples(
	ctx context.Context,
	params ListSamplesParams,
) ([]Sample, error) {
	query := strings.Replace(listSamplesSQL, orderByPlaceholder, sampleOrderBy(params), 1)

	rows, err := c.conn.Query(ctx, query,
		param("statement_id", params.StatementID),
		timeParam("from", params.From),
		timeParam("to", params.To),
		countParam("row_limit", params.RowLimit),
		countParam("offset_rows", params.OffsetRows),
	)
	if err != nil {
		return nil, fmt.Errorf("query statement samples: %w", err)
	}
	defer rows.Close()

	var out []Sample

	for rows.Next() {
		var (
			sample Sample
			id     uint64
		)

		if err = rows.Scan(
			&id, &sample.OccurredAt, &sample.Query, &sample.DurationMs,
			&sample.Parameters, &sample.ExplainPlanJSON, &sample.Tags,
		); err != nil {
			return nil, fmt.Errorf("scan statement sample: %w", err)
		}

		sample.ID = id
		out = append(out, sample)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read statement samples: %w", err)
	}

	return out, nil
}

func sampleOrderBy(params ListSamplesParams) string {
	direction := direction(params.SortDesc)

	switch params.SortKey {
	case "duration":
		return "s.duration_ms" + direction
	case "plan":
		return "(s.explain_plan_json != '')" + direction
	default:
		return "s.occurred_at" + direction + " NULLS LAST"
	}
}

type LogSample struct {
	ID             uint64
	StatementID    uint64
	Query          string
	DurationMs     float64
	HasExplainPlan bool
}

const listLogSamplesSQL = `
SELECT id, statement_id, query, duration_ms, explain_plan_json != '' AS has_explain_plan
FROM statement_samples
WHERE id IN {sample_ids:Array(UInt64)}
  AND collected_at >= {from:DateTime('UTC')}`

// ListLogStatementSamples returns samples referenced by log events.
// Example: sampleIDs=[10,20] -> matching sample summaries.
func (c *Client) ListLogStatementSamples(
	ctx context.Context,
	sampleIDs []uint64,
	from time.Time,
) ([]LogSample, error) {
	if len(sampleIDs) == 0 {
		return nil, nil
	}

	rows, err := c.conn.Query(ctx, listLogSamplesSQL,
		listParam("sample_ids", sampleIDs),
		timeParam("from", from),
	)
	if err != nil {
		return nil, fmt.Errorf("query log statement samples: %w", err)
	}
	defer rows.Close()

	var out []LogSample

	for rows.Next() {
		var (
			sample          LogSample
			id, statementID uint64
		)

		if err = rows.Scan(
			&id, &statementID, &sample.Query, &sample.DurationMs, &sample.HasExplainPlan,
		); err != nil {
			return nil, fmt.Errorf("scan log statement sample: %w", err)
		}

		sample.ID = id
		sample.StatementID = statementID
		out = append(out, sample)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read log statement samples: %w", err)
	}

	return out, nil
}

const sampleTextSQL = `
SELECT query, parameters, explain_plan_json
FROM statement_samples
WHERE id = {id:UInt64}
LIMIT 1`

// SampleText returns query, parameters, and explain plan for one sample.
// Example: sampleID=42 -> ("SELECT ...", ["1"], "{...}", nil).
func (c *Client) SampleText(ctx context.Context, sampleID uint64) (string, []string, string, error) {
	var (
		query      string
		parameters []string
		plan       string
	)

	err := c.conn.QueryRow(ctx, sampleTextSQL, param("id", sampleID)).
		Scan(&query, &parameters, &plan)
	if err != nil {
		return "", nil, "", fmt.Errorf("query sample text: %w", err)
	}

	return query, parameters, plan, nil
}

// SampleID creates a stable ID from sample fields.
// Example: same server/time/query/duration/index -> same ID.
func SampleID(serverName string, collectedAt time.Time, query string, durationMs float64, index int64) uint64 {
	key := serverName + "\x00" +
		strconv.FormatInt(collectedAt.UTC().UnixNano(), 10) + "\x00" +
		query + "\x00" +
		strconv.FormatFloat(durationMs, 'g', -1, 64) + "\x00" +
		strconv.FormatInt(index, 10)

	return xxhash.Sum64String(key) & math.MaxInt64
}

const sampleServerSQL = `SELECT server_name FROM statement_samples WHERE id = {id:UInt64} LIMIT 1`

// SampleServer returns the server name for one sample.
// Example: sampleID=42 -> "prod-1".
func (c *Client) SampleServer(ctx context.Context, sampleID uint64) (string, error) {
	var serverName string
	if err := c.conn.QueryRow(ctx, sampleServerSQL, param("id", sampleID)).Scan(&serverName); err != nil {
		return "", fmt.Errorf("query sample server: %w", err)
	}

	return serverName, nil
}
