package clickhouse

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
)

type Sample struct {
	ID              uint64            `ch:"id"`
	CollectedAt     time.Time         `ch:"collected_at"`
	ServerName      string            `ch:"server_name"`
	DatabaseName    string            `ch:"database_name"`
	OccurredAt      time.Time         `ch:"occurred_at"`
	StatementID     uint64            `ch:"statement_id"`
	Query           string            `ch:"query"`
	DurationMs      float64           `ch:"duration_ms"`
	Parameters      []string          `ch:"parameters"`
	ExplainPlanJSON string            `ch:"explain_plan_json"`
	Tags            map[string]string `ch:"tags"`
	HasPlan         bool              `ch:"has_plan"`
}

// InsertSamples inserts statement samples in one ClickHouse batch.
// Example: [sample1, sample2] -> 2 rows in statement_samples.
func (c *Client) InsertSamples(ctx context.Context, samples []Sample) error {
	return insertBatch(ctx, c, "statement_samples", samples, func(sample Sample) []any {
		tags := sample.Tags
		if tags == nil {
			tags = map[string]string{}
		}

		occurred := sample.OccurredAt
		if occurred.IsZero() {
			occurred = sample.CollectedAt
		}

		return []any{
			sample.ID, sample.StatementID, sample.CollectedAt.UTC(), occurred.UTC(),
			sample.ServerName, sample.DatabaseName, sample.Query, sample.DurationMs, sample.Parameters,
			sample.ExplainPlanJSON, tags,
		}
	})
}

type ListSamplesParams struct {
	ServerName   string
	DatabaseName string
	StatementID  uint64
	From, To     time.Time

	SortKey    string
	SortDesc   bool
	OffsetRows int32
	RowLimit   int32
}

const listSamplesSQL = `
WITH scoped AS (
    SELECT id, occurred_at, query, duration_ms, parameters, tags,
           explain_plan_json != '' AS has_plan,
           mapSort(tags) AS sorted_tags
    FROM statement_samples
    WHERE statement_id = {statement_id:UInt64}
      AND server_name = {server_name:String}
      AND database_name = {database_name:String}
      AND collected_at >= {from:DateTime('UTC')}
      AND collected_at <= {to:DateTime('UTC')}
),
explained AS (
    SELECT query, parameters, sorted_tags, occurred_at, duration_ms,
           arrayJoin([toStartOfSecond(occurred_at) - toIntervalSecond(1),
                      toStartOfSecond(occurred_at),
                      toStartOfSecond(occurred_at) + toIntervalSecond(1)]) AS second_bucket
    FROM scoped
    WHERE has_plan
),
superseded AS (
    SELECT DISTINCT s.id AS id
    FROM scoped AS s
    INNER JOIN explained AS e
        ON e.query = s.query
       AND e.parameters = s.parameters
       AND toString(e.sorted_tags) = toString(s.sorted_tags)
       AND e.second_bucket = toStartOfSecond(s.occurred_at)
    WHERE NOT s.has_plan
      AND e.occurred_at BETWEEN s.occurred_at - toIntervalSecond(1)
                            AND s.occurred_at + toIntervalSecond(1)
      AND s.duration_ms >= e.duration_ms - 50
)
SELECT s.id AS id, s.occurred_at AS occurred_at, s.query AS query, s.duration_ms AS duration_ms,
       s.parameters AS parameters, s.tags AS tags, s.has_plan AS has_plan
FROM scoped AS s
WHERE s.has_plan OR s.id NOT IN (SELECT id FROM superseded)
ORDER BY %s, s.id DESC
LIMIT {row_limit:UInt32} OFFSET {offset_rows:UInt32}`

// ListStatementSamples returns filtered, sorted, paginated samples for one statement.
// Example: StatementID=42, sort=duration DESC, limit=50 -> 50 slowest samples.
func (c *Client) ListStatementSamples(ctx context.Context, params ListSamplesParams) ([]Sample, error) {
	var out []Sample
	if err := c.conn.Select(ctx, &out, fmt.Sprintf(listSamplesSQL, sampleOrderBy(params)),
		param("statement_id", params.StatementID),
		param("server_name", params.ServerName),
		param("database_name", params.DatabaseName),
		timeParam("from", params.From),
		timeParam("to", params.To),
		countParam("row_limit", params.RowLimit),
		countParam("offset_rows", params.OffsetRows),
	); err != nil {
		return nil, fmt.Errorf("query statement samples: %w", err)
	}

	return out, nil
}

func sampleOrderBy(params ListSamplesParams) string {
	direction := direction(params.SortDesc)

	switch params.SortKey {
	case "duration":
		return "s.duration_ms" + direction
	default:
		return "s.occurred_at" + direction
	}
}

const samplesByIDSQL = `
SELECT id, statement_id, query, duration_ms, explain_plan_json != '' AS has_plan
FROM statement_samples
WHERE id IN {sample_ids:Array(UInt64)}
  AND collected_at >= {from:DateTime('UTC')}`

// SamplesByID returns the samples log events point at, collected since from.
// Example: sampleIDs=[10,20] -> those two samples without their plans.
func (c *Client) SamplesByID(ctx context.Context, sampleIDs []uint64, from time.Time) ([]Sample, error) {
	if len(sampleIDs) == 0 {
		return nil, nil
	}

	var out []Sample
	if err := c.conn.Select(ctx, &out, samplesByIDSQL,
		listParam("sample_ids", sampleIDs),
		timeParam("from", from),
	); err != nil {
		return nil, fmt.Errorf("query samples by id: %w", err)
	}

	return out, nil
}

const sampleSQL = `
SELECT server_name, query, parameters, explain_plan_json
FROM statement_samples
WHERE id = {id:UInt64}
LIMIT 1`

// Sample returns one sample's server, query, parameters, and explain plan.
// Example: sampleID=42 -> {ServerName:"prod", Query:"SELECT ...", Parameters:["1"], ExplainPlanJSON:"{...}"}.
func (c *Client) Sample(ctx context.Context, sampleID uint64) (Sample, error) {
	var sample Sample
	if err := c.conn.QueryRow(ctx, sampleSQL, param("id", sampleID)).ScanStruct(&sample); err != nil {
		return Sample{}, fmt.Errorf("query sample: %w", err)
	}

	return sample, nil
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

type TagScope struct {
	ServerName   string
	DatabaseName string
	From, To     time.Time
}

func (s TagScope) args(extra ...any) []any {
	return append([]any{
		param("server_name", s.ServerName),
		param("database_name", s.DatabaseName),
		timeParam("from", s.From),
		timeParam("to", s.To),
	}, extra...)
}

const tagScopeSQL = `
WHERE server_name = {server_name:String}
  AND database_name = {database_name:String}
  AND collected_at >= {from:DateTime('UTC')}
  AND collected_at <= {to:DateTime('UTC')}`

// baseTagsSQL drops *_id tags, which stay on samples only.
const baseTagsSQL = "mapFilter((key, value) -> NOT endsWith(key, '_id'), tags)"

// tagsByStatementSQL merges the tags of each statement's samples in scope.
const tagsByStatementSQL = `
SELECT statement_id, groupUniqArrayMap(` + baseTagsSQL + `) AS tags
FROM statement_samples` + tagScopeSQL + `
GROUP BY statement_id`

const statementTagsSQL = `
SELECT groupUniqArrayMap(` + baseTagsSQL + `)
FROM statement_samples` + tagScopeSQL + `
  AND statement_id = {statement_id:UInt64}`

// StatementTags returns the tags of one statement's samples in scope.
// Example: statement 42 sampled with op=A and op=B -> {op:[A, B]}.
func (c *Client) StatementTags(ctx context.Context, scope TagScope, statementID uint64) (map[string][]string, error) {
	var tags map[string][]string
	if err := c.conn.QueryRow(ctx, statementTagsSQL,
		scope.args(param("statement_id", statementID))...,
	).Scan(&tags); err != nil {
		return nil, fmt.Errorf("query statement tags: %w", err)
	}

	return tags, nil
}

type TagKey struct {
	Key        string `ch:"key"`
	ValueCount int64  `ch:"value_count"`
}

const tagKeysSQL = `
SELECT key, toInt64(uniqExact(value)) AS value_count
FROM statement_samples
ARRAY JOIN mapKeys(tags) AS key, mapValues(tags) AS value` + tagScopeSQL + `
  AND statement_id != 0
  AND NOT endsWith(key, '_id')
GROUP BY key
ORDER BY uniqExact(statement_id) DESC, key`

// TagKeys returns the tag keys in scope, on most statements first, with their distinct value counts.
// Example: -> [{Key:"env", ValueCount:3}, {Key:"team", ValueCount:5}].
func (c *Client) TagKeys(ctx context.Context, scope TagScope) ([]TagKey, error) {
	var out []TagKey
	if err := c.conn.Select(ctx, &out, tagKeysSQL, scope.args()...); err != nil {
		return nil, fmt.Errorf("query tag keys: %w", err)
	}

	return out, nil
}

type TagValue struct {
	Value          string `ch:"value"`
	StatementCount int64  `ch:"statement_count"`
}

const tagValuesSQL = `
SELECT tags[{key:String}] AS value, toInt64(uniqExact(statement_id)) AS statement_count
FROM statement_samples` + tagScopeSQL + `
  AND statement_id != 0
  AND NOT endsWith({key:String}, '_id')
  AND mapContains(tags, {key:String})
GROUP BY value
ORDER BY statement_count DESC, value`

// TagValues returns one tag key's values in scope with their statement counts.
// Example: key="env" -> [{Value:"prod", StatementCount:20}, {Value:"dev", StatementCount:5}].
func (c *Client) TagValues(ctx context.Context, scope TagScope, key string) ([]TagValue, error) {
	var out []TagValue
	if err := c.conn.Select(ctx, &out, tagValuesSQL, scope.args(param("key", key))...); err != nil {
		return nil, fmt.Errorf("query tag values: %w", err)
	}

	return out, nil
}
