package clickhouse

import (
	"context"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/querysheriff/backend/internal/tagfilter"
)

type ListStatementStatsParams struct {
	From, To time.Time

	ServerName   string
	DatabaseName string

	StatementIDs []uint64
	TagFilters   []tagfilter.Filter

	Kinds []int32

	SortKey    string
	SortDesc   bool
	OffsetRows int32
	RowLimit   int32
}

type StatementStatRow struct {
	ID            uint64            `ch:"id"`
	Preview       string            `ch:"preview"`
	UserName      string            `ch:"user_name"`
	Calls         int64             `ch:"calls"`
	Rows          int64             `ch:"row_count"`
	TotalExecTime float64           `ch:"total_exec_time"`
	PctOfTotal    float64           `ch:"pct_of_total"`
	PctIo         float64           `ch:"pct_io"`
	Tags          map[string]string `ch:"tags"`
}

const listStatementStatsSQL = `
WITH
    dimension AS (
        SELECT id, user_name, query_short, query_kind, tags
        FROM statements FINAL
        WHERE server_name = {server_name:String}
          AND database_name = {database_name:String}
    ),
    per_statement AS (
        SELECT statement_id,
               sum(calls)           AS calls,
               sum("rows")          AS row_count,
               sum(total_exec_time) AS total_exec_time,
               sum(total_io_time)   AS total_io_time
        FROM statement_deltas
        WHERE server_name = {server_name:String}
          AND database_name = {database_name:String}
          AND collected_at >= {from:DateTime('UTC')}
          AND collected_at <= {to:DateTime('UTC')}
        GROUP BY statement_id
    ),
    with_totals AS (
        SELECT statement_id,
               calls,
               row_count,
               total_exec_time,
               total_io_time,
               sum(total_exec_time) OVER () AS every_exec_time,
               sum(total_io_time)   OVER () AS every_io_time
        FROM per_statement
    )
SELECT d.statement_id AS id,
       s.query_short AS preview,
       s.user_name AS user_name,
       toInt64(d.calls) AS calls,
       toInt64(d.row_count) AS row_count,
       d.total_exec_time AS total_exec_time,
       if(d.every_exec_time = 0, 0, d.total_exec_time / d.every_exec_time) * 100 AS pct_of_total,
       if(d.every_io_time   = 0, 0, d.total_io_time   / d.every_io_time)   * 100 AS pct_io,
       s.tags AS tags
FROM with_totals AS d
INNER JOIN dimension AS s ON s.id = d.statement_id
WHERE has({kinds:Array(Int32)}, s.query_kind)
  AND ({every_statement:UInt8} = 1 OR d.statement_id IN {statement_ids:Array(UInt64)})
  %s
ORDER BY %s, id DESC
LIMIT {row_limit:UInt32} OFFSET {offset_rows:UInt32}`

// ListStatementStats returns filtered, sorted, paginated statement metrics.
// Example: server=prod, db=app, sort=calls DESC -> statements with calls, rows, exec time, % total, % I/O.
func (c *Client) ListStatementStats(
	ctx context.Context,
	params ListStatementStatsParams,
) ([]StatementStatRow, error) {
	tagConditions, tagArgs := tagFilterConditions(params.TagFilters)
	query := fmt.Sprintf(listStatementStatsSQL, tagConditions, orderBy(params))

	var out []StatementStatRow
	if err := c.conn.Select(ctx, &out, query, append(tagArgs,
		timeParam("from", params.From),
		timeParam("to", params.To),
		param("server_name", params.ServerName),
		param("database_name", params.DatabaseName),
		flagParam("every_statement", params.StatementIDs == nil),
		listParam("statement_ids", params.StatementIDs),
		listParam("kinds", params.Kinds),
		countParam("row_limit", params.RowLimit),
		countParam("offset_rows", params.OffsetRows),
	)...); err != nil {
		return nil, fmt.Errorf("query statement stats: %w", err)
	}

	return out, nil
}

// tagFilterConditions builds SQL conditions for statement-tag filters.
// Missing tags read as ""; filter values are always non-empty.
// Example: env=prod -> AND has({tag_values_0:Array(String)}, s.tags[{tag_key_0:String}]).
func tagFilterConditions(filters []tagfilter.Filter) (string, []any) {
	var (
		sql  strings.Builder
		args []any
	)

	for i, filter := range filters {
		key, values := fmt.Sprintf("tag_key_%d", i), fmt.Sprintf("tag_values_%d", i)
		value := fmt.Sprintf("s.tags[{%s:String}]", key)

		switch filter.Op {
		case tagfilter.OpExists:
			fmt.Fprintf(&sql, "\n  AND mapContains(s.tags, {%s:String})", key)
		case tagfilter.OpEqual:
			fmt.Fprintf(&sql, "\n  AND has({%s:Array(String)}, %s)", values, value)
		case tagfilter.OpNotEqual:
			fmt.Fprintf(&sql, "\n  AND NOT has({%s:Array(String)}, %s)", values, value)
		}

		args = append(args, param(key, filter.Key), listParam(values, filter.Values))
	}

	return sql.String(), args
}

func orderBy(params ListStatementStatsParams) string {
	direction := direction(params.SortDesc)

	return sortColumn(params.SortKey) + direction
}

func sortColumn(sortKey string) string {
	switch sortKey {
	case "avg":
		return "d.total_exec_time / nullIf(d.calls, 0)"
	case "calls":
		return "d.calls"
	case "rows_per_call":
		return "d.row_count / nullIf(d.calls, 0)"
	case "pct_io":
		return "d.total_io_time"
	default:
		return "d.total_exec_time"
	}
}

const statementIDsByTextSQL = `
SELECT groupUniqArray(id)
FROM statements
WHERE server_name = {server_name:String}
  AND database_name = {database_name:String}
  AND positionCaseInsensitiveUTF8(query_full, {text_filter:String}) > 0
SETTINGS max_threads = 2, max_block_size = 4096`

// StatementIDsByText returns statement IDs whose full query contains text.
// Example: "users" -> [12, 42, 91].
func (c *Client) StatementIDsByText(
	ctx context.Context,
	serverName, databaseName, textFilter string,
) ([]uint64, error) {
	ids := []uint64{} // non-nil: an empty match must filter out everything
	if err := c.conn.QueryRow(ctx, statementIDsByTextSQL,
		param("server_name", serverName),
		param("database_name", databaseName),
		param("text_filter", textFilter),
	).Scan(&ids); err != nil {
		return nil, fmt.Errorf("query statements matching text: %w", err)
	}

	return ids, nil
}

// StatementID creates a stable ID from statement identity fields.
// Example: same server/database/user/queryID -> same ID.
func StatementID(serverName, databaseName, userName string, queryID int64) uint64 {
	key := serverName + "\x00" + databaseName + "\x00" + userName + "\x00" + strconv.FormatInt(queryID, 10)

	return xxhash.Sum64String(key) & math.MaxInt64
}

const statementsWithTextSQL = `
SELECT groupArray(id)
FROM statements FINAL
WHERE id IN {ids:Array(UInt64)} AND query_full != ''`

// StatementsWithText reports which statement IDs have full query text.
// Example: [10,20,30] -> {10:true, 30:true}.
func (c *Client) StatementsWithText(ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	var withText []uint64
	if err := c.conn.QueryRow(ctx, statementsWithTextSQL, listParam("ids", ids)).Scan(&withText); err != nil {
		return nil, fmt.Errorf("query statements with text: %w", err)
	}

	known := make(map[uint64]bool, len(withText))
	for _, id := range withText {
		known[id] = true
	}

	return known, nil
}

type StatementDetail struct {
	Query        string            `ch:"query_full"`
	ServerName   string            `ch:"server_name"`
	DatabaseName string            `ch:"database_name"`
	Tags         map[string]string `ch:"tags"`
}

const statementDetailSQL = `
SELECT query_full, server_name, database_name, tags
FROM statements FINAL
WHERE id = {id:UInt64}`

// StatementDetail returns query text, its server/database scope, and tags.
// Example: id=42 -> {Query:"SELECT ...", ServerName:"prod", DatabaseName:"app", Tags:{"env":"prod"}}.
func (c *Client) StatementDetail(ctx context.Context, id uint64) (StatementDetail, error) {
	var detail StatementDetail
	if err := c.conn.QueryRow(ctx, statementDetailSQL, param("id", id)).ScanStruct(&detail); err != nil {
		return StatementDetail{}, fmt.Errorf("query statement detail: %w", err)
	}

	return detail, nil
}

type Statement struct {
	ID           uint64            `ch:"id"`
	ServerName   string            `ch:"server_name"`
	DatabaseName string            `ch:"database_name"`
	UserName     string            `ch:"user_name"`
	QueryID      int64             `ch:"query_id"`
	QueryFull    string            `ch:"query_full"`
	QueryShort   string            `ch:"query_short"`
	QueryKind    int32             `ch:"query_kind"`
	Tags         map[string]string `ch:"tags"`
}

// UpsertStatements inserts or replaces statements using the latest version.
// Example: existing ID=42 with new tags -> latest row becomes active.
func (c *Client) UpsertStatements(ctx context.Context, statements []Statement) error {
	version := uint64(time.Now().UnixNano())

	return insertBatch(ctx, c, "statements", statements, func(statement Statement) []any {
		tags := statement.Tags
		if tags == nil {
			tags = map[string]string{}
		}

		return []any{
			statement.ID, statement.ServerName, statement.DatabaseName, statement.UserName,
			statement.QueryID, statement.QueryFull, statement.QueryShort, enumOf(statement.QueryKind),
			tags, version,
		}
	})
}

const statementRowsSQL = `
SELECT id, server_name, database_name, user_name, query_id, query_full, query_short,
       toInt32(query_kind) AS query_kind, tags
FROM statements FINAL
WHERE id IN {ids:Array(UInt64)}`

// ReplaceStatementTags updates tags only when they changed.
// Example: {42:{"env":"prod"}} -> statement 42 gets those tags.
func (c *Client) ReplaceStatementTags(ctx context.Context, tagsByStatement map[uint64]map[string]string) error {
	if len(tagsByStatement) == 0 {
		return nil
	}

	ids := make([]uint64, 0, len(tagsByStatement))
	for id := range tagsByStatement {
		ids = append(ids, id)
	}

	var current []Statement
	if err := c.conn.Select(ctx, &current, statementRowsSQL, listParam("ids", ids)); err != nil {
		return fmt.Errorf("query statement rows: %w", err)
	}

	updated := make([]Statement, 0, len(current))

	for _, statement := range current {
		tags := tagsByStatement[statement.ID]
		if maps.Equal(statement.Tags, tags) {
			continue
		}

		statement.Tags = tags
		updated = append(updated, statement)
	}

	return c.UpsertStatements(ctx, updated)
}

type TagKey struct {
	Key        string `ch:"key"`
	ValueCount int64  `ch:"value_count"`
}

const tagKeysSQL = `
SELECT key, toInt64(uniqExact(tags[key])) AS value_count
FROM statements FINAL
ARRAY JOIN mapKeys(tags) AS key
WHERE server_name = {server_name:String}
  AND database_name = {database_name:String}
GROUP BY key
ORDER BY count() DESC, key`

// TagKeys returns the tag keys in one server/database, most used first, with their distinct value counts.
// Example: ("prod","app") -> [{Key:"env", ValueCount:3}, {Key:"team", ValueCount:5}].
func (c *Client) TagKeys(ctx context.Context, serverName, databaseName string) ([]TagKey, error) {
	var out []TagKey
	if err := c.conn.Select(ctx, &out, tagKeysSQL,
		param("server_name", serverName),
		param("database_name", databaseName),
	); err != nil {
		return nil, fmt.Errorf("query tag keys: %w", err)
	}

	return out, nil
}

type TagValue struct {
	Value          string `ch:"value"`
	StatementCount int64  `ch:"statement_count"`
}

const tagValuesSQL = `
SELECT tags[{key:String}] AS value, toInt64(count()) AS statement_count
FROM statements FINAL
WHERE server_name = {server_name:String}
  AND database_name = {database_name:String}
  AND mapContains(tags, {key:String})
GROUP BY value
ORDER BY count() DESC, value`

// TagValues returns one tag key's values in one server/database with their statement counts.
// Example: key="env" -> [{Value:"prod", StatementCount:20}, {Value:"dev", StatementCount:5}].
func (c *Client) TagValues(ctx context.Context, serverName, databaseName, key string) ([]TagValue, error) {
	var out []TagValue
	if err := c.conn.Select(ctx, &out, tagValuesSQL,
		param("server_name", serverName),
		param("database_name", databaseName),
		param("key", key),
	); err != nil {
		return nil, fmt.Errorf("query tag values: %w", err)
	}

	return out, nil
}
