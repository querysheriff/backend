package clickhouse

import (
	"context"
	"fmt"
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
	ID            uint64              `ch:"id"`
	Preview       string              `ch:"preview"`
	UserName      string              `ch:"user_name"`
	Calls         int64               `ch:"calls"`
	Rows          int64               `ch:"row_count"`
	TotalExecTime float64             `ch:"total_exec_time"`
	PctOfTotal    float64             `ch:"pct_of_total"`
	PctIo         float64             `ch:"pct_io"`
	Tags          map[string][]string `ch:"tags"`
}

const listStatementStatsSQL = `
WITH
    dimension AS (
        SELECT id, user_name, query_short, query_kind
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
    ),
    tagged AS (` + tagsByStatementSQL + `)
SELECT d.statement_id AS id,
       s.query_short AS preview,
       s.user_name AS user_name,
       toInt64(d.calls) AS calls,
       toInt64(d.row_count) AS row_count,
       d.total_exec_time AS total_exec_time,
       if(d.every_exec_time = 0, 0, d.total_exec_time / d.every_exec_time) * 100 AS pct_of_total,
       if(d.every_io_time   = 0, 0, d.total_io_time   / d.every_io_time)   * 100 AS pct_io,
       t.tags AS tags
FROM with_totals AS d
INNER JOIN dimension AS s ON s.id = d.statement_id
LEFT JOIN tagged AS t ON t.statement_id = d.statement_id
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
// A statement carries a tag value when any of its samples in range did, and *_id tags never match.
// Example: env=prod -> AND d.statement_id IN (SELECT statement_id FROM statement_samples WHERE ... tags['env'] ...).
func tagFilterConditions(filters []tagfilter.Filter) (string, []any) {
	var (
		sql  strings.Builder
		args []any
	)

	for i, filter := range filters {
		key, values := fmt.Sprintf("tag_key_%d", i), fmt.Sprintf("tag_values_%d", i)
		samples := fmt.Sprintf("(SELECT statement_id FROM statement_samples%s\n  AND NOT endsWith({%s:String}, '_id')",
			tagScopeSQL, key)
		withKey := fmt.Sprintf("%s\n  AND mapContains(tags, {%s:String}))", samples, key)
		withValue := fmt.Sprintf("%s\n  AND has({%s:Array(String)}, tags[{%s:String}]))", samples, values, key)

		switch filter.Op {
		case tagfilter.OpExists:
			fmt.Fprintf(&sql, "\n  AND d.statement_id IN %s", withKey)
		case tagfilter.OpEqual:
			fmt.Fprintf(&sql, "\n  AND d.statement_id IN %s", withValue)
		case tagfilter.OpNotEqual:
			fmt.Fprintf(&sql, "\n  AND d.statement_id NOT IN %s", withValue)
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

// query_short is empty exactly when query_full is, and ~90x cheaper to read.
const statementsWithTextSQL = `
SELECT groupArray(id)
FROM statements FINAL
WHERE id IN {ids:Array(UInt64)} AND query_short != ''`

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
	Query        string `ch:"query_full"`
	ServerName   string `ch:"server_name"`
	DatabaseName string `ch:"database_name"`
}

const statementDetailSQL = `
SELECT query_full, server_name, database_name
FROM statements FINAL
WHERE id = {id:UInt64}`

// StatementDetail returns query text and its server/database scope.
// Example: id=42 -> {Query:"SELECT ...", ServerName:"prod", DatabaseName:"app"}.
func (c *Client) StatementDetail(ctx context.Context, id uint64) (StatementDetail, error) {
	var detail StatementDetail
	if err := c.conn.QueryRow(ctx, statementDetailSQL, param("id", id)).ScanStruct(&detail); err != nil {
		return StatementDetail{}, fmt.Errorf("query statement detail: %w", err)
	}

	return detail, nil
}

type Statement struct {
	ID           uint64
	ServerName   string
	DatabaseName string
	UserName     string
	QueryID      int64
	QueryFull    string
	QueryShort   string
	QueryKind    int32
}

// UpsertStatements inserts or replaces statements using the latest version.
func (c *Client) UpsertStatements(ctx context.Context, statements []Statement) error {
	version := uint64(time.Now().UnixNano())

	return insertBatch(ctx, c, "statements", statements, func(statement Statement) []any {
		return []any{
			statement.ID, statement.ServerName, statement.DatabaseName, statement.UserName,
			statement.QueryID, statement.QueryFull, statement.QueryShort, enumOf(statement.QueryKind),
			version,
		}
	})
}
