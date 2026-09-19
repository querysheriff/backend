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

type ListStatementStatsParams struct {
	From, To time.Time

	ServerName   string
	DatabaseName string

	StatementIDs []uint64

	Kinds []int32

	SortKey    string
	SortDesc   bool
	OffsetRows int32
	RowLimit   int32
}

type StatementStatRow struct {
	ID            uint64
	Preview       string
	UserName      string
	Calls         int64
	Rows          int64
	TotalExecTime float64
	PctOfTotal    float64
	PctIo         float64
}

const orderByPlaceholder = "{{order_by}}"

const listStatementStatsSQL = `
WITH
    dimension AS (
        SELECT id, user_name, query_short, query_kind
        FROM statements FINAL
        WHERE server_name = {server_name:String}
          AND database_name = {database_name:String}
    ),
    in_range AS (
        SELECT statement_id, calls, "rows" AS row_count, total_exec_time, total_io_time
        FROM statement_deltas
        WHERE server_name = {server_name:String}
          AND database_name = {database_name:String}
          AND collected_at >= {from:DateTime('UTC')}
          AND collected_at <= {to:DateTime('UTC')}
    ),
    per_statement AS (
        SELECT statement_id,
               sum(calls)           AS calls,
               sum(row_count)       AS row_count,
               sum(total_exec_time) AS total_exec_time,
               sum(total_io_time)   AS total_io_time
        FROM in_range
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
       s.query_short,
       s.user_name,
       toInt64(d.calls),
       toInt64(d.row_count),
       d.total_exec_time,
       if(d.every_exec_time = 0, 0, d.total_exec_time / d.every_exec_time) * 100 AS pct_of_total,
       if(d.every_io_time   = 0, 0, d.total_io_time   / d.every_io_time)   * 100 AS pct_io
FROM with_totals AS d
INNER JOIN dimension AS s ON s.id = d.statement_id
WHERE has({kinds:Array(Int32)}, s.query_kind)
  AND ({every_statement:UInt8} = 1 OR d.statement_id IN {statement_ids:Array(UInt64)})
ORDER BY ` + orderByPlaceholder + `, id DESC
LIMIT {row_limit:UInt32} OFFSET {offset_rows:UInt32}`

// ListStatementStats returns filtered, sorted, paginated statement metrics.
// Example: server=prod, db=app, sort=calls DESC -> statements with calls, rows, exec time, % total, % I/O.
func (c *Client) ListStatementStats(
	ctx context.Context,
	params ListStatementStatsParams,
) ([]StatementStatRow, error) {
	query := strings.Replace(listStatementStatsSQL, orderByPlaceholder, orderBy(params), 1)

	rows, err := c.conn.Query(ctx, query,
		timeParam("from", params.From),
		timeParam("to", params.To),
		param("server_name", params.ServerName),
		param("database_name", params.DatabaseName),
		flagParam("every_statement", params.StatementIDs == nil),
		listParam("statement_ids", params.StatementIDs),
		listParam("kinds", params.Kinds),
		countParam("row_limit", params.RowLimit),
		countParam("offset_rows", params.OffsetRows),
	)
	if err != nil {
		return nil, fmt.Errorf("query statement stats: %w", err)
	}
	defer rows.Close()

	var out []StatementStatRow

	for rows.Next() {
		var row StatementStatRow
		if err = rows.Scan(
			&row.ID, &row.Preview, &row.UserName, &row.Calls, &row.Rows,
			&row.TotalExecTime, &row.PctOfTotal, &row.PctIo,
		); err != nil {
			return nil, fmt.Errorf("scan statement stats row: %w", err)
		}

		out = append(out, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read statement stats rows: %w", err)
	}

	return out, nil
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
SELECT DISTINCT id
FROM statements
WHERE server_name = {server_name:String}
  AND database_name = {database_name:String}
  AND query_full ILIKE concat('%', {text_filter:String}, '%')
SETTINGS max_threads = 2, max_block_size = 4096`

// StatementIDsByText returns statement IDs whose full query contains text.
// Example: "users" -> [12, 42, 91].
func (c *Client) StatementIDsByText(
	ctx context.Context,
	serverName, databaseName, textFilter string,
) ([]uint64, error) {
	rows, err := c.conn.Query(ctx, statementIDsByTextSQL,
		param("server_name", serverName),
		param("database_name", databaseName),
		param("text_filter", textFilter),
	)
	if err != nil {
		return nil, fmt.Errorf("query statements matching text: %w", err)
	}
	defer rows.Close()

	ids := []uint64{}

	for rows.Next() {
		var id uint64
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan statement id: %w", err)
		}

		ids = append(ids, id)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read statement ids: %w", err)
	}

	return ids, nil
}

// StatementID creates a stable ID from statement identity fields.
// Example: same server/database/user/queryID -> same ID.
func StatementID(serverName, databaseName, userName string, queryID int64) uint64 {
	key := serverName + "\x00" + databaseName + "\x00" + userName + "\x00" + strconv.FormatInt(queryID, 10)

	return xxhash.Sum64String(key) & math.MaxInt64
}

type StatementIdentity struct {
	UserName     string
	DatabaseName string
	QueryID      int64
}

const statementsWithTextSQL = `
SELECT id
FROM statements FINAL
WHERE id IN {ids:Array(UInt64)} AND query_full != ''`

// StatementsWithText reports which statement IDs have full query text.
// Example: [10,20,30] -> {10:true, 30:true}.
func (c *Client) StatementsWithText(ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	known := map[uint64]bool{}
	if len(ids) == 0 {
		return known, nil
	}

	rows, err := c.conn.Query(ctx, statementsWithTextSQL, listParam("ids", ids))
	if err != nil {
		return nil, fmt.Errorf("query statements with text: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id uint64
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan statement id: %w", err)
		}

		known[id] = true
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read statement ids: %w", err)
	}

	return known, nil
}

const statementTextSQL = `
SELECT query_full
FROM statements FINAL
WHERE id = {id:UInt64}`

// StatementText returns the full query text for one statement.
// Example: id=42 -> "SELECT * FROM users".
func (c *Client) StatementText(ctx context.Context, id uint64) (string, error) {
	var text string

	err := c.conn.QueryRow(ctx, statementTextSQL, param("id", id)).Scan(&text)
	if err != nil {
		return "", fmt.Errorf("query statement text: %w", err)
	}

	return text, nil
}

type StatementDetail struct {
	Query        string
	ServerName   string
	DatabaseName string
}

const statementDetailSQL = `
SELECT query_full, server_name, database_name
FROM statements FINAL
WHERE id = {id:UInt64}`

// StatementDetail returns query text and its server/database scope.
// Example: id=42 -> {Query:"SELECT ...", ServerName:"prod", DatabaseName:"app"}.
func (c *Client) StatementDetail(ctx context.Context, id uint64) (StatementDetail, error) {
	var detail StatementDetail

	err := c.conn.QueryRow(ctx, statementDetailSQL, param("id", id)).
		Scan(&detail.Query, &detail.ServerName, &detail.DatabaseName)
	if err != nil {
		return StatementDetail{}, fmt.Errorf("query statement detail: %w", err)
	}

	return detail, nil
}

const statementScopeSQL = `
SELECT server_name, database_name FROM statements FINAL WHERE id = {id:UInt64}`

// StatementScope returns the server and database for one statement.
// Example: id=42 -> ("prod", "app").
func (c *Client) StatementScope(ctx context.Context, id uint64) (string, string, error) {
	var serverName, databaseName string

	err := c.conn.QueryRow(ctx, statementScopeSQL, param("id", id)).Scan(&serverName, &databaseName)
	if err != nil {
		return "", "", fmt.Errorf("query statement scope: %w", err)
	}

	return serverName, databaseName, nil
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
	Tags         map[string]string
}

// UpsertStatements inserts or replaces statements using the latest version.
// Example: existing ID=42 with new tags -> latest row becomes active.
func (c *Client) UpsertStatements(ctx context.Context, statements []Statement) error {
	if len(statements) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO statements")
	if err != nil {
		return fmt.Errorf("prepare statement batch: %w", err)
	}
	defer batch.Close()

	version := uint64(time.Now().UnixNano())

	for _, statement := range statements {
		tags := statement.Tags
		if tags == nil {
			tags = map[string]string{}
		}

		if err = batch.Append(
			statement.ID, statement.ServerName, statement.DatabaseName, statement.UserName,
			statement.QueryID, statement.QueryFull, statement.QueryShort, enumOf(statement.QueryKind),
			tags, version,
		); err != nil {
			return fmt.Errorf("append statement: %w", err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("send statements: %w", err)
	}

	return nil
}

const statementRowsSQL = `
SELECT id, server_name, database_name, user_name, query_id, query_full, query_short, query_kind, tags
FROM statements FINAL
WHERE id IN {ids:Array(UInt64)}`

func (c *Client) statementRows(ctx context.Context, ids []uint64) ([]Statement, error) {
	rows, err := c.conn.Query(ctx, statementRowsSQL, listParam("ids", ids))
	if err != nil {
		return nil, fmt.Errorf("query statement rows: %w", err)
	}
	defer rows.Close()

	var out []Statement

	for rows.Next() {
		var (
			statement Statement
			id        uint64
			queryKind uint8
		)

		if err = rows.Scan(&id, &statement.ServerName, &statement.DatabaseName, &statement.UserName,
			&statement.QueryID, &statement.QueryFull, &statement.QueryShort, &queryKind,
			&statement.Tags); err != nil {
			return nil, fmt.Errorf("scan statement row: %w", err)
		}

		statement.ID = id
		statement.QueryKind = int32(queryKind)
		out = append(out, statement)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read statement rows: %w", err)
	}

	return out, nil
}

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

	current, err := c.statementRows(ctx, ids)
	if err != nil {
		return err
	}

	updated := make([]Statement, 0, len(current))

	for _, statement := range current {
		tags := tagsByStatement[statement.ID]
		if mapsEqual(statement.Tags, tags) {
			continue
		}

		statement.Tags = tags
		updated = append(updated, statement)
	}

	return c.UpsertStatements(ctx, updated)
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}

	for key, value := range left {
		if right[key] != value {
			return false
		}
	}

	return true
}

type StatementTagSet struct {
	StatementID uint64
	Tags        map[string]string
}

const statementTagsSQL = `
SELECT id, tags
FROM statements FINAL
WHERE id IN {ids:Array(UInt64)} AND length(tags) > 0`

// StatementTags returns tags keyed by statement ID.
// Example: [10,20] -> {10:{"env":"prod"}, 20:{"team":"api"}}.
func (c *Client) StatementTags(ctx context.Context, ids []uint64) (map[uint64]map[string]string, error) {
	out := map[uint64]map[string]string{}
	if len(ids) == 0 {
		return out, nil
	}

	tags, err := c.scanTagSets(ctx, statementTagsSQL, listParam("ids", ids))
	if err != nil {
		return nil, err
	}

	for _, set := range tags {
		out[set.StatementID] = set.Tags
	}

	return out, nil
}

const scopedTagsSQL = `
SELECT id, tags
FROM statements FINAL
WHERE server_name = {server_name:String}
  AND database_name = {database_name:String}`

// TagsInScope returns statement IDs and tags for one server/database.
// Example: ("prod","app") -> [{StatementID:10, Tags:{"env":"prod"}}].
func (c *Client) TagsInScope(ctx context.Context, serverName, databaseName string) ([]StatementTagSet, error) {
	return c.scanTagSets(ctx, scopedTagsSQL,
		param("server_name", serverName),
		param("database_name", databaseName),
	)
}

func (c *Client) scanTagSets(ctx context.Context, query string, args ...any) ([]StatementTagSet, error) {
	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query statement tags: %w", err)
	}
	defer rows.Close()

	var out []StatementTagSet

	for rows.Next() {
		var (
			id   uint64
			tags map[string]string
		)

		if err = rows.Scan(&id, &tags); err != nil {
			return nil, fmt.Errorf("scan statement tags: %w", err)
		}

		out = append(out, StatementTagSet{StatementID: id, Tags: tags})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read statement tags: %w", err)
	}

	return out, nil
}
