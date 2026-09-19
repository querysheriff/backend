package clickhouse

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
)

type LogEvent struct {
	ID                uint64
	ServerName        string
	CollectedAt       time.Time
	OccurredAt        time.Time
	LogLevel          int32
	Classification    int32
	Message           string
	Pid               uint32
	UserName          string
	DatabaseName      string
	ApplicationName   string
	Detail            string
	Hint              string
	Context           string
	Statement         string
	BackendType       string
	StateCode         string
	StatementSampleID uint64
}

// LogEventID creates a stable ID from log event fields.
// Example: same server/time/pid/message/index -> same ID.
func LogEventID(serverName string, collectedAt time.Time, pid uint32, message string, index int64) uint64 {
	key := serverName + "\x00" +
		strconv.FormatInt(collectedAt.UTC().UnixNano(), 10) + "\x00" +
		strconv.FormatUint(uint64(pid), 10) + "\x00" +
		message + "\x00" +
		strconv.FormatInt(index, 10)

	return xxhash.Sum64String(key) & math.MaxInt64
}

// InsertLogEvents inserts log events in one ClickHouse batch.
// Example: [event1, event2] -> 2 rows in log_events.
func (c *Client) InsertLogEvents(ctx context.Context, events []LogEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO log_events")
	if err != nil {
		return fmt.Errorf("prepare log event batch: %w", err)
	}
	defer batch.Close()

	for _, event := range events {
		if err = batch.Append(
			event.ID, event.ServerName, event.CollectedAt.UTC(), event.OccurredAt.UTC(),
			enumOf(event.LogLevel), enumOf(event.Classification), event.Message, event.Pid,
			event.UserName, event.DatabaseName, event.ApplicationName,
			event.Detail, event.Hint, event.Context, event.Statement,
			event.BackendType, event.StateCode, event.StatementSampleID,
		); err != nil {
			return fmt.Errorf("append log event: %w", err)
		}
	}

	if err = batch.Send(); err != nil {
		return fmt.Errorf("send log events: %w", err)
	}

	return nil
}

type LogFilter struct {
	ServerName       string
	From, To         time.Time
	Levels           []int32
	Classifications  []int32
	Databases        []string
	Usernames        []string
	ApplicationNames []string
	BackendTypes     []string
	Search           string
}

func (f LogFilter) conditions(lowerExclusive bool) *conditions {
	lower := "occurred_at >= {from:DateTime('UTC')}"
	if lowerExclusive {
		lower = "occurred_at > {from:DateTime('UTC')}"
	}

	c := &conditions{}
	c.add("server_name = {server_name:String}", param("server_name", f.ServerName))
	c.add(lower, timeParam("from", f.From))
	c.add("occurred_at <= {to:DateTime('UTC')}", timeParam("to", f.To))
	c.add("collected_at >= {from:DateTime('UTC')}")

	if len(f.Classifications) > 0 {
		c.add("classification IN {classifications:Array(Int32)}",
			listParam("classifications", f.Classifications))
	}

	if len(f.Databases) > 0 {
		c.add("database_name IN {databases:Array(String)}", listParam("databases", f.Databases))
	}

	if len(f.Usernames) > 0 {
		c.add("user_name IN {usernames:Array(String)}", listParam("usernames", f.Usernames))
	}

	if len(f.ApplicationNames) > 0 {
		c.add("application_name IN {application_names:Array(String)}",
			listParam("application_names", f.ApplicationNames))
	}

	if len(f.BackendTypes) > 0 {
		c.add("backend_type IN {backend_types:Array(String)}",
			listParam("backend_types", f.BackendTypes))
	}

	if f.Search != "" {
		c.add(`(message ILIKE {search_like:String}
       OR detail ILIKE {search_like:String}
       OR statement ILIKE {search_like:String}
       OR toString(pid) = {search:String})`,
			param("search_like", "%"+f.Search+"%"), param("search", f.Search))
	}

	return c
}

type LogSortOrder struct {
	Key        string
	Desc       bool
	SeverityOf []int32
	CategoryOf []int32
}

func (o LogSortOrder) expression() string {
	direction := direction(o.Desc)

	switch o.Key {
	case "database":
		return "database_name" + direction
	case "user":
		return "user_name" + direction
	case "level":
		return "{severity_of:Array(Int32)}[log_level + 1]" + direction
	case "event":
		return "classification" + direction
	case "category":
		return "{category_of:Array(Int32)}[classification + 1]" + direction
	default:
		return "occurred_at" + direction
	}
}

const listLogEventsSQL = `
SELECT id, occurred_at, log_level, classification, message, pid, user_name,
       database_name, application_name, detail, hint, context, statement,
       backend_type, state_code, statement_sample_id
FROM log_events
WHERE %s
ORDER BY %s, occurred_at DESC, id DESC
LIMIT {row_limit:UInt32} OFFSET {row_offset:UInt32}`

// ListLogEvents returns filtered, sorted, paginated log events.
// Example: server=prod, levels=[ERROR], limit=50 -> latest 50 matching events.
func (c *Client) ListLogEvents(
	ctx context.Context,
	filter LogFilter,
	order LogSortOrder,
	limit, offset int32,
) ([]LogEvent, error) {
	where := filter.conditions(false)
	if len(filter.Levels) > 0 {
		where.add("log_level IN {levels:Array(Int32)}", listParam("levels", filter.Levels))
	}

	args := where.with(
		listParam("severity_of", order.SeverityOf),
		listParam("category_of", order.CategoryOf),
		countParam("row_limit", limit),
		countParam("row_offset", offset),
	)

	rows, err := c.conn.Query(ctx,
		fmt.Sprintf(listLogEventsSQL, where.sql(), order.expression()), args...)
	if err != nil {
		return nil, fmt.Errorf("query log events: %w", err)
	}
	defer rows.Close()

	var out []LogEvent

	for rows.Next() {
		var (
			event                    LogEvent
			logLevel, classification uint8
		)

		if err = rows.Scan(
			&event.ID, &event.OccurredAt, &logLevel, &classification,
			&event.Message, &event.Pid, &event.UserName, &event.DatabaseName,
			&event.ApplicationName, &event.Detail, &event.Hint, &event.Context,
			&event.Statement, &event.BackendType, &event.StateCode, &event.StatementSampleID,
		); err != nil {
			return nil, fmt.Errorf("scan log event: %w", err)
		}

		event.LogLevel = int32(logLevel)
		event.Classification = int32(classification)
		out = append(out, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read log events: %w", err)
	}

	return out, nil
}

type LogHistogramBin struct {
	BucketEnd      time.Time
	LogLevel       int32
	Classification int32
	Count          int64
}

const logHistogramSQL = `
SELECT {from:DateTime('UTC')} + toIntervalSecond(
           (intDiv(toUInt32(toDateTime(occurred_at)) - toUInt32({from:DateTime('UTC')}) - 1,
                   {bucket:UInt32}) + 1) * {bucket:UInt32}) AS bucket_end,
       log_level,
       classification,
       toInt64(count()) AS n
FROM log_events
WHERE %s
GROUP BY bucket_end, log_level, classification
ORDER BY bucket_end, log_level, classification`

// LogEventHistogram counts events per time bucket, level, and classification.
// Example: 1m bucket -> {12:01, ERROR, LOCK_TIMEOUT, 15}.
func (c *Client) LogEventHistogram(
	ctx context.Context,
	filter LogFilter,
	bucket time.Duration,
) ([]LogHistogramBin, error) {
	where := filter.conditions(true)
	args := where.with(secondsParam("bucket", bucket))

	rows, err := c.conn.Query(ctx, fmt.Sprintf(logHistogramSQL, where.sql()), args...)
	if err != nil {
		return nil, fmt.Errorf("query log histogram: %w", err)
	}
	defer rows.Close()

	var out []LogHistogramBin

	for rows.Next() {
		var (
			bin                      LogHistogramBin
			logLevel, classification uint8
		)

		if err = rows.Scan(&bin.BucketEnd, &logLevel, &classification, &bin.Count); err != nil {
			return nil, fmt.Errorf("scan log histogram bin: %w", err)
		}

		bin.LogLevel = int32(logLevel)
		bin.Classification = int32(classification)

		out = append(out, bin)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read log histogram: %w", err)
	}

	return out, nil
}

type LogFacetDimension uint8

const (
	FacetClassification LogFacetDimension = iota
	FacetLogLevel
	FacetDatabaseName
	FacetUserName
	FacetApplicationName
	FacetBackendType
)

const facetDimensions = 6

type LogFacetRow struct {
	Dimension       LogFacetDimension
	Classification  int32
	LogLevel        int32
	DatabaseName    string
	UserName        string
	ApplicationName string
	BackendType     string
	Count           int64
}

const logFacetsSQL = `
SELECT toInt32(grouping(classification, log_level, database_name, user_name,
                        application_name, backend_type)) AS grouping_id,
       classification,
       log_level,
       database_name,
       user_name,
       application_name,
       backend_type,
       toInt64(count()) AS n
FROM log_events
WHERE %s
GROUP BY GROUPING SETS (
    (classification),
    (log_level),
    (database_name),
    (user_name),
    (application_name),
    (backend_type)
)
ORDER BY grouping_id, n DESC`

// LogEventFacets counts matching events by each filter dimension.
// Example: database_name -> postgres=120, app=40; log_level -> ERROR=30, WARNING=20.
func (c *Client) LogEventFacets(ctx context.Context, filter LogFilter) ([]LogFacetRow, error) {
	where := filter.conditions(false)

	rows, err := c.conn.Query(ctx, fmt.Sprintf(logFacetsSQL, where.sql()), where.args...)
	if err != nil {
		return nil, fmt.Errorf("query log facets: %w", err)
	}
	defer rows.Close()

	var out []LogFacetRow

	for rows.Next() {
		var (
			row                      LogFacetRow
			groupingID               int32
			logLevel, classification uint8
		)

		if err = rows.Scan(&groupingID, &classification, &logLevel,
			&row.DatabaseName, &row.UserName, &row.ApplicationName,
			&row.BackendType, &row.Count); err != nil {
			return nil, fmt.Errorf("scan log facet row: %w", err)
		}

		dimension, ok := facetDimensionOf(groupingID)
		if !ok {
			continue
		}

		row.Dimension = dimension
		row.LogLevel = int32(logLevel)
		row.Classification = int32(classification)

		out = append(out, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read log facets: %w", err)
	}

	return out, nil
}

const countLogErrorsSQL = `
SELECT
    toInt64(countIf(occurred_at >= {current_start:DateTime('UTC')})) AS current_errors,
    toInt64(countIf(occurred_at <  {current_start:DateTime('UTC')})) AS previous_errors
FROM log_events
WHERE server_name = {server_name:String}
  AND occurred_at >= {previous_start:DateTime('UTC')}
  AND occurred_at <  {current_end:DateTime('UTC')}
  AND log_level IN {levels:Array(Int32)}`

// CountLogErrors returns error counts for current and previous periods.
// Example: previous=10, current=15 -> (15, 10).
func (c *Client) CountLogErrors(
	ctx context.Context,
	serverName string,
	previousStart, currentStart, currentEnd time.Time,
	levels []int32,
) (int64, int64, error) {
	var current, previous int64

	err := c.conn.QueryRow(ctx, countLogErrorsSQL,
		param("server_name", serverName),
		timeParam("previous_start", previousStart),
		timeParam("current_start", currentStart),
		timeParam("current_end", currentEnd),
		listParam("levels", levels),
	).Scan(&current, &previous)
	if err != nil {
		return 0, 0, fmt.Errorf("count log errors: %w", err)
	}

	return current, previous, nil
}

func facetDimensionOf(groupingID int32) (LogFacetDimension, bool) {
	for dimension := range LogFacetDimension(facetDimensions) {
		if groupingID == facetGroupingID(dimension) {
			return dimension, true
		}
	}

	return 0, false
}

func facetGroupingID(dimension LogFacetDimension) int32 {
	const allSet = int32(1)<<facetDimensions - 1

	return allSet &^ (int32(1) << (facetDimensions - 1 - dimension))
}
