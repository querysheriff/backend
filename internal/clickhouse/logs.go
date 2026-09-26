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
	ID                uint64    `ch:"id"`
	ServerName        string    `ch:"server_name"`
	CollectedAt       time.Time `ch:"collected_at"`
	OccurredAt        time.Time `ch:"occurred_at"`
	LogLevel          int32     `ch:"level_id"`
	Classification    int32     `ch:"classification_id"`
	Message           string    `ch:"message"`
	Pid               uint32    `ch:"pid"`
	UserName          string    `ch:"user_name"`
	DatabaseName      string    `ch:"database_name"`
	ApplicationName   string    `ch:"application_name"`
	Detail            string    `ch:"detail"`
	Hint              string    `ch:"hint"`
	Context           string    `ch:"context"`
	Statement         string    `ch:"statement"`
	BackendType       string    `ch:"backend_type"`
	StateCode         string    `ch:"state_code"`
	StatementSampleID uint64    `ch:"statement_sample_id"`
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
	return insertBatch(ctx, c, "log_events", events, func(event LogEvent) []any {
		return []any{
			event.ID, event.ServerName, event.CollectedAt.UTC(), event.OccurredAt.UTC(),
			enumOf(event.LogLevel), enumOf(event.Classification), event.Message, event.Pid,
			event.UserName, event.DatabaseName, event.ApplicationName,
			event.Detail, event.Hint, event.Context, event.Statement,
			event.BackendType, event.StateCode, event.StatementSampleID,
		}
	})
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

	if len(f.Levels) > 0 {
		c.add("log_level IN {levels:Array(Int32)}", listParam("levels", f.Levels))
	}

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

const listLogEventsSQL = `
SELECT id, occurred_at, toInt32(log_level) AS level_id, toInt32(classification) AS classification_id,
       message, pid, user_name, database_name, application_name, detail, hint, context, statement,
       backend_type, state_code, statement_sample_id
FROM log_events
WHERE %s
ORDER BY occurred_at%[2]s, id%[2]s
LIMIT {row_limit:UInt32} OFFSET {row_offset:UInt32}`

// ListLogEvents returns filtered log events in time order, paginated.
// Example: server=prod, levels=[ERROR], desc, limit=50 -> latest 50 matching events.
func (c *Client) ListLogEvents(
	ctx context.Context,
	filter LogFilter,
	desc bool,
	limit, offset int32,
) ([]LogEvent, error) {
	where := filter.conditions(false)

	var out []LogEvent
	if err := c.conn.Select(ctx, &out, fmt.Sprintf(listLogEventsSQL, where.sql(), direction(desc)),
		where.with(countParam("row_limit", limit), countParam("row_offset", offset))...); err != nil {
		return nil, fmt.Errorf("query log events: %w", err)
	}

	return out, nil
}

type LogHistogramBin struct {
	BucketEnd      time.Time `ch:"bucket_end"`
	LogLevel       int32     `ch:"log_level"`
	Classification int32     `ch:"classification"`
	Count          int64     `ch:"count"`
}

const logHistogramSQL = `
SELECT {from:DateTime('UTC')} + toIntervalSecond(
           (intDiv(toUInt32(toDateTime(occurred_at)) - toUInt32({from:DateTime('UTC')}) - 1,
                   {bucket:UInt32}) + 1) * {bucket:UInt32}) AS bucket_end,
       toInt32(log_level) AS log_level,
       toInt32(classification) AS classification,
       toInt64(count()) AS count
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

	var out []LogHistogramBin
	if err := c.conn.Select(ctx, &out, fmt.Sprintf(logHistogramSQL, where.sql()),
		where.with(secondsParam("bucket", bucket))...); err != nil {
		return nil, fmt.Errorf("query log histogram: %w", err)
	}

	return out, nil
}

type LogFacetDimension = uint8

const (
	FacetClassification LogFacetDimension = iota
	FacetLogLevel
	FacetDatabaseName
	FacetUserName
	FacetApplicationName
	FacetBackendType
)

type LogFacetRow struct {
	Dimension LogFacetDimension `ch:"dimension"`
	Value     string            `ch:"value"`
	Count     int64             `ch:"count"`
}

const logFacetsSQL = `
SELECT toUInt8(multiIf(grouping(classification) = 0, 0, grouping(log_level) = 0, 1,
                       grouping(database_name) = 0, 2, grouping(user_name) = 0, 3,
                       grouping(application_name) = 0, 4, 5)) AS dimension,
       multiIf(dimension = 0, toString(classification), dimension = 1, toString(log_level),
               dimension = 2, database_name, dimension = 3, user_name,
               dimension = 4, application_name, backend_type) AS value,
       toInt64(count()) AS count
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
ORDER BY dimension, count DESC, value`

// LogEventFacets counts matching events by each filter dimension, most common values first.
// Example: database_name -> postgres=120, app=40; log_level -> ERROR=30, WARNING=20.
func (c *Client) LogEventFacets(ctx context.Context, filter LogFilter) ([]LogFacetRow, error) {
	where := filter.conditions(false)

	var out []LogFacetRow
	if err := c.conn.Select(ctx, &out, fmt.Sprintf(logFacetsSQL, where.sql()), where.args...); err != nil {
		return nil, fmt.Errorf("query log facets: %w", err)
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
