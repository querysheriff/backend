package clickhouse

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
)

type TransactionActivity struct {
	TransactionID   uint64
	ServerName      string
	DatabaseName    string
	UserName        string
	ApplicationName string
	Pid             uint32
	BackendStart    time.Time
	XactStart       time.Time
	CollectedAt     time.Time
	QueryStart      time.Time
	Query           string
	QueryTags       map[string]string
	State           string
	WaitEventType   string
	WaitEvent       string
	BlockedByPid    uint32
	LockWaitStart   *time.Time
	LockMode        string
}

// TransactionID creates a stable ID from transaction identity fields.
// Example: same server/pid/backendStart/xactStart -> same ID.
func TransactionID(serverName string, pid uint32, backendStart, xactStart time.Time) uint64 {
	key := serverName + "\x00" +
		strconv.FormatUint(uint64(pid), 10) + "\x00" +
		strconv.FormatInt(backendStart.UTC().UnixNano(), 10) + "\x00" +
		strconv.FormatInt(xactStart.UTC().UnixNano(), 10)

	return xxhash.Sum64String(key) & math.MaxInt64
}

// InsertTransactionActivity inserts transaction activity snapshots in one ClickHouse batch.
// Example: [row1, row2] -> 2 rows in transaction_activity.
func (c *Client) InsertTransactionActivity(ctx context.Context, rows []TransactionActivity) error {
	return insertBatch(ctx, c, "transaction_activity", rows, func(row TransactionActivity) []any {
		tags := row.QueryTags
		if tags == nil {
			tags = map[string]string{}
		}

		return []any{
			row.TransactionID, row.ServerName, row.DatabaseName, row.UserName,
			row.ApplicationName, row.Pid, row.BackendStart.UTC(), row.XactStart.UTC(),
			row.CollectedAt.UTC(), row.QueryStart.UTC(), row.Query, tags,
			row.State, row.WaitEventType, row.WaitEvent, row.BlockedByPid,
			row.LockWaitStart, row.LockMode,
		}
	})
}

type TransactionScope struct {
	ServerName   string
	DatabaseName string
	From, To     time.Time
}

func (s TransactionScope) conditions() *conditions {
	c := &conditions{}
	c.add("server_name = {server_name:String}", param("server_name", s.ServerName))
	c.add("database_name = {database_name:String}", param("database_name", s.DatabaseName))

	return c
}

type Transaction struct {
	ID              uint64    `ch:"transaction_id"`
	Pid             uint32    `ch:"pid"`
	ApplicationName string    `ch:"application_name"`
	XactStart       time.Time `ch:"xact_start"`
	LastSeenAt      time.Time `ch:"last_seen_at"`
}

const listTransactionsSQL = `
SELECT transaction_id,
       any(pid)              AS pid,
       any(application_name) AS application_name,
       xact_start,
       max(collected_at)     AS last_seen_at
FROM transaction_activity
WHERE %s
  AND xact_start <= {to_time:DateTime('UTC')}
  AND collected_at >= {from_time:DateTime('UTC')}
GROUP BY transaction_id, xact_start
HAVING last_seen_at - xact_start >= {min_open:UInt32}
ORDER BY %s, transaction_id DESC
LIMIT {row_limit:UInt32} OFFSET {offset_rows:UInt32}`

func transactionOrderBy(sortKey string, desc bool) string {
	direction := direction(desc)

	if sortKey == "started" {
		return "xact_start" + direction
	}

	return "(last_seen_at - xact_start)" + direction
}

// ListTransactions returns filtered, sorted, paginated long-running transactions.
// Example: minOpen=30s -> transactions observed open for at least 30s.
func (c *Client) ListTransactions(
	ctx context.Context,
	scope TransactionScope,
	minOpen time.Duration,
	sortKey string,
	sortDesc bool,
	limit, offset int32,
) ([]Transaction, error) {
	where := scope.conditions()

	var out []Transaction
	if err := c.conn.Select(ctx, &out,
		fmt.Sprintf(listTransactionsSQL, where.sql(), transactionOrderBy(sortKey, sortDesc)),
		where.with(
			timeParam("to_time", scope.To),
			timeParam("from_time", scope.From),
			secondsParam("min_open", minOpen),
			countParam("row_limit", limit),
			countParam("offset_rows", offset),
		)...); err != nil {
		return nil, fmt.Errorf("query transactions: %w", err)
	}

	return out, nil
}

type TransactionEvent struct {
	TransactionID uint64            `ch:"transaction_id"`
	State         string            `ch:"state"`
	WaitEventType string            `ch:"wait_event_type"`
	WaitEvent     string            `ch:"wait_event"`
	LockMode      string            `ch:"lock_mode"`
	Query         string            `ch:"query"`
	QueryTags     map[string]string `ch:"query_tags"`
	QueryStart    time.Time         `ch:"query_start"`
	FirstSeenAt   time.Time         `ch:"first_seen_at"`
	LastSeenAt    time.Time         `ch:"last_seen_at"`
}

const transactionRunsCTE = `
    scoped AS (
        SELECT transaction_id, server_name, database_name, pid, application_name,
               collected_at, xact_start, query_start, query, query_tags,
               state, wait_event_type, wait_event, blocked_by_pid, lock_wait_start, lock_mode,
               tuple(query_start, state, wait_event_type, wait_event,
                     blocked_by_pid, lock_wait_start, lock_mode) AS attrs
        FROM transaction_activity
        WHERE %s
          %s
    ),
    ordered AS (
        SELECT transaction_id, server_name, database_name, pid, application_name,
               collected_at, xact_start, query_start, query, query_tags,
               state, wait_event_type, wait_event, blocked_by_pid, lock_wait_start, lock_mode,
               attrs,
               row_number() OVER w AS position,
               lagInFrame(attrs) OVER (PARTITION BY transaction_id ORDER BY collected_at
                                       ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_attrs
        FROM scoped
        WINDOW w AS (PARTITION BY transaction_id ORDER BY collected_at)
    ),
    runs AS (
        SELECT transaction_id, server_name, database_name, pid, application_name,
               collected_at, xact_start, query_start, query, query_tags,
               state, wait_event_type, wait_event, blocked_by_pid, lock_wait_start, lock_mode,
               attrs,
               sum(if(position = 1 OR attrs != previous_attrs, 1, 0))
                   OVER (PARTITION BY transaction_id ORDER BY collected_at
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_id
        FROM ordered
    ),
    events AS (
        SELECT transaction_id,
               any(server_name)      AS server_name,
               any(database_name)    AS database_name,
               any(pid)              AS pid,
               any(application_name) AS application_name,
               any(state)            AS state,
               any(wait_event_type)  AS wait_event_type,
               any(wait_event)       AS wait_event,
               any(lock_mode)        AS lock_mode,
               any(blocked_by_pid)   AS blocked_by_pid,
               any(lock_wait_start)  AS lock_wait_start,
               any(query)            AS query,
               any(query_tags)       AS query_tags,
               any(query_start)      AS query_start,
               any(xact_start)       AS xact_start,
               min(collected_at)     AS first_seen_at,
               max(collected_at)     AS last_seen_at
        FROM runs
        GROUP BY transaction_id, run_id
    )`

const everyTransaction = ""

// collectedBeforeStartSlack lets event lookup include snapshots taken just before xact_start.
// This is needed because collected_at is recorded before pg_stat_activity is read.
// Example: collected_at=12:00:00, xact_start=12:00:04 -> keep that snapshot by looking back 1 minute.
const collectedBeforeStartSlack = time.Minute

const blockedTransactionsOnly = `AND transaction_id IN (
              SELECT transaction_id FROM transaction_activity WHERE %s AND blocked_by_pid != 0
          )`

const listTransactionEventsSQL = `
WITH` + transactionRunsCTE + `
SELECT transaction_id, state, wait_event_type, wait_event, lock_mode,
       query, query_tags, query_start, first_seen_at, last_seen_at
FROM events
ORDER BY transaction_id, first_seen_at`

// ListTransactionEvents returns consecutive state/query/wait runs for transactions.
// Example: active -> Lock wait -> active becomes 3 events.
func (c *Client) ListTransactionEvents(
	ctx context.Context,
	scope TransactionScope,
	transactions []Transaction,
) ([]TransactionEvent, error) {
	if len(transactions) == 0 {
		return nil, nil
	}

	ids := make([]uint64, len(transactions))
	xactStarts := make([]string, len(transactions))
	oldestStart := transactions[0].XactStart

	for i, transaction := range transactions {
		ids[i] = transaction.ID
		xactStarts[i] = transaction.XactStart.UTC().Format(dateTime64Layout)

		if transaction.XactStart.Before(oldestStart) {
			oldestStart = transaction.XactStart
		}
	}

	where := scope.conditions()
	where.add("xact_start IN {xact_starts:Array(DateTime64(3, 'UTC'))}", listParam("xact_starts", xactStarts))
	where.add("collected_at >= {oldest_start:DateTime('UTC')}",
		timeParam("oldest_start", oldestStart.Add(-collectedBeforeStartSlack)))
	where.add("transaction_id IN {transaction_ids:Array(UInt64)}", listParam("transaction_ids", ids))

	var out []TransactionEvent
	if err := c.conn.Select(ctx, &out,
		fmt.Sprintf(listTransactionEventsSQL, where.sql(), everyTransaction), where.args...); err != nil {
		return nil, fmt.Errorf("query transaction events: %w", err)
	}

	return out, nil
}

type TransactionAgeBin struct {
	BucketEnd   time.Time `ch:"bucket_end"`
	EndedAge    float64   `ch:"ended_age"`
	OldestStart time.Time `ch:"oldest_start"`
}

const transactionAgeSeriesSQL = `
WITH per_transaction AS (
    SELECT transaction_id, xact_start, max(collected_at) AS last_seen_at
    FROM transaction_activity
    WHERE %s
      AND xact_start <= {range_end:DateTime('UTC')}
      AND collected_at > {range_start:DateTime('UTC')}
    GROUP BY transaction_id, xact_start
)
SELECT toStartOfInterval(last_seen_at - toIntervalSecond(1),
                         INTERVAL {bucket:UInt32} SECOND,
                         {range_start:DateTime('UTC')})
           + INTERVAL {bucket:UInt32} SECOND AS bucket_end,
       max(toFloat64(last_seen_at - xact_start)) AS ended_age,
       min(xact_start)                          AS oldest_start
FROM per_transaction
GROUP BY bucket_end
ORDER BY bucket_end`

// TransactionAgeSeries returns the oldest transaction age per time bucket.
// Example: 12:01 -> oldest transaction ended/last seen at age 120s.
func (c *Client) TransactionAgeSeries(
	ctx context.Context,
	scope TransactionScope,
	bucket time.Duration,
) ([]TransactionAgeBin, error) {
	where := scope.conditions()

	var out []TransactionAgeBin
	if err := c.conn.Select(ctx, &out, fmt.Sprintf(transactionAgeSeriesSQL, where.sql()),
		where.with(
			timeParam("range_end", scope.To),
			timeParam("range_start", scope.From),
			secondsParam("bucket", bucket),
		)...); err != nil {
		return nil, fmt.Errorf("query transaction age series: %w", err)
	}

	return out, nil
}

type LockWaitBin struct {
	BucketEnd   time.Time `ch:"bucket_end"`
	WaitSeconds float64   `ch:"wait_seconds"`
}

const lockWaitEpisodesCTE = `
    episodes AS (
        SELECT server_name, database_name, pid AS waiting_pid, blocked_by_pid, lock_mode,
               ifNull(lock_wait_start, first_seen_at) AS wait_start,
               max(last_seen_at)                      AS last_seen,
               argMin(application_name, first_seen_at) AS waiting_app,
               argMin(query, first_seen_at)            AS waiting_query,
               argMin(query_tags, first_seen_at)       AS waiting_tags
        FROM events
        WHERE blocked_by_pid != 0
        GROUP BY server_name, database_name, waiting_pid, blocked_by_pid, lock_mode, wait_start
    )`

const lockWaitSeriesSQL = `
WITH` + transactionRunsCTE + `,` + lockWaitEpisodesCTE + `,
    waits AS (
        SELECT wait_start, max(last_seen) AS last_seen
        FROM episodes
        GROUP BY server_name, database_name, waiting_pid, wait_start
    ),
    spread AS (
        SELECT arrayJoin(range(
                   toUInt32(toStartOfInterval(greatest(toDateTime(wait_start) - toIntervalSecond(1),
                                                       {range_start:DateTime('UTC')}),
                                              INTERVAL {bucket:UInt32} SECOND,
                                              {range_start:DateTime('UTC')}) + INTERVAL {bucket:UInt32} SECOND),
                   toUInt32(toStartOfInterval(toDateTime(last_seen) - toIntervalSecond(1),
                                              INTERVAL {bucket:UInt32} SECOND,
                                              {range_start:DateTime('UTC')}) + INTERVAL {bucket:UInt32} SECOND) + 1,
                   {bucket:UInt32})) AS bucket_end_epoch,
               wait_start,
               last_seen
        FROM waits
    )
SELECT toDateTime(bucket_end_epoch, 'UTC') AS bucket_end,
       sum(toFloat64(least(last_seen, toDateTime(bucket_end_epoch, 'UTC'))
                   - greatest(wait_start, toDateTime(bucket_end_epoch - {bucket:UInt32}, 'UTC')))) AS wait_seconds
FROM spread
WHERE bucket_end > {range_start:DateTime('UTC')}
  AND bucket_end <= {range_end:DateTime('UTC')}
GROUP BY bucket_end
ORDER BY bucket_end`

// LockWaitSeries returns total lock-wait seconds per time bucket.
// Example: two 10s waits in 12:01 -> WaitSeconds=20.
func (c *Client) LockWaitSeries(
	ctx context.Context,
	scope TransactionScope,
	bucket time.Duration,
) ([]LockWaitBin, error) {
	where := scope.conditions()
	where.add("collected_at <= {range_end:DateTime('UTC')}", timeParam("range_end", scope.To))
	where.add("collected_at > {range_start:DateTime('UTC')}", timeParam("range_start", scope.From))

	var out []LockWaitBin
	if err := c.conn.Select(ctx, &out,
		fmt.Sprintf(lockWaitSeriesSQL, where.sql(), fmt.Sprintf(blockedTransactionsOnly, where.sql())),
		where.with(secondsParam("bucket", bucket))...); err != nil {
		return nil, fmt.Errorf("query lock wait series: %w", err)
	}

	return out, nil
}

type LockWait struct {
	WaitingPid    uint32            `ch:"waiting_pid"`
	WaitingApp    string            `ch:"waiting_app"`
	WaitingQuery  string            `ch:"waiting_query"`
	WaitingTags   map[string]string `ch:"waiting_tags"`
	BlockedByPid  uint32            `ch:"blocked_by_pid"`
	LockMode      string            `ch:"lock_mode"`
	WaitStart     time.Time         `ch:"wait_start"`
	LastSeen      time.Time         `ch:"last_seen"`
	BlockingApp   string            `ch:"blocking_app"`
	BlockingQuery string            `ch:"blocking_query"`
	BlockingTags  map[string]string `ch:"blocking_tags"`
}

func lockWaitOrderBy(sortKey string, desc bool) string {
	direction := direction(desc)

	if sortKey == "started" {
		return "wait_start" + direction
	}

	return "(last_seen - wait_start)" + direction
}

const listLockWaitsSQL = `
WITH` + transactionRunsCTE + `,` + lockWaitEpisodesCTE + `
SELECT waiting_pid, waiting_app, waiting_query, waiting_tags, blocked_by_pid, lock_mode, wait_start, last_seen
FROM episodes
ORDER BY %s, wait_start DESC, server_name, database_name,
         waiting_pid, blocked_by_pid, lock_mode
LIMIT {row_limit:UInt32} OFFSET {offset_rows:UInt32}`

const blockingSideSQL = `
WITH` + transactionRunsCTE + `,
    targets AS (
        SELECT tupleElement(pair, 1) AS blocker_pid,
               tupleElement(pair, 2) AS wait_start
        FROM (SELECT arrayJoin(arrayZip(
                  {pids:Array(UInt32)},
                  arrayMap(x -> toDateTime64(x, 3, 'UTC'), {wait_starts:Array(String)}))) AS pair)
    )
SELECT blocker_pid                       AS blocked_by_pid,
       wait_start,
       argMin(application_name, nearest) AS blocking_app,
       argMin(query, nearest)            AS blocking_query,
       argMin(query_tags, nearest)       AS blocking_tags
FROM (
    SELECT t.blocker_pid AS blocker_pid,
           t.wait_start  AS wait_start,
           e.application_name,
           e.query,
           e.query_tags,
           (e.first_seen_at > t.wait_start,
            abs(toInt64(e.first_seen_at) - toInt64(t.wait_start))) AS nearest
    FROM targets AS t
    INNER JOIN events AS e ON e.pid = t.blocker_pid
    WHERE e.xact_start <= t.wait_start
      AND e.last_seen_at >= toDateTime(t.wait_start)
)
GROUP BY blocker_pid, wait_start`

type lockWaitKey struct {
	pid       uint32
	waitStart time.Time
}

// ListLockWaits returns paginated lock waits with waiting and blocking query details.
// Example: pid 10 blocked by pid 20 -> waiting query + blocking query.
func (c *Client) ListLockWaits(
	ctx context.Context,
	scope TransactionScope,
	sortKey string,
	sortDesc bool,
	limit, offset int32,
) ([]LockWait, error) {
	where := scope.conditions()
	where.add("collected_at <= {to_time:DateTime('UTC')}", timeParam("to_time", scope.To))
	where.add("collected_at >= {from_time:DateTime('UTC')}", timeParam("from_time", scope.From))

	var waits []LockWait
	if err := c.conn.Select(ctx, &waits,
		fmt.Sprintf(listLockWaitsSQL, where.sql(), fmt.Sprintf(blockedTransactionsOnly, where.sql()),
			lockWaitOrderBy(sortKey, sortDesc)),
		where.with(countParam("row_limit", limit), countParam("offset_rows", offset))...); err != nil {
		return nil, fmt.Errorf("query lock waits: %w", err)
	}

	if len(waits) == 0 {
		return nil, nil
	}

	blocking, err := c.blockingSides(ctx, where, waits)
	if err != nil {
		return nil, err
	}

	for i, wait := range waits {
		side := blocking[lockWaitKey{pid: wait.BlockedByPid, waitStart: wait.WaitStart.UTC()}]
		waits[i].BlockingApp = side.BlockingApp
		waits[i].BlockingQuery = side.BlockingQuery
		waits[i].BlockingTags = side.BlockingTags
	}

	return waits, nil
}

// blockingSides finds what each blocking PID was running when its wait began.
// Example: pid 20 blocking since 12:00 -> {(20,12:00): {BlockingQuery:"UPDATE users ..."}}.
func (c *Client) blockingSides(
	ctx context.Context,
	where *conditions,
	waits []LockWait,
) (map[lockWaitKey]LockWait, error) {
	pids := make([]uint32, len(waits))
	waitStarts := make([]string, len(waits))

	for i, wait := range waits {
		pids[i] = wait.BlockedByPid
		waitStarts[i] = wait.WaitStart.UTC().Format(dateTime64Layout)
	}

	var sides []LockWait
	if err := c.conn.Select(ctx, &sides,
		fmt.Sprintf(blockingSideSQL, where.sql(), "AND pid IN {pids:Array(UInt32)}"),
		where.with(listParam("pids", pids), listParam("wait_starts", waitStarts))...); err != nil {
		return nil, fmt.Errorf("query blocking side: %w", err)
	}

	out := make(map[lockWaitKey]LockWait, len(sides))
	for _, side := range sides {
		out[lockWaitKey{pid: side.BlockedByPid, waitStart: side.WaitStart.UTC()}] = side
	}

	return out, nil
}
