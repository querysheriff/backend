package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/humanize"
	"github.com/querysheriff/backend/internal/timeseries"
)

const (
	activeState        = "active"
	longQueryThreshold = time.Minute
	blockingThreshold  = 10 * time.Second
	openTxnThreshold   = 10 * time.Minute
)

type ActivityServer struct {
	pool     *pgxpool.Pool
	queries  *db.Queries
	stats    *clickhouse.Client
	notifier *alerts.Notifier
}

func NewActivityServer(pool *pgxpool.Pool, stats *clickhouse.Client, notifier *alerts.Notifier) *ActivityServer {
	return &ActivityServer{pool: pool, queries: db.New(pool), stats: stats, notifier: notifier}
}

// ReportActivity stores transaction activity snapshots and evaluates alerts.
// Example: 3 transaction snapshots -> 3 rows in transaction_activity.
func (s *ActivityServer) ReportActivity(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportActivityRequest],
) (*connect.Response[querysheriffv1.ReportActivityResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	txnSnapshots := make([]*querysheriffv1.ActivitySnapshot, 0, len(msg.GetActivitySnapshots()))
	for _, snap := range msg.GetActivitySnapshots() {
		if snap.GetXactStart() != nil && snap.GetBackendStart() != nil {
			txnSnapshots = append(txnSnapshots, snap)
		}
	}

	if len(txnSnapshots) == 0 {
		return connect.NewResponse(&querysheriffv1.ReportActivityResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	collectedAt := msg.GetCollectedAt().AsTime()

	rows := make([]clickhouse.TransactionActivity, len(txnSnapshots))
	for i, snap := range txnSnapshots {
		rows[i] = transactionActivityRow(serverName, collectedAt, snap)
	}

	if err = s.stats.InsertTransactionActivity(ctx, rows); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.evaluateAlerts(serverName, msg.GetCollectedAt().AsTime(), txnSnapshots)

	return connect.NewResponse(&querysheriffv1.ReportActivityResponse{}), nil
}

func (s *ActivityServer) evaluateAlerts(
	serverName string,
	collectedAt time.Time,
	snapshots []*querysheriffv1.ActivitySnapshot,
) {
	blocked, blockedFor := longestOver(snapshots, blockingThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) (time.Duration, bool) {
			if snap.GetBlockedByPid() == 0 || snap.GetLockWaitStart() == nil {
				return 0, false
			}

			return collectedAt.Sub(snap.GetLockWaitStart().AsTime()), true
		})

	running, runningFor := longestOver(snapshots, longQueryThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) (time.Duration, bool) {
			if snap.GetState() != activeState || snap.GetQueryStart() == nil {
				return 0, false
			}

			return collectedAt.Sub(snap.GetQueryStart().AsTime()), true
		})

	open, openFor := longestOver(snapshots, openTxnThreshold,
		func(snap *querysheriffv1.ActivitySnapshot) (time.Duration, bool) {
			return collectedAt.Sub(snap.GetXactStart().AsTime()), true
		})

	if blocked != nil {
		s.notifier.Fire(serverName, alerts.KeyBlockedQuery, blockedQueryText(blocked, blockedFor, snapshots))
	}
	if running != nil {
		s.notifier.Fire(serverName, alerts.KeyLongQuery, sessionText("query", "running", running, runningFor))
	}
	if open != nil {
		s.notifier.Fire(serverName, alerts.KeyLongTransaction, sessionText("transaction", "open", open, openFor))
	}
}

func longestOver(
	snapshots []*querysheriffv1.ActivitySnapshot,
	threshold time.Duration,
	measure func(*querysheriffv1.ActivitySnapshot) (time.Duration, bool),
) (*querysheriffv1.ActivitySnapshot, time.Duration) {
	var worst *querysheriffv1.ActivitySnapshot
	var longest time.Duration

	for _, snap := range snapshots {
		measured, ok := measure(snap)
		if ok && measured >= threshold && measured > longest {
			worst, longest = snap, measured
		}
	}

	return worst, longest
}

func inDatabase(name string) string {
	if name == "" {
		return ""
	}

	return " in " + name
}

func sessionText(
	subject, verb string,
	snap *querysheriffv1.ActivitySnapshot,
	elapsed time.Duration,
) string {
	return fmt.Sprintf(
		"A %s%s has been %s %s.\n\nquery: %s\npid: %d",
		subject,
		inDatabase(snap.GetDatabaseName()),
		verb,
		humanize.Duration(elapsed),
		humanize.QueryPreview(snap.GetQuery()),
		snap.GetPid(),
	)
}

func blockedQueryText(
	blocked *querysheriffv1.ActivitySnapshot,
	waited time.Duration,
	snapshots []*querysheriffv1.ActivitySnapshot,
) string {
	blockerQuery := "not captured"
	for _, snap := range snapshots {
		if snap.GetPid() == blocked.GetBlockedByPid() {
			blockerQuery = humanize.QueryPreview(snap.GetQuery())

			break
		}
	}

	return fmt.Sprintf(
		"A query%s has been waiting %s for a lock.\n\nwaiting: %s\nblocking: %s\npids: %d blocked by %d",
		inDatabase(blocked.GetDatabaseName()),
		humanize.Duration(waited),
		humanize.QueryPreview(blocked.GetQuery()),
		blockerQuery,
		blocked.GetPid(),
		blocked.GetBlockedByPid(),
	)
}

// QueryTransactions returns filtered, sorted, paginated transactions with reconstructed events.
// Example: minOpen=30s, limit=50 -> transactions open for at least 30s.
func (s *ActivityServer) QueryTransactions(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryTransactionsRequest],
) (*connect.Response[querysheriffv1.QueryTransactionsResponse], error) {
	msg := req.Msg

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	if name := msg.GetServerName(); name != "" && !principal.CanViewServer(name) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err = requireRange(from, to); err != nil {
		return nil, err
	}

	limit := resolveLimit(msg.GetLimit())

	scope := clickhouse.TransactionScope{
		ServerName:   msg.GetServerName(),
		DatabaseName: msg.GetDatabaseName(),
		From:         from.AsTime(),
		To:           to.AsTime(),
	}

	rows, err := s.stats.ListTransactions(ctx, scope,
		time.Duration(msg.GetMinOpenMs())*time.Millisecond,
		transactionSortKey(msg.GetSortColumn()), msg.GetSortDesc(),
		limit+1, resolveOffset(msg.GetOffset()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	if len(rows) == 0 {
		return connect.NewResponse(&querysheriffv1.QueryTransactionsResponse{}), nil
	}

	ids := make([]uint64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}

	eventRows, err := s.stats.ListTransactionEvents(ctx, scope, ids)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	eventsByTxn := make(map[uint64][]reconstructedEvent, len(rows))
	for _, row := range eventRows {
		eventsByTxn[row.TransactionID] = append(
			eventsByTxn[row.TransactionID], reconstructedEventFromRow(row))
	}

	transactions := make([]*querysheriffv1.Transaction, len(rows))
	for i, row := range rows {
		events := eventsByTxn[row.ID]
		transactions[i] = &querysheriffv1.Transaction{
			Pid:             signedPid(row.Pid),
			ApplicationName: row.ApplicationName,
			Start:           timestamppb.New(row.XactStart),
			End:             timestamppb.New(row.LastSeenAt),
			Events:          buildTransactionEvents(row.XactStart, events),
		}
	}

	return connect.NewResponse(&querysheriffv1.QueryTransactionsResponse{
		Transactions: transactions,
		HasMore:      hasMore,
	}), nil
}

func transactionSortKey(col querysheriffv1.TransactionSortColumn) string {
	switch col {
	case querysheriffv1.TransactionSortColumn_TRANSACTION_SORT_COLUMN_STARTED:
		return "started"
	case querysheriffv1.TransactionSortColumn_TRANSACTION_SORT_COLUMN_OPEN,
		querysheriffv1.TransactionSortColumn_TRANSACTION_SORT_COLUMN_UNSPECIFIED:
		return "open"
	}

	return "open"
}

type activitySeriesScope struct {
	bounds       timeseries.Bounds
	serverName   string
	databaseName string
}

func (s *ActivityServer) resolveSeriesScope(
	ctx context.Context,
	serverName, databaseName string,
	from, to *timestamppb.Timestamp,
) (activitySeriesScope, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return activitySeriesScope{}, err
	}

	if serverName != "" && !principal.CanViewServer(serverName) {
		return activitySeriesScope{}, connect.NewError(
			connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	if err = requireRange(from, to); err != nil {
		return activitySeriesScope{}, err
	}

	return activitySeriesScope{
		bounds:       timeseries.NewBounds(from.AsTime(), to.AsTime(), time.Now()),
		serverName:   serverName,
		databaseName: databaseName,
	}, nil
}

func (a activitySeriesScope) transactionScope() clickhouse.TransactionScope {
	return clickhouse.TransactionScope{
		ServerName:   a.serverName,
		DatabaseName: a.databaseName,
		From:         a.bounds.RangeStart,
		To:           a.bounds.Anchor,
	}
}

// QueryTransactionAgeSeries returns the oldest transaction age per time bucket.
// Example: 1m buckets -> [{12:01, 120s}, {12:02, 180s}, ...].
func (s *ActivityServer) QueryTransactionAgeSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryTransactionAgeSeriesRequest],
) (*connect.Response[querysheriffv1.QueryTransactionAgeSeriesResponse], error) {
	msg := req.Msg

	scope, err := s.resolveSeriesScope(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	rows, err := s.stats.TransactionAgeSeries(ctx, scope.transactionScope(), scope.bounds.Bucket)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.QueryTransactionAgeSeriesResponse{
		Series:   transactionAgePoints(scope.bounds.Ends(), rows),
		BucketMs: scope.bounds.Bucket.Milliseconds(),
	}), nil
}

func transactionAgePoints(ends []time.Time, rows []clickhouse.TransactionAgeBin) []*querysheriffv1.TransactionAgePoint {
	endedAge := make(map[int64]float64, len(rows))
	for _, r := range rows {
		endedAge[r.BucketEnd.UnixNano()] = r.EndedAge
	}

	next := len(rows) - 1
	var oldestOpen time.Time

	points := make([]*querysheriffv1.TransactionAgePoint, len(ends))
	for i, end := range slices.Backward(ends) {
		for next >= 0 && rows[next].BucketEnd.After(end) {
			if start := rows[next].OldestStart; oldestOpen.IsZero() || start.Before(oldestOpen) {
				oldestOpen = start
			}
			next--
		}

		age := endedAge[end.UnixNano()]
		if !oldestOpen.IsZero() {
			if stillOpen := end.Sub(oldestOpen).Seconds(); stillOpen > age {
				age = stillOpen
			}
		}

		points[i] = &querysheriffv1.TransactionAgePoint{At: timestamppb.New(end), AgeSeconds: age}
	}

	return points
}

func transactionActivityRow(
	serverName string,
	collectedAt time.Time,
	snap *querysheriffv1.ActivitySnapshot,
) clickhouse.TransactionActivity {
	pid := uint32(max(snap.GetPid(), 0))
	backendStart := snap.GetBackendStart().AsTime()
	xactStart := snap.GetXactStart().AsTime()

	row := clickhouse.TransactionActivity{
		TransactionID:   clickhouse.TransactionID(serverName, pid, backendStart, xactStart),
		ServerName:      serverName,
		DatabaseName:    snap.GetDatabaseName(),
		UserName:        snap.GetUserName(),
		ApplicationName: snap.GetApplicationName(),
		Pid:             pid,
		BackendStart:    backendStart,
		XactStart:       xactStart,
		CollectedAt:     collectedAt,
		QueryStart:      snap.GetQueryStart().AsTime(),
		Query:           snap.GetQuery(),
		QueryTags:       snap.GetQueryTags(),
		State:           snap.GetState(),
		WaitEventType:   snap.GetWaitEventType(),
		WaitEvent:       snap.GetWaitEvent(),
		BlockedByPid:    uint32(max(snap.GetBlockedByPid(), 0)),
		LockMode:        snap.GetLockMode(),
	}

	if lockWait := snap.GetLockWaitStart(); lockWait != nil {
		at := lockWait.AsTime()
		row.LockWaitStart = &at
	}

	return row
}

const (
	stateActive                   = "active"
	stateIdleInTransaction        = "idle in transaction"
	stateIdleInTransactionAborted = "idle in transaction (aborted)"
)

const (
	statusActive  = querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ACTIVE
	statusIdle    = querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_IDLE
	statusAborted = querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ABORTED
)

type reconstructedEvent struct {
	state         string
	waitEventType string
	waitEvent     string
	lockMode      string
	query         string
	queryTags     map[string]string
	queryStart    time.Time
	firstSeen     time.Time
	lastSeen      time.Time
}

func reconstructedEventFromRow(row clickhouse.TransactionEvent) reconstructedEvent {
	return reconstructedEvent{
		state:         row.State,
		waitEventType: row.WaitEventType,
		waitEvent:     row.WaitEvent,
		lockMode:      row.LockMode,
		query:         row.Query,
		queryTags:     row.QueryTags,
		queryStart:    row.QueryStart,
		firstSeen:     row.FirstSeenAt,
		lastSeen:      row.LastSeenAt,
	}
}

func buildTransactionEvents(start time.Time, events []reconstructedEvent) []*querysheriffv1.TransactionEvent {
	out := make([]*querysheriffv1.TransactionEvent, len(events))
	for i, e := range events {
		from := e.firstSeen
		if i == 0 && start.Before(from) {
			from = start
		}

		to := e.lastSeen
		if i+1 < len(events) {
			to = events[i+1].firstSeen
		}

		out[i] = &querysheriffv1.TransactionEvent{
			From:          timestamppb.New(from),
			To:            timestamppb.New(to),
			Status:        eventStatus(e.state),
			WaitEventType: e.waitEventType,
			WaitEvent:     e.waitEvent,
			LockMode:      e.lockMode,
			Query:         e.query,
			QueryTags:     e.queryTags,
			QueryStart:    timestamppb.New(e.queryStart),
		}
	}

	return out
}

func eventStatus(state string) querysheriffv1.TransactionEventStatus {
	switch state {
	case stateIdleInTransactionAborted:
		return statusAborted
	case stateIdleInTransaction:
		return statusIdle
	default:
		return statusActive
	}
}
