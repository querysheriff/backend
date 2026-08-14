package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/db"
)

const (
	activeState        = "active"
	longQueryThreshold = time.Minute
	blockingThreshold  = 10 * time.Second
	openTxnThreshold   = 10 * time.Minute

	microsPerMilli = 1000
)

type ActivityServer struct {
	pool     *pgxpool.Pool
	queries  *db.Queries
	notifier *alerts.Notifier
}

func NewActivityServer(pool *pgxpool.Pool, notifier *alerts.Notifier) *ActivityServer {
	return &ActivityServer{pool: pool, queries: db.New(pool), notifier: notifier}
}

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

	collectedAt := pgtype.Timestamptz{Time: msg.GetCollectedAt().AsTime(), Valid: true}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.queries.WithTx(tx)

	params := make([]db.RecordTransactionEventParams, len(txnSnapshots))
	for i, snap := range txnSnapshots {
		param, paramErr := transactionEventParams(serverName, collectedAt, snap)
		if paramErr != nil {
			return nil, connect.NewError(connect.CodeInternal, paramErr)
		}

		params[i] = param
	}

	if err = drainRecordBatch(q.RecordTransactionEvent(ctx, params)); err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
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
	// The lock fields sit on the waiting snapshot, so this fires from the victim.
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

	// No state filter: an idle transaction holds its snapshot and locks just as long.
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

// longestOver returns the worst case in the batch: the snapshot whose measured
// duration is the largest of those at or past threshold.
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

// inDatabase is empty for the backends datname is null for.
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
		alerts.HumanDuration(elapsed),
		alerts.QueryPreview(snap.GetQuery()),
		snap.GetPid(),
	)
}

// blockedQueryText names both sides of the wait. The blocker can be missing from
// the batch: a session holding a lock while idle outside a transaction is never sampled.
func blockedQueryText(
	blocked *querysheriffv1.ActivitySnapshot,
	waited time.Duration,
	snapshots []*querysheriffv1.ActivitySnapshot,
) string {
	blockerQuery := "not captured"
	for _, snap := range snapshots {
		if snap.GetPid() == blocked.GetBlockedByPid() {
			blockerQuery = alerts.QueryPreview(snap.GetQuery())

			break
		}
	}

	return fmt.Sprintf(
		"A query%s has been waiting %s for a lock.\n\nwaiting: %s\nblocking: %s\npids: %d blocked by %d",
		inDatabase(blocked.GetDatabaseName()),
		alerts.HumanDuration(waited),
		alerts.QueryPreview(blocked.GetQuery()),
		blockerQuery,
		blocked.GetPid(),
		blocked.GetBlockedByPid(),
	)
}

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

	allowedServers := principal.AllowedServerFilter()

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.queries.ListTransactions(ctx, db.ListTransactionsParams{
		ServerName:     textFilter(msg.GetServerName()),
		DatabaseName:   textFilter(msg.GetDatabaseName()),
		AllowedServers: allowedServers,
		FromTime:       timestamptzFromProto(from),
		ToTime:         timestamptzFromProto(to),
		MinOpen:        pgtype.Interval{Microseconds: msg.GetMinOpenMs() * microsPerMilli, Valid: true},
		SortKey:        transactionSortKey(msg.GetSortColumn()),
		SortDesc:       msg.GetSortDesc(),
		// One extra row answers "is there another page" without a second count query.
		RowLimit:   limit + 1,
		OffsetRows: resolveOffset(msg.GetOffset()),
	})
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

	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}

	eventRows, err := s.queries.ListTransactionEvents(ctx, db.ListTransactionEventsParams{
		TransactionIds: ids,
		AllowedServers: allowedServers,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	eventsByTxn := make(map[int64][]reconstructedEvent, len(rows))
	for _, row := range eventRows {
		event, convErr := reconstructedEventFromRow(row)
		if convErr != nil {
			return nil, connect.NewError(connect.CodeInternal, convErr)
		}
		eventsByTxn[row.TransactionID] = append(eventsByTxn[row.TransactionID], event)
	}

	transactions := make([]*querysheriffv1.Transaction, len(rows))
	for i, row := range rows {
		events := eventsByTxn[row.ID]
		transactions[i] = &querysheriffv1.Transaction{
			Pid:             row.Pid,
			ApplicationName: row.ApplicationName,
			Start:           protoFromTimestamptz(row.XactStart),
			End:             protoFromTimestamptz(row.LastSeenAt),
			Events:          buildTransactionEvents(row.XactStart.Time, events),
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

// activitySeriesScope is the authorized scope and bucket grid shared by the
// bucketed activity charts, which differ only in what they sum.
type activitySeriesScope struct {
	bounds         seriesBounds
	serverName     pgtype.Text
	databaseName   pgtype.Text
	allowedServers []string
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

	// The same grid the query charts use, so a spike lines up across sections.
	return activitySeriesScope{
		bounds:         newSeriesBounds(from.AsTime(), to.AsTime(), time.Now()),
		serverName:     textFilter(serverName),
		databaseName:   textFilter(databaseName),
		allowedServers: principal.AllowedServerFilter(),
	}, nil
}

func (a activitySeriesScope) lockWaitParams() db.LockWaitSeriesParams {
	return db.LockWaitSeriesParams{
		RangeStart:     pgtype.Timestamptz{Time: a.bounds.rangeStart, Valid: true},
		RangeEnd:       pgtype.Timestamptz{Time: a.bounds.anchor, Valid: true},
		Anchor:         pgtype.Timestamptz{Time: a.bounds.anchor, Valid: true},
		Bucket:         pgtype.Interval{Microseconds: a.bounds.bucket.Microseconds(), Valid: true},
		ServerName:     a.serverName,
		DatabaseName:   a.databaseName,
		AllowedServers: a.allowedServers,
	}
}

func (a activitySeriesScope) transactionAgeParams() db.TransactionAgeSeriesParams {
	return db.TransactionAgeSeriesParams{
		RangeStart:     pgtype.Timestamptz{Time: a.bounds.rangeStart, Valid: true},
		RangeEnd:       pgtype.Timestamptz{Time: a.bounds.anchor, Valid: true},
		Anchor:         pgtype.Timestamptz{Time: a.bounds.anchor, Valid: true},
		Bucket:         pgtype.Interval{Microseconds: a.bounds.bucket.Microseconds(), Valid: true},
		ServerName:     a.serverName,
		DatabaseName:   a.databaseName,
		AllowedServers: a.allowedServers,
	}
}

// QueryTransactionAgeSeries returns how old the oldest open transaction was in
// each bucket.
func (s *ActivityServer) QueryTransactionAgeSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryTransactionAgeSeriesRequest],
) (*connect.Response[querysheriffv1.QueryTransactionAgeSeriesResponse], error) {
	msg := req.Msg

	scope, err := s.resolveSeriesScope(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	rows, err := s.queries.TransactionAgeSeries(ctx, scope.transactionAgeParams())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.QueryTransactionAgeSeriesResponse{
		Series:   transactionAgePoints(scope.bounds.bucketEnds(), rows),
		BucketMs: scope.bounds.bucket.Milliseconds(),
	}), nil
}

// transactionAgePoints turns per-end-bucket rows into one point per bucket.
func transactionAgePoints(ends []time.Time, rows []db.TransactionAgeSeriesRow) []*querysheriffv1.TransactionAgePoint {
	endedAge := make(map[int64]float64, len(rows))
	for _, r := range rows {
		endedAge[r.BucketEnd.Time.UnixNano()] = r.EndedAge
	}

	// Rows arrive oldest bucket first; they are folded in from the newest back.
	next := len(rows) - 1
	var oldestOpen time.Time

	points := make([]*querysheriffv1.TransactionAgePoint, len(ends))
	for i, end := range slices.Backward(ends) {
		for next >= 0 && rows[next].BucketEnd.Time.After(end) {
			if start := rows[next].OldestStart.Time; oldestOpen.IsZero() || start.Before(oldestOpen) {
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

func transactionEventParams(
	serverName string,
	collectedAt pgtype.Timestamptz,
	snap *querysheriffv1.ActivitySnapshot,
) (db.RecordTransactionEventParams, error) {
	tags, err := jsonbFromStringMap(snap.GetQueryTags())
	if err != nil {
		return db.RecordTransactionEventParams{}, err
	}

	return db.RecordTransactionEventParams{
		ServerName:      serverName,
		Pid:             snap.GetPid(),
		BackendStart:    timestamptzFromProto(snap.GetBackendStart()),
		XactStart:       timestamptzFromProto(snap.GetXactStart()),
		DatabaseName:    snap.GetDatabaseName(),
		UserName:        snap.GetUserName(),
		ApplicationName: snap.GetApplicationName(),
		CollectedAt:     collectedAt,
		State:           snap.GetState(),
		WaitEventType:   snap.GetWaitEventType(),
		WaitEvent:       snap.GetWaitEvent(),
		QueryStart:      timestamptzFromProto(snap.GetQueryStart()),
		Query:           snap.GetQuery(),
		QueryTags:       tags,
		BlockedByPid:    snap.GetBlockedByPid(),
		LockWaitStart:   timestamptzFromProto(snap.GetLockWaitStart()),
		LockMode:        snap.GetLockMode(),
	}, nil
}

func drainRecordBatch(results *db.RecordTransactionEventBatchResults) error {
	var execErr error

	results.Exec(func(_ int, err error) {
		if err != nil && execErr == nil {
			execErr = err
		}
	})

	if execErr != nil {
		return connect.NewError(connect.CodeInternal, execErr)
	}

	return nil
}
