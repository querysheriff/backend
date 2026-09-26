package server

import (
	"context"
	"slices"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/timeseries"
)

type ActivityServer struct {
	stats    *clickhouse.Client
	notifier *alerts.Notifier
}

func NewActivityServer(stats *clickhouse.Client, notifier *alerts.Notifier) *ActivityServer {
	return &ActivityServer{stats: stats, notifier: notifier}
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

	s.notifier.CheckActivity(serverName, collectedAt, txnSnapshots)

	return connect.NewResponse(&querysheriffv1.ReportActivityResponse{}), nil
}

// ListTransactions returns filtered, sorted, paginated transactions with reconstructed events.
// Example: minOpen=30s, limit=50 -> transactions open for at least 30s.
func (s *ActivityServer) ListTransactions(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListTransactionsRequest],
) (*connect.Response[querysheriffv1.ListTransactionsResponse], error) {
	msg := req.Msg

	from, to := msg.GetFrom(), msg.GetTo()
	if err := authorizeDatabaseQuery(ctx, msg.GetServerName(), msg.GetDatabaseName(), from, to); err != nil {
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

	rows, hasMore := trimPage(rows, limit)
	if len(rows) == 0 {
		return connect.NewResponse(&querysheriffv1.ListTransactionsResponse{}), nil
	}

	eventRows, err := s.stats.ListTransactionEvents(ctx, scope, rows)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	eventsByTxn := make(map[uint64][]clickhouse.TransactionEvent, len(rows))
	for _, row := range eventRows {
		eventsByTxn[row.TransactionID] = append(eventsByTxn[row.TransactionID], row)
	}

	transactions := make([]*querysheriffv1.Transaction, len(rows))
	for i, row := range rows {
		events := eventsByTxn[row.ID]
		transactions[i] = &querysheriffv1.Transaction{
			Pid:             signedPid(row.Pid),
			ApplicationName: row.ApplicationName,
			StartedAt:       timestamppb.New(row.XactStart),
			LastSeenAt:      timestamppb.New(row.LastSeenAt),
			Events:          transactionEventsProto(row.XactStart, events),
		}
	}

	return connect.NewResponse(&querysheriffv1.ListTransactionsResponse{
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

// activitySeries authorizes a chart request and returns its chart bounds and the matching scope.
func activitySeries(
	ctx context.Context,
	serverName, databaseName string,
	from, to *timestamppb.Timestamp,
) (clickhouse.TransactionScope, timeseries.Bounds, error) {
	if err := authorizeDatabaseQuery(ctx, serverName, databaseName, from, to); err != nil {
		return clickhouse.TransactionScope{}, timeseries.Bounds{}, err
	}

	bounds := timeseries.NewBounds(from.AsTime(), to.AsTime(), time.Now())

	return clickhouse.TransactionScope{
		ServerName:   serverName,
		DatabaseName: databaseName,
		From:         bounds.RangeStart,
		To:           bounds.Anchor,
	}, bounds, nil
}

// GetTransactionAgeSeries returns the oldest transaction age per time bucket.
// Example: 1m buckets -> [{12:01, 120s}, {12:02, 180s}, ...].
func (s *ActivityServer) GetTransactionAgeSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetTransactionAgeSeriesRequest],
) (*connect.Response[querysheriffv1.GetTransactionAgeSeriesResponse], error) {
	msg := req.Msg

	scope, bounds, err := activitySeries(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	rows, err := s.stats.TransactionAgeSeries(ctx, scope, bounds.Bucket)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.GetTransactionAgeSeriesResponse{
		AgeSeconds: transactionAgePointsProto(bounds.Ends(), rows),
		BucketMs:   bounds.Bucket.Milliseconds(),
	}), nil
}

func transactionAgePointsProto(
	ends []time.Time,
	rows []clickhouse.TransactionAgeBin,
) []*querysheriffv1.MetricPoint {
	endedAge := make(map[int64]float64, len(rows))
	for _, r := range rows {
		endedAge[r.BucketEnd.UnixNano()] = r.EndedAge
	}

	next := len(rows) - 1
	var oldestOpen time.Time

	points := make([]*querysheriffv1.MetricPoint, len(ends))
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

		points[i] = metricPointProto(end, age)
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

func transactionEventsProto(start time.Time, events []clickhouse.TransactionEvent) []*querysheriffv1.TransactionEvent {
	out := make([]*querysheriffv1.TransactionEvent, len(events))
	for i, e := range events {
		from := e.FirstSeenAt
		if i == 0 && start.Before(from) {
			from = start
		}

		to := e.LastSeenAt
		if i+1 < len(events) {
			to = events[i+1].FirstSeenAt
		}

		out[i] = &querysheriffv1.TransactionEvent{
			From:       timestamppb.New(from),
			To:         timestamppb.New(to),
			Status:     transactionEventStatusProto(e.State),
			Query:      e.Query,
			QueryStart: timestamppb.New(e.QueryStart),
			QueryTags:  e.QueryTags,
			WaitEvent:  e.WaitEvent,
			LockMode:   e.LockMode,
		}
	}

	return out
}

func transactionEventStatusProto(state string) querysheriffv1.TransactionEventStatus {
	switch state {
	case "idle in transaction (aborted)":
		return querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ABORTED
	case "idle in transaction":
		return querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_IDLE
	default:
		return querysheriffv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ACTIVE
	}
}
