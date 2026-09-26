package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
)

// ListLockWaits returns filtered, sorted, paginated lock waits with waiting/blocking details.
// Example: server=prod, limit=50 -> up to 50 lock waits with both involved queries.
func (s *ActivityServer) ListLockWaits(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ListLockWaitsRequest],
) (*connect.Response[querysheriffv1.ListLockWaitsResponse], error) {
	msg := req.Msg

	from, to := msg.GetFrom(), msg.GetTo()
	if err := authorizeDatabaseQuery(ctx, msg.GetServerName(), msg.GetDatabaseName(), from, to); err != nil {
		return nil, err
	}

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.stats.ListLockWaits(ctx, clickhouse.TransactionScope{
		ServerName:   msg.GetServerName(),
		DatabaseName: msg.GetDatabaseName(),
		From:         from.AsTime(),
		To:           to.AsTime(),
	}, lockWaitSortKey(msg.GetSortColumn()), msg.GetSortDesc(),
		limit+1, resolveOffset(msg.GetOffset()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	rows, hasMore := trimPage(rows, limit)

	return connect.NewResponse(&querysheriffv1.ListLockWaitsResponse{
		Waits:   lockWaitsProto(rows),
		HasMore: hasMore,
	}), nil
}

func lockWaitSortKey(col querysheriffv1.LockWaitSortColumn) string {
	switch col {
	case querysheriffv1.LockWaitSortColumn_LOCK_WAIT_SORT_COLUMN_STARTED:
		return "started"
	case querysheriffv1.LockWaitSortColumn_LOCK_WAIT_SORT_COLUMN_WAITED,
		querysheriffv1.LockWaitSortColumn_LOCK_WAIT_SORT_COLUMN_UNSPECIFIED:
		return "waited"
	}

	return "waited"
}

func lockWaitsProto(rows []clickhouse.LockWait) []*querysheriffv1.LockWait {
	waits := make([]*querysheriffv1.LockWait, len(rows))
	for i, row := range rows {
		waits[i] = &querysheriffv1.LockWait{
			Waiting: &querysheriffv1.LockParty{
				Pid:             signedPid(row.WaitingPid),
				ApplicationName: row.WaitingApp,
				Query:           row.WaitingQuery,
				QueryTags:       row.WaitingTags,
			},
			Blocking: &querysheriffv1.LockParty{
				Pid:             signedPid(row.BlockedByPid),
				ApplicationName: row.BlockingApp,
				Query:           row.BlockingQuery,
				QueryTags:       row.BlockingTags,
			},
			LockMode:   row.LockMode,
			StartedAt:  timestamppb.New(row.WaitStart),
			LastSeenAt: timestamppb.New(row.LastSeen),
		}
	}

	return waits
}

// GetLockWaitSeries returns total lock-wait time per time bucket.
// Example: 1m buckets -> [{12:01, 20s}, {12:02, 5s}, ...].
func (s *ActivityServer) GetLockWaitSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetLockWaitSeriesRequest],
) (*connect.Response[querysheriffv1.GetLockWaitSeriesResponse], error) {
	msg := req.Msg

	scope, bounds, err := activitySeries(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	rows, err := s.stats.LockWaitSeries(ctx, scope, bounds.Bucket)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	waited := make(map[int64]float64, len(rows))
	for _, r := range rows {
		waited[r.BucketEnd.UnixNano()] = r.WaitSeconds
	}

	ends := bounds.Ends()
	series := make([]*querysheriffv1.MetricPoint, len(ends))
	for i, end := range ends {
		series[i] = metricPointProto(end, waited[end.UnixNano()])
	}

	return connect.NewResponse(&querysheriffv1.GetLockWaitSeriesResponse{
		WaitSeconds: series,
		BucketMs:    bounds.Bucket.Milliseconds(),
	}), nil
}
