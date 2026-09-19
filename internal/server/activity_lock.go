package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
)

// QueryLockWaits returns filtered, sorted, paginated lock waits with waiting/blocking details.
// Example: server=prod, limit=50 -> up to 50 lock waits with both involved queries.
func (s *ActivityServer) QueryLockWaits(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryLockWaitsRequest],
) (*connect.Response[querysheriffv1.QueryLockWaitsResponse], error) {
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

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	return connect.NewResponse(&querysheriffv1.QueryLockWaitsResponse{
		Waits:   buildLockWaits(rows),
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

func buildLockWaits(rows []clickhouse.LockWait) []*querysheriffv1.LockWait {
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
			StartedWaiting: timestamppb.New(row.WaitStart),
			LastSeen:       timestamppb.New(row.LastSeen),
			LockMode:       row.LockMode,
		}
	}

	return waits
}

// QueryLockWaitSeries returns total lock-wait time per time bucket.
// Example: 1m buckets -> [{12:01, 20s}, {12:02, 5s}, ...].
func (s *ActivityServer) QueryLockWaitSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryLockWaitSeriesRequest],
) (*connect.Response[querysheriffv1.QueryLockWaitSeriesResponse], error) {
	msg := req.Msg

	scope, err := s.resolveSeriesScope(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	rows, err := s.stats.LockWaitSeries(ctx, scope.transactionScope(), scope.bounds.Bucket, scope.bounds.Anchor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	waited := make(map[int64]float64, len(rows))
	for _, r := range rows {
		waited[r.BucketEnd.UnixNano()] = r.WaitSeconds
	}

	ends := scope.bounds.Ends()
	series := make([]*querysheriffv1.LockWaitPoint, len(ends))
	for i, end := range ends {
		series[i] = &querysheriffv1.LockWaitPoint{At: timestamppb.New(end), WaitSeconds: waited[end.UnixNano()]}
	}

	return connect.NewResponse(&querysheriffv1.QueryLockWaitSeriesResponse{
		Series:   series,
		BucketMs: scope.bounds.Bucket.Milliseconds(),
	}), nil
}
