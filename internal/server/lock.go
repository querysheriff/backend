package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/db"
)

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

	rows, err := s.queries.ListLockWaits(ctx, db.ListLockWaitsParams{
		ServerName:     textFilter(msg.GetServerName()),
		DatabaseName:   textFilter(msg.GetDatabaseName()),
		AllowedServers: principal.AllowedServerFilter(),
		FromTime:       timestamptzFromProto(from),
		ToTime:         timestamptzFromProto(to),
		SortKey:        lockWaitSortKey(msg.GetSortColumn()),
		SortDesc:       msg.GetSortDesc(),
		RowLimit:       limit + 1,
		OffsetRows:     resolveOffset(msg.GetOffset()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	waits, err := buildLockWaits(rows)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.QueryLockWaitsResponse{
		Waits:   waits,
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

func buildLockWaits(rows []db.ListLockWaitsRow) ([]*querysheriffv1.LockWait, error) {
	waits := make([]*querysheriffv1.LockWait, len(rows))
	for i, row := range rows {
		waitingTags, err := protoFromJSONB(row.WaitingTags)
		if err != nil {
			return nil, err
		}

		blockingTags, err := protoFromJSONB(row.BlockingTags)
		if err != nil {
			return nil, err
		}

		waits[i] = &querysheriffv1.LockWait{
			Waiting: &querysheriffv1.LockParty{
				Pid:             row.WaitingPid,
				ApplicationName: row.WaitingApp,
				Query:           row.WaitingQuery,
				QueryTags:       waitingTags,
			},
			Blocking: &querysheriffv1.LockParty{
				Pid:             protoFromInt4(row.BlockedByPid),
				ApplicationName: row.BlockingApp,
				Query:           row.BlockingQuery,
				QueryTags:       blockingTags,
			},
			StartedWaiting: protoFromTimestamptz(row.WaitStart),
			LastSeen:       protoFromTimestamptz(row.LastSeen),
			LockMode:       protoFromText(row.LockMode),
		}
	}

	return waits, nil
}

func (s *ActivityServer) QueryLockWaitSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryLockWaitSeriesRequest],
) (*connect.Response[querysheriffv1.QueryLockWaitSeriesResponse], error) {
	msg := req.Msg

	scope, err := s.resolveSeriesScope(ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	rows, err := s.queries.LockWaitSeries(ctx, scope.lockWaitParams())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	waited := make(map[int64]float64, len(rows))
	for _, r := range rows {
		waited[r.BucketEnd.Time.UnixNano()] = r.WaitSeconds
	}

	ends := scope.bounds.bucketEnds()
	series := make([]*querysheriffv1.LockWaitPoint, len(ends))
	for i, end := range ends {
		series[i] = &querysheriffv1.LockWaitPoint{At: timestamppb.New(end), WaitSeconds: waited[end.UnixNano()]}
	}

	return connect.NewResponse(&querysheriffv1.QueryLockWaitSeriesResponse{
		Series:   series,
		BucketMs: scope.bounds.bucket.Milliseconds(),
	}), nil
}
