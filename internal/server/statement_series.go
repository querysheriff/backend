package server

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/timeseries"
)

// GetStatementSeries returns calls and average execution/I-O time per time bucket for a statement or database.
// Example: 1m buckets -> Calls=[12:01: 120], AvgMs=[12:01: 12.5], AvgIoMs=[12:01: 3.2].
func (s *StatementServer) GetStatementSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetStatementSeriesRequest],
) (*connect.Response[querysheriffv1.GetStatementSeriesResponse], error) {
	msg := req.Msg

	if err := authorizeDatabaseQuery(
		ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo()); err != nil {
		return nil, err
	}

	bounds := timeseries.NewBounds(msg.GetFrom().AsTime(), msg.GetTo().AsTime(), time.Now())

	buckets, err := s.stats.StatementMetricSeries(ctx, clickhouse.MetricSeriesParams{
		RangeStart:   bounds.RangeStart,
		RangeEnd:     bounds.Anchor,
		Bucket:       bounds.Bucket,
		ServerName:   msg.GetServerName(),
		DatabaseName: msg.GetDatabaseName(),
		StatementID:  msg.GetStatementId(),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &querysheriffv1.GetStatementSeriesResponse{BucketMs: bounds.Bucket.Milliseconds()}
	for _, b := range buckets {
		resp.Calls = append(resp.Calls, metricPointProto(b.BucketEnd, float64(b.Calls)))
		resp.AvgMs = append(resp.AvgMs, metricPointProto(b.BucketEnd, avgExecTime(b.TotalExecTime, b.Calls)))
		resp.AvgIoMs = append(resp.AvgIoMs, metricPointProto(b.BucketEnd, avgExecTime(b.TotalIoTime, b.Calls)))
	}

	return connect.NewResponse(resp), nil
}

// GetLatencySeries returns a database's call-weighted P90/P95/P99 latency per time bucket.
// Example: 1m buckets -> P90Ms=[12:01: 20], P95Ms=[25], P99Ms=[50].
func (s *StatementServer) GetLatencySeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetLatencySeriesRequest],
) (*connect.Response[querysheriffv1.GetLatencySeriesResponse], error) {
	msg := req.Msg

	if err := authorizeDatabaseQuery(
		ctx, msg.GetServerName(), msg.GetDatabaseName(), msg.GetFrom(), msg.GetTo()); err != nil {
		return nil, err
	}

	bounds := timeseries.NewBounds(msg.GetFrom().AsTime(), msg.GetTo().AsTime(), time.Now())

	buckets, err := s.stats.StatementLatencySeries(ctx, clickhouse.LatencySeriesParams{
		RangeStart:   bounds.RangeStart,
		RangeEnd:     bounds.Anchor,
		Bucket:       bounds.Bucket,
		ServerName:   msg.GetServerName(),
		DatabaseName: msg.GetDatabaseName(),
		UtilityKind:  int32(querysheriffv1.QueryKind_QUERY_KIND_OTHERS),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &querysheriffv1.GetLatencySeriesResponse{BucketMs: bounds.Bucket.Milliseconds()}
	for _, b := range buckets {
		resp.P90Ms = append(resp.P90Ms, metricPointProto(b.BucketEnd, b.P90))
		resp.P95Ms = append(resp.P95Ms, metricPointProto(b.BucketEnd, b.P95))
		resp.P99Ms = append(resp.P99Ms, metricPointProto(b.BucketEnd, b.P99))
	}

	return connect.NewResponse(resp), nil
}

func avgExecTime(totalExecTime float64, calls int64) float64 {
	if calls <= 0 {
		return 0
	}

	return totalExecTime / float64(calls)
}

func metricPointProto(at time.Time, value float64) *querysheriffv1.MetricPoint {
	return &querysheriffv1.MetricPoint{At: timestamppb.New(at), Value: value}
}
