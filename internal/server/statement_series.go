package server

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/latency"
	"github.com/querysheriff/backend/internal/timeseries"
)

type seriesRequest struct {
	statementID              uint64
	serverName, databaseName pgtype.Text
	from, to                 time.Time
}

func (s *StatementServer) resolveSeriesRequest(
	ctx context.Context,
	scope *querysheriffv1.SeriesScope,
) (seriesRequest, error) {
	serverName, databaseName := scope.GetServerName(), scope.GetDatabaseName()

	if id := scope.GetStatementId(); id != 0 && (serverName == "" || databaseName == "") {
		resolved, resolvedDatabase, err := s.stats.StatementScope(ctx, id)
		if err != nil {
			return seriesRequest{}, statementLookupError(id, err)
		}

		serverName, databaseName = resolved, resolvedDatabase
	}

	if err := s.authorizeStatementQuery(
		ctx, serverName, databaseName, scope.GetFrom(), scope.GetTo()); err != nil {
		return seriesRequest{}, err
	}

	return seriesRequest{
		statementID:  scope.GetStatementId(),
		serverName:   textFilter(serverName),
		databaseName: textFilter(databaseName),
		from:         scope.GetFrom().AsTime(),
		to:           scope.GetTo().AsTime(),
	}, nil
}

// QueryStatementCallsSeries returns statement call counts grouped by time bucket.
// Example: 1m buckets -> [{12:01, 120}, {12:02, 95}, ...].
func (s *StatementServer) QueryStatementCallsSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryStatementCallsSeriesRequest],
) (*connect.Response[querysheriffv1.QueryStatementCallsSeriesResponse], error) {
	scope, err := s.resolveSeriesRequest(ctx, req.Msg.GetScope())
	if err != nil {
		return nil, err
	}

	series, bucket, err := s.statementSums(ctx, scope)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.QueryStatementCallsSeriesResponse{
		Calls:    series.calls,
		BucketMs: bucket.Milliseconds(),
	}), nil
}

// QueryStatementTimingSeries returns average execution and I/O time per time bucket.
// Example: 1m buckets -> Avg=[12:01: 12.5ms], AvgIo=[12:01: 3.2ms].
func (s *StatementServer) QueryStatementTimingSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryStatementTimingSeriesRequest],
) (*connect.Response[querysheriffv1.QueryStatementTimingSeriesResponse], error) {
	scope, err := s.resolveSeriesRequest(ctx, req.Msg.GetScope())
	if err != nil {
		return nil, err
	}

	series, bucket, err := s.statementSums(ctx, scope)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.QueryStatementTimingSeriesResponse{
		Avg:      series.avg,
		AvgIo:    series.avgIo,
		BucketMs: bucket.Milliseconds(),
	}), nil
}

// QueryStatementPercentileSeries returns P90/P95/P99 latency per time bucket.
// Example: 1m buckets -> P90=[12:01: 20ms], P95=[25ms], P99=[50ms].
func (s *StatementServer) QueryStatementPercentileSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryStatementPercentileSeriesRequest],
) (*connect.Response[querysheriffv1.QueryStatementPercentileSeriesResponse], error) {
	scope, err := s.resolveSeriesRequest(ctx, req.Msg.GetScope())
	if err != nil {
		return nil, err
	}

	series, bucket, err := s.statementPercentiles(
		ctx, scope.serverName, scope.databaseName, scope.from, scope.to,
	)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&querysheriffv1.QueryStatementPercentileSeriesResponse{
		P90:      series.p90,
		P95:      series.p95,
		P99:      series.p99,
		BucketMs: bucket.Milliseconds(),
	}), nil
}

type sumSeries struct {
	calls, avg, avgIo *querysheriffv1.StatementMetric
}

func (s *StatementServer) statementSums(
	ctx context.Context,
	scope seriesRequest,
) (sumSeries, time.Duration, error) {
	bounds := timeseries.NewBounds(scope.from, scope.to, time.Now())

	buckets, err := s.stats.StatementMetricSeries(ctx, clickhouse.MetricSeriesParams{
		RangeStart:   bounds.RangeStart,
		RangeEnd:     bounds.Anchor,
		Bucket:       bounds.Bucket,
		ServerName:   scope.serverName.String,
		DatabaseName: scope.databaseName.String,
		StatementID:  scope.statementID,
	})
	if err != nil {
		return sumSeries{}, 0, connect.NewError(connect.CodeInternal, err)
	}

	n := len(buckets)
	calls := make([]*querysheriffv1.MetricPoint, n)
	avg := make([]*querysheriffv1.MetricPoint, n)
	avgIo := make([]*querysheriffv1.MetricPoint, n)

	for i, b := range buckets {
		at := timestamppb.New(b.BucketEnd)
		calls[i] = &querysheriffv1.MetricPoint{At: at, Value: float64(b.Calls)}
		avg[i] = &querysheriffv1.MetricPoint{At: at, Value: avgExecTime(b.TotalExecTime, b.Calls)}
		avgIo[i] = &querysheriffv1.MetricPoint{At: at, Value: avgExecTime(b.TotalIoTime, b.Calls)}
	}

	return sumSeries{
		calls: statementMetric(calls),
		avg:   statementMetric(avg),
		avgIo: statementMetric(avgIo),
	}, bounds.Bucket, nil
}

type percentileSeries struct {
	p90, p95, p99 *querysheriffv1.StatementMetric
}

func (s *StatementServer) statementPercentiles(
	ctx context.Context,
	serverName, databaseName pgtype.Text,
	from, to time.Time,
) (percentileSeries, time.Duration, error) {
	bounds := timeseries.NewBounds(from, to, time.Now())

	rolled, err := s.stats.StatementLatencySeries(ctx, clickhouse.LatencySeriesParams{
		RangeStart:   bounds.RangeStart,
		RangeEnd:     bounds.Anchor,
		Bucket:       bounds.Bucket,
		ServerName:   serverName.String,
		DatabaseName: databaseName.String,
		UtilityKind:  int32(querysheriffv1.QueryKind_QUERY_KIND_OTHERS),
	})
	if err != nil {
		return percentileSeries{}, 0, connect.NewError(connect.CodeInternal, err)
	}

	samples := make([]latency.Sample, len(rolled))
	for i, r := range rolled {
		samples[i] = latency.Sample{BucketEnd: r.BucketEnd, Bins: r.Bins, Weights: r.Weights}
	}

	buckets := latency.Percentiles(samples)

	p90 := make([]*querysheriffv1.MetricPoint, len(buckets))
	p95 := make([]*querysheriffv1.MetricPoint, len(buckets))
	p99 := make([]*querysheriffv1.MetricPoint, len(buckets))

	for i, b := range buckets {
		at := timestamppb.New(b.End)
		p90[i] = &querysheriffv1.MetricPoint{At: at, Value: b.P90}
		p95[i] = &querysheriffv1.MetricPoint{At: at, Value: b.P95}
		p99[i] = &querysheriffv1.MetricPoint{At: at, Value: b.P99}
	}

	return percentileSeries{
		p90: statementMetric(p90),
		p95: statementMetric(p95),
		p99: statementMetric(p99),
	}, bounds.Bucket, nil
}

func avgExecTime(totalExecTime float64, calls int64) float64 {
	if calls <= 0 {
		return 0
	}

	return totalExecTime / float64(calls)
}

func statementMetric(series []*querysheriffv1.MetricPoint) *querysheriffv1.StatementMetric {
	return &querysheriffv1.StatementMetric{Series: series}
}
