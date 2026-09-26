package server

import (
	"context"
	"sort"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/timeseries"
)

// GetLogSeries returns log counts grouped into time buckets, levels, and categories.
// Example: 1m buckets -> 12:01: ERROR=5, WARNING=3.
func (s *LogServer) GetLogSeries(
	ctx context.Context,
	req *connect.Request[querysheriffv1.GetLogSeriesRequest],
) (*connect.Response[querysheriffv1.GetLogSeriesResponse], error) {
	msg := req.Msg

	if err := authorizeServerQuery(ctx, msg.GetServerName(), msg.GetFrom(), msg.GetTo()); err != nil {
		return nil, err
	}

	bounds := timeseries.NewBounds(msg.GetFrom().AsTime(), msg.GetTo().AsTime(), time.Now())

	rows, err := s.stats.LogEventHistogram(ctx, clickhouse.LogFilter{
		ServerName: msg.GetServerName(),
		From:       bounds.RangeStart,
		To:         bounds.Anchor,
	}, bounds.Bucket)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(s.logSeriesProto(bounds, rows)), nil
}

func (s *LogServer) logSeriesProto(
	bounds timeseries.Bounds,
	rows []clickhouse.LogHistogramBin,
) *querysheriffv1.GetLogSeriesResponse {
	ends := bounds.Ends()
	slotOf := make(map[time.Time]int, len(ends))
	for i, end := range ends {
		slotOf[end.UTC()] = i
	}

	perBucketLevel := make([]map[int32]int64, len(ends))
	perBucketClass := make([]map[int32]int64, len(ends))
	levelTotals := map[int32]int64{}

	for _, row := range rows {
		idx, ok := slotOf[row.BucketEnd.UTC()]
		if !ok {
			continue
		}

		if perBucketLevel[idx] == nil {
			perBucketLevel[idx] = map[int32]int64{}
			perBucketClass[idx] = map[int32]int64{}
		}

		perBucketLevel[idx][row.LogLevel] += row.Count
		perBucketClass[idx][row.Classification] += row.Count
		levelTotals[row.LogLevel] += row.Count
	}

	buckets := make([]*querysheriffv1.LogHistogramBucket, len(ends))
	for i, end := range ends {
		buckets[i] = &querysheriffv1.LogHistogramBucket{
			At:         timestamppb.New(end),
			Levels:     levelCountsProto(perBucketLevel[i]),
			Categories: s.categoryCountsProto(perBucketClass[i]),
		}
	}

	return &querysheriffv1.GetLogSeriesResponse{
		Buckets:     buckets,
		LevelTotals: levelCountsProto(levelTotals),
		BucketMs:    bounds.Bucket.Milliseconds(),
	}
}

func (s *LogServer) categoryCountsProto(classes map[int32]int64) []*querysheriffv1.LogCategoryCount {
	grouped := map[querysheriffv1.LogCategory]map[int32]int64{}

	for classification, count := range classes {
		category := s.categories.Of(querysheriffv1.LogEvent_LogClassification(classification))
		if grouped[category] == nil {
			grouped[category] = map[int32]int64{}
		}
		grouped[category][classification] += count
	}

	out := make([]*querysheriffv1.LogCategoryCount, 0, len(grouped))

	for category, classifications := range grouped {
		var total int64
		for _, count := range classifications {
			total += count
		}

		out = append(out, &querysheriffv1.LogCategoryCount{
			Category:        category,
			Count:           total,
			Classifications: classificationCountsProto(classifications),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].GetCategory() < out[j].GetCategory() })

	return out
}

func classificationCountsProto(counts map[int32]int64) []*querysheriffv1.LogClassificationCount {
	out := make([]*querysheriffv1.LogClassificationCount, 0, len(counts))
	for classification, count := range counts {
		out = append(out, &querysheriffv1.LogClassificationCount{
			Classification: querysheriffv1.LogEvent_LogClassification(classification),
			Count:          count,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].GetCount() != out[j].GetCount() {
			return out[i].GetCount() > out[j].GetCount()
		}

		return out[i].GetClassification() < out[j].GetClassification()
	})

	return out
}

func levelCountsProto(counts map[int32]int64) []*querysheriffv1.LogLevelCount {
	out := make([]*querysheriffv1.LogLevelCount, 0, len(counts))
	for level, count := range counts {
		out = append(out, &querysheriffv1.LogLevelCount{
			Level: querysheriffv1.LogEvent_LogLevel(level),
			Count: count,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetLevel() < out[j].GetLevel() })

	return out
}
