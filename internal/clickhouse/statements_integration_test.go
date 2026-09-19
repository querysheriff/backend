//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
)

func TestListStatementStatsAggregatesAndRanks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "stats-aggregate")

	busy := f.seedStatement(t, 1, 1, []int64{10, 20, 30}, 2) // 60 calls, 120ms
	quiet := f.seedStatement(t, 2, 1, []int64{1, 1}, 5)      // 2 calls, 10ms
	from, to := f.window()

	rows, err := f.client.ListStatementStats(ctx, clickhouse.ListStatementStatsParams{
		From: from, To: to, ServerName: f.server, DatabaseName: f.database,
		Kinds: []int32{1}, SortKey: "pct_time", SortDesc: true, RowLimit: 10,
	})
	if err != nil {
		t.Fatalf("ListStatementStats: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("got %d statements, want 2", len(rows))
	}

	if rows[0].ID != busy.ID {
		t.Errorf("sorted by pct_time desc put %d first, want the busy statement %d", rows[0].ID, busy.ID)
	}

	if rows[0].Calls != 60 || rows[0].Rows != 120 {
		t.Errorf("busy statement = %d calls / %d rows, want 60/120", rows[0].Calls, rows[0].Rows)
	}

	if rows[0].TotalExecTime != 120 {
		t.Errorf("busy total_exec_time = %v, want 120", rows[0].TotalExecTime)
	}

	if want := 120.0 / 130.0 * 100; !closeTo(rows[0].PctOfTotal, want) {
		t.Errorf("busy pct_of_total = %v, want %v", rows[0].PctOfTotal, want)
	}

	if rows[1].ID != quiet.ID {
		t.Errorf("second row = %d, want the quiet statement %d", rows[1].ID, quiet.ID)
	}
}

func TestListStatementStatsFiltersByKind(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "stats-kind")

	read := f.seedStatement(t, 1, 1, []int64{5}, 1)
	f.seedStatement(t, 2, 2, []int64{5}, 1)
	from, to := f.window()

	rows, err := f.client.ListStatementStats(ctx, clickhouse.ListStatementStatsParams{
		From: from, To: to, ServerName: f.server, DatabaseName: f.database,
		Kinds: []int32{1}, SortKey: "calls", SortDesc: true, RowLimit: 10,
	})
	if err != nil {
		t.Fatalf("ListStatementStats: %v", err)
	}

	if len(rows) != 1 || rows[0].ID != read.ID {
		t.Fatalf("kind filter returned %d rows, want only the kind-1 statement", len(rows))
	}
}

func TestMetricSeriesAgreesWithTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "stats-series")
	f.seedStatement(t, 1, 1, []int64{10, 20, 30}, 2)
	from, to := f.window()

	buckets, err := f.client.StatementMetricSeries(ctx, clickhouse.MetricSeriesParams{
		RangeStart: from, RangeEnd: to, Bucket: time.Minute,
		ServerName: f.server, DatabaseName: f.database,
	})
	if err != nil {
		t.Fatalf("StatementMetricSeries: %v", err)
	}

	var seriesCalls int64

	var seriesExec float64

	for _, b := range buckets {
		seriesCalls += b.Calls
		seriesExec += b.TotalExecTime
	}

	if seriesCalls != 60 {
		t.Errorf("series calls = %d, want 60 to match the seeded deltas", seriesCalls)
	}

	if !closeTo(seriesExec, 120) {
		t.Errorf("series exec time = %v, want 120", seriesExec)
	}
}

func closeTo(got, want float64) bool {
	diff := got - want
	if diff < 0 {
		diff = -diff
	}

	return diff < 1e-6
}

func TestStatementIDsNilMatchesAllEmptyMatchesNone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "stats-id-filter")
	f.seedStatement(t, 1, 1, []int64{10}, 2)
	from, to := f.window()

	params := func(ids []uint64) clickhouse.ListStatementStatsParams {
		return clickhouse.ListStatementStatsParams{
			From: from, To: to, ServerName: f.server, DatabaseName: f.database,
			Kinds: []int32{1}, SortKey: "pct_time", SortDesc: true, RowLimit: 10,
			StatementIDs: ids,
		}
	}

	unfiltered, err := f.client.ListStatementStats(ctx, params(nil))
	if err != nil {
		t.Fatalf("ListStatementStats(nil): %v", err)
	}

	if len(unfiltered) != 1 {
		t.Errorf("a nil id filter returned %d statements, want every one (1)", len(unfiltered))
	}

	none, err := f.client.ListStatementStats(ctx, params([]uint64{}))
	if err != nil {
		t.Fatalf("ListStatementStats([]): %v", err)
	}

	if len(none) != 0 {
		t.Errorf("an empty id filter returned %d statements, want none", len(none))
	}
}

func TestLatencyAndMetricSeriesShareBucketEnds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "series-bucket-ends")
	f.seedStatement(t, 1, 1, []int64{5, 7, 9, 11}, 3)
	from, to := f.window()

	metric, err := f.client.StatementMetricSeries(ctx, clickhouse.MetricSeriesParams{
		RangeStart: from, RangeEnd: to, Bucket: time.Minute,
		ServerName: f.server, DatabaseName: f.database,
	})
	if err != nil {
		t.Fatalf("StatementMetricSeries: %v", err)
	}

	latency, err := f.client.StatementLatencySeries(ctx, clickhouse.LatencySeriesParams{
		RangeStart: from, RangeEnd: to, Bucket: time.Minute,
		ServerName: f.server, DatabaseName: f.database, UtilityKind: 3,
	})
	if err != nil {
		t.Fatalf("StatementLatencySeries: %v", err)
	}

	if len(metric) == 0 || len(latency) == 0 {
		t.Fatalf("got %d metric and %d latency buckets, want both populated", len(metric), len(latency))
	}

	ends := map[time.Time]bool{}
	for _, bucket := range metric {
		ends[bucket.BucketEnd] = true
	}

	for _, bucket := range latency {
		if !ends[bucket.BucketEnd] {
			t.Errorf("latency bucket %s has no matching metric bucket end; the two charts would disagree",
				bucket.BucketEnd)
		}
	}
}
