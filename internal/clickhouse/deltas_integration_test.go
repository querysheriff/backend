//go:build integration

package clickhouse_test

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
)

func TestTopStatementsRanksByTotalTimeAndLimits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "top-statements")

	busiest := f.seedStatement(t, 1, 1, []int64{20, 20}, 1500) // 60s in total
	second := f.seedStatement(t, 2, 1, []int64{1000}, 5)       // 5s
	f.seedStatement(t, 3, 1, []int64{1}, 1500)                 // 1.5s

	samples := make([]clickhouse.Sample, 0, 3)
	for i, sampled := range []struct {
		at   time.Time
		tags map[string]string
	}{
		{f.base, map[string]string{"op": "B", "process_id": "1"}},
		{f.base.Add(time.Minute), map[string]string{"op": "A"}},
		{f.base.Add(-time.Hour), map[string]string{"op": "before-the-window"}},
	} {
		samples = append(samples, clickhouse.Sample{
			ID: clickhouse.SampleID(f.server, sampled.at, "SELECT 1", 1, int64(i)), CollectedAt: sampled.at,
			ServerName: f.server, DatabaseName: f.database, StatementID: busiest.ID,
			Query: "SELECT 1", DurationMs: 1, Tags: sampled.tags,
		})
	}

	if err := f.client.InsertSamples(ctx, samples); err != nil {
		t.Fatalf("InsertSamples: %v", err)
	}

	rows, err := f.client.TopStatements(ctx, f.server, f.base.Add(-time.Minute), 2)
	if err != nil {
		t.Fatalf("TopStatements: %v", err)
	}

	if len(rows) != 2 || rows[0].ID != busiest.ID || rows[1].ID != second.ID {
		t.Fatalf("top statements = %+v, want the two with the most execution time, busiest first", rows)
	}

	if rows[0].Calls != 40 || rows[0].AvgMs != 1500 {
		t.Errorf("busiest = %d calls averaging %vms, want 40 calls averaging 1500ms", rows[0].Calls, rows[0].AvgMs)
	}

	for _, values := range rows[0].Tags {
		slices.Sort(values)
	}

	if want := map[string][]string{"op": {"A", "B"}}; !maps.EqualFunc(rows[0].Tags, want, slices.Equal) {
		t.Errorf("busiest tags = %v, want %v: every value sampled in the window, without *_id keys", rows[0].Tags, want)
	}
}
