//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"
)

func TestTopStatementsRanksByTotalTimeAndLimits(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "top-statements")

	busiest := f.seedStatement(t, 1, 1, []int64{20, 20}, 1500) // 60s in total
	second := f.seedStatement(t, 2, 1, []int64{1000}, 5)       // 5s
	f.seedStatement(t, 3, 1, []int64{1}, 1500)                 // 1.5s

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
}
