//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
)

func TestTopStatementsAppliesThresholds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "alerts-slow")

	slow := f.seedStatement(t, 1, 1, []int64{20, 20}, 1500) // 40 calls at 1500ms
	f.seedStatement(t, 2, 1, []int64{20, 20}, 5)            // fast
	f.seedStatement(t, 3, 1, []int64{1}, 1500)              // slow but rare

	rows, err := f.client.TopStatements(ctx, clickhouse.TopStatementsParams{
		ServerName: f.server, From: f.base.Add(-time.Minute),
		MinCalls: 10, MinAvgMs: 1000, MaxRows: 10,
	})
	if err != nil {
		t.Fatalf("TopStatements: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("got %d slow statements, want only the one over both thresholds", len(rows))
	}

	if rows[0].ID != slow.ID {
		t.Errorf("reported statement %d, want %d", rows[0].ID, slow.ID)
	}

	if rows[0].Calls != 40 {
		t.Errorf("calls = %d, want 40", rows[0].Calls)
	}
}

func TestWeeklyReportPartsReadBack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "alerts-weekly")
	f.seedStatement(t, 1, 1, []int64{10, 10, 10}, 40)

	from, to := f.window()

	calls, err := f.client.CallsBetween(ctx, f.server, from, to)
	if err != nil {
		t.Fatalf("CallsBetween: %v", err)
	}

	if calls != 30 {
		t.Errorf("CallsBetween = %d, want 30", calls)
	}

	top, err := f.client.TopStatements(
		ctx,
		clickhouse.TopStatementsParams{ServerName: f.server, From: from, MaxRows: 5},
	)
	if err != nil {
		t.Fatalf("TopStatements: %v", err)
	}

	if len(top) != 1 || top[0].Calls != 30 {
		t.Fatalf("top statements = %+v, want one row with 30 calls", top)
	}

	quantile, err := f.client.LatencyQuantile(ctx, f.server, from, to, 0.99, 4)
	if err != nil {
		t.Fatalf("LatencyQuantile: %v", err)
	}

	if quantile <= 0 {
		t.Errorf("p99 = %v, want a positive latency from the seeded deltas", quantile)
	}
}
