//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
)

func TestStatementSamplesHideTheDuplicateOfAnExplainedSample(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := newFixture(t, "samples-superseded")
	statementID := clickhouse.StatementID(f.server, f.database, "app", 1)

	sample := func(index int64, after time.Duration, plan string) clickhouse.Sample {
		return clickhouse.Sample{
			ID: clickhouse.SampleID(f.server, f.base, "SELECT 1", 100, index), CollectedAt: f.base,
			ServerName: f.server, DatabaseName: f.database, OccurredAt: f.base.Add(after),
			StatementID: statementID, Query: "SELECT 1", DurationMs: 100, ExplainPlanJSON: plan,
		}
	}

	duplicate, explained, later := sample(
		0,
		0,
		"",
	), sample(
		1,
		500*time.Millisecond,
		"{}",
	), sample(
		2,
		10*time.Second,
		"",
	)

	if err := f.client.InsertSamples(ctx, []clickhouse.Sample{duplicate, explained, later}); err != nil {
		t.Fatalf("InsertSamples: %v", err)
	}

	from, to := f.window()

	got, err := f.client.ListStatementSamples(ctx, clickhouse.ListSamplesParams{
		ServerName: f.server, DatabaseName: f.database, StatementID: statementID,
		From: from, To: to, SortKey: "at", SortDesc: true, RowLimit: 10,
	})
	if err != nil {
		t.Fatalf("ListStatementSamples: %v", err)
	}

	if len(got) != 2 || got[0].ID != later.ID || got[1].ID != explained.ID || !got[1].HasPlan {
		t.Errorf("samples = %+v, want the later sample then the explained one, without its plan-less duplicate", got)
	}
}
