//go:build integration

package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/testdb"
)

type fixture struct {
	client     *clickhouse.Client
	server     string
	database   string
	base       time.Time
	statements []clickhouse.Statement
}

func newFixture(t *testing.T, server string) *fixture {
	t.Helper()

	client := clickhouse.New(testdb.ClickHouse(t))

	return &fixture{
		client:   client,
		server:   server,
		database: "shop",
		base:     time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute),
	}
}

func (f *fixture) seedStatement(
	t *testing.T,
	queryID int64,
	kind int32,
	perMinute []int64,
	execMs float64,
) clickhouse.Statement {
	t.Helper()

	ctx := context.Background()
	statement := clickhouse.Statement{
		ID:           clickhouse.StatementID(f.server, f.database, "app", queryID),
		ServerName:   f.server,
		DatabaseName: f.database,
		UserName:     "app",
		QueryID:      queryID,
		QueryFull:    "SELECT * FROM t WHERE id = $1",
		QueryShort:   "SELECT * FROM t",
		QueryKind:    kind,
	}

	if err := f.client.UpsertStatements(ctx, []clickhouse.Statement{statement}); err != nil {
		t.Fatalf("seed statement %d: %v", queryID, err)
	}

	deltas := make([]clickhouse.StatementDelta, 0, len(perMinute))

	for i, calls := range perMinute {
		deltas = append(deltas, clickhouse.StatementDelta{
			StatementID:   statement.ID,
			CollectedAt:   f.base.Add(time.Duration(i) * time.Minute),
			ServerName:    f.server,
			DatabaseName:  f.database,
			Calls:         calls,
			Rows:          calls * 2,
			TotalExecTime: float64(calls) * execMs,
			TotalIoTime:   float64(calls) * execMs / 4,
		})
	}

	if err := f.client.InsertStatementDeltas(ctx, deltas); err != nil {
		t.Fatalf("seed deltas %d: %v", queryID, err)
	}

	f.statements = append(f.statements, statement)

	return statement
}

func (f *fixture) window() (time.Time, time.Time) {
	return f.base.Add(-time.Minute), f.base.Add(time.Hour)
}
