//go:build integration

package server_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/auth"
	chstats "github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

func TestStatementSeriesSumsTheStatementsCalls(t *testing.T) {
	t.Parallel()

	const serverName = "series-scope-test"

	ctx := context.Background()
	stats := chstats.New(testdb.ClickHouse(t))
	now := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)
	id := chstats.StatementID(serverName, "shop", "app", 5150)

	if err := stats.UpsertStatements(ctx, []chstats.Statement{{
		ID: id, ServerName: serverName, DatabaseName: "shop", UserName: "app",
		QueryID: 5150, QueryFull: "SELECT 1", QueryShort: "SELECT 1", QueryKind: 1,
	}}); err != nil {
		t.Fatalf("seed statement: %v", err)
	}

	if err := stats.InsertStatementDeltas(ctx, []chstats.StatementDelta{{
		StatementID: id, CollectedAt: now, ServerName: serverName, DatabaseName: "shop",
		Calls: 12, Rows: 24, TotalExecTime: 60, TotalIoTime: 6,
	}}); err != nil {
		t.Fatalf("seed delta: %v", err)
	}

	srv := server.NewStatementServer(stats)
	viewer := auth.WithPrincipal(ctx, &auth.Principal{
		UserID: 1, Email: "viewer@dev.dev", IsSuperAdmin: true,
	})

	resp, err := srv.GetStatementSeries(viewer,
		connect.NewRequest(&querysheriffv1.GetStatementSeriesRequest{
			ServerName:   serverName,
			DatabaseName: "shop",
			StatementId:  id,
			From:         timestamppb.New(now.Add(-time.Hour)),
			To:           timestamppb.New(now.Add(time.Hour)),
		}))
	if err != nil {
		t.Fatalf("GetStatementSeries: %v", err)
	}

	var total float64
	for _, point := range resp.Msg.GetCalls() {
		total += point.GetValue()
	}

	if total != 12 {
		t.Errorf("series totalled %v calls, want the 12 seeded for this statement", total)
	}
}
