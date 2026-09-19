//go:build integration

package server_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/auth"
	chstats "github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

func TestReportStatementsLazyText(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	pool := testdb.Postgres(t)
	stats := chstats.New(testdb.ClickHouse(t))
	srv := server.NewStatementServer(db.New(pool), stats, slog.New(slog.DiscardHandler))

	serverName := fmt.Sprintf("lazytext-test-%d", time.Now().UnixNano())

	collectorCtx := auth.WithServerName(ctx, serverName)

	const queryID = int64(990001)
	deltas := []*querysheriffv1.StatementDelta{
		{UserName: "u1", DatabaseName: "d1", QueryId: queryID, Calls: 5, Rows: 10, TotalExecTime: 1, TotalIoTime: 1},
		{UserName: "u2", DatabaseName: "d1", QueryId: queryID, Calls: 5, Rows: 10, TotalExecTime: 1, TotalIoTime: 1},
	}

	report := func() []*querysheriffv1.StatementIdentity {
		resp, reportErr := srv.ReportStatements(
			collectorCtx,
			connect.NewRequest(&querysheriffv1.ReportStatementsRequest{
				CollectedAt:     timestamppb.New(time.Now()),
				StatementDeltas: deltas,
			}),
		)
		if reportErr != nil {
			t.Fatalf("ReportStatements: %v", reportErr)
		}

		return resp.Msg.GetUnknownStatements()
	}

	users := func(unknown []*querysheriffv1.StatementIdentity) map[string]bool {
		out := make(map[string]bool, len(unknown))
		for _, s := range unknown {
			if s.GetDatabaseName() != "d1" || s.GetQueryId() != queryID {
				t.Fatalf("unexpected unknown identity: %+v", s)
			}
			out[s.GetUserName()] = true
		}

		return out
	}

	first := users(report())
	if !first["u1"] || !first["u2"] || len(first) != 2 {
		t.Fatalf("first report unknown users = %v, want {u1, u2}", first)
	}

	if fillErr := fillTexts(collectorCtx, t, srv, queryID, map[string]string{"u1": "SELECT 1"}); fillErr != nil {
		t.Fatalf("ReportStatementTexts: %v", fillErr)
	}

	second := users(report())
	if second["u1"] || !second["u2"] || len(second) != 1 {
		t.Fatalf("after filling u1, unknown users = %v, want {u2} only", second)
	}

	assertText(ctx, t, stats, serverName, queryID, "u1", "SELECT 1")
	assertText(ctx, t, stats, serverName, queryID, "u2", "")
}

func fillTexts(
	ctx context.Context,
	t *testing.T,
	srv *server.StatementServer,
	queryID int64,
	byUser map[string]string,
) error {
	t.Helper()

	texts := make([]*querysheriffv1.StatementText, 0, len(byUser))
	for user, query := range byUser {
		texts = append(texts, &querysheriffv1.StatementText{
			Identity: &querysheriffv1.StatementIdentity{UserName: user, DatabaseName: "d1", QueryId: queryID},
			Query:    query,
		})
	}

	_, err := srv.ReportStatementTexts(ctx, connect.NewRequest(&querysheriffv1.ReportStatementTextsRequest{
		StatementTexts: texts,
	}))

	return err
}

func assertText(
	ctx context.Context,
	t *testing.T,
	stats *chstats.Client,
	serverName string,
	queryID int64,
	userName string,
	want string,
) {
	t.Helper()

	got, err := stats.StatementText(ctx, chstats.StatementID(serverName, "d1", userName, queryID))
	if err != nil {
		t.Fatalf("read query_full for %s: %v", userName, err)
	}

	if got != want {
		t.Fatalf("query_full for %s = %q, want %q", userName, got, want)
	}
}
