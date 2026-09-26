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

const authzServer = "authz-db-test"

func TestStatementIDsRejectForeignServer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stats := chstats.New(testdb.ClickHouse(t))
	statementID := chstats.StatementID(authzServer, "db", "app", 4242)
	occurred := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	sampleID := chstats.SampleID(authzServer, occurred, "SELECT 1", 1, 0)

	if err := stats.UpsertStatements(ctx, []chstats.Statement{{
		ID: statementID, ServerName: authzServer, DatabaseName: "db", UserName: "app",
		QueryID: 4242, QueryFull: "SELECT 1", QueryShort: "SELECT 1", QueryKind: 1,
	}}); err != nil {
		t.Fatalf("seed statement: %v", err)
	}

	if err := stats.InsertSamples(ctx, []chstats.Sample{{
		ID: sampleID, StatementID: statementID, CollectedAt: occurred, OccurredAt: occurred,
		ServerName: authzServer, Query: "SELECT 1", DurationMs: 1,
	}}); err != nil {
		t.Fatalf("seed sample: %v", err)
	}

	outsider := auth.WithPrincipal(ctx, &auth.Principal{
		UserID: 1, Email: "outsider@dev.dev", AllowedServers: []string{"some-other-server"},
	})

	srv := server.NewStatementServer(stats)
	from, to := timestamppb.New(occurred.Add(-time.Hour)), timestamppb.New(occurred.Add(time.Hour))

	calls := map[string]func() error{
		"GetStatement": func() error {
			_, callErr := srv.GetStatement(outsider,
				connect.NewRequest(&querysheriffv1.GetStatementRequest{Id: statementID}))

			return callErr
		},
		"ListStatementSamples": func() error {
			_, callErr := srv.ListStatementSamples(outsider,
				connect.NewRequest(&querysheriffv1.ListStatementSamplesRequest{
					ServerName: authzServer, DatabaseName: "db", StatementId: statementID, From: from, To: to,
				}))

			return callErr
		},
		"GetStatementSample": func() error {
			_, callErr := srv.GetStatementSample(outsider,
				connect.NewRequest(&querysheriffv1.GetStatementSampleRequest{Id: sampleID}))

			return callErr
		},
		"GetStatementSeries": func() error {
			_, callErr := srv.GetStatementSeries(outsider,
				connect.NewRequest(&querysheriffv1.GetStatementSeriesRequest{
					ServerName: authzServer, DatabaseName: "db", StatementId: statementID, From: from, To: to,
				}))

			return callErr
		},
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			callErr := call()
			if callErr == nil {
				t.Fatalf("%s returned another server's data to an unauthorized user", name)
			}

			if got := connect.CodeOf(callErr); got != connect.CodePermissionDenied {
				t.Errorf("%s error code = %s, want permission_denied", name, got)
			}
		})
	}
}
