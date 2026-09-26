//go:build integration

package server_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	chstats "github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

const tagFilterQueryID = int64(991001)

func everyKind() []querysheriffv1.QueryKind {
	return []querysheriffv1.QueryKind{
		querysheriffv1.QueryKind_QUERY_KIND_READS,
		querysheriffv1.QueryKind_QUERY_KIND_WRITES,
		querysheriffv1.QueryKind_QUERY_KIND_OTHERS,
	}
}

func tagFilterFixture(t *testing.T, serverName string) *chstats.Client {
	t.Helper()

	ctx := context.Background()
	stats := chstats.New(testdb.ClickHouse(t))
	id := chstats.StatementID(serverName, "db", "app", tagFilterQueryID)
	occurred := time.Now().Add(-5 * time.Minute)

	if err := stats.UpsertStatements(ctx, []chstats.Statement{{
		ID: id, ServerName: serverName, DatabaseName: "db", UserName: "app",
		QueryID: tagFilterQueryID, QueryShort: "SELECT 1", QueryFull: "SELECT 1", QueryKind: 1,
	}}); err != nil {
		t.Fatalf("seed statement: %v", err)
	}

	if err := stats.InsertStatementDeltas(ctx, []chstats.StatementDelta{{
		CollectedAt: occurred, ServerName: serverName, DatabaseName: "db",
		StatementID: id, Calls: 10, Rows: 10, TotalExecTime: 100, TotalIoTime: 1,
	}}); err != nil {
		t.Fatalf("seed delta: %v", err)
	}

	if err := stats.ReplaceStatementTags(ctx, map[uint64]map[string]string{
		id: {"service": "payments"},
	}); err != nil {
		t.Fatalf("seed statement tags: %v", err)
	}

	return stats
}

func listStatements(
	ctx context.Context,
	t *testing.T,
	srv *server.StatementServer,
	serverName string,
	window time.Duration,
	filters ...*querysheriffv1.TagFilter,
) int {
	t.Helper()

	to := time.Now()

	resp, err := srv.ListStatements(ctx, connect.NewRequest(&querysheriffv1.ListStatementsRequest{
		ServerName:   serverName,
		DatabaseName: "db",
		From:         timestamppb.New(to.Add(-window)),
		To:           timestamppb.New(to),
		TagFilters:   filters,
		Kinds:        everyKind(),
		Limit:        50,
	}))
	if err != nil {
		t.Fatalf("ListStatements: %v", err)
	}

	return len(resp.Msg.GetStatements())
}

func serviceIs(value string) *querysheriffv1.TagFilter {
	return tagFilter("service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL, value)
}

func TestTagFiltersFollowTheLatestSample(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-latest-sample"

	ctx := context.Background()
	stats := tagFilterFixture(t, serverName)
	srv := server.NewStatementServer(stats)
	viewing := viewer(ctx)
	id := chstats.StatementID(serverName, "db", "app", tagFilterQueryID)

	if n := listStatements(viewing, t, srv, serverName, 24*time.Hour, serviceIs("payments")); n != 1 {
		t.Fatalf("service=payments matched %d statements, want 1", n)
	}

	if err := stats.ReplaceStatementTags(ctx, map[uint64]map[string]string{
		id: {"service": "billing"},
	}); err != nil {
		t.Fatalf("ReplaceStatementTags: %v", err)
	}

	if n := listStatements(viewing, t, srv, serverName, 24*time.Hour, serviceIs("billing")); n != 1 {
		t.Errorf("service=billing matched %d statements after a newer sample, want 1", n)
	}

	if n := listStatements(viewing, t, srv, serverName, 24*time.Hour, serviceIs("payments")); n != 0 {
		t.Errorf("service=payments still matched %d statements after a newer sample, want 0", n)
	}
}

func TestTagFiltersIgnoreTheQueryWindow(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-window"

	ctx := context.Background()
	stats := tagFilterFixture(t, serverName)
	srv := server.NewStatementServer(stats)
	viewing := viewer(ctx)

	unfiltered := listStatements(viewing, t, srv, serverName, 24*time.Hour)
	if unfiltered != 1 {
		t.Fatalf("the fixture statement is not visible unfiltered: got %d, want 1", unfiltered)
	}

	for _, window := range []time.Duration{10 * time.Minute, 24 * time.Hour, 60 * 24 * time.Hour} {
		if n := listStatements(viewing, t, srv, serverName, window, serviceIs("payments")); n != 1 {
			t.Errorf("a %s window matched %d statements, want 1", window, n)
		}
	}
}

func TestTagFiltersRejectMalformedRequests(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-validation"

	ctx := context.Background()
	stats := tagFilterFixture(t, serverName)
	srv := server.NewStatementServer(stats)
	to := time.Now()

	cases := map[string]*querysheriffv1.TagFilter{
		"uppercase key":     tagFilter("Service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS),
		"missing op":        {Key: "service", Values: []string{"payments"}},
		"exists with value": tagFilter("service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS, "x"),
		"equal without a value": tagFilter(
			"service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL,
		),
	}

	for name, filter := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := srv.ListStatements(viewer(ctx), connect.NewRequest(&querysheriffv1.ListStatementsRequest{
				ServerName:   serverName,
				DatabaseName: "db",
				From:         timestamppb.New(to.Add(-time.Hour)),
				To:           timestamppb.New(to),
				TagFilters:   []*querysheriffv1.TagFilter{filter},
				Kinds:        everyKind(),
				Limit:        50,
			}))

			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Errorf("ListStatements(%+v) = %v, want an invalid_argument error", filter, err)
			}
		})
	}
}

func TestReportedSampleTagsDropHighCardinalityKeys(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-shape"

	ctx := context.Background()
	stats := tagFilterFixture(t, serverName)
	logs := server.NewLogServer(stats, nil)
	statements := server.NewStatementServer(stats)
	at := time.Now().Add(-time.Minute)

	_, err := logs.ReportLogs(collector(ctx, serverName), connect.NewRequest(&querysheriffv1.ReportLogsRequest{
		CollectedAt: timestamppb.New(at),
		LogEvents: []*querysheriffv1.LogEvent{{
			OccurredAt:     timestamppb.New(at),
			DatabaseName:   "db",
			Username:       "app",
			QueryId:        tagFilterQueryID,
			Message:        "duration: 900ms",
			Classification: querysheriffv1.LogEvent_LOG_CLASSIFICATION_STATEMENT_DURATION,
			LogLevel:       querysheriffv1.LogEvent_LOG_LEVEL_LOG,
			StatementSample: &querysheriffv1.LogStatementSample{
				OccurredAt: timestamppb.New(at),
				Query:      "SELECT 1",
				DurationMs: 900,
				Tags: map[string]string{
					"service":  "payments",
					"user_id":  "8123",
					"order_id": "55",
				},
			},
		}},
	}))
	if err != nil {
		t.Fatalf("ReportLogs: %v", err)
	}

	resp, err := statements.ListTagKeys(viewer(ctx), connect.NewRequest(&querysheriffv1.ListTagKeysRequest{
		ServerName:   serverName,
		DatabaseName: "db",
	}))
	if err != nil {
		t.Fatalf("ListTagKeys: %v", err)
	}

	keys := make([]string, 0, len(resp.Msg.GetKeys()))
	for _, key := range resp.Msg.GetKeys() {
		keys = append(keys, key.GetKey())
	}

	if !slices.Contains(keys, "service") {
		t.Errorf("tag keys = %v, want the shape tag `service` kept", keys)
	}

	for _, dropped := range []string{"user_id", "order_id"} {
		if slices.Contains(keys, dropped) {
			t.Errorf("tag keys = %v, want the per-execution key %q dropped", keys, dropped)
		}
	}

	values, err := statements.ListTagValues(viewer(ctx), connect.NewRequest(&querysheriffv1.ListTagValuesRequest{
		ServerName: serverName, DatabaseName: "db", Key: "service",
	}))
	if err != nil {
		t.Fatalf("ListTagValues: %v", err)
	}

	if got := values.Msg.GetValues(); len(got) != 1 || got[0].GetValue() != "payments" ||
		got[0].GetStatementCount() != 1 {
		t.Errorf("service values = %v, want payments on 1 statement", got)
	}
}

func TestNotEqualTagFilterMatchesUntaggedStatements(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-not-equal"

	ctx := context.Background()
	stats := tagFilterFixture(t, serverName)
	untagged := chstats.StatementID(serverName, "db", "app", tagFilterQueryID+1)

	if err := stats.UpsertStatements(ctx, []chstats.Statement{{
		ID: untagged, ServerName: serverName, DatabaseName: "db", UserName: "app",
		QueryID: tagFilterQueryID + 1, QueryShort: "SELECT 2", QueryFull: "SELECT 2", QueryKind: 1,
	}}); err != nil {
		t.Fatalf("seed untagged statement: %v", err)
	}

	if err := stats.InsertStatementDeltas(ctx, []chstats.StatementDelta{{
		CollectedAt: time.Now().Add(-5 * time.Minute), ServerName: serverName, DatabaseName: "db",
		StatementID: untagged, Calls: 1, Rows: 1, TotalExecTime: 1,
	}}); err != nil {
		t.Fatalf("seed untagged delta: %v", err)
	}

	notPayments := tagFilter("service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL, "payments")
	if n := listStatements(
		viewer(ctx),
		t,
		server.NewStatementServer(stats),
		serverName,
		time.Hour,
		notPayments,
	); n != 1 {
		t.Errorf("service!=payments matched %d statements, want only the untagged one", n)
	}
}
