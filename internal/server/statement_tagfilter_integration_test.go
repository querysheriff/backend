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

// tagFilterFixture seeds one statement that ran 5 minutes ago, sampled 5 minutes ago with service=payments.
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

	seedTaggedSample(t, stats, serverName, occurred, map[string]string{"service": "payments"})

	return stats
}

func seedTaggedSample(t *testing.T, stats *chstats.Client, serverName string, at time.Time, tags map[string]string) {
	t.Helper()

	if err := stats.InsertSamples(context.Background(), []chstats.Sample{{
		ID:          chstats.SampleID(serverName, at, "SELECT 1", 1, 0),
		CollectedAt: at, OccurredAt: at, ServerName: serverName, DatabaseName: "db",
		StatementID: chstats.StatementID(serverName, "db", "app", tagFilterQueryID),
		Query:       "SELECT 1", DurationMs: 1, Tags: tags,
	}}); err != nil {
		t.Fatalf("seed sample: %v", err)
	}
}

func lastWindow(window time.Duration) (*timestamppb.Timestamp, *timestamppb.Timestamp) {
	to := time.Now()

	return timestamppb.New(to.Add(-window)), timestamppb.New(to)
}

func listStatements(
	ctx context.Context,
	t *testing.T,
	srv *server.StatementServer,
	serverName string,
	window time.Duration,
	filters ...*querysheriffv1.TagFilter,
) []*querysheriffv1.StatementStat {
	t.Helper()

	from, to := lastWindow(window)

	resp, err := srv.ListStatements(ctx, connect.NewRequest(&querysheriffv1.ListStatementsRequest{
		ServerName:   serverName,
		DatabaseName: "db",
		From:         from,
		To:           to,
		TagFilters:   filters,
		Kinds:        everyKind(),
		Limit:        50,
	}))
	if err != nil {
		t.Fatalf("ListStatements: %v", err)
	}

	return resp.Msg.GetStatements()
}

func statementTags(
	ctx context.Context,
	t *testing.T,
	srv *server.StatementServer,
	serverName string,
	window time.Duration,
) []string {
	t.Helper()

	from, to := lastWindow(window)

	resp, err := srv.GetStatementTags(ctx, connect.NewRequest(&querysheriffv1.GetStatementTagsRequest{
		ServerName:   serverName,
		DatabaseName: "db",
		StatementId:  chstats.StatementID(serverName, "db", "app", tagFilterQueryID),
		From:         from,
		To:           to,
	}))
	if err != nil {
		t.Fatalf("GetStatementTags: %v", err)
	}

	return pairs(resp.Msg.GetTags())
}

func pairs(tags []*querysheriffv1.Tag) []string {
	out := make([]string, len(tags))
	for i, tag := range tags {
		out[i] = tag.GetKey() + "=" + tag.GetValue()
	}

	return out
}

func serviceIs(value string) *querysheriffv1.TagFilter {
	return tagFilter("service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL, value)
}

func TestStatementTagsMergeEveryValueSampledInTheRange(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-merge"

	ctx := context.Background()
	stats := tagFilterFixture(t, serverName)
	srv := server.NewStatementServer(stats)
	viewing := viewer(ctx)
	now := time.Now()

	seedTaggedSample(t, stats, serverName, now.Add(-20*time.Minute),
		map[string]string{"service": "billing", "process_id": "17"})
	seedTaggedSample(t, stats, serverName, now.Add(-48*time.Hour), map[string]string{"service": "legacy"})

	want := []string{"service=billing", "service=payments"}

	listed := listStatements(viewing, t, srv, serverName, time.Hour)
	if len(listed) != 1 || !slices.Equal(pairs(listed[0].GetTags()), want) {
		t.Errorf("listed statements = %v, want one tagged %v", listed, want)
	}

	if got := statementTags(viewing, t, srv, serverName, time.Hour); !slices.Equal(got, want) {
		t.Errorf("statement tags over 1h = %v, want %v", got, want)
	}

	if got := statementTags(
		viewing,
		t,
		srv,
		serverName,
		10*time.Minute,
	); !slices.Equal(
		got,
		[]string{"service=payments"},
	) {
		t.Errorf("statement tags over 10m = %v, want only the sample inside the range", got)
	}

	cases := []struct {
		window time.Duration
		filter *querysheriffv1.TagFilter
		want   int
	}{
		{time.Hour, serviceIs("billing"), 1},
		{time.Hour, serviceIs("payments"), 1},
		{time.Hour, serviceIs("legacy"), 0},
		{72 * time.Hour, serviceIs("legacy"), 1},
		{time.Hour, tagFilter("service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL, "billing"), 0},
		{time.Hour, tagFilter("service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS), 1},
		{time.Hour, tagFilter("process_id", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS), 0},
	}

	for _, c := range cases {
		if n := len(listStatements(viewing, t, srv, serverName, c.window, c.filter)); n != c.want {
			t.Errorf("%s over %s matched %d statements, want %d", c.filter, c.window, n, c.want)
		}
	}

	for _, c := range []struct {
		window time.Duration
		want   []string
	}{
		{time.Hour, []string{"billing", "payments"}},
		{72 * time.Hour, []string{"billing", "legacy", "payments"}},
	} {
		from, to := lastWindow(c.window)

		resp, err := srv.ListTagValues(viewing, connect.NewRequest(&querysheriffv1.ListTagValuesRequest{
			ServerName: serverName, DatabaseName: "db", Key: "service", From: from, To: to,
		}))
		if err != nil {
			t.Fatalf("ListTagValues: %v", err)
		}

		values := make([]string, 0, len(resp.Msg.GetValues()))
		for _, value := range resp.Msg.GetValues() {
			values = append(values, value.GetValue())
		}

		if !slices.Equal(values, c.want) {
			t.Errorf("service values over %s = %v, want %v", c.window, values, c.want)
		}
	}
}

func TestTagFiltersRejectMalformedRequests(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-validation"

	ctx := context.Background()
	stats := tagFilterFixture(t, serverName)
	srv := server.NewStatementServer(stats)
	from, to := lastWindow(time.Hour)

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
				From:         from,
				To:           to,
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
					"service":  "checkout",
					"user_id":  "8123",
					"order_id": "55",
				},
			},
		}},
	}))
	if err != nil {
		t.Fatalf("ReportLogs: %v", err)
	}

	from, to := lastWindow(time.Hour)

	resp, err := statements.ListTagKeys(viewer(ctx), connect.NewRequest(&querysheriffv1.ListTagKeysRequest{
		ServerName: serverName, DatabaseName: "db", From: from, To: to,
	}))
	if err != nil {
		t.Fatalf("ListTagKeys: %v", err)
	}

	keys := resp.Msg.GetKeys()
	if len(keys) != 1 || keys[0].GetKey() != "service" || keys[0].GetValueCount() != 2 {
		t.Errorf("tag keys = %v, want only `service` with its 2 values, per-execution `*_id` keys dropped", keys)
	}

	values, err := statements.ListTagValues(viewer(ctx), connect.NewRequest(&querysheriffv1.ListTagValuesRequest{
		ServerName: serverName, DatabaseName: "db", Key: "service", From: from, To: to,
	}))
	if err != nil {
		t.Fatalf("ListTagValues: %v", err)
	}

	got := values.Msg.GetValues()
	if len(got) != 2 || got[0].GetValue() != "checkout" || got[1].GetValue() != "payments" ||
		got[0].GetStatementCount() != 1 || got[1].GetStatementCount() != 1 {
		t.Errorf("service values = %v, want checkout and payments on 1 statement each", got)
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

	listed := listStatements(viewer(ctx), t, server.NewStatementServer(stats), serverName, time.Hour, notPayments)
	if len(listed) != 1 || listed[0].GetId() != untagged || len(listed[0].GetTags()) != 0 {
		t.Errorf("service!=payments matched %v, want only the untagged statement", listed)
	}
}
