//go:build integration

package server_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	chstats "github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

func logTestClient(t *testing.T) *chstats.Client {
	t.Helper()

	return chstats.New(testdb.ClickHouse(t))
}

func logEvent(serverName string, at time.Time, level, classification int32) chstats.LogEvent {
	return chstats.LogEvent{
		ID:         chstats.LogEventID(serverName, at, 0, strconv.FormatInt(at.UnixNano(), 10), 0),
		ServerName: serverName, CollectedAt: at, OccurredAt: at,
		LogLevel: level, Classification: classification,
	}
}

func TestLogEventFacetsRouteEveryDimension(t *testing.T) {
	t.Parallel()

	const serverName = "facet-routing-test"

	ctx := context.Background()
	stats := logTestClient(t)
	now := time.Now().UTC().Truncate(time.Second)

	deadlockOne := logEvent(serverName, now.Add(-5*time.Minute), 5, 49)
	deadlockOne.Message = "deadlock detected"
	deadlockOne.Pid, deadlockOne.UserName, deadlockOne.DatabaseName = 10, "app", "shop"
	deadlockOne.ApplicationName, deadlockOne.BackendType, deadlockOne.StateCode = "checkout", "client backend", "40P01"

	deadlockTwo := deadlockOne
	deadlockTwo.OccurredAt = now.Add(-4 * time.Minute)
	deadlockTwo.CollectedAt = deadlockTwo.OccurredAt
	deadlockTwo.Pid = 11
	deadlockTwo.ID = chstats.LogEventID(serverName, deadlockTwo.OccurredAt, 11, "deadlock detected", 0)

	syntax := deadlockOne
	syntax.OccurredAt = now.Add(-3 * time.Minute)
	syntax.CollectedAt = syntax.OccurredAt
	syntax.Classification, syntax.Message, syntax.Pid, syntax.StateCode = 68, "syntax error", 12, "42601"
	syntax.ID = chstats.LogEventID(serverName, syntax.OccurredAt, 12, "syntax error", 0)

	checkpoint := logEvent(serverName, now.Add(-2*time.Minute), 6, 28)
	checkpoint.Message, checkpoint.Pid, checkpoint.BackendType = "checkpoint complete", 13, "checkpointer"

	if err := stats.InsertLogEvents(ctx,
		[]chstats.LogEvent{deadlockOne, deadlockTwo, syntax, checkpoint}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	srv := server.NewLogServer(nil, stats, nil)

	resp, err := srv.ListLogFacets(viewer(ctx), connect.NewRequest(&querysheriffv1.ListLogFacetsRequest{
		ServerName: serverName,
		From:       timestamppb.New(now.Add(-time.Hour)),
		To:         timestamppb.New(now.Add(time.Hour)),
	}))
	if err != nil {
		t.Fatalf("ListLogFacets: %v", err)
	}

	facets := map[querysheriffv1.LogFacetField]map[string]int64{}
	for _, facet := range resp.Msg.GetFacets() {
		facets[facet.GetField()] = map[string]int64{}
		for _, value := range facet.GetValues() {
			facets[facet.GetField()][value.GetValue()] = value.GetCount()
		}
	}

	for number, name := range querysheriffv1.LogFacetField_name {
		field := querysheriffv1.LogFacetField(number)
		if field == querysheriffv1.LogFacetField_LOG_FACET_FIELD_UNSPECIFIED {
			continue
		}

		if _, ok := facets[field]; !ok {
			t.Errorf("%s is missing from the facet response", name)
		}
	}

	deadlock := strconv.Itoa(int(querysheriffv1.LogEvent_LOG_CLASSIFICATION_LOCK_DEADLOCK_DETECTED))
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_CLASSIFICATION, deadlock, 2)

	lock := strconv.Itoa(int(querysheriffv1.LogEvent_LOG_CATEGORY_LOCK))
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY, lock, 2)

	level := strconv.Itoa(int(querysheriffv1.LogEvent_LOG_LEVEL_ERROR))
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_LEVEL, level, 3)

	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE, "shop", 3)
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME, "app", 3)
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_APPLICATION_NAME, "checkout", 3)
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_BACKEND_TYPE, "checkpointer", 1)

	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE, "", 1)
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME, "", 1)

	standby := strconv.Itoa(int(querysheriffv1.LogEvent_LOG_CATEGORY_STANDBY))
	if _, ok := facets[querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY][standby]; !ok {
		t.Error("CATEGORY facet should list every family, including ones with no events")
	}
}

func TestListLogStatementSamplesHydratesSlowQueryRows(t *testing.T) {
	t.Parallel()

	const serverName = "facet-sample-test"

	ctx := context.Background()
	stats := logTestClient(t)
	occurred := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	sampleID := chstats.SampleID(serverName, occurred, "SELECT pg_sleep(2)", 2000.0, 0)

	if err := stats.InsertSamples(ctx, []chstats.Sample{{
		ID: sampleID, CollectedAt: occurred, ServerName: serverName, OccurredAt: occurred,
		Query: "SELECT pg_sleep(2)", DurationMs: 2000.0, ExplainPlanJSON: `{"Plan":{}}`,
	}}); err != nil {
		t.Fatalf("seed sample: %v", err)
	}

	event := logEvent(serverName, occurred, 6, 51)
	event.Pid, event.BackendType, event.Hint = 20, "client backend", "consider an index"
	event.StatementSampleID = sampleID

	if err := stats.InsertLogEvents(ctx, []chstats.LogEvent{event}); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	srv := server.NewLogServer(nil, stats, nil)

	resp, err := srv.QueryLogs(viewer(ctx), connect.NewRequest(&querysheriffv1.QueryLogsRequest{
		ServerName: serverName,
		From:       timestamppb.New(occurred.Add(-time.Hour)),
		To:         timestamppb.New(occurred.Add(time.Hour)),
		Limit:      10,
	}))
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}

	records := resp.Msg.GetRecords()
	if len(records) != 1 {
		t.Fatalf("expected 1 log event, got %d", len(records))
	}

	record := records[0]

	sample := record.GetStatementSample()
	if sample == nil {
		t.Fatal("a STATEMENT_DURATION row must carry its statement sample")
	}

	if sample.GetQuery() != "SELECT pg_sleep(2)" {
		t.Errorf("sample query = %q, want the sampled statement", sample.GetQuery())
	}

	if sample.GetDurationMs() != 2000.0 {
		t.Errorf("sample duration = %v, want 2000", sample.GetDurationMs())
	}

	if !sample.GetHasExplainPlan() {
		t.Error("a sample with explain_plan_json must report a plan so the row can link to it")
	}

	if record.GetHint() != "consider an index" {
		t.Errorf("hint = %q, want it preserved alongside the sample", record.GetHint())
	}

	if record.GetCategory() != querysheriffv1.LogEvent_LOG_CATEGORY_STATEMENT {
		t.Errorf("category = %s, want STATEMENT", record.GetCategory())
	}
}

func assertFacetCount(
	t *testing.T,
	facets map[querysheriffv1.LogFacetField]map[string]int64,
	field querysheriffv1.LogFacetField,
	value string,
	want int64,
) {
	t.Helper()

	if got := facets[field][value]; got != want {
		t.Errorf("%s facet value %q = %d, want %d", field, value, got, want)
	}
}
