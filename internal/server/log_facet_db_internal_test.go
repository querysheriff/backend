package server

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/db"
)

// Each test owns a distinct server_name so the parallel cases cannot clobber each
// other's fixtures, and so neither touches real collector data.
func logFacetTestPool(t *testing.T, serverName string) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping log facet integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM log_events WHERE server_name = $1`, serverName)
		_, _ = pool.Exec(ctx, `DELETE FROM statement_samples WHERE server_name = $1`, serverName)
	}

	cleanup()
	t.Cleanup(cleanup)

	return pool
}

// The GROUPING SETS query hands back one flat row set covering seven dimensions, and
// grouping() bits are the only thing distinguishing them. This drives the real SQL and
// the real assembly code so a drift between the two shows up as mis-filed values rather
// than as a silent wrong count.
func TestLogEventFacetsRouteEveryDimension(t *testing.T) {
	t.Parallel()

	const serverName = "facet-routing-test"

	ctx := context.Background()
	pool := logFacetTestPool(t, serverName)

	// Two deadlocks and one syntax error from an app session, plus a checkpoint from a
	// background worker, which has no database, user or application at all.
	if _, err := pool.Exec(ctx, `
		INSERT INTO log_events (
		    server_name, collected_at, occurred_at, log_level, classification, message,
		    pid, username, database_name, application_name, backend_type, state_code
		) VALUES
		    ($1, now() - interval '5 minutes', now() - interval '5 minutes', 5, 49, 'deadlock detected',
		     10, 'app', 'shop', 'checkout', 'client backend', '40P01'),
		    ($1, now() - interval '4 minutes', now() - interval '4 minutes', 5, 49, 'deadlock detected',
		     11, 'app', 'shop', 'checkout', 'client backend', '40P01'),
		    ($1, now() - interval '3 minutes', now() - interval '3 minutes', 5, 68, 'syntax error',
		     12, 'app', 'shop', 'checkout', 'client backend', '42601'),
		    ($1, now() - interval '2 minutes', now() - interval '2 minutes', 6, 28, 'checkpoint complete',
		     13, NULL, NULL, NULL, 'checkpointer', NULL)
	`, serverName); err != nil {
		t.Fatalf("seed: %v", err)
	}

	queries := db.New(pool)
	srv := NewLogServer(queries, nil)

	rows, err := queries.LogEventFacets(ctx, db.LogEventFacetsParams{
		ServerName: serverName,
		Since:      pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
		Until:      pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
	})
	if err != nil {
		t.Fatalf("LogEventFacets: %v", err)
	}

	facets := map[querysheriffv1.LogFacetField]map[string]int64{}
	for _, facet := range srv.buildLogFacets(rows) {
		facets[facet.GetField()] = map[string]int64{}
		for _, value := range facet.GetValues() {
			facets[facet.GetField()][value.GetValue()] = value.GetCount()
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

	// The background worker's NULL columns must collapse to one "(none)" value, not
	// vanish — a whole family of events (checkpointer, autovacuum, walwriter) lives there.
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_DATABASE, "", 1)
	assertFacetCount(t, facets, querysheriffv1.LogFacetField_LOG_FACET_FIELD_USERNAME, "", 1)

	// Every family is listed even at zero, so the picker can dim "Standby · 0" rather
	// than leaving the user wondering where it went.
	standby := strconv.Itoa(int(querysheriffv1.LogEvent_LOG_CATEGORY_STANDBY))
	if _, ok := facets[querysheriffv1.LogFacetField_LOG_FACET_FIELD_CATEGORY][standby]; !ok {
		t.Error("CATEGORY facet should list every family, including ones with no events")
	}
}

// The slow-query rows are the whole point of the LOGS table, and ingest deliberately
// blanks their message and statement because the sample holds the same bytes. If the
// hydration query stops matching them the table silently shows empty rows again.
func TestListLogStatementSamplesHydratesSlowQueryRows(t *testing.T) {
	t.Parallel()

	const serverName = "facet-sample-test"

	ctx := context.Background()
	pool := logFacetTestPool(t, serverName)

	var sampleID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO statement_samples (
		    server_name, collected_at, occurred_at, query, duration_ms, explain_plan_json
		) VALUES ($1, now() - interval '5 minutes', now() - interval '5 minutes',
		          'SELECT pg_sleep(2)', 2000.0, '{"Plan":{}}')
		RETURNING id
	`, serverName).Scan(&sampleID); err != nil {
		t.Fatalf("seed sample: %v", err)
	}

	// Mirrors what ReportLogs writes: the text columns blanked, one shared collected_at.
	if _, err := pool.Exec(ctx, `
		INSERT INTO log_events (
		    server_name, collected_at, occurred_at, log_level, classification, message,
		    pid, backend_type, statement_sample_id, hint
		) VALUES ($1, now() - interval '5 minutes', now() - interval '5 minutes', 6, 51, NULL,
		          20, 'client backend', $2, 'consider an index')
	`, serverName, sampleID); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	queries := db.New(pool)
	srv := NewLogServer(queries, nil)

	rows, err := queries.ListLogEvents(ctx, db.ListLogEventsParams{
		ServerName: serverName,
		Since:      pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
		Until:      pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		RowLimit:   10,
	})
	if err != nil {
		t.Fatalf("ListLogEvents: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 log event, got %d", len(rows))
	}

	filter := logFilter{since: pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true}}

	samples, err := srv.logStatementSamples(ctx, rows, filter)
	if err != nil {
		t.Fatalf("logStatementSamples: %v", err)
	}

	record := srv.logRecordFromRow(rows[0], samples)

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

	// hint used to be blanked at ingest with nothing carrying it — pure data loss.
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
