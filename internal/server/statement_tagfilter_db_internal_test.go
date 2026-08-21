package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/db"
)

// A distinct server_name per test, so parallel cases cannot clobber each other's fixtures.
func tagFilterTestPool(t *testing.T, serverName string) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping tag-filter integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	cleanup := func() {
		_, _ = pool.Exec(ctx,
			`DELETE FROM statement_samples ss USING statements s
			 WHERE s.id = ss.statement_id AND s.server_name = $1`, serverName)
		_, _ = pool.Exec(ctx,
			`DELETE FROM statement_deltas d USING statements s
			 WHERE s.id = d.statement_id AND s.server_name = $1`, serverName)
		_, _ = pool.Exec(ctx, `DELETE FROM statements WHERE server_name = $1`, serverName)
	}

	cleanup()
	t.Cleanup(cleanup)

	if _, err = pool.Exec(ctx, `
		WITH s AS (
		    INSERT INTO statements (server_name, database_name, user_name, query_id, query_full, query_short, query_kind)
		    VALUES ($1, 'db', 'app', 991001, 'SELECT 1', 'SELECT 1', 1)
		    RETURNING id
		), d AS (
		    INSERT INTO statement_deltas (statement_id, collected_at, calls, rows, total_exec_time, total_io_time)
		    SELECT s.id, now() - interval '5 minutes', 10, 10, 100.0, 1.0 FROM s
		)
		INSERT INTO statement_samples (server_name, collected_at, occurred_at, statement_id, query, duration_ms, tags)
		SELECT $1, now() - interval '5 minutes', now() - interval '5 minutes', s.id, 'SELECT 1', 1.0,
		       '{"service":"payments"}'::jsonb
		FROM s`, serverName); err != nil {
		t.Fatalf("seed: %v", err)
	}

	return pool
}

// nil ids means "no filters, match everything", empty means "matched nothing": NULL vs '{}' in pgx.
func TestStatementIDsNilMatchesAllEmptyMatchesNone(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-db-test-nil-empty"

	pool := tagFilterTestPool(t, serverName)
	ctx := context.Background()
	q := db.New(pool)

	to := time.Now()
	from := to.Add(-24 * time.Hour)

	params := func(ids []int64) db.ListStatementStatsParams {
		return db.ListStatementStatsParams{
			RowLimit:     1000,
			ServerName:   pgtype.Text{String: serverName, Valid: true},
			Since:        pgtype.Timestamptz{Time: from, Valid: true},
			Until:        pgtype.Timestamptz{Time: to, Valid: true},
			StatementIds: ids,
			Kinds:        []int32{1, 2, 3},
		}
	}

	unfiltered, err := q.ListStatementStats(ctx, params(nil))
	if err != nil {
		t.Fatalf("ListStatementStats(nil): %v", err)
	}

	if len(unfiltered) != 1 {
		t.Fatalf("ListStatementStats(nil) returned %d statements, want 1", len(unfiltered))
	}

	none, err := q.ListStatementStats(ctx, params([]int64{}))
	if err != nil {
		t.Fatalf("ListStatementStats([]): %v", err)
	}

	if len(none) != 0 {
		t.Fatalf("ListStatementStats([]) returned %d statements, want 0", len(none))
	}
}

func TestFilterStatementIDsByTagsIsTimeScoped(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-db-test-time-scope"

	pool := tagFilterTestPool(t, serverName)
	ctx := context.Background()
	q := db.New(pool)

	// A different key on purpose: reusing `service` would make its value disagree across the window,
	// and the statement would then hide the tag for a reason this test is not about.
	if _, err := pool.Exec(ctx, `
		INSERT INTO statement_samples (server_name, collected_at, occurred_at, statement_id, query, duration_ms, tags)
		SELECT $1, now() - interval '30 days', now() - interval '30 days', s.id, 'SELECT 1', 1.0,
		       '{"legacy_flag":"yes"}'::jsonb
		FROM statements s WHERE s.server_name = $1`, serverName); err != nil {
		t.Fatalf("seed legacy sample: %v", err)
	}

	filters, err := buildTagFilterJSON([]*querysheriffv1.TagFilter{
		tagFilter("legacy_flag", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL, "yes"),
	})
	if err != nil {
		t.Fatalf("buildTagFilterJSON: %v", err)
	}

	match := func(since time.Time) []int64 {
		t.Helper()

		ids, idErr := q.FilterStatementIDsByTags(ctx, db.FilterStatementIDsByTagsParams{
			ServerName: pgtype.Text{String: serverName, Valid: true},
			TagFilters: filters,
			Since:      pgtype.Timestamptz{Time: since, Valid: true},
			Until:      pgtype.Timestamptz{Time: time.Now(), Valid: true},
		})
		if idErr != nil {
			t.Fatalf("FilterStatementIDsByTags: %v", idErr)
		}

		return ids
	}

	if ids := match(time.Now().Add(-24 * time.Hour)); len(ids) != 0 {
		t.Errorf("a 24h window matched %d statements on a 30-day-old tag, want 0", len(ids))
	}

	if ids := match(time.Now().Add(-60 * 24 * time.Hour)); len(ids) != 1 {
		t.Errorf("a 60-day window matched %d statements on a 30-day-old tag, want 1", len(ids))
	}
}

func TestFilterIgnoresTagsTheStatementDoesNotDisplay(t *testing.T) {
	t.Parallel()

	const serverName = "tagfilter-db-test-disagree"

	pool := tagFilterTestPool(t, serverName)
	ctx := context.Background()
	q := db.New(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO statement_samples (server_name, collected_at, occurred_at, statement_id, query, duration_ms, tags)
		SELECT $1, now() - interval '4 minutes', now() - interval '4 minutes', s.id, 'SELECT 1', 1.0,
		       '{"service":"billing"}'::jsonb
		FROM statements s WHERE s.server_name = $1`, serverName); err != nil {
		t.Fatalf("seed disagreeing sample: %v", err)
	}

	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()

	matched := func(op querysheriffv1.TagFilterOperator, values ...string) int {
		t.Helper()

		filters, err := buildTagFilterJSON([]*querysheriffv1.TagFilter{tagFilter("service", op, values...)})
		if err != nil {
			t.Fatalf("buildTagFilterJSON: %v", err)
		}

		ids, err := q.FilterStatementIDsByTags(ctx, db.FilterStatementIDsByTagsParams{
			ServerName: pgtype.Text{String: serverName, Valid: true},
			TagFilters: filters,
			Since:      pgtype.Timestamptz{Time: from, Valid: true},
			Until:      pgtype.Timestamptz{Time: to, Valid: true},
		})
		if err != nil {
			t.Fatalf("FilterStatementIDsByTags: %v", err)
		}

		return len(ids)
	}

	suggested, err := q.ListTagValues(ctx, db.ListTagValuesParams{
		TagKey:     "service",
		ServerName: pgtype.Text{String: serverName, Valid: true},
		Since:      pgtype.Timestamptz{Time: from, Valid: true},
		Until:      pgtype.Timestamptz{Time: to, Valid: true},
	})
	if err != nil {
		t.Fatalf("ListTagValues: %v", err)
	}

	if len(suggested) != 0 {
		t.Errorf("ListTagValues suggested %d values for a key no statement displays, want 0", len(suggested))
	}

	if n := matched(querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL, "payments"); n != 0 {
		t.Errorf("service=payments matched %d statements that never display it, want 0", n)
	}

	if n := matched(querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS); n != 0 {
		t.Errorf("service exists matched %d statements that display no service, want 0", n)
	}

	if n := matched(querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL, "payments"); n != 1 {
		t.Errorf("service!=payments matched %d statements, want 1 (absent tag passes)", n)
	}
}
