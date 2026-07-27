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

// originalPercentileSeries is StatementPercentileSeries exactly as it stood before the
// bounds were moved into Go, the grid was dropped and the scope filter moved off the
// join. It is the reference the current query must still agree with.
const originalPercentileSeries = `
WITH bounds AS (
    SELECT
        $1::interval AS bucket,
        date_trunc('minute', least($3::timestamptz, now())) AS anchor,
        date_bin($1::interval, $2::timestamptz,
                 date_trunc('minute', least($3::timestamptz, now()))) AS first_end
),
grid AS (
    SELECT generate_series(b.first_end, b.anchor, b.bucket) AS bucket_end
    FROM bounds b
),
scoped AS (
    SELECT
        date_bin(b.bucket, d.collected_at - interval '1 microsecond', b.anchor) + b.bucket AS bucket_end,
        (d.total_exec_time / nullif(d.calls, 0))::double precision AS mean_ms,
        d.calls AS weight,
        (d.calls > 0
         AND s.query_kind <> $4::int
         AND ($6::text IS NULL OR s.database_name = $6)) AS matched
    FROM statement_deltas d
    JOIN statements s ON s.id = d.statement_id
    CROSS JOIN bounds b
    WHERE ($5::text IS NULL OR s.server_name = $5)
      AND ($7::text[] IS NULL OR s.server_name = ANY($7::text[]))
      AND d.collected_at > b.first_end - b.bucket
      AND d.collected_at <= b.anchor
),
ordered AS (
    SELECT
        bucket_end,
        mean_ms,
        sum(weight) OVER (PARTITION BY bucket_end ORDER BY mean_ms
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weight,
        sum(weight) OVER (PARTITION BY bucket_end) AS total_weight
    FROM scoped
    WHERE matched
),
agg AS (
    SELECT
        bucket_end,
        min(mean_ms) FILTER (WHERE cum_weight >= 0.90 * total_weight) AS p90,
        min(mean_ms) FILTER (WHERE cum_weight >= 0.95 * total_weight) AS p95,
        min(mean_ms) FILTER (WHERE cum_weight >= 0.99 * total_weight) AS p99
    FROM ordered
    GROUP BY bucket_end
)
SELECT
    g.bucket_end::timestamptz AS bucket_end,
    coalesce(a.p90, 0)::double precision AS p90,
    coalesce(a.p95, 0)::double precision AS p95,
    coalesce(a.p99, 0)::double precision AS p99
FROM grid g
JOIN (SELECT DISTINCT bucket_end FROM scoped) live ON live.bucket_end = g.bucket_end
LEFT JOIN agg a ON a.bucket_end = g.bucket_end
ORDER BY g.bucket_end`

type percentilePoint struct {
	at            time.Time
	p90, p95, p99 float64
}

func TestPercentileSeriesMatchesOriginalQuery(t *testing.T) {
	t.Parallel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping percentile parity test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	const serverName = "parity-test-server"

	// Anchored in the past so least(until, now()) is a no-op and both sides are
	// comparable; the Go side clamps against its own clock.
	until := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	seedParityData(ctx, t, pool, serverName, until)

	q := db.New(pool)
	utility := int32(querysheriffv1.QueryKind_QUERY_KIND_OTHERS)

	cases := []struct {
		name         string
		window       time.Duration
		databaseName pgtype.Text
		serverName   pgtype.Text
		allowed      []string
	}{
		{name: "1h unfiltered", window: time.Hour},
		{name: "1h server", window: time.Hour, serverName: pgtype.Text{String: serverName, Valid: true}},
		{
			name:         "1h server and database",
			window:       time.Hour,
			serverName:   pgtype.Text{String: serverName, Valid: true},
			databaseName: pgtype.Text{String: "db_a", Valid: true},
		},
		{name: "1h allowed servers", window: time.Hour, allowed: []string{serverName}},
		{name: "3h coarser bucket", window: 3 * time.Hour, serverName: pgtype.Text{String: serverName, Valid: true}},
		{name: "24h coarser bucket", window: 24 * time.Hour, serverName: pgtype.Text{String: serverName, Valid: true}},
		{
			name:       "70m indivisible bucket",
			window:     70 * time.Minute,
			serverName: pgtype.Text{String: serverName, Valid: true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			from := until.Add(-tc.window)
			bounds := newSeriesBounds(from, until, until)

			want := runOriginalPercentiles(ctx, t, pool, bounds.bucket, from, until,
				utility, tc.serverName, tc.databaseName, tc.allowed)

			rows, seriesErr := q.StatementPercentileSeries(ctx, db.StatementPercentileSeriesParams{
				RangeStart:     pgtype.Timestamptz{Time: bounds.rangeStart, Valid: true},
				RangeEnd:       pgtype.Timestamptz{Time: bounds.anchor, Valid: true},
				Anchor:         pgtype.Timestamptz{Time: bounds.anchor, Valid: true},
				Bucket:         pgtype.Interval{Microseconds: bounds.bucket.Microseconds(), Valid: true},
				ServerName:     tc.serverName,
				DatabaseName:   tc.databaseName,
				AllowedServers: tc.allowed,
				UtilityKind:    utility,
			})
			if seriesErr != nil {
				t.Fatalf("StatementPercentileSeries: %v", seriesErr)
			}

			got := make([]percentilePoint, len(rows))
			for i, r := range rows {
				got[i] = percentilePoint{at: r.BucketEnd.Time.UTC(), p90: r.P90, p95: r.P95, p99: r.P99}
			}

			requireSubstance(t, got)
			comparePercentiles(t, want, got)
		})
	}
}

// requireSubstance guards against the comparison passing on an empty or degenerate
// result, which would make the parity assertion meaningless.
func requireSubstance(t *testing.T, got []percentilePoint) {
	t.Helper()

	if len(got) < 2 {
		t.Fatalf("only %d bucket(s); fixture is too thin to compare", len(got))
	}

	distinct := map[float64]struct{}{}
	for _, p := range got {
		if p.p90 <= 0 {
			t.Fatalf("bucket %s has p90 %v; expected real percentiles", p.at, p.p90)
		}
		if p.p99 < p.p90 {
			t.Fatalf("bucket %s has p99 %v below p90 %v", p.at, p.p99, p.p90)
		}
		distinct[p.p90] = struct{}{}
	}

	if len(distinct) < 2 {
		t.Fatalf("every bucket has the same p90; fixture does not vary")
	}
}

func comparePercentiles(t *testing.T, want, got []percentilePoint) {
	t.Helper()

	if len(want) != len(got) {
		t.Fatalf("bucket count: original %d, current %d", len(want), len(got))
	}

	for i := range want {
		if !want[i].at.Equal(got[i].at) {
			t.Fatalf("bucket %d: original at %s, current at %s", i, want[i].at, got[i].at)
		}
		if want[i].p90 != got[i].p90 || want[i].p95 != got[i].p95 || want[i].p99 != got[i].p99 {
			t.Fatalf("bucket %d (%s): original p90/p95/p99 = %v/%v/%v, current = %v/%v/%v",
				i, want[i].at,
				want[i].p90, want[i].p95, want[i].p99,
				got[i].p90, got[i].p95, got[i].p99)
		}
	}
}

func runOriginalPercentiles(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	bucket time.Duration,
	from, until time.Time,
	utility int32,
	serverName, databaseName pgtype.Text,
	allowed []string,
) []percentilePoint {
	t.Helper()

	rows, err := pool.Query(ctx, originalPercentileSeries,
		pgtype.Interval{Microseconds: bucket.Microseconds(), Valid: true},
		pgtype.Timestamptz{Time: from, Valid: true},
		pgtype.Timestamptz{Time: until, Valid: true},
		utility, serverName, databaseName, allowed)
	if err != nil {
		t.Fatalf("original query: %v", err)
	}
	defer rows.Close()

	var out []percentilePoint
	for rows.Next() {
		var (
			at            pgtype.Timestamptz
			p90, p95, p99 float64
		)
		if scanErr := rows.Scan(&at, &p90, &p95, &p99); scanErr != nil {
			t.Fatalf("scan original: %v", scanErr)
		}
		out = append(out, percentilePoint{at: at.Time.UTC(), p90: p90, p95: p95, p99: p99})
	}
	if rows.Err() != nil {
		t.Fatalf("original rows: %v", rows.Err())
	}

	return out
}

// seedParityData writes 24h of minute-cadence deltas across two databases, including a
// utility statement the percentile filter must exclude and a zero-call row, so buckets
// that are live but have nothing matching are exercised too.
func seedParityData(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	serverName string,
	until time.Time,
) {
	t.Helper()

	cleanup := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM statement_deltas WHERE server_name = $1`, serverName)
		_, _ = pool.Exec(ctx, `DELETE FROM statements WHERE server_name = $1`, serverName)
	}
	cleanup()
	t.Cleanup(cleanup)

	if _, err := pool.Exec(ctx, `
		WITH ins AS (
		    INSERT INTO statements (server_name, database_name, user_name, query_id,
		                            query_full, query_short, query_kind)
		    SELECT $1, d.name, 'app', n, 'SELECT ' || n, 'SELECT ' || n, k.kind
		    FROM generate_series(1, 6) AS n
		    CROSS JOIN (VALUES ('db_a'), ('db_b')) AS d(name)
		    CROSS JOIN LATERAL (SELECT CASE WHEN n % 4 = 0 THEN 3 ELSE 1 + (n % 3) END AS kind) k
		    RETURNING id, database_name, query_id
		)
		INSERT INTO statement_deltas (statement_id, collected_at, server_name, database_name,
		                              calls, rows, total_exec_time, total_io_time)
		SELECT ins.id, ts, $1, ins.database_name,
		       CASE WHEN ins.query_id = 5 THEN 0 ELSE 10 + (ins.query_id * 7) % 43 END,
		       100 + ins.query_id,
		       ((ins.query_id * 13 + extract(epoch FROM ts)::bigint / 60) % 97) * 1.37,
		       ins.query_id * 0.11
		FROM ins
		CROSS JOIN generate_series($2::timestamptz - interval '24 hours',
		                           $2::timestamptz - interval '1 minute',
		                           interval '1 minute') AS ts`,
		serverName, until,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
}
