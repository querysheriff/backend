package server

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/db"
)

// v11PercentileSeries is the generated SQL from v0.0.11 (966f629), copied verbatim.
// Params, in order: bucket, until, since, utility_kind, database_name, statement_id, text_filter, statement_ids, server_name, allowed_servers.
const v11PercentileSeries = `-- name: StatementPercentileSeries :many
WITH bounds AS (
    SELECT
        $1::interval AS bucket,
        date_trunc('minute', least($2::timestamptz, now())) AS anchor,
        date_bin(
            $1::interval,
            $3::timestamptz,
            date_trunc('minute', least($2::timestamptz, now()))
        ) AS first_end
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
         AND ($5::text IS NULL OR s.database_name = $5)
         AND ($6::bigint IS NULL OR d.statement_id = $6)
         AND ($7::text IS NULL
              OR s.query_full ILIKE '%' || $7::text || '%')
         AND ($8::bigint[] IS NULL
              OR s.id = ANY($8::bigint[]))) AS matched
    FROM statement_deltas d
    JOIN statements s ON s.id = d.statement_id
    CROSS JOIN bounds b
    WHERE ($9::text IS NULL OR s.server_name = $9)
      AND ($10::text[] IS NULL OR s.server_name = ANY($10::text[]))
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

// v11MetricSeries is the generated SQL from v0.0.11 (966f629), copied verbatim.
// Params, in order: bucket, until, since, database_name, statement_id, text_filter, statement_ids, server_name, allowed_servers.
const v11MetricSeries = `-- name: StatementMetricSeries :many
WITH bounds AS (
    SELECT
        $1::interval AS bucket,
        date_trunc('minute', least($2::timestamptz, now())) AS anchor,
        date_bin(
            $1::interval,
            $3::timestamptz,
            date_trunc('minute', least($2::timestamptz, now()))
        ) AS first_end
),
grid AS (
    SELECT generate_series(b.first_end, b.anchor, b.bucket) AS bucket_end
    FROM bounds b
),
scoped AS (
    SELECT
        date_bin(b.bucket, d.collected_at - interval '1 microsecond', b.anchor) + b.bucket AS bucket_end,
        d.total_exec_time,
        d.total_io_time,
        d.calls,
        (($4::text IS NULL OR s.database_name = $4)
         AND ($5::bigint IS NULL OR d.statement_id = $5)
         AND ($6::text IS NULL
              OR s.query_full ILIKE '%' || $6::text || '%')
         AND ($7::bigint[] IS NULL
              OR s.id = ANY($7::bigint[]))) AS matched
    FROM statement_deltas d
    JOIN statements s ON s.id = d.statement_id
    CROSS JOIN bounds b
    WHERE ($8::text IS NULL OR s.server_name = $8)
      AND ($9::text[] IS NULL OR s.server_name = ANY($9::text[]))
      AND d.collected_at > b.first_end - b.bucket
      AND d.collected_at <= b.anchor
)
SELECT
    g.bucket_end::timestamptz AS bucket_end,
    coalesce(sum(sc.total_exec_time) FILTER (WHERE sc.matched), 0)::double precision AS total_exec_time,
    coalesce(sum(sc.total_io_time) FILTER (WHERE sc.matched), 0)::double precision    AS total_io_time,
    coalesce(sum(sc.calls) FILTER (WHERE sc.matched), 0)::bigint                      AS calls
FROM grid g
JOIN scoped sc ON sc.bucket_end = g.bucket_end
GROUP BY g.bucket_end
ORDER BY g.bucket_end`

type percentilePoint struct {
	at            time.Time
	p90, p95, p99 float64
}

// The shipping percentile path reads a pre-aggregated log histogram, so it cannot match
// the exact per-delta computation bit for bit -- 1% bins with a midpoint estimate put it
// within about half a percent. This asserts that bound against v0.0.11's exact query, and
// asserts the buckets themselves line up exactly.
func TestPercentileSeriesApproximatesExactPercentiles(t *testing.T) {
	t.Parallel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping percentile accuracy test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	const serverName = "percentile-accuracy-server"

	until := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	seedParityData(ctx, t, pool, serverName, until)

	queries := db.New(pool)
	utility := int32(querysheriffv1.QueryKind_QUERY_KIND_OTHERS)

	// Roll up the seeded window so the read is served from bins rather than the raw tail.
	if err = queries.RollupStatementLatencyBins(ctx, db.RollupStatementLatencyBinsParams{
		RangeStart:  pgtype.Timestamptz{Time: until.Add(-25 * time.Hour), Valid: true},
		RangeEnd:    pgtype.Timestamptz{Time: until, Valid: true},
		UtilityKind: utility,
	}); err != nil {
		t.Fatalf("rollup: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM statement_latency_bins WHERE server_name = $1`, serverName)
	})

	server := NewStatementServer(queries, alerts.NewNotifier(queries, slog.New(slog.DiscardHandler)))
	scoped := pgtype.Text{String: serverName, Valid: true}

	for _, window := range []time.Duration{time.Hour, 3 * time.Hour, 24 * time.Hour, 70 * time.Minute} {
		t.Run(window.String(), func(t *testing.T) {
			t.Parallel()

			from := until.Add(-window)
			bounds := newSeriesBounds(from, until, until)

			want := runOriginalPercentiles(ctx, t, pool, bounds.bucket, from, until,
				utility, scoped, pgtype.Text{}, nil)

			p90, p95, p99, seriesErr := server.statementPercentiles(ctx, scoped, pgtype.Text{}, from, until, nil)
			if seriesErr != nil {
				t.Fatalf("statementPercentiles: %v", seriesErr)
			}

			got := make([]percentilePoint, len(p90.GetSeries()))
			for i := range p90.GetSeries() {
				got[i] = percentilePoint{
					at:  p90.GetSeries()[i].GetAt().AsTime(),
					p90: p90.GetSeries()[i].GetValue(),
					p95: p95.GetSeries()[i].GetValue(),
					p99: p99.GetSeries()[i].GetValue(),
				}
			}

			requireSubstance(t, got)
			comparePercentilesWithinBinWidth(t, want, got)
		})
	}
}

// comparePercentilesWithinBinWidth requires the buckets to match exactly and the values to
// sit inside the histogram's own resolution.
func comparePercentilesWithinBinWidth(t *testing.T, want, got []percentilePoint) {
	t.Helper()

	const tolerance = 0.01

	if len(want) != len(got) {
		t.Fatalf("bucket count: exact %d, histogram %d", len(want), len(got))
	}

	var worst float64

	for i := range want {
		if !want[i].at.Equal(got[i].at) {
			t.Fatalf("bucket %d: exact at %s, histogram at %s", i, want[i].at, got[i].at)
		}

		for _, pair := range [][2]float64{
			{want[i].p90, got[i].p90}, {want[i].p95, got[i].p95}, {want[i].p99, got[i].p99},
		} {
			if pair[0] == 0 && pair[1] == 0 {
				continue
			}

			rel := relativeDiff(pair[0], pair[1])
			worst = max(worst, rel)

			if rel > tolerance {
				t.Fatalf("bucket %d (%s): exact %v, histogram %v (relative %g)",
					i, want[i].at, pair[0], pair[1], rel)
			}
		}
	}

	t.Logf("%d buckets matched; worst relative difference %g", len(got), worst)
}

func requireSubstance(t *testing.T, got []percentilePoint) {
	t.Helper()

	if len(got) < 2 {
		t.Fatalf("only %d bucket(s); fixture is too thin to compare", len(got))
	}

	// A zero p90 is legitimate: the bucket held rows but none matched the filter.
	distinct := map[float64]struct{}{}
	nonZero := 0

	for _, p := range got {
		if p.p90 > 0 {
			nonZero++

			if p.p99 < p.p90 {
				t.Fatalf("bucket %s has p99 %v below p90 %v", p.at, p.p99, p.p90)
			}
		}

		distinct[p.p90] = struct{}{}
	}

	if nonZero == 0 {
		t.Fatal("every bucket is zeroed; fixture would not compare percentile math")
	}

	if len(distinct) < 2 {
		t.Fatalf("every bucket has the same p90; fixture does not vary")
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

	rows, err := pool.Query(ctx, v11PercentileSeries,
		pgtype.Interval{Microseconds: bucket.Microseconds(), Valid: true},
		pgtype.Timestamptz{Time: until, Valid: true},
		pgtype.Timestamptz{Time: from, Valid: true},
		utility, databaseName, pgtype.Int8{}, pgtype.Text{}, []int64(nil),
		serverName, allowed)
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
		_, _ = pool.Exec(ctx, `DELETE FROM statement_deltas d USING statements s
			WHERE s.id = d.statement_id AND s.server_name = $1`, serverName)
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
		INSERT INTO statement_deltas (statement_id, collected_at,
		                              calls, rows, total_exec_time, total_io_time)
		SELECT ins.id, ts,
		       CASE WHEN ins.query_id = 5 THEN 0 ELSE 10 + (ins.query_id * 7) % 43 END,
		       100 + ins.query_id,
		       ((ins.query_id * 13 + extract(epoch FROM ts)::bigint / 60) % 97) * 1.37,
		       ins.query_id * 0.11
		FROM ins
		CROSS JOIN generate_series($2::timestamptz - interval '24 hours',
		                           $2::timestamptz - interval '1 minute',
		                           interval '1 minute') AS ts
		-- db_b reports only on even minutes, so a database filter leaves buckets that
		-- are live but have nothing matching. A fixture where every database appears in
		-- every bucket hides exactly that difference.
		WHERE ins.database_name = 'db_a'
		   OR extract(minute FROM ts)::int % 2 = 0`,
		serverName, until,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

type metricPoint struct {
	at                                time.Time
	totalExecTime, totalIoTime, calls float64
}

// The metric series feeds the calls and avg lines on the same graph, so it has to
// match v0.0.11 as exactly as the percentiles do.
func TestMetricSeriesMatchesV11Query(t *testing.T) {
	t.Parallel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping metric parity test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	const serverName = "parity-metric-server"

	until := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	seedParityData(ctx, t, pool, serverName, until)

	q := db.New(pool)

	cases := []struct {
		name         string
		window       time.Duration
		serverName   pgtype.Text
		databaseName pgtype.Text
	}{
		{name: "1h all databases", window: time.Hour, serverName: pgtype.Text{String: serverName, Valid: true}},
		{
			name:         "1h sparse database",
			window:       time.Hour,
			serverName:   pgtype.Text{String: serverName, Valid: true},
			databaseName: pgtype.Text{String: "db_b", Valid: true},
		},
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

			want := runV11Metrics(ctx, t, pool, bounds.bucket, from, until, tc.serverName, tc.databaseName)

			rows, seriesErr := q.StatementMetricSeries(ctx, db.StatementMetricSeriesParams{
				RangeStart:   pgtype.Timestamptz{Time: bounds.rangeStart, Valid: true},
				RangeEnd:     pgtype.Timestamptz{Time: bounds.anchor, Valid: true},
				Anchor:       pgtype.Timestamptz{Time: bounds.anchor, Valid: true},
				Bucket:       pgtype.Interval{Microseconds: bounds.bucket.Microseconds(), Valid: true},
				ServerName:   tc.serverName,
				DatabaseName: tc.databaseName,
			})
			if seriesErr != nil {
				t.Fatalf("StatementMetricSeries: %v", seriesErr)
			}

			if len(rows) < 2 {
				t.Fatalf("only %d bucket(s); too thin to compare", len(rows))
			}

			got := make([]metricPoint, len(rows))
			for i, r := range rows {
				got[i] = metricPoint{
					at:            r.BucketEnd.Time.UTC(),
					totalExecTime: r.TotalExecTime,
					totalIoTime:   r.TotalIoTime,
					calls:         float64(r.Calls),
				}
			}

			if len(want) != len(got) {
				t.Fatalf("bucket count: v0.0.11 %d, current %d", len(want), len(got))
			}

			compareMetrics(t, want, got)
		})
	}
}

func runV11Metrics(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	bucket time.Duration,
	from, until time.Time,
	serverName, databaseName pgtype.Text,
) []metricPoint {
	t.Helper()

	rows, err := pool.Query(ctx, v11MetricSeries,
		pgtype.Interval{Microseconds: bucket.Microseconds(), Valid: true},
		pgtype.Timestamptz{Time: until, Valid: true},
		pgtype.Timestamptz{Time: from, Valid: true},
		databaseName, pgtype.Int8{}, pgtype.Text{}, []int64(nil),
		serverName, []string(nil))
	if err != nil {
		t.Fatalf("v0.0.11 metric query: %v", err)
	}
	defer rows.Close()

	var out []metricPoint
	for rows.Next() {
		var (
			at                      pgtype.Timestamptz
			execTime, ioTime, calls float64
		)
		if scanErr := rows.Scan(&at, &execTime, &ioTime, &calls); scanErr != nil {
			t.Fatalf("scan v0.0.11 metric: %v", scanErr)
		}
		out = append(out, metricPoint{at: at.Time.UTC(), totalExecTime: execTime, totalIoTime: ioTime, calls: calls})
	}
	if rows.Err() != nil {
		t.Fatalf("v0.0.11 metric rows: %v", rows.Err())
	}

	return out
}

func relativeDiff(a, b float64) float64 {
	if a == b {
		return 0
	}

	scale := a
	if scale < 0 {
		scale = -scale
	}

	if scale == 0 {
		return 1
	}

	diff := a - b
	if diff < 0 {
		diff = -diff
	}

	return diff / scale
}

// compareMetrics requires exact agreement on bucket boundaries and the bigint calls
// sum. The two float sums are added in a different row order because the plans differ,
// and float addition is not associative, so those agree only to within rounding.
func compareMetrics(t *testing.T, want, got []metricPoint) {
	t.Helper()

	const tolerance = 1e-12

	var worst float64

	for i := range want {
		if !want[i].at.Equal(got[i].at) {
			t.Fatalf("bucket %d: v0.0.11 at %s, current at %s", i, want[i].at, got[i].at)
		}

		if want[i].calls != got[i].calls {
			t.Fatalf("bucket %d: v0.0.11 calls %v, current %v", i, want[i].calls, got[i].calls)
		}

		worst = max(worst, worstFloatDiff(t, i, want[i], got[i], tolerance))
	}

	t.Logf("%d buckets matched; worst relative difference %g", len(got), worst)
}

func worstFloatDiff(t *testing.T, bucket int, want, got metricPoint, tolerance float64) float64 {
	t.Helper()

	var worst float64

	for _, pair := range [][2]float64{
		{want.totalExecTime, got.totalExecTime},
		{want.totalIoTime, got.totalIoTime},
	} {
		rel := relativeDiff(pair[0], pair[1])
		worst = max(worst, rel)

		if rel > tolerance {
			t.Fatalf("bucket %d (%s): v0.0.11 %v, current %v (relative %g)",
				bucket, want.at, pair[0], pair[1], rel)
		}
	}

	return worst
}
