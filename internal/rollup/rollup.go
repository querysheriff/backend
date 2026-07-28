package rollup

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/db"
)

const (
	refreshEvery   = 30 * time.Second
	trailingWindow = 10 * time.Minute
	backfillChunk  = 24 * time.Hour
)

func Run(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) {
	queries := db.New(pool)

	if err := catchUp(ctx, queries, logger); err != nil {
		logger.ErrorContext(ctx, "latency rollup catch-up failed", "error", err)
	}

	ticker := time.NewTicker(refreshEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			if err := rollRange(ctx, queries, now.Add(-trailingWindow), now); err != nil {
				logger.ErrorContext(ctx, "latency rollup refresh failed", "error", err)
			}
		}
	}
}

// catchUp rolls up everything between where the table left off and now.
func catchUp(ctx context.Context, queries *db.Queries, logger *slog.Logger) error {
	resume, err := queries.StatementLatencyRollupResume(ctx)
	if err != nil {
		return err
	}

	now := time.Now()
	for start := resume.Time; start.Before(now); start = start.Add(backfillChunk) {
		if err = rollRange(ctx, queries, start, start.Add(backfillChunk)); err != nil {
			return err
		}
	}

	logger.InfoContext(ctx, "latency rollup caught up", "from", resume.Time)

	return nil
}

func rollRange(ctx context.Context, queries *db.Queries, start, end time.Time) error {
	return queries.RollupStatementLatencyBins(ctx, db.RollupStatementLatencyBinsParams{
		RangeStart:  pgtype.Timestamptz{Time: start.Truncate(time.Minute), Valid: true},
		RangeEnd:    pgtype.Timestamptz{Time: end, Valid: true},
		UtilityKind: int32(querysheriffv1.QueryKind_QUERY_KIND_OTHERS),
	})
}
