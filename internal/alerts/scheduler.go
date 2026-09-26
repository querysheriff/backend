package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/humanize"
)

const (
	monitoringScanEvery  = time.Minute
	monitoringStaleAfter = 10 * time.Minute
	pruneScanEvery       = 24 * time.Hour
	reportScanEvery      = 15 * time.Minute
	reportHourUTC        = 8
	weeklyWindow         = 7 * 24 * time.Hour
	weeklyTopRows        = 10
)

type scheduler struct {
	queries      *db.Queries
	stats        *clickhouse.Client
	notifier     *Notifier
	dashboardURL string
	logger       *slog.Logger
}

func RunScheduler(
	ctx context.Context,
	queries *db.Queries,
	stats *clickhouse.Client,
	notifier *Notifier,
	dashboardURL string,
	logger *slog.Logger,
) {
	s := scheduler{queries: queries, stats: stats, notifier: notifier, dashboardURL: dashboardURL, logger: logger}

	monitoringTicker := time.NewTicker(monitoringScanEvery)
	defer monitoringTicker.Stop()

	reportTicker := time.NewTicker(reportScanEvery)
	defer reportTicker.Stop()

	pruneTicker := time.NewTicker(pruneScanEvery)
	defer pruneTicker.Stop()

	s.prune(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-monitoringTicker.C:
			s.fireStaleServers(ctx)
		case <-reportTicker.C:
			s.sendWeeklyReports(ctx)
		case <-pruneTicker.C:
			s.prune(ctx)
		}
	}
}

func (s scheduler) fireStaleServers(ctx context.Context) {
	servers, err := s.queries.ListStaleServers(ctx, intervalFromDuration(monitoringStaleAfter))
	if err != nil {
		s.logger.ErrorContext(ctx, "stale-server scan failed", "error", err)

		return
	}

	for _, server := range servers {
		s.notifier.Fire(server, "", KeyMonitoringStopped,
			fmt.Sprintf("No data for over %s.", humanize.Duration(monitoringStaleAfter)))
	}
}

func (s scheduler) sendWeeklyReports(ctx context.Context) {
	now := time.Now().UTC()
	if now.Weekday() != time.Monday || now.Hour() != reportHourUTC {
		return
	}

	servers, err := s.queries.ListServersWithAlertEnabled(ctx, KeyWeeklyReport)
	if err != nil {
		s.logger.ErrorContext(ctx, "weekly report scan failed", "error", err)

		return
	}

	for _, server := range servers {
		text, buildErr := s.weeklyReport(ctx, server)
		if buildErr != nil {
			s.logger.ErrorContext(ctx, "weekly report build failed", "server", server, "error", buildErr)

			continue
		}

		if text != "" {
			s.notifier.Fire(server, "", KeyWeeklyReport, text)
		}
	}
}

// weeklyReport lists the server's busiest queries of the last week; it is empty when nothing ran.
func (s scheduler) weeklyReport(ctx context.Context, server string) (string, error) {
	rows, err := s.stats.TopStatements(ctx, server, time.Now().Add(-weeklyWindow), weeklyTopRows)
	if err != nil || len(rows) == 0 {
		return "", err
	}

	var b strings.Builder

	fmt.Fprintf(&b, "Top %d busiest queries in the last week:\n", weeklyTopRows)

	for _, row := range rows {
		fmt.Fprintf(&b, "\n• *%d calls averaging %s*%s", row.Calls, humanize.Millis(row.AvgMs), tagPills(row.Tags))

		if s.dashboardURL != "" {
			fmt.Fprintf(&b, "\n  <%s/queries/%d|open in querysheriff>", s.dashboardURL, row.ID)
		}
	}

	return b.String(), nil
}

func (s scheduler) prune(ctx context.Context) {
	if err := s.queries.PruneAlertFires(ctx, intervalFromDuration(FireHistoryWindow)); err != nil {
		s.logger.ErrorContext(ctx, "alert fire prune failed", "error", err)
	}

	if err := s.queries.DeleteExpiredSessions(ctx); err != nil {
		s.logger.ErrorContext(ctx, "expired session prune failed", "error", err)
	}
}
