package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/humanize"
)

const (
	monitoringScanEvery  = time.Minute
	monitoringStaleAfter = 10 * time.Minute
	pruneScanEvery       = 24 * time.Hour
	pctScale             = 100.0
	reportScanEvery      = 15 * time.Minute
	reportHourUTC        = 8
	weeklyWindow         = 7 * 24 * time.Hour
	weeklyQuantile       = 0.99
	utilityKind          = int32(querysheriffv1.QueryKind_QUERY_KIND_OTHERS)
	slowQueryWindow      = 24 * time.Hour
	slowQueryMinCalls    = 10
	slowQueryMinAvgMs    = 1000.0
	slowQueryMaxRows     = 10
	weeklyTopRows        = 3
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
			s.sendReports(ctx)
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
		s.notifier.Fire(server, KeyMonitoringStopped,
			fmt.Sprintf("No data for over %s.", humanize.Duration(monitoringStaleAfter)))
	}
}

func (s scheduler) sendReports(ctx context.Context) {
	now := time.Now().UTC()
	if now.Hour() != reportHourUTC {
		return
	}

	s.sendReport(ctx, KeySlowQueryReport, s.slowQueryReport)

	if now.Weekday() == time.Monday {
		s.sendReport(ctx, KeyWeeklyReport, s.weeklyReport)
	}
}

// sendReport fires the report to every server that has it enabled; an empty report is not sent.
func (s scheduler) sendReport(
	ctx context.Context,
	alertKey string,
	build func(ctx context.Context, server string) (string, error),
) {
	servers, err := s.queries.ListServersWithAlertEnabled(ctx, alertKey)
	if err != nil {
		s.logger.ErrorContext(ctx, "report scan failed", "alert", alertKey, "error", err)

		return
	}

	for _, server := range servers {
		text, buildErr := build(ctx, server)
		if buildErr != nil {
			s.logger.ErrorContext(ctx, "report build failed", "alert", alertKey, "server", server, "error", buildErr)

			continue
		}

		if text != "" {
			s.notifier.Fire(server, alertKey, text)
		}
	}
}

func (s scheduler) slowQueryReport(ctx context.Context, server string) (string, error) {
	rows, err := s.stats.TopStatements(ctx, clickhouse.TopStatementsParams{
		ServerName: server,
		From:       time.Now().Add(-slowQueryWindow),
		MinCalls:   slowQueryMinCalls,
		MinAvgMs:   slowQueryMinAvgMs,
		MaxRows:    slowQueryMaxRows,
	})
	if err != nil || len(rows) == 0 {
		return "", err
	}

	var b strings.Builder

	fmt.Fprintf(&b, "%s in the last 24 hours.\n",
		humanize.Plural(len(rows), "slow query", "slow queries"))

	for _, row := range rows {
		headline := fmt.Sprintf("%d calls averaging %s", row.Calls, humanize.Millis(row.AvgMs))
		s.writeStatement(&b, headline, row.Tags, row.ID)
	}

	return b.String(), nil
}

func (s scheduler) weeklyReport(ctx context.Context, server string) (string, error) {
	now := time.Now()
	currentStart := now.Add(-weeklyWindow)
	previousStart := now.Add(-2 * weeklyWindow)

	p99Current, err := s.stats.LatencyQuantile(ctx, server, currentStart, now, weeklyQuantile, utilityKind)
	if err != nil {
		return "", err
	}

	p99Previous, err := s.stats.LatencyQuantile(ctx, server, previousStart, currentStart, weeklyQuantile, utilityKind)
	if err != nil {
		return "", err
	}

	callsCurrent, err := s.stats.CallsBetween(ctx, server, currentStart, now)
	if err != nil {
		return "", err
	}

	callsPrevious, err := s.stats.CallsBetween(ctx, server, previousStart, currentStart)
	if err != nil {
		return "", err
	}

	errorsCurrent, errorsPrevious, err := s.stats.CountLogErrors(
		ctx, server, previousStart, currentStart, now, errorLogLevels())
	if err != nil {
		return "", err
	}

	top, err := s.stats.TopStatements(ctx, clickhouse.TopStatementsParams{
		ServerName: server, From: currentStart, MaxRows: weeklyTopRows,
	})
	if err != nil {
		return "", err
	}

	var b strings.Builder

	b.WriteString("Last 7 days vs the week before.\n")
	fmt.Fprintf(&b, "\n• *Query time p99:* %s (%s)",
		humanize.Millis(p99Current), pctTrend(p99Current, p99Previous))
	fmt.Fprintf(&b, "\n• *Queries run:* %s (%s)",
		humanize.Count(callsCurrent), pctTrend(float64(callsCurrent), float64(callsPrevious)))
	fmt.Fprintf(&b, "\n• *Errors logged:* %d (%s)",
		errorsCurrent, countTrend(errorsCurrent, errorsPrevious))

	if len(top) > 0 {
		b.WriteString("\n\nBusiest queries:")
		for _, statement := range top {
			headline := humanize.Duration(time.Duration(statement.TotalMs) * time.Millisecond)
			s.writeStatement(&b, headline, statement.Tags, statement.ID)
		}
	}

	return b.String(), nil
}

func errorLogLevels() []int32 {
	return []int32{
		int32(querysheriffv1.LogEvent_LOG_LEVEL_ERROR),
		int32(querysheriffv1.LogEvent_LOG_LEVEL_FATAL),
		int32(querysheriffv1.LogEvent_LOG_LEVEL_PANIC),
	}
}

func (s scheduler) writeStatement(b *strings.Builder, headline string, tags map[string]string, statementID uint64) {
	fmt.Fprintf(b, "\n• *%s*", headline)

	if list := formatTags(tags); list != "" {
		fmt.Fprintf(b, " — %s", list)
	}

	if s.dashboardURL != "" {
		fmt.Fprintf(b, "\n  <%s/queries/%d|open in querysheriff>", s.dashboardURL, statementID)
	}
}

func formatTags(tags map[string]string) string {
	pairs := make([]string, 0, len(tags))
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		pairs = append(pairs, key+"="+tags[key])
	}

	return strings.Join(pairs, ", ")
}

func (s scheduler) prune(ctx context.Context) {
	if err := s.queries.PruneAlertFires(ctx, intervalFromDuration(FireHistoryWindow)); err != nil {
		s.logger.ErrorContext(ctx, "alert fire prune failed", "error", err)
	}

	if err := s.queries.DeleteExpiredSessions(ctx); err != nil {
		s.logger.ErrorContext(ctx, "expired session prune failed", "error", err)
	}
}

func pctTrend(current, previous float64) string {
	if previous == 0 {
		if current == 0 {
			return "▲ 0%"
		}

		return "new this week"
	}

	pct := (current - previous) / previous * pctScale
	if pct >= 0 {
		return fmt.Sprintf("▲ %.0f%%", pct)
	}

	return fmt.Sprintf("▼ %.0f%%", -pct)
}

func countTrend(current, previous int64) string {
	delta := current - previous
	if delta < 0 {
		return fmt.Sprintf("▼ %d", -delta)
	}

	return fmt.Sprintf("▲ %d", delta)
}
