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

func RunScheduler(
	ctx context.Context,
	queries *db.Queries,
	stats *clickhouse.Client,
	notifier *Notifier,
	dashboardURL string,
	logger *slog.Logger,
) {
	monitoringTicker := time.NewTicker(monitoringScanEvery)
	defer monitoringTicker.Stop()

	reportTicker := time.NewTicker(reportScanEvery)
	defer reportTicker.Stop()

	pruneTicker := time.NewTicker(pruneScanEvery)
	defer pruneTicker.Stop()

	pruneFireHistory(ctx, queries, logger)

	for {
		select {
		case <-ctx.Done():
			return
		case <-monitoringTicker.C:
			evalStaleServers(ctx, queries, notifier, logger)
		case <-reportTicker.C:
			evalReports(ctx, queries, stats, notifier, dashboardURL, logger)
		case <-pruneTicker.C:
			pruneFireHistory(ctx, queries, logger)
		}
	}
}

func evalStaleServers(ctx context.Context, queries *db.Queries, notifier *Notifier, logger *slog.Logger) {
	servers, err := queries.ListStaleServers(ctx, intervalFromDuration(monitoringStaleAfter))
	if err != nil {
		logger.ErrorContext(ctx, "stale-server scan failed", "error", err)

		return
	}

	for _, server := range servers {
		notifier.Fire(server, KeyMonitoringStopped, "No data for over 10 minutes.")
	}
}

func evalReports(
	ctx context.Context,
	queries *db.Queries,
	stats *clickhouse.Client,
	notifier *Notifier,
	dashboardURL string,
	logger *slog.Logger,
) {
	now := time.Now().UTC()
	if now.Hour() != reportHourUTC {
		return
	}

	sendSlowQueryReports(ctx, queries, stats, notifier, dashboardURL, logger)

	if now.Weekday() == time.Monday {
		sendWeeklyReports(ctx, queries, stats, notifier, dashboardURL, logger)
	}
}

func sendSlowQueryReports(
	ctx context.Context,
	queries *db.Queries,
	stats *clickhouse.Client,
	notifier *Notifier,
	dashboardURL string,
	logger *slog.Logger,
) {
	servers, err := queries.ListServersWithAlertEnabled(ctx, KeySlowQueryReport)
	if err != nil {
		logger.ErrorContext(ctx, "slow-query report scan failed", "error", err)

		return
	}

	for _, server := range servers {
		rows, listErr := stats.ListSlowStatements(ctx, clickhouse.SlowStatementsParams{
			ServerName: server,
			From:       time.Now().Add(-slowQueryWindow),
			MinCalls:   slowQueryMinCalls,
			MinAvgMs:   slowQueryMinAvgMs,
			MaxRows:    slowQueryMaxRows,
		})
		if listErr != nil {
			logger.ErrorContext(ctx, "slow-query lookup failed", "server", server, "error", listErr)

			continue
		}

		if len(rows) == 0 {
			continue
		}

		notifier.Fire(server, KeySlowQueryReport, slowQueryReportText(rows, dashboardURL))
	}
}

func sendWeeklyReports(
	ctx context.Context,
	queries *db.Queries,
	stats *clickhouse.Client,
	notifier *Notifier,
	dashboardURL string,
	logger *slog.Logger,
) {
	servers, err := queries.ListServersWithAlertEnabled(ctx, KeyWeeklyReport)
	if err != nil {
		logger.ErrorContext(ctx, "weekly report scan failed", "error", err)

		return
	}

	for _, server := range servers {
		text, buildErr := weeklyReportText(ctx, stats, server, dashboardURL)
		if buildErr != nil {
			logger.ErrorContext(ctx, "weekly report build failed", "server", server, "error", buildErr)

			continue
		}

		notifier.Fire(server, KeyWeeklyReport, text)
	}
}

func slowQueryReportText(rows []clickhouse.SlowStatement, dashboardURL string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "%s in the last 24 hours.\n",
		humanize.Plural(len(rows), "slow query", "slow queries"))

	for _, row := range rows {
		headline := fmt.Sprintf("%d calls averaging %s", row.Calls, humanize.Millis(row.AvgMs))
		writeStatement(&b, headline, row.Tags, row.ID, dashboardURL)
	}

	return b.String()
}

func weeklyReportText(
	ctx context.Context,
	stats *clickhouse.Client,
	server, dashboardURL string,
) (string, error) {
	now := time.Now()
	currentStart := now.Add(-weeklyWindow)
	previousStart := now.Add(-2 * weeklyWindow)

	p99Current, err := stats.LatencyQuantile(ctx, server, currentStart, now, weeklyQuantile, utilityKind)
	if err != nil {
		return "", err
	}

	p99Previous, err := stats.LatencyQuantile(ctx, server, previousStart, currentStart, weeklyQuantile, utilityKind)
	if err != nil {
		return "", err
	}

	callsCurrent, err := stats.CallsBetween(ctx, server, currentStart, now)
	if err != nil {
		return "", err
	}

	callsPrevious, err := stats.CallsBetween(ctx, server, previousStart, currentStart)
	if err != nil {
		return "", err
	}

	errorsCurrent, errorsPrevious, err := stats.CountLogErrors(
		ctx, server, previousStart, currentStart, now, errorLogLevels())
	if err != nil {
		return "", err
	}

	top, err := stats.TopStatementsByExecTime(ctx, server, currentStart, weeklyTopRows)
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
			writeStatement(&b, headline, statement.Tags, statement.ID, dashboardURL)
		}
	}

	return b.String(), nil
}

func errorLogLevels() []int32 {
	return []int32{5, 7, 8}
}

func writeStatement(
	b *strings.Builder,
	headline string,
	tags map[string]string,
	statementID uint64,
	dashboardURL string,
) {
	fmt.Fprintf(b, "\n• *%s*", headline)

	if list := formatTags(tags); list != "" {
		fmt.Fprintf(b, " — %s", list)
	}

	if dashboardURL != "" {
		fmt.Fprintf(b, "\n  <%s/queries/%d|open in querysheriff>", dashboardURL, statementID)
	}
}

func formatTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}

	pairs := make([]string, 0, len(tags))
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		pairs = append(pairs, key+"="+tags[key])
	}

	return strings.Join(pairs, ", ")
}

func pruneFireHistory(ctx context.Context, queries *db.Queries, logger *slog.Logger) {
	if err := queries.PruneAlertFires(ctx, intervalFromDuration(FireHistoryWindow)); err != nil {
		logger.ErrorContext(ctx, "alert fire prune failed", "error", err)
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
