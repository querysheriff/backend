//go:build integration

package alerts_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/clickhouse"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/testdb"
)

func TestServersWithAlertEnabledNeedAWebhookAndNoOptOut(t *testing.T) {
	t.Parallel()

	const (
		defaulted = "enabled-by-default"
		optedIn   = "enabled-explicitly"
		optedOut  = "enabled-opted-out"
		noWebhook = "enabled-no-webhook"
		webhook   = "https://hooks.slack.com/services/T000/B000/secret"
	)

	ctx := context.Background()
	queries := db.New(testdb.Postgres(t))

	for _, server := range []string{defaulted, optedIn, optedOut} {
		seedServerWithWebhook(ctx, t, queries, server, webhook)
	}

	if _, err := queries.CreateCollectorToken(ctx, db.CreateCollectorTokenParams{
		ServerName: noWebhook, TokenHash: noWebhook,
	}); err != nil {
		t.Fatalf("CreateCollectorToken: %v", err)
	}

	toggles := []db.UpsertAlertToggleParams{
		{ServerName: optedIn, AlertKey: alerts.KeyWeeklyReport, Enabled: true},
		{ServerName: optedIn, AlertKey: alerts.KeyPanic, Enabled: false},
		{ServerName: optedOut, AlertKey: alerts.KeyWeeklyReport, Enabled: false},
	}
	for _, toggle := range toggles {
		if err := queries.UpsertAlertToggle(ctx, toggle); err != nil {
			t.Fatalf("UpsertAlertToggle(%+v): %v", toggle, err)
		}
	}

	servers, err := queries.ListServersWithAlertEnabled(ctx, alerts.KeyWeeklyReport)
	if err != nil {
		t.Fatalf("ListServersWithAlertEnabled: %v", err)
	}

	for server, want := range map[string]bool{defaulted: true, optedIn: true, optedOut: false, noWebhook: false} {
		if got := slices.Contains(servers, server); got != want {
			t.Errorf("%s listed = %v, want %v (servers: %v)", server, got, want, servers)
		}
	}
}

func TestWeeklyReportShowsEveryTagValueSampledThisWeek(t *testing.T) {
	t.Parallel()

	const serverName = "weekly-report-tags"

	ctx := context.Background()
	stats := clickhouse.New(testdb.ClickHouse(t))
	id := clickhouse.StatementID(serverName, "db", "app", 1)
	ran := time.Now().Add(-time.Hour)

	if err := stats.InsertStatementDeltas(ctx, []clickhouse.StatementDelta{{
		StatementID: id, CollectedAt: ran, ServerName: serverName, DatabaseName: "db",
		Calls: 10, TotalExecTime: 100,
	}}); err != nil {
		t.Fatalf("InsertStatementDeltas: %v", err)
	}

	samples := make([]clickhouse.Sample, 0, 3)
	for i, tags := range []map[string]string{
		{"op": "B", "process_id": "1"},
		{"op": "A"},
		{"op": "A", "process_id": "2"},
	} {
		samples = append(samples, clickhouse.Sample{
			ID: clickhouse.SampleID(serverName, ran, "SELECT 1", 1, int64(i)), CollectedAt: ran,
			ServerName: serverName, DatabaseName: "db", StatementID: id, Query: "SELECT 1", DurationMs: 1, Tags: tags,
		})
	}

	if err := stats.InsertSamples(ctx, samples); err != nil {
		t.Fatalf("InsertSamples: %v", err)
	}

	got, err := alerts.WeeklyReport(ctx, stats, serverName, "https://qs.example")
	if err != nil {
		t.Fatalf("WeeklyReport: %v", err)
	}

	want := "Top 10 busiest queries in the last week:\n" +
		"\n• *10 calls averaging 10 ms* `op=A` `op=B`" +
		fmt.Sprintf("\n  <https://qs.example/queries/%d|open in querysheriff>", id)
	if got != want {
		t.Errorf("WeeklyReport =\n%q\nwant\n%q", got, want)
	}
}
