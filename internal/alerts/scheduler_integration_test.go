//go:build integration

package alerts_test

import (
	"context"
	"slices"
	"testing"

	"github.com/querysheriff/backend/internal/alerts"
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
