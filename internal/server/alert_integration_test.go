//go:build integration

package server_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/server"
	"github.com/querysheriff/backend/internal/testdb"
)

func TestAlertSettingsReadBackWhatWasSaved(t *testing.T) {
	t.Parallel()

	const (
		serverName = "alert-settings-round-trip"
		webhook    = "https://hooks.slack.com/services/T000/B000/secret"
	)

	ctx := viewer(context.Background())
	queries := db.New(testdb.Postgres(t))
	srv := server.NewAlertServer(queries)

	if err := queries.UpsertCollectorHealth(ctx, db.UpsertCollectorHealthParams{
		ServerName:  serverName,
		CollectedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Databases:   []string{"shop"},
	}); err != nil {
		t.Fatalf("UpsertCollectorHealth: %v", err)
	}

	if _, err := srv.UpdateAlertWebhook(ctx, connect.NewRequest(&querysheriffv1.UpdateAlertWebhookRequest{
		ServerName: serverName, SlackWebhookUrl: webhook,
	})); err != nil {
		t.Fatalf("UpdateAlertWebhook: %v", err)
	}

	for _, enabled := range []bool{true, false} {
		if _, err := srv.UpdateAlertSetting(ctx, connect.NewRequest(&querysheriffv1.UpdateAlertSettingRequest{
			ServerName: serverName, Key: alerts.KeyWeeklyReport, Enabled: enabled,
		})); err != nil {
			t.Fatalf("UpdateAlertSetting(%v): %v", enabled, err)
		}
	}

	resp, err := srv.ListAlertSettings(ctx, connect.NewRequest(&querysheriffv1.ListAlertSettingsRequest{}))
	if err != nil {
		t.Fatalf("ListAlertSettings: %v", err)
	}

	for _, settings := range resp.Msg.GetServers() {
		if settings.GetServerName() != serverName {
			continue
		}

		if settings.GetSlackWebhookUrl() != webhook {
			t.Errorf("webhook = %q, want %q", settings.GetSlackWebhookUrl(), webhook)
		}

		for _, alert := range settings.GetAlerts() {
			if want := alert.GetKey() != alerts.KeyWeeklyReport; alert.GetEnabled() != want {
				t.Errorf("%s enabled = %v, want %v (the last update wins, the rest default on)",
					alert.GetKey(), alert.GetEnabled(), want)
			}
		}

		return
	}

	t.Fatalf("server %s not listed", serverName)
}
