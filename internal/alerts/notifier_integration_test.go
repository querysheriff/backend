//go:build integration

package alerts_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/gen/db"
	"github.com/querysheriff/backend/internal/testdb"
)

func TestFailedDeliveryReleasesTheCooldownAndHidesTheWebhook(t *testing.T) {
	t.Parallel()

	const (
		serverName = "notifier-failed-delivery"
		secret     = "webhook-secret-token"
	)

	ctx := context.Background()
	queries := db.New(testdb.Postgres(t))

	unreachable := httptest.NewServer(nil)
	webhook := unreachable.URL + "/services/T000/B000/" + secret
	unreachable.Close()

	if err := queries.UpsertAlertWebhook(ctx, db.UpsertAlertWebhookParams{
		ServerName: serverName, SlackWebhookUrl: webhook,
	}); err != nil {
		t.Fatalf("UpsertAlertWebhook: %v", err)
	}

	var logs bytes.Buffer

	notifier := alerts.NewNotifier(queries, slog.New(slog.NewTextHandler(&logs, nil)))
	notifier.Fire(serverName, alerts.KeyPanic, "server crashed")
	notifier.Wait()

	if !strings.Contains(logs.String(), "alert delivery failed") {
		t.Fatalf("logs = %q, want the delivery failure reported", logs.String())
	}

	if strings.Contains(logs.String(), secret) {
		t.Errorf("logs leak the webhook URL: %q", logs.String())
	}

	week := pgtype.Interval{Microseconds: (7 * 24 * time.Hour).Microseconds(), Valid: true}

	fires, err := queries.CountRecentAlertFires(ctx, db.CountRecentAlertFiresParams{
		HistoryWindow: week, AllowedServers: []string{serverName},
	})
	if err != nil {
		t.Fatalf("CountRecentAlertFires: %v", err)
	}

	if len(fires) != 0 {
		t.Errorf("fires = %+v, want none recorded for an undelivered alert", fires)
	}

	if _, err = queries.TryClaimAlertNotification(ctx, db.TryClaimAlertNotificationParams{
		ServerName: serverName, AlertKey: alerts.KeyPanic, Cooldown: week,
	}); err != nil {
		t.Errorf("claim after a failed delivery = %v, want the cooldown released", err)
	}
}
