//go:build integration

package alerts_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

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
	pool := testdb.Postgres(t)
	queries := db.New(pool)

	unreachable := httptest.NewServer(nil)
	webhook := unreachable.URL + "/services/T000/B000/" + secret
	unreachable.Close()

	seedServerWithWebhook(ctx, t, queries, serverName, webhook)

	var logs bytes.Buffer

	notifier := alerts.NewNotifier(pool, slog.New(slog.NewTextHandler(&logs, nil)))

	for range 2 {
		notifier.Fire(serverName, "", alerts.KeyPanic, "server crashed")
		notifier.Wait()
	}

	if got := strings.Count(logs.String(), "alert delivery failed"); got != 2 {
		t.Fatalf("logs = %q, want both deliveries attempted (a failure must release the cooldown)", logs.String())
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
}

func TestAClaimInFlightElsewhereSuppressesTheFire(t *testing.T) {
	t.Parallel()

	const serverName = "notifier-claim-in-flight"

	ctx := context.Background()
	pool := testdb.Postgres(t)
	queries := db.New(pool)

	var posts atomic.Int32

	slack := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { posts.Add(1) }))
	defer slack.Close()

	seedServerWithWebhook(ctx, t, queries, serverName, slack.URL)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	other := queries.WithTx(tx)
	if err = other.LockAlertClaim(
		ctx,
		db.LockAlertClaimParams{ServerName: serverName, AlertKey: alerts.KeyLongQuery},
	); err != nil {
		t.Fatalf("LockAlertClaim: %v", err)
	}

	if _, err = other.ClaimAlertFire(ctx, db.ClaimAlertFireParams{
		ServerName: serverName,
		AlertKey:   alerts.KeyLongQuery,
		Cooldown:   pgtype.Interval{Microseconds: time.Minute.Microseconds(), Valid: true},
	}); err != nil {
		t.Fatalf("ClaimAlertFire: %v", err)
	}

	notifier := alerts.NewNotifier(pool, slog.New(slog.DiscardHandler))
	notifier.Fire(serverName, "", alerts.KeyLongQuery, "a query is slow")

	waitForAdvisoryLockWaiter(ctx, t, pool)

	if err = tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	notifier.Wait()

	if got := posts.Load(); got != 0 {
		t.Errorf("Slack posts = %d, want 0: the claim committed elsewhere is within the cooldown", got)
	}
}

func waitForAdvisoryLockWaiter(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	const waiters = "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		var count int
		if err := pool.QueryRow(ctx, waiters).Scan(&count); err != nil {
			t.Fatalf("count advisory lock waiters: %v", err)
		}

		if count > 0 {
			return
		}
	}

	t.Fatal("the notifier's claim never waited for the one in flight")
}

func seedServerWithWebhook(ctx context.Context, t *testing.T, queries *db.Queries, serverName, webhook string) {
	t.Helper()

	if _, err := queries.CreateCollectorToken(ctx, db.CreateCollectorTokenParams{
		ServerName: serverName, TokenHash: serverName,
	}); err != nil {
		t.Fatalf("CreateCollectorToken: %v", err)
	}

	if _, err := queries.UpdateServerWebhook(ctx, db.UpdateServerWebhookParams{
		ServerName: serverName, SlackWebhookUrl: webhook,
	}); err != nil {
		t.Fatalf("UpdateServerWebhook: %v", err)
	}
}
