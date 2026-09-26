package alerts

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/querysheriff/backend/internal/gen/db"
)

const (
	fireTimeout    = 15 * time.Second
	releaseTimeout = 5 * time.Second
)

type Notifier struct {
	queries  *db.Queries
	client   *http.Client
	logger   *slog.Logger
	inFlight sync.WaitGroup
}

func NewNotifier(queries *db.Queries, logger *slog.Logger) *Notifier {
	return &Notifier{
		queries: queries,
		client:  &http.Client{Timeout: slackTimeout},
		logger:  logger,
	}
}

// Fire sends an alert asynchronously to the server's Slack webhook.
// databaseName is optional and is used only in the message footer.
func (n *Notifier) Fire(serverName, databaseName, alertKey, text string) {
	def, ok := defByKey(alertKey)
	if !ok {
		n.logger.ErrorContext(context.Background(), "fire requested for unknown alert", "alert", alertKey)

		return
	}

	footer := serverName
	if databaseName != "" {
		footer += " · " + databaseName
	}

	n.inFlight.Go(func() { n.deliver(def, serverName, footer, text) })
}

// Wait blocks until every fired alert has been delivered or given up on.
func (n *Notifier) Wait() {
	n.inFlight.Wait()
}

func (n *Notifier) deliver(def Def, serverName, footer, text string) {
	ctx, cancel := context.WithTimeout(context.Background(), fireTimeout)
	defer cancel()

	webhookURL, err := n.queries.GetEnabledAlertWebhook(ctx, db.GetEnabledAlertWebhookParams{
		ServerName: serverName, AlertKey: def.Key,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return // no Slack destination, or this alert is switched off
	}
	if err != nil {
		n.logger.ErrorContext(ctx, "alert webhook lookup failed", "server", serverName, "alert", def.Key, "error", err)

		return
	}

	firedAt, claimed := n.claim(ctx, serverName, def)
	if !claimed {
		return // suppressed by the cooldown window
	}

	if err = postToSlack(ctx, n.client, webhookURL, def, footer, text); err != nil {
		n.logger.ErrorContext(ctx, "alert delivery failed", "server", serverName, "alert", def.Key, "error", err)
		n.release(serverName, def.Key, firedAt)
	}
}

func (n *Notifier) claim(ctx context.Context, serverName string, def Def) (pgtype.Timestamptz, bool) {
	firedAt, err := n.queries.TryClaimAlertNotification(ctx, db.TryClaimAlertNotificationParams{
		ServerName: serverName,
		AlertKey:   def.Key,
		Cooldown:   intervalFromDuration(def.Cooldown),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return firedAt, false
	}
	if err != nil {
		n.logger.ErrorContext(ctx, "alert claim failed", "server", serverName, "alert", def.Key, "error", err)

		return firedAt, false
	}

	return firedAt, true
}

// release undoes a claim whose delivery failed, so the next fire is not held back by the cooldown.
func (n *Notifier) release(serverName, alertKey string, firedAt pgtype.Timestamptz) {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()

	err := n.queries.ReleaseAlertClaim(ctx, db.ReleaseAlertClaimParams{
		ServerName: serverName,
		AlertKey:   alertKey,
		FiredAt:    firedAt,
	})
	if err != nil {
		n.logger.ErrorContext(ctx, "alert claim release failed", "server", serverName, "alert", alertKey, "error", err)
	}
}

func intervalFromDuration(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}
