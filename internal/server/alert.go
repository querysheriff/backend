package server

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/gen/db"
)

const slackWebhookHost = "hooks.slack.com"

type AlertServer struct {
	queries *db.Queries
}

func NewAlertServer(queries *db.Queries) *AlertServer {
	return &AlertServer{queries: queries}
}

type serverAlert struct {
	server, key string
}

// ListAlertSettings returns every visible server's Slack webhook and alert switches.
// Example: prod -> {webhook, [{monitoring_stopped, enabled, fires: 2}, ...]}.
func (s *AlertServer) ListAlertSettings(
	ctx context.Context,
	_ *connect.Request[querysheriffv1.ListAlertSettingsRequest],
) (*connect.Response[querysheriffv1.ListAlertSettingsResponse], error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	allowed := principal.AllowedServerFilter()

	servers, err := s.queries.ListMonitoredServers(ctx, allowed)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	webhooks, err := s.queries.ListAlertWebhooks(ctx, allowed)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	toggles, err := s.queries.ListAlertToggles(ctx, allowed)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	fires, err := s.queries.CountRecentAlertFires(ctx, db.CountRecentAlertFiresParams{
		HistoryWindow:  pgtype.Interval{Microseconds: alerts.FireHistoryWindow.Microseconds(), Valid: true},
		AllowedServers: allowed,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	webhookByServer := make(map[string]string, len(webhooks))
	for _, webhook := range webhooks {
		webhookByServer[webhook.ServerName] = webhook.SlackWebhookUrl
	}

	enabled := make(map[serverAlert]bool, len(toggles))
	for _, toggle := range toggles {
		enabled[serverAlert{toggle.ServerName, toggle.AlertKey}] = toggle.Enabled
	}

	firesLastWeek := make(map[serverAlert]int64, len(fires))
	for _, fire := range fires {
		firesLastWeek[serverAlert{fire.ServerName, fire.AlertKey}] = fire.Fires
	}

	result := make([]*querysheriffv1.ServerAlertSettings, len(servers))
	for i, server := range servers {
		result[i] = &querysheriffv1.ServerAlertSettings{
			ServerName:      server.ServerName,
			SlackWebhookUrl: webhookByServer[server.ServerName],
			Alerts:          alertSettingsProto(server.ServerName, enabled, firesLastWeek),
		}
	}

	return connect.NewResponse(&querysheriffv1.ListAlertSettingsResponse{Servers: result}), nil
}

// UpdateAlertWebhook sets the Slack webhook a server's alerts go to; an empty URL stops them.
// Example: prod, "https://hooks.slack.com/services/..." -> prod alerts post there.
func (s *AlertServer) UpdateAlertWebhook(
	ctx context.Context,
	req *connect.Request[querysheriffv1.UpdateAlertWebhookRequest],
) (*connect.Response[querysheriffv1.UpdateAlertWebhookResponse], error) {
	msg := req.Msg

	if err := authorizeServer(ctx, msg.GetServerName()); err != nil {
		return nil, err
	}

	webhookURL, err := validateWebhookURL(msg.GetSlackWebhookUrl())
	if err != nil {
		return nil, err
	}

	if err = s.queries.UpsertAlertWebhook(ctx, db.UpsertAlertWebhookParams{
		ServerName:      msg.GetServerName(),
		SlackWebhookUrl: webhookURL,
	}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.UpdateAlertWebhookResponse{}), nil
}

// UpdateAlertSetting switches one alert on or off for a server.
// Example: prod, weekly_report, false -> no more weekly reports for prod.
func (s *AlertServer) UpdateAlertSetting(
	ctx context.Context,
	req *connect.Request[querysheriffv1.UpdateAlertSettingRequest],
) (*connect.Response[querysheriffv1.UpdateAlertSettingResponse], error) {
	msg := req.Msg

	if err := authorizeServer(ctx, msg.GetServerName()); err != nil {
		return nil, err
	}

	if !alerts.IsKnownKey(msg.GetKey()) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown alert key %q", msg.GetKey()))
	}

	if err := s.queries.UpsertAlertToggle(ctx, db.UpsertAlertToggleParams{
		ServerName: msg.GetServerName(),
		AlertKey:   msg.GetKey(),
		Enabled:    msg.GetEnabled(),
	}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.UpdateAlertSettingResponse{}), nil
}

func validateWebhookURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid slack_webhook_url: %w", err))
	}

	if parsed.Scheme != "https" || parsed.Host != slackWebhookHost {
		return "", connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("slack_webhook_url must be an https://%s URL", slackWebhookHost),
		)
	}

	return trimmed, nil
}

func alertSettingsProto(
	serverName string,
	overrides map[serverAlert]bool,
	fires map[serverAlert]int64,
) []*querysheriffv1.AlertSetting {
	catalog := alerts.Catalog()
	settings := make([]*querysheriffv1.AlertSetting, len(catalog))
	for i, def := range catalog {
		key := serverAlert{serverName, def.Key}

		enabled := true
		if override, ok := overrides[key]; ok {
			enabled = override
		}

		settings[i] = &querysheriffv1.AlertSetting{
			Key:           def.Key,
			Title:         def.Title,
			Level:         def.Level,
			Enabled:       enabled,
			FiresLastWeek: fires[key],
		}
	}

	return settings
}
