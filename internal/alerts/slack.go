package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

const (
	slackTimeout      = 10 * time.Second
	slackMaxAttempts  = 3
	slackRetryBackoff = 500 * time.Millisecond
)

const (
	colorCritical = "#8B1E1E"
	colorWarning  = "#C7771A"
	colorInfo     = "#6C8392"
)

const (
	severityPrefix = "🔥 "
	reportPrefix   = "📊 "
)

type slackAttachment struct {
	Color    string   `json:"color"`
	Title    string   `json:"title"`
	Text     string   `json:"text"`
	Footer   string   `json:"footer"`
	MrkdwnIn []string `json:"mrkdwn_in"`
}

type slackPayload struct {
	Attachments []slackAttachment `json:"attachments"`
}

func slackColor(level querysheriffv1.AlertLevel) string {
	if level == critical {
		return colorCritical
	}

	if level == warning {
		return colorWarning
	}

	return colorInfo
}

func slackTitle(def Def) string {
	if def.Level == info {
		return reportPrefix + def.Title
	}

	return severityPrefix + def.Title
}

func postToSlack(ctx context.Context, client *http.Client, webhookURL string, def Def, footer, text string) error {
	body, err := json.Marshal(slackPayload{Attachments: []slackAttachment{{
		Color:    slackColor(def.Level),
		Title:    slackTitle(def),
		Text:     text,
		Footer:   footer,
		MrkdwnIn: []string{"text"},
	}}})
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := range slackMaxAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(slackRetryBackoff):
			}
		}

		if lastErr = sendSlackRequest(ctx, client, webhookURL, body); lastErr == nil {
			return nil
		}
	}

	return lastErr
}

func sendSlackRequest(ctx context.Context, client *http.Client, webhookURL string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return withoutURL(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return withoutURL(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// withoutURL drops the request URL from err: the webhook URL is a secret and errors get logged.
func withoutURL(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("%s slack webhook: %w", urlErr.Op, urlErr.Err)
	}

	return err
}

// slackEscape escapes the characters Slack reads as markup, so query text like "a < b" shows as written.
func slackEscape(text string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(text)
}

// tagPills renders tags as inline-code pills, each prefixed by a space.
func tagPills(tags map[string]string) string {
	var pills strings.Builder
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		fmt.Fprintf(&pills, " `%s=%s`", key, tags[key])
	}

	return slackEscape(pills.String())
}
